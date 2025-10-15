// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package client is an AgentCommunication client library.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/agentcommunication_client/gapic"
	"google.golang.org/api/option"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	acpb "github.com/GoogleCloudPlatform/agentcommunication_client/gapic/agentcommunicationpb"
)

func init() {
	logger = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile)
}

const (
	metadataMessageRateLimit = "agent-communication-message-rate-limit"
	metadataBandwidthLimit   = "agent-communication-bandwidth-limit"
)

var (
	// DebugLogging enables debug logging.
	DebugLogging = false
	// ErrConnectionClosed is an error indicating that the connection was closed by the caller.
	ErrConnectionClosed = errors.New("connection closed")
	// ErrMessageTimeout is an error indicating message send timed out.
	ErrMessageTimeout = errors.New("timed out waiting for response")
	// ErrResourceExhausted is an error indicating that the server responded to the send with
	// ResourceExhausted.
	ErrResourceExhausted = errors.New("resource exhausted")
	// ErrGettingInstanceToken is an error indicating that the instance token could not be retrieved.
	ErrGettingInstanceToken = errors.New("error getting instance token")

	// defaultOpts are the default options for used for creating ACS clients.
	defaultOpts = []option.ClientOption{
		option.WithoutAuthentication(), // Do not use oauth.
		option.WithGRPCDialOption(grpc.WithTransportCredentials(credentials.NewTLS(nil))), // Because we disabled Auth we need to specifically enable TLS.
		option.WithGRPCDialOption(grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 60 * time.Second, Timeout: 10 * time.Second})),
	}

	logger *log.Logger
)

// ErrUnsupportedUniverse is an error indicating that ACS is not supported in
// the specified TPC universe.
type ErrUnsupportedUniverse struct {
	universe string
}

// Error returns the error message for ErrUnsupportedUniverse.
func (e *ErrUnsupportedUniverse) Error() string {
	return fmt.Sprintf("Universe domain is not supported: %q", e.universe)
}

// Is returns true if the error is an ErrUnsupportedUniverse.
func (e *ErrUnsupportedUniverse) Is(target error) bool {
	_, ok := target.(*ErrUnsupportedUniverse)
	return ok
}

func loggerPrintf(format string, v ...any) {
	if DebugLogging {
		logger.Output(2, fmt.Sprintf(format, v...))
	}
}

func getEndpoint(regional bool) (string, error) {
	location := getZone()
	if regional {
		index := strings.LastIndex(location, "-")
		if index == -1 {
			return "", fmt.Errorf("zone %q is not a valid zone", getZone())
		}
		location = location[:index]
	}

	return fmt.Sprintf("%s-agentcommunication.%s.:443", location, getUniverseDomain()), nil
}

// NewClient creates a new agent communication grpc client.
// Caller must close the returned client when it is done being used to clean up its underlying
// connections.
func NewClient(ctx context.Context, regional bool, opts ...option.ClientOption) (*agentcommunication.Client, error) {
	if err := metadataInit(); err != nil {
		return nil, err
	}

	endpoint, err := getEndpoint(regional)
	if err != nil {
		return nil, err
	}
	optsWithEndpoint := append(defaultOpts, option.WithEndpoint(endpoint))
	return agentcommunication.NewClient(ctx, append(optsWithEndpoint, opts...)...)
}

// SendAgentMessage sends a message to the client. This is equivalent to sending a message via
// StreamAgentMessages with a single message and waiting for the response.
func SendAgentMessage(ctx context.Context, channelID string, client *agentcommunication.Client, msg *acpb.MessageBody) (*acpb.SendAgentMessageResponse, error) {
	if err := metadataInit(); err != nil {
		return nil, err
	}

	loggerPrintf("SendAgentMessage")
	token, err := getIdentityToken()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrGettingInstanceToken, err)
	}

	ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		"authentication":                  "Bearer " + token,
		"agent-communication-resource-id": getResourceID(),
		"agent-communication-channel-id":  channelID,
	}))

	loggerPrintf("Using ResourceID %q", getResourceID())
	loggerPrintf("Using ChannelID %q", channelID)

	return client.SendAgentMessage(ctx, &acpb.SendAgentMessageRequest{
		ChannelId:   channelID,
		ResourceId:  getResourceID(),
		MessageBody: msg,
	})
}

// Connection is an AgentCommunication connection.
type Connection struct {
	client *agentcommunication.Client
	// Indicates that the client is caller managed and should not be closed.
	callerManagedClient bool
	// Indicates that the entire connection is closed and will not reopen.
	closed     chan struct{}
	closeErr   error
	closeErrMx sync.RWMutex
	// Indicates that the underlying stream is ready to send.
	streamReady chan struct{}
	sends       chan *acpb.StreamAgentMessagesRequest
	resourceID  string
	channelID   string

	messages     chan *acpb.MessageBody
	responseSubs map[string]chan *status.Status
	responseMx   sync.Mutex

	regional          bool
	timeToWaitForResp time.Duration

	limitsMx              sync.Mutex
	messageRateLimit      int
	messageBandwidthLimit int
}

// MessageRateLimit returns the message limit (in messages/minute) for the connection.
func (c *Connection) MessageRateLimit() int {
	c.limitsMx.Lock()
	defer c.limitsMx.Unlock()
	return c.messageRateLimit
}

// MessageBandwidthLimit returns the message bandwidth limit (in bytes/minute) for the connection.
func (c *Connection) MessageBandwidthLimit() int {
	c.limitsMx.Lock()
	defer c.limitsMx.Unlock()
	return c.messageBandwidthLimit
}

// Close the connection.
func (c *Connection) Close() {
	c.close(ErrConnectionClosed)
}

func (c *Connection) setCloseErr(err error) {
	c.closeErrMx.Lock()
	defer c.closeErrMx.Unlock()
	c.closeErr = err
}

func (c *Connection) getCloseErr() error {
	c.closeErrMx.RLock()
	defer c.closeErrMx.RUnlock()
	return c.closeErr
}

func (c *Connection) close(err error) {
	loggerPrintf("closing connection with err: %v", err)
	st, _ := status.FromError(err)
	loggerPrintf("closing connection with status: %+v", st)
	select {
	case <-c.closed:
		return
	default:
		close(c.closed)
		c.setCloseErr(err)
		if !c.callerManagedClient {
			c.client.Close()
		}
	}
}

func (c *Connection) waitForResponse(key string, channel chan *status.Status) error {
	defer func() {
		c.responseMx.Lock()
		delete(c.responseSubs, key)
		c.responseMx.Unlock()
	}()

	timer := time.NewTimer(c.timeToWaitForResp)
	defer timer.Stop()
	select {
	case st := <-channel:
		if st != nil {
			switch st.Code() {
			case codes.OK:
			case codes.ResourceExhausted:
				return fmt.Errorf("%w: %s", ErrResourceExhausted, st.Message())
			default:
				return fmt.Errorf("unexpected status: %+v", st)
			}
		}
	case <-timer.C:
		return fmt.Errorf("%w: timed out waiting for response, MessageID: %q", ErrMessageTimeout, key)
	case <-c.closed:
		return fmt.Errorf("connection closed with err: %w", c.getCloseErr())
	}
	return nil
}

func (c *Connection) sendWithResp(req *acpb.StreamAgentMessagesRequest, channel chan *status.Status) error {
	loggerPrintf("Sending message %+v", req)

	select {
	case <-c.closed:
		return fmt.Errorf("connection closed with err: %w", c.getCloseErr())
	case c.sends <- req:
	}

	return c.waitForResponse(req.GetMessageId(), channel)
}

func (c *Connection) sendMessage(msg *acpb.MessageBody) error {
	req := &acpb.StreamAgentMessagesRequest{
		MessageId: uuid.New().String(),
		Type:      &acpb.StreamAgentMessagesRequest_MessageBody{MessageBody: msg},
	}

	channel := make(chan *status.Status)
	c.responseMx.Lock()
	c.responseSubs[req.GetMessageId()] = channel
	c.responseMx.Unlock()

	select {
	case <-c.closed:
		return fmt.Errorf("connection closed with err: %w", c.getCloseErr())
	case c.streamReady <- struct{}{}: // Only sends if the stream is ready to send.
	}

	return c.sendWithResp(req, channel)
}

// SendMessage sends a message to the client. Will automatically retry on message timeout (temporary
// disconnects) and in the case of ResourceExhausted with a backoff. Because retries are limited
// the returned error can in some cases be one of ErrMessageTimeout or ErrResourceExhausted, in
// which case send should be retried by the caller.
func (c *Connection) SendMessage(msg *acpb.MessageBody) error {
	var err error
	// Retry 4 times.
	for i := 1; i <= 5; i++ {
		err := c.sendMessage(msg)
		if errors.Is(err, ErrResourceExhausted) {
			// Start with 250ms sleep, then simply multiply by iteration.
			time.Sleep(time.Duration(i*250) * time.Millisecond)
			continue
		} else if errors.Is(err, ErrMessageTimeout) {
			continue
		}
		return err
	}
	return err
}

// Receive messages, Receive should be called continuously for the life of the stream connection,
// any delay in Receive when there are queued messages will cause the server to disconnect the
// stream. This means handling the MessageBody from Receive should not be blocking, offload message
// handling to another goroutine and immediately call Receive again.
func (c *Connection) Receive() (*acpb.MessageBody, error) {
	select {
	case msg := <-c.messages:
		return msg, nil
	case <-c.closed:
		return nil, fmt.Errorf("connection closed with err: %w", c.getCloseErr())
	}
}

func (c *Connection) streamSend(req *acpb.StreamAgentMessagesRequest, streamClosed, streamSendLock chan struct{}, stream acpb.AgentCommunication_StreamAgentMessagesClient) error {
	select {
	case <-streamClosed:
		return errors.New("stream closed")
	case streamSendLock <- struct{}{}:
		defer func() { <-streamSendLock }()
	}
	if err := stream.Send(req); err != nil {
		if err != io.EOF && !errors.Is(err, io.EOF) {
			// Something is very broken, just close the stream here.
			loggerPrintf("Unexpected send error, closing connection: %v", err)
			c.close(err)
			return err
		}
		// EOF error means the stream is closed, this should be picked up by recv, but that could be
		// blocked, close our sends for now and just allow the caller handle it, SendMessage will wait
		// for response which will never come and auto retry. acknowledgeMessage will fail and prevent
		// the message from being passed on to message handlers, allowing recv to handle the stream
		// close error.
		loggerPrintf("Error sending message, stream closed.")
		select {
		case <-streamClosed:
		default:
			close(streamClosed)
		}
		return ErrConnectionClosed
	}
	return nil
}

func (c *Connection) send(streamClosed, streamSendLock chan struct{}, stream acpb.AgentCommunication_StreamAgentMessagesClient) {
	defer func() {
		// Lock the stream sends so we can close the stream.
		streamSendLock <- struct{}{}
		stream.CloseSend()
	}()
	for {
		select {
		case req := <-c.sends:
			if err := c.streamSend(req, streamClosed, streamSendLock, stream); err != nil {
				return
			}
		case <-c.closed:
			return
		case <-streamClosed:
			return
		}
	}
}

func (c *Connection) acknowledgeMessage(messageID string, streamClosed, streamSendLock chan struct{}, stream acpb.AgentCommunication_StreamAgentMessagesClient) error {
	ackReq := &acpb.StreamAgentMessagesRequest{
		MessageId: messageID,
		Type:      &acpb.StreamAgentMessagesRequest_MessageResponse{},
	}
	select {
	case <-c.closed:
		return fmt.Errorf("connection closed with err: %w", c.closeErr)
	default:
		return c.streamSend(ackReq, streamClosed, streamSendLock, stream)
	}
}

func (c *Connection) readHeaders(stream acpb.AgentCommunication_StreamAgentMessagesClient) {
	md, err := stream.Header()
	if err != nil {
		loggerPrintf("Error getting stream header: %v", err)
		return
	}

	c.limitsMx.Lock()
	defer c.limitsMx.Unlock()

	value := md.Get(metadataMessageRateLimit)
	if len(value) > 0 {
		rateLimit, err := strconv.Atoi(value[0])
		if err != nil {
			loggerPrintf("Error parsing message rate limit: %v", err)
		} else {
			c.messageRateLimit = rateLimit
			loggerPrintf("Message rate limit: %d", c.messageRateLimit)
		}
	} else {
		loggerPrintf("No message rate limit")
	}
	value = md.Get(metadataBandwidthLimit)
	if len(value) > 0 {
		bandwidthLimit, err := strconv.Atoi(value[0])
		if err != nil {
			loggerPrintf("Error parsing message bandwidth limit: %v", err)
		} else {
			c.messageBandwidthLimit = bandwidthLimit
			loggerPrintf("Message bandwidth limit: %d", c.messageBandwidthLimit)
		}
	} else {
		loggerPrintf("No message bandwidth limit")
	}
}

// recv keeps receiving and acknowledging new messages.
func (c *Connection) recv(ctx context.Context, streamClosed, streamSendLock chan struct{}, stream acpb.AgentCommunication_StreamAgentMessagesClient) {
	loggerPrintf("Receiving messages")
	for {
		resp, err := stream.Recv()
		if err != nil {
			select {
			case <-streamClosed:
			default:
				// Causes the send goroutine to exit, c.sends will block.
				close(streamClosed)
			}
			select {
			case <-c.closed:
				// Connection is closed, return now.
				loggerPrintf("Connection closed, recv returning")
				return
			default:
			}
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.ResourceExhausted {
				loggerPrintf("Connection closed due to resource exhausted: %v", err)
			} else if ok && st.Code() == codes.Unavailable {
				loggerPrintf("Stream returned Unavailable, will reconnect: %v", err)
			} else if err != io.EOF && !errors.Is(err, io.EOF) && (ok && st.Code() != codes.Canceled) && (ok && st.Code() != codes.DeadlineExceeded) {
				// EOF is a normal stream close, Canceled will be set by the server when stream timeout is
				// reached, DeadlineExceeded would be because of the client side deadline we set.
				loggerPrintf("Unexpected error, closing connection: %v", err)
				c.close(err)
				return
			}
			// A new stream is created if:
			// 1. Resource exhausted is returned but we have not exceeded the max number of retries.
			// 2. Unavailable is returned but we have not exceeded the max number of retries.
			// 3. A known "normal disconnect" error is returned.
			loggerPrintf("Creating new stream")
			if err := c.createStream(ctx); err != nil {
				loggerPrintf("Error creating new stream: %v", err)
				c.close(err)
			}
			// Always return here, createStream launches a new recv goroutine.
			return
		}
		switch resp.GetType().(type) {
		case *acpb.StreamAgentMessagesResponse_MessageBody:
			// Acknowledge message first, if this ack fails dont forward the message on to the handling
			// logic since that indicates a stream disconnect.
			if err := c.acknowledgeMessage(resp.GetMessageId(), streamClosed, streamSendLock, stream); err != nil {
				loggerPrintf("Error acknowledging message %q: %v", resp.GetMessageId(), err)
				continue
			}
			c.messages <- resp.GetMessageBody()
		case *acpb.StreamAgentMessagesResponse_MessageResponse:
			st := resp.GetMessageResponse().GetStatus()
			c.responseMx.Lock()
			for key, sub := range c.responseSubs {
				if key != resp.GetMessageId() {
					continue
				}
				select {
				case sub <- status.FromProto(st):
				default:
				}
			}
			c.responseMx.Unlock()
		}
	}
}

func createStreamLoop(ctx context.Context, client *agentcommunication.Client, resourceID string, channelID string) (acpb.AgentCommunication_StreamAgentMessagesClient, error) {
	resourceExhaustedRetries := 0
	unavailableRetries := 0
	for {
		stream, err := client.StreamAgentMessages(ctx)
		if err != nil {
			return nil, fmt.Errorf("error creating stream: %v", err)
		}

		// RegisterConnection is a special message that must be sent before any other messages.
		req := &acpb.StreamAgentMessagesRequest{
			MessageId: uuid.New().String(),
			Type: &acpb.StreamAgentMessagesRequest_RegisterConnection{
				RegisterConnection: &acpb.RegisterConnection{ResourceId: resourceID, ChannelId: channelID}}}

		if err := stream.Send(req); err != nil {
			return nil, fmt.Errorf("error sending register connection: %v", err)
		}

		// We expect the first message to be a MessageResponse.
		resp, err := stream.Recv()
		if err == nil {
			switch resp.GetType().(type) {
			case *acpb.StreamAgentMessagesResponse_MessageResponse:
				if resp.GetMessageResponse().GetStatus().GetCode() != int32(codes.OK) {
					return nil, fmt.Errorf("unexpected register response: %+v", resp.GetMessageResponse().GetStatus())
				}
			}
			// Stream is connected.
			return stream, nil
		}

		st, ok := status.FromError(err)
		if ok && st.Code() == codes.ResourceExhausted {
			loggerPrintf("Resource exhausted, sleeping before reconnect: %v", err)
			if resourceExhaustedRetries > 20 {
				loggerPrintf("Stream returned ResourceExhausted, exceeded max number of reconnects, closing connection: %v", err)
			}
			sleep := time.Duration(resourceExhaustedRetries+1) * time.Second
			if resourceExhaustedRetries > 9 {
				sleep = 10 * time.Second
			}
			time.Sleep(sleep)
			resourceExhaustedRetries++
			continue
		} else if ok && st.Code() == codes.Unavailable {
			// Retry max 5 times (2s total).
			if unavailableRetries <= 5 {
				loggerPrintf("Stream returned Unavailable, will reconnect: %v", err)
				// Sleep for 200ms * num of unavailableRetries, first retry is immediate.
				time.Sleep(time.Duration(unavailableRetries*200) * time.Millisecond)
				unavailableRetries++
				continue
			}
			loggerPrintf("Stream returned Unavailable, exceeded max number of reconnects, closing connection: %v", err)
		}
		return nil, err
	}
}

func (c *Connection) createStream(ctx context.Context) error {
	loggerPrintf("Creating stream.")
	token, err := getIdentityToken()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrGettingInstanceToken, err)
	}

	ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		"authentication":                  "Bearer " + token,
		"agent-communication-resource-id": c.resourceID,
		"agent-communication-channel-id":  c.channelID,
	}))

	loggerPrintf("Using ResourceID %q", c.resourceID)
	loggerPrintf("Using ChannelID %q", c.channelID)

	// Set a timeout for the stream, this is well above service side timeout.
	cnclCtx, cancel := context.WithTimeout(ctx, 60*time.Minute)
	stream, err := createStreamLoop(cnclCtx, c.client, c.resourceID, c.channelID)
	if err != nil {
		cancel()
		c.close(err)
		return err
	}

	// Reading headers is best effort, if we fail to read headers we will just log the error and
	// continue.
	c.readHeaders(stream)

	// Used to signal that the stream is closed.
	streamClosed := make(chan struct{})
	// This ensures that only one send is happening at a time.
	streamSendLock := make(chan struct{}, 1)
	go c.recv(ctx, streamClosed, streamSendLock, stream)
	go c.send(streamClosed, streamSendLock, stream)

	go func() {
		defer cancel()
		for {
			select {
			// Indicates that the stream is setup and is ready to send, this is used by sendMessage to
			// block sends during reconnect.
			case <-c.streamReady:
			case <-streamClosed:
				return
			}
		}
	}()
	loggerPrintf("Stream established.")
	return nil
}

// NewConnection creates a new streaming connection.
// Caller is responsible for calling Close() on the connection when done, certain errors will cause
// the connection to be closed automatically. The passed in client will not be closed and can be
// reused.
func NewConnection(ctx context.Context, channelID string, client *agentcommunication.Client) (*Connection, error) {
	if err := metadataInit(); err != nil {
		return nil, err
	}

	conn := &Connection{
		channelID:           channelID,
		closed:              make(chan struct{}),
		messages:            make(chan *acpb.MessageBody),
		responseSubs:        make(map[string]chan *status.Status),
		streamReady:         make(chan struct{}),
		sends:               make(chan *acpb.StreamAgentMessagesRequest),
		timeToWaitForResp:   2 * time.Second,
		client:              client,
		callerManagedClient: true,
	}

	conn.resourceID = getResourceID()
	if err := conn.createStream(ctx); err != nil {
		conn.close(err)
		return nil, err
	}

	return conn, nil
}

// CreateConnection creates a new connection.
// DEPRECATED: Use NewConnection instead.
func CreateConnection(ctx context.Context, channelID string, regional bool, opts ...option.ClientOption) (*Connection, error) {
	if err := metadataInit(); err != nil {
		return nil, err
	}

	conn := &Connection{
		channelID:         channelID,
		closed:            make(chan struct{}),
		messages:          make(chan *acpb.MessageBody),
		responseSubs:      make(map[string]chan *status.Status),
		streamReady:       make(chan struct{}),
		sends:             make(chan *acpb.StreamAgentMessagesRequest),
		timeToWaitForResp: 2 * time.Second,
	}

	var err error
	conn.client, err = NewClient(ctx, regional, opts...)
	if err != nil {
		return nil, err
	}
	conn.resourceID = getResourceID()

	if err := conn.createStream(ctx); err != nil {
		conn.close(err)
		return nil, err
	}

	return conn, nil
}
