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
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/agentcommunication_client/gapic"
	cm "cloud.google.com/go/compute/metadata"
	"google.golang.org/api/option"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	acpb "github.com/GoogleCloudPlatform/agentcommunication_client/gapic/agentcommunicationpb"
)

func init() {
	logger = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lshortfile)
}

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

	logger *log.Logger
)

// Connection is an AgentCommunication connection.
type Connection struct {
	client      *agentcommunication.Client
	stream      acpb.AgentCommunication_StreamAgentMessagesClient
	closed      chan struct{}
	streamReady chan struct{}
	sends       chan *acpb.StreamAgentMessagesRequest
	closeErr    error
	resourceID  string
	channelID   string

	messages     chan *acpb.MessageBody
	responseSubs map[string]chan *status.Status
	responseMx   sync.Mutex

	regional bool
}

func loggerPrintf(format string, v ...any) {
	if DebugLogging {
		logger.Output(2, fmt.Sprintf(format, v...))
	}
}

// Close the connection.
func (c *Connection) Close() {
	c.close(ErrConnectionClosed)
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
		c.closeErr = err
		c.client.Close()
	}
}

func (c *Connection) waitForResponse(key string, channel chan *status.Status) error {
	timer := time.NewTimer(3 * time.Second)
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
		return fmt.Errorf("connection closed with err: %w", c.closeErr)
	}
	c.responseMx.Lock()
	delete(c.responseSubs, key)
	c.responseMx.Unlock()
	return nil
}

func (c *Connection) sendWithResp(req *acpb.StreamAgentMessagesRequest, channel chan *status.Status) error {
	loggerPrintf("Sending message %+v", req)

	select {
	case <-c.closed:
		return fmt.Errorf("connection closed with err: %w", c.closeErr)
	case c.sends <- req:
	}

	return c.waitForResponse(req.GetMessageId(), channel)
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
		return fmt.Errorf("connection closed with err: %w", c.closeErr)
	case c.streamReady <- struct{}{}: // Only sends if the stream is ready to send.
	}

	return c.sendWithResp(req, channel)
}

// Receive messages.
func (c *Connection) Receive() (*acpb.MessageBody, error) {
	select {
	case msg := <-c.messages:
		return msg, nil
	case <-c.closed:
		return nil, fmt.Errorf("connection closed with err: %w", c.closeErr)
	}
}

func (c *Connection) send(streamClosed chan struct{}) {
	for {
		select {
		case req := <-c.sends:
			if err := c.stream.Send(req); err != nil {
				c.close(err)
			}
		case <-c.closed:
			c.stream.CloseSend()
			return
		case <-streamClosed:
			return
		}
	}
}

// recv keeps receiving and acknowledging new messages.
func (c *Connection) recv(ctx context.Context, streamClosed chan struct{}) {
	loggerPrintf("Receiving messages")
	var unavailableRetries int
	for {
		resp, err := c.stream.Recv()
		if err != nil {
			close(streamClosed)
			select {
			case <-c.closed:
				return
			default:
			}
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.ResourceExhausted {
				loggerPrintf("Resource exhausted, sleeping before reconnect: %v", err)
			} else if ok && st.Code() == codes.Unavailable {
				// Retry max 5 times (2s total).
				if unavailableRetries > 5 {
					loggerPrintf("Stream returned Unavailable, exceeded max number of reconnects, closing connection: %v", err)
					c.close(err)
					return
				}
				loggerPrintf("Stream returned Unavailable, will reconnect: %v", err)
				// Sleep for 200ms * num of unavailableRetries, first retry is immediate.
				time.Sleep(time.Duration(unavailableRetries*200) * time.Millisecond)
				unavailableRetries++
			} else if err != io.EOF && !errors.Is(err, io.EOF) && (ok && st.Code() != codes.Canceled) {
				loggerPrintf("Unexpected error, closing connection: %v", err)
				c.close(err)
				return
			}
			loggerPrintf("Creating new stream")
			if err := c.createStream(ctx); err != nil {
				loggerPrintf("Error creating new stream: %v", err)
				c.close(err)
			}
			return
		}
		// Reset unavailable retries.
		unavailableRetries = 0
		switch resp.GetType().(type) {
		case *acpb.StreamAgentMessagesResponse_MessageBody:
			c.messages <- resp.GetMessageBody()
			if err := c.acknowledgeMessage(resp.GetMessageId()); err != nil {
				c.close(err)
				return
			}
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

func (c *Connection) acknowledgeMessage(messageID string) error {
	ackReq := &acpb.StreamAgentMessagesRequest{
		MessageId: messageID,
		Type:      &acpb.StreamAgentMessagesRequest_MessageResponse{},
	}
	select {
	case <-c.closed:
		return fmt.Errorf("connection closed with err: %w", c.closeErr)
	default:
		return c.stream.Send(ackReq)
	}
}

func (c *Connection) createStream(ctx context.Context) error {
	loggerPrintf("Creating stream.")
	token, err := cm.Get("instance/service-accounts/default/identity?audience=agentcommunication.googleapis.com&format=full")
	if err != nil {
		return fmt.Errorf("error getting instance token: %v", err)
	}

	newCtx := metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		"authentication":                  "Bearer " + token,
		"agent-communication-resource-id": c.resourceID,
		"agent-communication-channel-id":  c.channelID,
	}))

	loggerPrintf("Using ResourceID %q", c.resourceID)
	loggerPrintf("Using ChannelID %q", c.channelID)

	c.stream, err = c.client.StreamAgentMessages(newCtx)
	if err != nil {
		return fmt.Errorf("error creating stream: %v", err)
	}

	streamClosed := make(chan struct{})
	go c.recv(newCtx, streamClosed)
	go c.send(streamClosed)

	req := &acpb.StreamAgentMessagesRequest{
		MessageId: uuid.New().String(),
		Type: &acpb.StreamAgentMessagesRequest_RegisterConnection{
			RegisterConnection: &acpb.RegisterConnection{ResourceId: c.resourceID, ChannelId: c.channelID}}}

	channel := make(chan *status.Status)
	c.responseMx.Lock()
	c.responseSubs[req.GetMessageId()] = channel
	c.responseMx.Unlock()
	if err := c.sendWithResp(req, channel); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-c.streamReady:
			case <-streamClosed:
				return
			}
		}
	}()
	loggerPrintf("Stream established.")
	return nil
}

// CreateConnection creates a new connection.
func CreateConnection(ctx context.Context, channelID string, regional bool, opts ...option.ClientOption) (*Connection, error) {
	conn := &Connection{
		regional:     regional,
		channelID:    channelID,
		closed:       make(chan struct{}),
		messages:     make(chan *acpb.MessageBody, 5),
		responseSubs: make(map[string]chan *status.Status),
		streamReady:  make(chan struct{}),
		sends:        make(chan *acpb.StreamAgentMessagesRequest),
	}

	zone, err := cm.Zone()
	if err != nil {
		return nil, err
	}
	projectNum, err := cm.NumericProjectID()
	if err != nil {
		return nil, err
	}
	instanceID, err := cm.InstanceID()
	if err != nil {
		return nil, err
	}
	conn.resourceID = fmt.Sprintf("projects/%s/zones/%s/instances/%s", projectNum, zone, instanceID)

	location := zone
	if conn.regional {
		location = location[:len(location)-2]
	}

	defaultOpts := []option.ClientOption{
		option.WithoutAuthentication(), // Do not use oauth.
		option.WithGRPCDialOption(grpc.WithTransportCredentials(credentials.NewTLS(nil))), // Because we disabled Auth we need to specifically enable TLS.
		option.WithEndpoint(fmt.Sprintf("%s-agentcommunication.googleapis.com:443", location)),
	}

	opts = append(defaultOpts, opts...)

	conn.client, err = agentcommunication.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	if err := conn.createStream(ctx); err != nil {
		return nil, err
	}

	return conn, nil
}
