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

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/testing/protocmp"

	apb "google.golang.org/protobuf/types/known/anypb"
	acpb "github.com/GoogleCloudPlatform/agentcommunication_client/gapic/agentcommunicationpb"
)

const (
	testChannelID  = "test-channel"
	testResourceID = "projects/test-project/zones/test-region-zone/instances/test-instance"
	bufSize        = 1024 * 1024

	metadataMessageRateLimitValue = 100
	metadataBandwidthLimitValue   = 1000000000

	// This is a minimal token that our code can parse, it should be a valid JWT token but was
	// generated just for this test.
	rawToken = "eyJhbGciOiIiLCJ0eXAiOiIifQ.eyJpc3MiOiIiLCJhdWQiOiIiLCJleHAiOjEyNTc4OTc1OTAsImlhdCI6MTI1Nzg5Mzk5MH0.P1kofb3I0Eaxd6xAWI0mLrfR2k48sIU9K_iWpwXQIX66Cd95dXtkGJ8JQ74KIWHK_HSYB7i7kSbukDl6VjDc1HrZlRtM8pVNbIv0lHyDe8FZgvW2w33964hk96I0M2NcSLyj6jO42yvWEs0VFJwoAuWtX9jXUqb7vlQf-ElmUXbx5jsKvMqjS6KtT44wQzUg9MjsOTfU9AEKhn-p0liNb-QJxG2Z0NzGI6dCfKchd-mXgpnn0r_2OAZ0aCICNu50ye74hfPCkEpTK5w4PWDoLNhWhJabBSoM4umct49G3nZ5jO1Auh50QaprskS_c82ZzgttNvNzv3NShHAAODCI8w"
)

func TestMain(m *testing.M) {
	DebugLogging = true

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/computeMetadata/v1/instance/zone":
			fmt.Fprint(w, "test-region-zone")
		case "/computeMetadata/v1/project/numeric-project-id":
			fmt.Fprint(w, "test-project")
		case "/computeMetadata/v1/instance/id":
			fmt.Fprint(w, "test-instance")
		case "/computeMetadata/v1/instance/service-accounts/default/identity":
			fmt.Fprint(w, rawToken)
		}
	}))

	if err := os.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(ts.URL, "http://")); err != nil {
		log.Fatalf("Error running os.Setenv: %v", err)
	}
	// Best effort to get more logging during tests.
	os.Setenv("GOOGLE_SDK_GO_LOGGING_LEVEL", "debug")
	defer func() {
		if err := os.Unsetenv("GCE_METADATA_HOST"); err != nil {
			log.Fatalf("Error running os.Unsetenv: %v", err)
		}
		os.Unsetenv("GOOGLE_SDK_GO_LOGGING_LEVEL")
	}()

	out := m.Run()
	ts.Close()
	os.Exit(out)
}

type testSrv struct {
	sync.Mutex

	req        []*acpb.StreamAgentMessagesRequest
	reqMx      sync.Mutex
	headers    metadata.MD
	send       chan *acpb.StreamAgentMessagesResponse
	recvErr    chan error
	grpcServer *grpc.Server
}

func newTestSrv(grpcServer *grpc.Server) *testSrv {
	return &testSrv{
		recvErr:    make(chan error, 1),
		grpcServer: grpcServer,
	}
}

func (s *testSrv) StreamAgentMessages(stream acpb.AgentCommunication_StreamAgentMessagesServer) error {
	s.Lock()
	defer s.Unlock()
	s.send = make(chan *acpb.StreamAgentMessagesResponse)
	closed := make(chan struct{})
	defer close(closed)

	md, _ := metadata.FromIncomingContext(stream.Context())
	s.headers = md

	respHeaders := map[string]string{
		metadataMessageRateLimit: strconv.Itoa(metadataMessageRateLimitValue),
		metadataBandwidthLimit:   strconv.Itoa(metadataBandwidthLimitValue),
	}
	if err := stream.SetHeader(metadata.New(respHeaders)); err != nil {
		return err
	}

	go func() {
		for {
			rec, err := stream.Recv()
			select {
			case <-closed:
				return
			default:
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					s.recvErr <- nil
					return
				}
				s.recvErr <- err
				return
			}
			s.reqMx.Lock()
			s.req = append(s.req, rec)
			s.reqMx.Unlock()

			switch rec.GetType().(type) {
			case *acpb.StreamAgentMessagesRequest_MessageResponse:
				continue
			}
			if err := stream.Send(&acpb.StreamAgentMessagesResponse{MessageId: rec.GetMessageId(), Type: &acpb.StreamAgentMessagesResponse_MessageResponse{}}); err != nil {
				log.Printf("Server Send: %v\n", err)
				s.recvErr <- err
				return
			}
		}
	}()

	for {
		select {
		case msg := <-s.send:
			if err := stream.Send(msg); err != nil {
				return err
			}
		case err := <-s.recvErr:
			return err
		}
	}
}

func (s *testSrv) SendAgentMessage(ctx context.Context, req *acpb.SendAgentMessageRequest) (*acpb.SendAgentMessageResponse, error) {
	return &acpb.SendAgentMessageResponse{MessageBody: req.GetMessageBody()}, nil
}

func createTestSrv(t *testing.T) (*testSrv, *grpc.ClientConn, error) {
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	t.Cleanup(func() {
		s.GracefulStop()
	})
	srv := newTestSrv(s)
	acpb.RegisterAgentCommunicationServer(s, srv)

	started := make(chan struct{})
	go func() {
		close(started)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()
	// Give goroutine enough time to start
	<-started

	var bufDialer = func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	cc, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return srv, nil, err
	}

	return srv, cc, nil
}

func TestGetEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		regional bool
		want     string
	}{
		{
			name:     "regional endpoint",
			regional: true,
			want:     "test-region-agentcommunication.googleapis.com.:443",
		},
		{
			name:     "zonal endpoint",
			regional: false,
			want:     "test-region-zone-agentcommunication.googleapis.com.:443",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			endpoint, err := getEndpoint(tc.regional)
			if err != nil {
				t.Fatalf("getEndpoint(%v) returned an unexpected error: %v", tc.regional, err)
			}
			if endpoint != tc.want {
				t.Errorf("getEndpoint(%v) = %v, want: %v", tc.regional, endpoint, tc.want)
			}
		})
	}
}

func TestSendAgentMessage(t *testing.T) {
	ctx := context.Background()
	_, cc, err := createTestSrv(t)
	if err != nil {
		t.Fatalf("createTestCC() failed: %v", err)
	}

	client, err := NewClient(ctx, false, option.WithGRPCConn(cc))
	if err != nil {
		log.Fatal(err)
	}

	msg := &acpb.MessageBody{Labels: map[string]string{"key": "value"}, Body: &apb.Any{Value: []byte("test-body")}}
	got, err := SendAgentMessage(ctx, testChannelID, client, msg)
	if err != nil {
		t.Fatalf("SendAgentMessage returned an unexpected error: %v", err)
	}

	want := &acpb.SendAgentMessageResponse{MessageBody: msg}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("SendAgentMessage returned an unexpected diff (-want +got): %v", diff)
	}
}

func newTestConnection(ctx context.Context, t *testing.T) (*testSrv, *Connection, error) {
	srv, cc, err := createTestSrv(t)
	if err != nil {
		t.Fatalf("createTestSrv() failed: %v", err)
	}

	client, err := NewClient(ctx, false, option.WithGRPCConn(cc))
	if err != nil {
		return nil, nil, err
	}
	conn, err := NewConnection(ctx, testChannelID, client)
	if err != nil {
		return nil, nil, err
	}
	t.Cleanup(func() {
		conn.Close()
	})
	return srv, conn, nil
}

func TestNewConnection(t *testing.T) {
	ctx := context.Background()

	srv, conn, err := newTestConnection(ctx, t)
	if err != nil {
		t.Fatalf("createTestConnection() failed: %v", err)
	}

	if len(srv.req) != 1 {
		t.Fatalf("srv.req = %v, want 1", len(srv.req))
	}

	wantReq := &acpb.StreamAgentMessagesRequest{Type: &acpb.StreamAgentMessagesRequest_RegisterConnection{RegisterConnection: &acpb.RegisterConnection{ChannelId: testChannelID, ResourceId: testResourceID}}}
	if diff := cmp.Diff(srv.req[0], wantReq, protocmp.Transform(), cmpopts.IgnoreUnexported(), protocmp.IgnoreFields(&acpb.StreamAgentMessagesRequest{}, "message_id")); diff != "" {
		t.Errorf("srv.req[0] diff (-want +got):\n%s", diff)
	}

	wantHeaders := map[string][]string{
		"authentication":                  []string{fmt.Sprintf("Bearer %s", rawToken)},
		"agent-communication-channel-id":  []string{"test-channel"},
		"agent-communication-resource-id": []string{"projects/test-project/zones/test-region-zone/instances/test-instance"},
	}
	for k, v := range wantHeaders {
		if !reflect.DeepEqual(srv.headers.Get(k), v) {
			t.Errorf("srv.headers[%s] = %v, want %v", k, srv.headers[k], v)
		}
	}

	if conn.MessageBandwidthLimit() != metadataBandwidthLimitValue {
		t.Errorf("conn.MessageBandwidthLimit() = %v, want %v", conn.MessageBandwidthLimit(), metadataBandwidthLimitValue)
	}
	if conn.MessageRateLimit() != metadataMessageRateLimitValue {
		t.Errorf("conn.MessageRateLimit() = %v, want %v", conn.MessageRateLimit(), metadataMessageRateLimitValue)
	}
}

func TestNewConnectionErrors(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		expectError bool
	}{
		{
			name:        "ResourceExhausted",
			err:         status.Error(codes.ResourceExhausted, ""),
			expectError: false,
		},
		{
			name:        "Unavailable",
			err:         status.Error(codes.Unavailable, ""),
			expectError: false,
		},
		{
			name:        "Internal",
			err:         status.Error(codes.Internal, ""),
			expectError: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			srv, cc, err := createTestSrv(t)
			if err != nil {
				t.Fatalf("createTestSrv() failed: %v", err)
			}
			srv.recvErr <- tc.err

			client, err := NewClient(ctx, false, option.WithGRPCConn(cc))
			if err != nil {
				t.Fatalf("NewClient() failed: %v", err)
			}

			conn, err := NewConnection(ctx, testChannelID, client)
			if err != nil {
				if tc.expectError {
					return
				}
				t.Fatalf("NewConnection() failed: %v", err)
			}
			defer conn.Close()
			if tc.expectError {
				t.Fatalf("NewConnection() expected to fail")
			}

			if len(srv.req) != 1 {
				t.Fatalf("srv.req = %v, want 1", len(srv.req))
			}

			wantReq := &acpb.StreamAgentMessagesRequest{Type: &acpb.StreamAgentMessagesRequest_RegisterConnection{RegisterConnection: &acpb.RegisterConnection{ChannelId: testChannelID, ResourceId: testResourceID}}}
			if diff := cmp.Diff(srv.req[0], wantReq, protocmp.Transform(), cmpopts.IgnoreUnexported(), protocmp.IgnoreFields(&acpb.StreamAgentMessagesRequest{}, "message_id")); diff != "" {
				t.Errorf("srv.req[0] diff (-want +got):\n%s", diff)
			}

			wantHeaders := map[string][]string{
				"authentication":                  []string{fmt.Sprintf("Bearer %s", rawToken)},
				"agent-communication-channel-id":  []string{"test-channel"},
				"agent-communication-resource-id": []string{"projects/test-project/zones/test-region-zone/instances/test-instance"},
			}
			for k, v := range wantHeaders {
				if !reflect.DeepEqual(srv.headers.Get(k), v) {
					t.Errorf("srv.headers[%s] = %v, want %v", k, srv.headers[k], v)
				}
			}
		})
	}
}

func TestSendMessage(t *testing.T) {
	ctx := context.Background()
	srv, conn, err := newTestConnection(ctx, t)
	if err != nil {
		t.Fatalf("newTestConnection() failed: %v", err)
	}

	msg := &acpb.MessageBody{Labels: map[string]string{"key": "value"}, Body: &apb.Any{Value: []byte("test-body")}}
	if err := conn.SendMessage(msg); err != nil {
		t.Fatalf("SendMessage() failed: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	if len(srv.req) != 2 {
		t.Fatalf("srv.req = %v, want 2", len(srv.req))
	}

	wantReq := &acpb.StreamAgentMessagesRequest{Type: &acpb.StreamAgentMessagesRequest_MessageBody{MessageBody: msg}}
	if diff := cmp.Diff(srv.req[1], wantReq, protocmp.Transform(), cmpopts.IgnoreUnexported(), protocmp.IgnoreFields(&acpb.StreamAgentMessagesRequest{}, "message_id")); diff != "" {
		t.Errorf("srv.req[1] diff (-want +got):\n%s", diff)
	}
}

func TestSendMessage_ClosedConnection(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		expectError bool
	}{
		{
			name:        "EOF",
			err:         nil,
			expectError: false,
		},
		{
			name:        "Canceled",
			err:         status.Error(codes.Canceled, ""),
			expectError: false,
		},
		{
			name:        "DeadlineExceeded",
			err:         status.Error(codes.DeadlineExceeded, ""),
			expectError: false,
		},
		{
			name:        "ResourceExhausted",
			err:         status.Error(codes.ResourceExhausted, ""),
			expectError: false,
		},
		{
			name:        "Unavailable",
			err:         status.Error(codes.Unavailable, ""),
			expectError: false,
		},
		{
			name:        "Internal",
			err:         status.Error(codes.Internal, ""),
			expectError: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			srv, conn, err := newTestConnection(ctx, t)
			if err != nil {
				t.Fatalf("newTestConnection() failed: %v", err)
			}
			conn.timeToWaitForResp = 100 * time.Millisecond

			// Lock the recv loop.
			srv.send <- &acpb.StreamAgentMessagesResponse{Type: &acpb.StreamAgentMessagesResponse_MessageBody{}}
			time.Sleep(5 * time.Millisecond)
			// Close the connection server side, client should not autoreconnect because it is blocked on recv.
			srv.recvErr <- tc.err

			// Should wait for reconnect.
			msg := &acpb.MessageBody{Labels: map[string]string{"key": "value"}, Body: &apb.Any{Value: []byte("test-body")}}
			var sendErr = make(chan error)
			go func() {
				sendErr <- conn.SendMessage(msg)
			}()
			// Give goroutine enough time to start
			time.Sleep(5 * time.Millisecond)

			// Should only have 2 messages, register and the ack.
			srv.reqMx.Lock()
			if len(srv.req) != 2 {
				t.Fatalf("srv.req = %v, want 2", len(srv.req))
			}
			srv.req = nil
			srv.reqMx.Unlock()

			// Unblock recv
			if _, err := conn.Receive(); err != nil {
				t.Fatalf("Receive() failed: %v", err)
			}

			// Wait for resend.
			timer := time.NewTimer(500 * time.Millisecond)
			select {
			case err := <-sendErr:
				if err != nil && !tc.expectError {
					t.Fatalf("SendMessage() failed: %v", err)
				}
				if tc.expectError {
					if err == nil {
						t.Fatalf("SendMessage() expected to fail")
					}
					return
				}
			case <-timer.C:
				t.Errorf("SendMessage() timed out")
			}

			// Should have 2 more messages now, register and the send.
			srv.reqMx.Lock()
			defer srv.reqMx.Unlock()
			if len(srv.req) != 2 {
				t.Fatalf("srv.req = %v, want 2", len(srv.req))
			}
			wantReq := &acpb.StreamAgentMessagesRequest{Type: &acpb.StreamAgentMessagesRequest_MessageBody{MessageBody: msg}}
			if diff := cmp.Diff(srv.req[1], wantReq, protocmp.Transform(), cmpopts.IgnoreUnexported(), protocmp.IgnoreFields(&acpb.StreamAgentMessagesRequest{}, "message_id")); diff != "" {
				t.Errorf("srv.req[1] diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestClose(t *testing.T) {
	ctx := context.Background()
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	defer s.GracefulStop()
	srv := newTestSrv(s)
	acpb.RegisterAgentCommunicationServer(s, srv)

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	var bufDialer = func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	cc, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	client, err := NewClient(ctx, false, option.WithGRPCConn(cc))
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}

	conn, err := NewConnection(ctx, testChannelID, client)
	if err != nil {
		t.Fatalf("NewConnection() failed: %v", err)
	}

	// Close the connection client side.
	conn.Close()
	// We expect these both to fail with ErrConnectionClosed.
	_, err = conn.Receive()
	if !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("Receive() unexpected err: %v", err)
	}
	err = conn.SendMessage(&acpb.MessageBody{Labels: map[string]string{"key": "value"}, Body: &apb.Any{Value: []byte("test-body")}})
	if !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("SendMessage() unexpected err: %v", err)
	}

	// Create a new connection using the same client, this should work.
	conn, err = NewConnection(ctx, testChannelID, client)
	if err != nil {
		t.Fatalf("NewConnection() failed: %v", err)
	}
	conn.Close()

	// Create a new connection using a closed client, this should fail.
	client.Close()
	_, err = NewConnection(ctx, testChannelID, client)
	if err == nil {
		t.Fatal("NewConnection() expected to fail")
	}

	// Create a new connection with a new client, this should work
	t.Logf("Creating new client and a new connection")
	cc, err = grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	client, err = NewClient(ctx, false, option.WithGRPCConn(cc))
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	conn, err = NewConnection(ctx, testChannelID, client)
	if err != nil {
		t.Fatalf("NewConnection() failed: %v", err)
	}
	defer conn.Close()

	// Closing server stream, connection should auto reconnect.
	srv.recvErr <- nil
	// Wait for closed.
	time.Sleep(5 * time.Millisecond)
	err = conn.SendMessage(&acpb.MessageBody{Labels: map[string]string{"key": "value"}, Body: &apb.Any{Value: []byte("test-body")}})
	if err != nil {
		t.Fatalf("SendMessage() failed: %v", err)
	}
}

func TestReceive(t *testing.T) {
	ctx := context.Background()
	srv, conn, err := newTestConnection(ctx, t)
	if err != nil {
		t.Fatalf("createTestConnection() failed: %v", err)
	}
	body := &acpb.MessageBody{Labels: map[string]string{"key": "value"}, Body: &apb.Any{Value: []byte("test-body")}}
	srv.send <- &acpb.StreamAgentMessagesResponse{MessageId: "test-message-id", Type: &acpb.StreamAgentMessagesResponse_MessageBody{MessageBody: body}}

	msg, err := conn.Receive()
	if err != nil {
		t.Fatalf("Receive() failed: %v", err)
	}
	if diff := cmp.Diff(msg, body, protocmp.Transform(), cmpopts.IgnoreUnexported()); diff != "" {
		t.Errorf("Receive() diff (-want +got):\n%s", diff)
	}
}

func TestGetIdentityToken(t *testing.T) {
	// Setup Token
	future := time.Now().Add(time.Hour)
	idToken.expTime = &future
	idToken.raw = "first-token"

	// Validate raw token is returned if exp is in the future.
	token, err := getIdentityToken()
	if err != nil {
		t.Fatalf("getIdentityToken() failed: %v", err)
	}
	if token != "first-token" {
		t.Errorf("idToken.raw = %v, want first-token", idToken.raw)
	}

	// Validate token is refreshed if exp is in the past.
	past := time.Now().Add(-time.Hour)
	idToken.expTime = &past
	token, err = getIdentityToken()
	if err != nil {
		t.Fatalf("getIdentityToken() failed: %v", err)
	}
	if token != rawToken {
		t.Errorf("idToken.raw = %v, want %v", idToken.raw, rawToken)
	}
}
