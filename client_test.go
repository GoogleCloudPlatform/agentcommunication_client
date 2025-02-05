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
	testResourceID = "projects/test-project/zones/test-zone/instances/test-instance"
	bufSize        = 1024 * 1024
)

func TestMain(m *testing.M) {
	DebugLogging = true

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/computeMetadata/v1/instance/zone":
			fmt.Fprint(w, "test-zone")
		case "/computeMetadata/v1/project/numeric-project-id":
			fmt.Fprint(w, "test-project")
		case "/computeMetadata/v1/instance/id":
			fmt.Fprint(w, "test-instance")
		case "/computeMetadata/v1/instance/service-accounts/default/identity":
			fmt.Fprint(w, "test-token")
		}
	}))

	if err := os.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(ts.URL, "http://")); err != nil {
		log.Fatalf("Error running os.Setenv: %v", err)
		os.Exit(1)
	}
	defer os.Unsetenv("GCE_METADATA_HOST")

	out := m.Run()
	ts.Close()
	os.Exit(out)
}

type testSrv struct {
	sync.Mutex

	req     []*acpb.StreamAgentMessagesRequest
	reqMx   sync.Mutex
	headers metadata.MD
	send    chan *acpb.StreamAgentMessagesResponse
	recvErr chan error
}

func newTestSrv() *testSrv {
	return &testSrv{
		recvErr: make(chan error, 1),
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

func createTestConnection(ctx context.Context) (*testSrv, *Connection, error) {
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	srv := newTestSrv()
	acpb.RegisterAgentCommunicationServer(s, srv)

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	var bufDialer = func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	cc, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}

	conn, err := CreateConnection(context.Background(), testChannelID, false, option.WithGRPCConn(cc))
	return srv, conn, err
}

func TestCreateConnection(t *testing.T) {
	srv, _, err := createTestConnection(context.Background())
	if err != nil {
		t.Fatalf("CreateConnection() failed: %v", err)
	}

	if len(srv.req) != 1 {
		t.Fatalf("srv.req = %v, want 1", len(srv.req))
	}

	wantReq := &acpb.StreamAgentMessagesRequest{Type: &acpb.StreamAgentMessagesRequest_RegisterConnection{RegisterConnection: &acpb.RegisterConnection{ChannelId: testChannelID, ResourceId: testResourceID}}}
	if diff := cmp.Diff(srv.req[0], wantReq, protocmp.Transform(), cmpopts.IgnoreUnexported(), protocmp.IgnoreFields(&acpb.StreamAgentMessagesRequest{}, "message_id")); diff != "" {
		t.Errorf("srv.req[0] diff (-want +got):\n%s", diff)
	}

	wantHeaders := map[string][]string{
		"authentication":                  []string{"Bearer test-token"},
		"agent-communication-channel-id":  []string{"test-channel"},
		"agent-communication-resource-id": []string{"projects/test-project/zones/test-zone/instances/test-instance"},
	}
	for k, v := range wantHeaders {
		if !reflect.DeepEqual(srv.headers.Get(k), v) {
			t.Errorf("srv.headers[%s] = %v, want %v", k, srv.headers[k], v)
		}
	}
}

func TestCreateConnectionErrors(t *testing.T) {
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

			lis := bufconn.Listen(bufSize)
			s := grpc.NewServer()
			srv := newTestSrv()
			srv.recvErr <- tc.err
			acpb.RegisterAgentCommunicationServer(s, srv)

			go func() {
				if err := s.Serve(lis); err != nil {
					log.Fatalf("Server exited with error: %v", err)
				}
			}()

			var bufDialer = func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}

			cc, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}

			_, err = CreateConnection(context.Background(), testChannelID, false, option.WithGRPCConn(cc))
			if err != nil {
				if tc.expectError {
					return
				}
				t.Fatalf("CreateConnection() failed: %v", err)
			}
			if tc.expectError {
				t.Fatalf("CreateConnection() expected to fail")
			}

			if len(srv.req) != 1 {
				t.Fatalf("srv.req = %v, want 1", len(srv.req))
			}

			wantReq := &acpb.StreamAgentMessagesRequest{Type: &acpb.StreamAgentMessagesRequest_RegisterConnection{RegisterConnection: &acpb.RegisterConnection{ChannelId: testChannelID, ResourceId: testResourceID}}}
			if diff := cmp.Diff(srv.req[0], wantReq, protocmp.Transform(), cmpopts.IgnoreUnexported(), protocmp.IgnoreFields(&acpb.StreamAgentMessagesRequest{}, "message_id")); diff != "" {
				t.Errorf("srv.req[0] diff (-want +got):\n%s", diff)
			}

			wantHeaders := map[string][]string{
				"authentication":                  []string{"Bearer test-token"},
				"agent-communication-channel-id":  []string{"test-channel"},
				"agent-communication-resource-id": []string{"projects/test-project/zones/test-zone/instances/test-instance"},
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
	srv, conn, err := createTestConnection(context.Background())
	if err != nil {
		t.Fatalf("CreateConnection() failed: %v", err)
	}

	msg := &acpb.MessageBody{Labels: map[string]string{"key": "value"}, Body: &apb.Any{Value: []byte("test-body")}}
	if err := conn.SendMessage(msg); err != nil {
		t.Fatalf("SendMessage() failed: %v", err)
	}

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
			srv, conn, err := createTestConnection(context.Background())
			if err != nil {
				t.Fatalf("CreateConnection() failed: %v", err)
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
	defer s.Stop()
	srv := newTestSrv()
	acpb.RegisterAgentCommunicationServer(s, srv)

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	var bufDialer = func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	cc, err := grpc.Dial("bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}

	conn, err := CreateConnection(ctx, testChannelID, false, option.WithGRPCConn(cc))
	if err != nil {
		t.Fatalf("CreateConnection() failed: %v", err)
	}

	// Close the connection client side.
	conn.Close()
	// We expect this to fail with ErrConnectionClosed.
	err = conn.SendMessage(&acpb.MessageBody{Labels: map[string]string{"key": "value"}, Body: &apb.Any{Value: []byte("test-body")}})
	if !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("SendMessage() unexpected err: %v", err)
	}

	// Create a new connection using the same cc, this should fail
	_, err = CreateConnection(ctx, testChannelID, false, option.WithGRPCConn(cc))
	if err == nil {
		t.Fatal("CreateConnection() expected to fail")
	}

	// Create a new connection with a new cc, this should work
	cc, err = grpc.Dial("bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	conn, err = CreateConnection(ctx, testChannelID, false, option.WithGRPCConn(cc))
	if err != nil {
		t.Fatalf("CreateConnection() failed: %v", err)
	}

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
	srv, conn, err := createTestConnection(context.Background())
	if err != nil {
		t.Fatalf("CreateConnection() failed: %v", err)
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
