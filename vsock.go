// Copyright 2026 Google LLC
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
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/agentcommunication_client/gapic"
	"google.golang.org/api/option"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"github.com/mdlayher/vsock"
)

const (
	vsockContextID   = uint32(vsock.Hypervisor)
	defaultVSOCKPort = uint32(6769)
)

var (
	// DefaultAllowVSOCK sets the default for VSOCK usage, can be overridden with an environment
	// variable at runtime.
	DefaultAllowVSOCK = true

	// vsockAvailable is a function that checks if vsock is available on the system.
	vsockAvailable func() bool = func() bool {
		allowVSOCK := DefaultAllowVSOCK
		// Allow overriding DefaultAllowVSOCK with an environment variable at runtime.
		if env := os.Getenv("ACS_ALLOW_VSOCK"); env != "" {
			allowVSOCK = strings.ToLower(env) == "true"
		}
		if !allowVSOCK {
			loggerPrintf("VSOCK not allowed")
			return false
		}
		loggerPrintf("VSOCK allowed, checking if available")

		// Check if vsock is available on the system, currently this only works on Linux, once we add
		// support for other OSes we can update this to check.
		conn, err := vsock.Dial(vsockContextID, vsockPort, nil)
		if err != nil {
			loggerPrintf("Failed to dial vsock, falling back to network: %v", err)
			return false
		}
		if err := conn.Close(); err != nil {
			loggerPrintf("Failed to close vsock connection: %v", err)
		}
		loggerPrintf("VSOCK available")

		return allowVSOCK
	}

	vsockPort   = defaultVSOCKPort
	vsockTarget string
)

func init() {
	// ACS_VSOCK_PORT is an environment variable that can be set to change the default vsock port, this is
	// mainly used for testing.
	if env := os.Getenv("ACS_VSOCK_PORT"); env != "" {
		parsedPort, err := strconv.ParseUint(env, 10, 32)
		if err != nil {
			loggerPrintf("Failed to parse vsock port: %v", err)
		} else {
			vsockPort = uint32(parsedPort)
		}
	}
	vsockTarget = fmt.Sprintf("passthrough:%d:%d", vsockContextID, vsockPort)
}

func clientUsingVSOCK(client *agentcommunication.Client) bool {
	// Check the target to see if it is a vsock connection, target is set at creation and the
	// Connection() method properly returns the underlying connection for this use case.
	return client.Connection().Target() == vsockTarget
}

func vsockDialer(_ context.Context, addr string) (net.Conn, error) {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid vsock address: %q", addr)
	}

	ctxID, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse context ID: %w", err)
	}

	port, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse port: %w", err)
	}

	conn, err := vsock.Dial(uint32(ctxID), uint32(port), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}
	return conn, nil
}

func newVSOCKClient(ctx context.Context, port uint32, opts ...option.ClientOption) (*agentcommunication.Client, error) {
	vsockConn, err := grpc.NewClient(
		vsockTarget,
		grpc.WithContextDialer(vsockDialer),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 60 * time.Second, Timeout: 10 * time.Second}),
		// Do not use TLS for VSOCK.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	// Do not use oauth.
	opts = append([]option.ClientOption{
		option.WithoutAuthentication(),
		option.WithGRPCConn(vsockConn),
	}, opts...)

	return agentcommunication.NewClient(ctx, opts...)
}
