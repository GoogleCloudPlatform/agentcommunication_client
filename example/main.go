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

// Example usage of the ACS client library.
package main

import (
	"context"
	"flag"
	"log"
	"time"

	client "github.com/GoogleCloudPlatform/agentcommunication_client"
	"google.golang.org/api/option"

	anypb "google.golang.org/protobuf/types/known/anypb"
	acpb "github.com/GoogleCloudPlatform/agentcommunication_client/gapic/agentcommunicationpb"
)

var (
	endpoint = flag.String("endpoint", "", "endpoint override, don't set if not needed")
	channel  = flag.String("channel", "my-channel", "channel")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	client.DebugLogging = true

	opts := []option.ClientOption{}
	if *endpoint != "" {
		opts = append(opts, option.WithEndpoint(*endpoint))
	}
	conn, err := client.CreateConnection(ctx, *channel, false, opts...)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			msg, err := conn.Receive()
			if err != nil {
				log.Println(err)
				return
			}
			log.Printf("Got message: %+v", msg)
		}
	}()

	if err := conn.SendMessage(&acpb.MessageBody{Body: &anypb.Any{Value: []byte("hello world")}}); err != nil {
		log.Fatal(err)
	}
	time.Sleep(5 * time.Second)
	if err := conn.SendMessage(&acpb.MessageBody{Body: &anypb.Any{Value: []byte("hello world")}}); err != nil {
		log.Fatal(err)
	}
	time.Sleep(60 * time.Second)

	conn.Close()

	time.Sleep(1 * time.Second)
}
