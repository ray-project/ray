// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package agent

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
)

// blockingStreamService is a minimal gRPC service whose single server-stream
// method sends one reply and then hangs forever, standing in for the agent's
// long-lived StreamLog keep-alive streams.
type blockingStreamService struct {
	release chan struct{}
}

var blockingStreamDesc = grpc.ServiceDesc{
	ServiceName: "test.BlockingStreamService",
	HandlerType: (*blockingStreamService)(nil),
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Block",
			Handler:       (*blockingStreamService).block,
			ServerStreams: true,
		},
	},
}

func (b *blockingStreamService) block(srv interface{}, stream grpc.ServerStream) error {
	if err := stream.SendMsg(&proto.ListLogsReply{}); err != nil {
		return err
	}
	<-b.release
	return nil
}

// startBlockingGRPCServer binds an ephemeral port and serves the blocking
// service, returning the server wrapper and the address to dial.
func startBlockingGRPCServer(t *testing.T) (*grpcServer, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	svc := &blockingStreamService{release: make(chan struct{})}
	s := grpc.NewServer()
	s.RegisterService(&blockingStreamDesc, svc)
	return &grpcServer{server: s, listener: lis}, lis.Addr().String()
}

// TestStopReturnsDespiteActiveStream verifies stop() does not hang forever on
// an in-flight server stream: the graceful drain is bounded by
// grpcStopTimeout, after which the server is force-stopped, mirroring the
// Python agent's exit-immediately-on-SIGTERM contract.
func TestStopReturnsDespiteActiveStream(t *testing.T) {
	srv, addr := startBlockingGRPCServer(t)
	go srv.serve()

	conn, err := grpc.NewClient(addr, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := conn.NewStream(context.Background(), &grpc.StreamDesc{StreamName: "Block", ServerStreams: true}, "/test.BlockingStreamService/Block")
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	if err := stream.SendMsg(&proto.ListLogsRequest{}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}
	reply := &proto.ListLogsReply{}
	if err := stream.RecvMsg(reply); err != nil {
		t.Fatalf("recv first message: %v", err)
	}

	done := make(chan struct{})
	go func() {
		srv.stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grpcStopTimeout + 5*time.Second):
		t.Fatalf("stop() did not return within grpcStopTimeout+%s with an active stream", 5*time.Second)
	}
}

// TestStopReturnsWhenIdle verifies the fast path: with no in-flight RPCs the
// graceful drain finishes well inside the bound.
func TestStopReturnsWhenIdle(t *testing.T) {
	srv, _ := startBlockingGRPCServer(t)
	done := make(chan struct{})
	go func() {
		srv.stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stop() on an idle server did not return promptly")
	}
}
