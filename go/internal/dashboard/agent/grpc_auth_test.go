// Copyright 2026 The Ray Authors.
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

package agent_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/internal/dashboard/log"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// echoModule registers a single unary method so the unary interceptor chain can
// be exercised without depending on a specific agent module. HandlerType of
// *interface{} satisfies grpc's RegisterService check because every type
// implements the empty interface.
type echoModule struct{}

func (echoModule) Start(context.Context) error       { return nil }
func (echoModule) RegisterHTTP(*http.ServeMux) error { return nil }
func (echoModule) RegisterGRPC(s *grpc.Server) error {
	s.RegisterService(&echoServiceDesc, echoServer{})
	return nil
}

type echoServer struct{}

var echoServiceDesc = grpc.ServiceDesc{
	ServiceName: "ray.test.EchoService",
	HandlerType: (*interface{})(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Echo",
		// grpc-go hands the server's interceptor chain to the method handler
		// and expects the handler to apply it, which is what a generated stub
		// does. Ignoring the interceptor argument would bypass authentication.
		Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
			in := new(proto.GpuProfilingRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			handler := func(context.Context, interface{}) (interface{}, error) {
				return &proto.GpuProfilingReply{}, nil
			}
			if interceptor == nil {
				return handler(ctx, in)
			}
			return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/ray.test.EchoService/Echo"}, handler)
		},
	}},
	Metadata: "ray/test/echo",
}

// panicStreamModule registers a stream method whose handler always panics so
// the stream recovery interceptor can be observed end to end.
type panicStreamModule struct{}

func (panicStreamModule) Start(context.Context) error       { return nil }
func (panicStreamModule) RegisterHTTP(*http.ServeMux) error { return nil }
func (panicStreamModule) RegisterGRPC(s *grpc.Server) error {
	s.RegisterService(&panicStreamServiceDesc, panicStreamServer{})
	return nil
}

type panicStreamServer struct{}

func (panicStreamServer) boom(interface{}, grpc.ServerStream) error {
	panic("boom")
}

var panicStreamServiceDesc = grpc.ServiceDesc{
	ServiceName: "ray.test.PanicStream",
	HandlerType: (*interface{})(nil),
	// grpc-go routes stream calls through Streams[i].Handler; this service has
	// exactly one stream method.
	Streams: []grpc.StreamDesc{{
		StreamName: "Boom",
		Handler:    func(srv interface{}, stream grpc.ServerStream) error { return panicStreamServer{}.boom(srv, stream) },
	}},
	Metadata: "ray/test/panicstream",
}

// TestAgentGRPCRecoversFromAPanickingStreamHandler proves a panic inside a
// stream handler becomes an Internal status instead of crashing the agent
// process. grpc-go does not recover handler panics itself, so without the
// interceptor the panic propagates out of Serve and kills the agent — while
// the unary path has been protected since the recovery interceptor landed.
func TestAgentGRPCRecoversFromAPanickingStreamHandler(t *testing.T) {
	addr, stop, err := agent.ServeGRPCServerForTest(agent.Config{NodeIP: "127.0.0.1"}, []agent.Module{panicStreamModule{}, echoModule{}})
	if err != nil {
		t.Fatalf("ServeGRPCServerForTest: %v", err)
	}
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{StreamName: "Boom", ServerStreams: true, ClientStreams: true}, "/ray.test.PanicStream/Boom")
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := stream.SendMsg(&proto.GpuProfilingRequest{}); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}
	var reply proto.GpuProfilingReply
	err = stream.RecvMsg(&reply)
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("code = %v (err %v), want Internal: the panic must become a status, not a crash", got, err)
	}
	if status.Convert(err).Message() != "internal error" {
		t.Errorf("message = %q, want the unary recovery's exact wording %q", status.Convert(err).Message(), "internal error")
	}

	// The process survived the panic: a normal unary RPC on the same server
	// still succeeds.
	err = conn.Invoke(context.Background(), "/ray.test.EchoService/Echo", &proto.GpuProfilingRequest{}, &proto.GpuProfilingReply{})
	if err != nil {
		t.Fatalf("server did not survive the stream panic: %v", err)
	}
}

// TestAgentGRPCRequiresTokenWithAuthEnabled proves the authentication
// interceptors are actually installed on the agent server: with token mode on,
// a client that presents no token is rejected before it reaches StreamLog.
func TestAgentGRPCRequiresTokenWithAuthEnabled(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret")

	mod, err := log.New(agent.Config{LogDir: t.TempDir()})
	if err != nil {
		t.Fatalf("log.New: %v", err)
	}
	addr, stop, err := agent.ServeGRPCServerForTest(agent.Config{NodeIP: "127.0.0.1"}, []agent.Module{mod})
	if err != nil {
		t.Fatalf("ServeGRPCServerForTest: %v", err)
	}
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := proto.NewLogServiceClient(conn)

	stream, err := client.StreamLog(context.Background(), &proto.StreamLogRequest{})
	if err == nil {
		_, err = stream.Recv()
	}
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("code = %v (err %v), want Unauthenticated", got, err)
	}
}

// TestAgentGRPCIsOpenInK8sMode is the regression test for the token-mode-only
// gate. Enforcement is token-mode only, mirroring Python's
// is_token_auth_enabled(), so a tokenless call in k8s mode must reach the
// handler. An implementation that gates on TokenAuthEnabled() (true for k8s as
// well) rejects it with Unauthenticated and fails this test.
func TestAgentGRPCIsOpenInK8sMode(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "k8s")
	// A static comparison would accept this token, so a rejection here cannot
	// be blamed on a missing expected value.
	t.Setenv("RAY_AUTH_TOKEN", "secret")

	mod, err := log.New(agent.Config{LogDir: t.TempDir()})
	if err != nil {
		t.Fatalf("log.New: %v", err)
	}
	addr, stop, err := agent.ServeGRPCServerForTest(agent.Config{NodeIP: "127.0.0.1"}, []agent.Module{mod})
	if err != nil {
		t.Fatalf("ServeGRPCServerForTest: %v", err)
	}
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := proto.NewLogServiceClient(conn)

	stream, err := client.StreamLog(context.Background(), &proto.StreamLogRequest{})
	if err == nil {
		_, err = stream.Recv()
	}
	// Any status other than Unauthenticated proves the interceptor let the call
	// reach the handler; the log service itself may then fail for reasons
	// unrelated to authentication.
	if got := status.Code(err); got == codes.Unauthenticated {
		t.Fatalf("code = %v (err %v), want anything but Unauthenticated: k8s mode must not enforce", got, err)
	}
}

// TestAgentGRPCRequiresTokenForUnaryRPC covers the unary chain: the stream
// test alone cannot tell whether GRPCUnaryServerInterceptor is chained on, so a
// tokenless unary call must also be rejected.
func TestAgentGRPCRequiresTokenForUnaryRPC(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret")

	addr, stop, err := agent.ServeGRPCServerForTest(agent.Config{NodeIP: "127.0.0.1"}, []agent.Module{echoModule{}})
	if err != nil {
		t.Fatalf("ServeGRPCServerForTest: %v", err)
	}
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	err = conn.Invoke(context.Background(), "/ray.test.EchoService/Echo", &proto.GpuProfilingRequest{}, &proto.GpuProfilingReply{})
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("code = %v (err %v), want Unauthenticated", got, err)
	}
}

// TestAgentGRPCAllowsTokenlessCallWithAuthDisabled is deliberately absent: a
// pre-cancelled context never reaches the server, so such a test would pass
// whether or not the interceptor is installed. The disabled path is covered by
// the unit cases in Task 1.

// TestAgentGRPCBindsLoopbackForLocalhostNode mirrors agent.py, which binds the
// resolved localhost address on a localhost node and every interface otherwise.
// Binding all interfaces on a localhost node exposes the metrics and log gRPC
// services to the whole network.
func TestAgentGRPCBindsLoopbackForLocalhostNode(t *testing.T) {
	for _, tc := range []struct {
		nodeIP       string
		wantLoopback bool
	}{
		{nodeIP: "127.0.0.1", wantLoopback: true},
		{nodeIP: "localhost", wantLoopback: true},
		{nodeIP: "192.0.2.10", wantLoopback: false},
	} {
		t.Run(tc.nodeIP, func(t *testing.T) {
			addr, stop, err := agent.ServeGRPCServerForTest(agent.Config{NodeIP: tc.nodeIP}, nil)
			if err != nil {
				t.Fatalf("ServeGRPCServerForTest: %v", err)
			}
			defer stop()

			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				t.Fatalf("split %q: %v", addr, err)
			}
			ip := net.ParseIP(host)
			if ip == nil {
				t.Fatalf("host %q is not an IP", host)
			}
			if got := ip.IsLoopback(); got != tc.wantLoopback {
				t.Fatalf("bound on %q (loopback=%v), want loopback=%v", host, got, tc.wantLoopback)
			}
		})
	}
}
