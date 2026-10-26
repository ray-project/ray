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
	"fmt"
	"net"
	"time"

	"github.com/ray-project/ray/go/pkg/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpcMaxMessageSize caps both outgoing and incoming gRPC messages handled by
// the dashboard agent, aligned with the C++ dashboard agent limit of 16 MiB.
const grpcMaxMessageSize = 16 << 20 // 16 MiB

// grpcStopTimeout bounds the graceful drain of in-flight RPCs on shutdown.
// The agent hosts long-lived server streams (e.g. StreamLog with keep_alive),
// which would otherwise hold GracefulStop open indefinitely; the Python agent
// exits immediately on SIGTERM with no graceful drain at all, so a short
// bounded drain followed by a hard stop matches that contract closely enough.
const grpcStopTimeout = 5 * time.Second

// grpcServer wraps the dashboard agent gRPC server and its service registration.
type grpcServer struct {
	server   *grpc.Server
	listener net.Listener
}

// unaryRecoveryInterceptor converts a panic in a unary handler into an
// Internal error instead of letting it crash the long-lived agent process. A
// malformed ReportOCMetrics payload, for example, must not take the agent down.
func unaryRecoveryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Log.Error(fmt.Errorf("recovered panic: %v", r), "gRPC handler panicked", "method", info.FullMethod)
			resp = nil
			err = status.Error(codes.Internal, "internal error")
		}
	}()
	return handler(ctx, req)
}

// newGRPCServer builds the gRPC server, registers services from every module,
// and binds the configured listen address.
func newGRPCServer(cfg Config, mods []Module) (*grpcServer, error) {
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(grpcMaxMessageSize),
		grpc.MaxSendMsgSize(grpcMaxMessageSize),
		grpc.ChainUnaryInterceptor(unaryRecoveryInterceptor),
	)
	for _, m := range mods {
		if err := m.RegisterGRPC(s); err != nil {
			return nil, fmt.Errorf("register gRPC services: %w", err)
		}
	}
	// Bind on all interfaces: the raylet's MetricsAgentClient dials localhost
	// from inside the container, while head-side consumers dial the pod IP
	// resolved through ports_by_node, so both must reach this listener. Mirror
	// the Python agent, which binds non-localhost nodes on all interfaces.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		return nil, fmt.Errorf("listen on gRPC addr: %w", err)
	}
	return &grpcServer{server: s, listener: lis}, nil
}

// serve accepts connections on the bound listener until the server is stopped.
func (s *grpcServer) serve() error {
	return s.server.Serve(s.listener)
}

// stop drains in-flight RPCs for at most grpcStopTimeout, then force-stops
// the server so shutdown cannot hang on a long-lived stream.
func (s *grpcServer) stop() {
	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grpcStopTimeout):
		s.server.Stop()
	}
}
