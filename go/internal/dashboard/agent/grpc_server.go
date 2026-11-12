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
	"strconv"
	"time"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/dashboard/auth"
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

// streamRecoveryInterceptor converts a panic in a stream handler into an
// Internal error instead of letting it crash the long-lived agent process,
// mirroring unaryRecoveryInterceptor for the stream chain.
func streamRecoveryInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Log.Error(fmt.Errorf("recovered panic: %v", r), "gRPC stream handler panicked", "method", info.FullMethod)
			err = status.Error(codes.Internal, "internal error")
		}
	}()
	return handler(srv, ss)
}

// newGRPCServer builds the gRPC server, registers services from every module,
// and binds the configured listen address.
//
// The recovery interceptors are chained outermost on both the unary and the
// stream chain so a panic anywhere after them — including inside the
// authentication interceptor — still becomes an Internal status instead of
// taking the agent process down.
func newGRPCServer(cfg Config, mods []Module) (*grpcServer, error) {
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(grpcMaxMessageSize),
		grpc.MaxSendMsgSize(grpcMaxMessageSize),
		grpc.ChainUnaryInterceptor(unaryRecoveryInterceptor, auth.GRPCUnaryServerInterceptor),
		grpc.ChainStreamInterceptor(streamRecoveryInterceptor, auth.GRPCStreamServerInterceptor),
	)
	for _, m := range mods {
		if err := m.RegisterGRPC(s); err != nil {
			return nil, fmt.Errorf("register gRPC services: %w", err)
		}
	}
	lis, err := net.Listen("tcp", grpcBindAddr(cfg))
	if err != nil {
		return nil, fmt.Errorf("listen on gRPC addr: %w", err)
	}
	return &grpcServer{server: s, listener: lis}, nil
}

// grpcBindAddr returns the address the gRPC server binds, mirroring the Python
// agent: a localhost node binds the resolved loopback address, any other node
// binds every interface so the raylet and head-side consumers can both reach
// it. Binding all interfaces on a localhost node would publish the metrics and
// log services to the network for no reason.
func grpcBindAddr(cfg Config) string {
	if common.IsLocalhost(cfg.NodeIP) {
		return bindAddrForHost(common.GetLocalhostIP(), cfg.GRPCPort)
	}
	return bindAddrForHost("", cfg.GRPCPort)
}

// bindAddrForHost joins a host and port into a listen address. An empty
// host means every interface. net.JoinHostPort brackets an IPv6 literal,
// which a plain "%s:%d" format would turn into an invalid address.
func bindAddrForHost(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
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
