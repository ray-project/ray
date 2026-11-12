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

package auth

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// unauthenticatedMessage is the status message a rejected RPC carries. It
// reproduces _UNAUTHENTICATED_MESSAGE of the Python
// grpc_authentication_server_interceptor verbatim.
const unauthenticatedMessage = "Invalid or missing authentication token"

// authenticateGRPC validates the authorization metadata of an incoming gRPC
// request, mirroring the Python AsyncAuthenticationServerInterceptor: when
// token authentication is not enabled every request is allowed, and when it is
// enabled the request must carry a valid bearer token. Token authentication
// here means RAY_AUTH_MODE=token only, not the broader TokenAuthEnabled() the
// HTTP middleware uses.
//
// Only the "authorization" metadata entry is consulted. The HTTP middleware
// additionally accepts the x-ray-authorization header and the authentication
// cookie, but the Python gRPC interceptor does not, so neither does this.
func authenticateGRPC(ctx context.Context) error {
	// Enforce only in token mode, mirroring Python's is_token_auth_enabled(),
	// which is true only when the mode is TOKEN, and the gate agent.py uses
	// before installing its gRPC interceptor. See TokenModeEnabled for why k8s
	// mode is not enforced here.
	if !TokenModeEnabled() {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, unauthenticatedMessage)
	}
	values := md.Get(AuthorizationHeaderName)
	if len(values) == 0 {
		return status.Error(codes.Unauthenticated, unauthenticatedMessage)
	}
	if !ValidateRequestToken(values[0]) {
		return status.Error(codes.Unauthenticated, unauthenticatedMessage)
	}
	return nil
}

// GRPCUnaryServerInterceptor authenticates unary RPCs before they reach the
// service handler. The dashboard agent installs it on its gRPC server, which
// otherwise serves the metrics, log and reporter services to anyone who can
// reach the port.
func GRPCUnaryServerInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if err := authenticateGRPC(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// GRPCStreamServerInterceptor authenticates streaming RPCs. StreamLog is a
// long-lived server stream, so the unary interceptor alone would leave the log
// endpoint open.
func GRPCStreamServerInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := authenticateGRPC(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

// TokenCredential attaches the cluster bearer token to outgoing gRPC calls,
// mirroring the client-side interceptor Python installs from
// ray._private.grpc_utils.init_grpc_channel when token authentication is on.
//
// The token is resolved per RPC through RequestToken, which re-reads
// RAY_AUTH_TOKEN_PATH every time. A credential that cached the token would keep
// presenting a rotated-out value for the life of a long-lived channel.
type TokenCredential struct{}

var _ credentials.PerRPCCredentials = TokenCredential{}

// GetRequestMetadata implements credentials.PerRPCCredentials. With
// authentication disabled, or with no token source configured, it returns no
// metadata so the call is byte-identical to an unauthenticated one.
func (TokenCredential) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	token, ok := RequestToken()
	if !ok {
		return map[string]string{}, nil
	}
	return map[string]string{AuthorizationHeaderName: BearerPrefix + token}, nil
}

// RequireTransportSecurity reports false because agent and head communicate
// over plaintext inside the cluster; grpc-go would otherwise refuse to send the
// credential at all.
func (TokenCredential) RequireTransportSecurity() bool { return false }
