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

// Package gcs provides the Go client for Ray Global Control Store (GCS).
package gcs

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// MaxGRPCMessageSize matches the C++ max_grpc_message_size default
// (src/ray/common/ray_config_def.h) and the Python create_gcs_channel cap
// (python/ray/_private/gcs_utils.py), both 512 MiB. Large queries such as
// GetTaskEvents / GetAllActorInfo can exceed smaller limits.
const MaxGRPCMessageSize = 512 << 20

// Protocol-level metadata and auth header constants, aligned with
// src/ray/common/constants.h (kClusterIdKey) and the dashboard head auth.go.
const (
	// MetadataClusterIDKey is the outgoing metadata key carrying the Ray
	// cluster id, aligned with kClusterIdKey in src/ray/common/constants.h.
	MetadataClusterIDKey = "ray_cluster_id"
	authorizationHeader  = "authorization"
	bearerPrefix         = "Bearer "
)

// AppendClusterID injects the ray_cluster_id metadata, aligned with
// src/ray/common/constants.h. It returns ctx unchanged when clusterID is empty.
func AppendClusterID(ctx context.Context, clusterID string) context.Context {
	if clusterID == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, MetadataClusterIDKey, clusterID)
}

// DialOptions configures a GCS gRPC connection.
type DialOptions struct {
	// CAFile is the TLS CA bundle path. When empty an insecure (plaintext)
	// connection is used, matching the Python init_grpc_channel flow when
	// RAY_USE_TLS is unset.
	CAFile   string
	CertFile string
	KeyFile  string
	// Token enables per-RPC bearer token authentication.
	Token string
	// TokenSource, when set, re-reads a token that rotates on disk and
	// supersedes Token once its first read completes. Kubelet rewrites the
	// projected Ray service-account token roughly hourly, so a long-lived
	// process that captured the token once would start failing authentication
	// an hour into its life; the C++ AuthenticationTokenLoader invalidates its
	// cache on the same trigger.
	TokenSource func() (string, error)
	// ConnectTimeoutMs bounds, in milliseconds, the wait for the channel to
	// become Ready after the connection attempt is started. Zero means
	// non-blocking: return immediately without waiting for Ready.
	ConnectTimeoutMs int64
	// Dialer, when set, replaces grpc-go's default dialer. The dashboard head
	// needs this: grpc-go's default dialer stalls the HTTP/2 cleartext
	// handshake against the Ray GCS, so the head dials plain TCP itself.
	Dialer func(ctx context.Context, address string) (net.Conn, error)
}

// Override returns a copy of d with the non-empty fields of o applied, leaving
// d's other fields unchanged. It is the merge used by clients that resolve
// base options from the environment and then layer explicit per-connection
// settings on top, where an empty explicit value must not clobber the
// environment-derived one.
func (d DialOptions) Override(o DialOptions) DialOptions {
	if o.CAFile != "" {
		d.CAFile = o.CAFile
	}
	if o.CertFile != "" {
		d.CertFile = o.CertFile
	}
	if o.KeyFile != "" {
		d.KeyFile = o.KeyFile
	}
	if o.Token != "" {
		d.Token = o.Token
	}
	if o.TokenSource != nil {
		d.TokenSource = o.TokenSource
	}
	if o.ConnectTimeoutMs != 0 {
		d.ConnectTimeoutMs = o.ConnectTimeoutMs
	}
	if o.Dialer != nil {
		d.Dialer = o.Dialer
	}
	return d
}

// DialGCS establishes a gRPC connection to GCS (or another Ray component
// speaking the same gRPC surface) with the shared message-size cap and
// optional TLS/token. The returned conn must be closed by the caller.
//
// The connection is started immediately (Connect) but dialing is non-blocking;
// when ConnectTimeoutMs > 0 the function waits for the channel to become Ready
// within that bound and fails otherwise. A zero ConnectTimeoutMs returns as
// soon as the connection attempt has been kicked off, matching the
// non-blocking behavior of the deprecated grpc.DialContext used elsewhere in
// the repo.
func DialGCS(ctx context.Context, address string, opts DialOptions) (*grpc.ClientConn, error) {
	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxGRPCMessageSize),
			grpc.MaxCallSendMsgSize(MaxGRPCMessageSize),
		),
	}
	if opts.Dialer != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(opts.Dialer))
	}
	tlsCfg, err := buildTLSConfig(opts.CAFile, opts.CertFile, opts.KeyFile)
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if opts.Token != "" || opts.TokenSource != nil {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&tokenCred{
			token:           opts.Token,
			source:          opts.TokenSource,
			refreshInterval: tokenCredRefreshInterval,
		}))
	}
	// grpc.NewClient is lazy: the channel stays idle until the first RPC or an
	// explicit Connect. Kick it out of idle so connection attempts start now
	// (the deprecated DialContext did this implicitly).
	conn, err := grpc.NewClient(address, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial GCS %s: %w", address, err)
	}
	conn.Connect()
	if opts.ConnectTimeoutMs <= 0 {
		return conn, nil
	}
	connectTimeout := time.Duration(opts.ConnectTimeoutMs) * time.Millisecond
	ctxTimeout, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return conn, nil
		}
		if !conn.WaitForStateChange(ctxTimeout, state) {
			conn.Close()
			return nil, fmt.Errorf("dial GCS %s: connection not ready within %v",
				address, connectTimeout)
		}
	}
}

// PlainTCPDialer dials the GCS address directly with a plain TCP dialer.
// grpc-go's default dialer fails to complete the HTTP/2 cleartext handshake
// with the Ray GCS server (the connection stalls in CONNECTING and RPCs fail
// with "waiting for new LB policy update"), whereas a plain net.Dial completes
// it. This was reproduced against a live GCS and verified in isolation, so
// every in-process caller dials GCS through this dialer unless it explicitly
// supplies its own.
func PlainTCPDialer(ctx context.Context, address string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", address)
}

// buildTLSConfig builds a *tls.Config from the CA/cert/key paths, or returns
// (nil, nil) when no caFile is set (plaintext). When certFile and keyFile are
// also set, mutual TLS is enabled with the client certificate.
func buildTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	if caFile == "" {
		return nil, nil
	}
	pool := x509.NewCertPool()
	ca, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS CA file: %w", err)
	}
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("no certificates found in TLS CA file %s", caFile)
	}
	cfg := &tls.Config{RootCAs: pool}
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, fmt.Errorf("mutual TLS requires both CertFile and KeyFile")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load TLS client key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// tokenCredRefreshInterval bounds how long a token read through TokenSource is
// reused. Kubelet rotates the projected service-account token roughly hourly,
// so a five-minute refresh keeps the client authenticated across rotations
// without re-reading the file on every RPC.
const tokenCredRefreshInterval = 5 * time.Minute

// tokenCred implements credentials.PerRPCCredentials for bearer token auth.
// When a TokenSource is present the token is re-read once refreshInterval has
// elapsed since the last read, so a token file rotated underneath a
// long-running process does not leave it authenticating with a stale value.
type tokenCred struct {
	token           string
	source          func() (string, error)
	refreshInterval time.Duration

	mu        sync.Mutex
	fetchedAt time.Time
}

// current returns the token to send, refreshing it through the source when the
// refresh interval has elapsed. A failed or empty read keeps the previous
// token and is retried on the next call rather than substituting nothing.
func (t *tokenCred) current() string {
	if t.source == nil {
		return t.token
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if time.Since(t.fetchedAt) >= t.refreshInterval {
		t.fetchedAt = time.Now()
		if fresh, err := t.source(); err == nil && fresh != "" {
			t.token = fresh
		}
	}
	return t.token
}

func (t *tokenCred) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	tok := t.current()
	if tok == "" {
		return map[string]string{}, nil
	}
	return map[string]string{authorizationHeader: bearerPrefix + tok}, nil
}

func (t *tokenCred) RequireTransportSecurity() bool { return false }
