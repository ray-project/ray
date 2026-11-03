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
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"

	"github.com/ray-project/ray/go/pkg/log"
)

// maxHTTPRequestBodySize caps any request body handled by the dashboard agent
// HTTP server, aligned with the 10 MiB limit used elsewhere in the codebase.
const maxHTTPRequestBodySize = 10 << 20 // 10 MiB

// httpServer wraps the dashboard agent HTTP server and its route registration.
type httpServer struct {
	server   *http.Server
	listener net.Listener
}

// newHTTPServer builds the HTTP server, registers routes from every module,
// and binds the configured listen address. Binding here (instead of inside
// serve) means the socket accepts connections before the agent publishes its
// address in GCS, closing the window where a head-side consumer could dial a
// registered-but-not-yet-listening address.
func newHTTPServer(cfg Config, mods []Module) (*httpServer, error) {
	mux := http.NewServeMux()
	for _, m := range mods {
		if err := m.RegisterHTTP(mux); err != nil {
			return nil, fmt.Errorf("register HTTP routes: %w", err)
		}
	}
	// Bind on all interfaces rather than only cfg.NodeIP: the in-container
	// readiness probe dials localhost, while head-side consumers dial the pod
	// IP resolved through ports_by_node, so both must reach this listener. The
	// Python agent's healthz likewise answers on localhost and the pod IP.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.ListenPort))
	if err != nil {
		if !errors.Is(err, syscall.EADDRINUSE) {
			return nil, fmt.Errorf("listen on HTTP addr: %w", err)
		}
		// A co-hosted agent (the Python and Go agents share the fixed default
		// listen port) holds the port. Fall back to a dynamic port instead of
		// disabling the HTTP server: Run registers the actually bound port in
		// GCS InternalKV, so head-side consumers resolving the agent address
		// get the real port, while a degraded agent serves nothing and
		// registers nothing. The registration is also the only channel that
		// carries the dynamic port: the raylet waits for the port files only
		// when the configured port is 0, and with the fixed default configured
		// it keeps publishing that (taken) port instead.
		log.Log.Error(err, "fixed listen port taken; falling back to a dynamic port")
		lis, err = net.Listen("tcp", ":0")
		if err != nil {
			return nil, fmt.Errorf("listen on HTTP addr fallback: %w", err)
		}
	}
	return &httpServer{
		server: &http.Server{
			Handler: http.MaxBytesHandler(mux, maxHTTPRequestBodySize),
		},
		listener: lis,
	}, nil
}

// serve accepts connections on the bound listener until the server is shut
// down.
func (s *httpServer) serve() error {
	if err := s.server.Serve(s.listener); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// shutdown gracefully stops the HTTP server.
func (s *httpServer) shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
