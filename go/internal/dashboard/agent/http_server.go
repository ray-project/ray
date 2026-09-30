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
	"net/http"
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
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.NodeIP, cfg.ListenPort))
	if err != nil {
		return nil, fmt.Errorf("listen on HTTP addr: %w", err)
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
