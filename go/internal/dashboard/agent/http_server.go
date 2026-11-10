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
	"strconv"
	"syscall"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/internal/dashboard/auth"
	"github.com/ray-project/ray/go/pkg/log"
)

// maxHTTPRequestBodySize caps any request body handled by the dashboard agent
// HTTP server, aligned with the 10 MiB limit used elsewhere in the codebase.
const maxHTTPRequestBodySize = 10 << 20 // 10 MiB

// PublicExactPaths are exact paths the agent serves without authentication,
// mirroring the Python agent's PUBLIC_EXACT_PATHS: the health endpoints must
// answer probes that carry no token.
var PublicExactPaths = []string{
	"/api/healthz",
	"/api/local_raylet_healthz",
}

// httpServer wraps the dashboard agent HTTP server and its route registration.
// A second listener on the loopback address serves the in-container readiness
// probe when the node IP is not a loopback address, mirroring the Python agent,
// which starts a TCPSite on the node IP and another on get_localhost_ip().
type httpServer struct {
	server           *http.Server
	listener         net.Listener
	loopback         *http.Server
	loopbackListener net.Listener
}

// newHTTPServer builds the HTTP server, registers routes from every module,
// and binds the configured listen addresses. Binding here (instead of inside
// serve) means the sockets accept connections before the agent publishes its
// address in GCS, closing the window where a head-side consumer could dial a
// registered-but-not-yet-listening address.
func newHTTPServer(cfg Config, mods []Module) (*httpServer, error) {
	mux := http.NewServeMux()
	for _, m := range mods {
		if err := m.RegisterHTTP(mux); err != nil {
			return nil, fmt.Errorf("register HTTP routes: %w", err)
		}
	}
	router := &aiohttpRouter{mux: mux}

	// The chain mirrors the Python agent's middleware: token authentication,
	// then the browser block, with CORS wrapping the adapter that reproduces
	// aiohttp's CORS and 404/405 semantics.
	var handler http.Handler = http.MaxBytesHandler(router, maxHTTPRequestBodySize)
	handler = auth.WithBrowserBlock(handler)
	handler = auth.WithTokenAuth(PublicExactPaths, nil, handler)
	handler = auth.WithCORS(router.isCORSEnabled, handler)

	lis, err := listenHTTP(cfg.NodeIP, cfg.ListenPort)
	if err != nil {
		return nil, err
	}
	srv := &httpServer{
		server:   &http.Server{Handler: handler},
		listener: lis,
	}

	// With a non-loopback node IP the node-ip listener does not answer on
	// localhost, so bind the loopback address too. Both listeners share the
	// port the node-ip listener actually bound, which also covers dynamic
	// allocation and the dynamic-port fallback below. A bind failure only
	// drops the extra listener: the agent still serves on the node IP.
	loopbackIP := common.GetLocalhostIP()
	if cfg.NodeIP != "" && !common.IsLocalhost(cfg.NodeIP) && loopbackIP != cfg.NodeIP {
		loopbackListener, err := net.Listen("tcp",
			net.JoinHostPort(loopbackIP, strconv.Itoa(listenerPort(lis))))
		if err != nil {
			log.Log.Error(err, "failed to bind the dashboard agent loopback listener; "+
				"the agent will only serve on the node IP")
		} else {
			srv.loopback = &http.Server{Handler: handler}
			srv.loopbackListener = loopbackListener
		}
	}
	return srv, nil
}

// listenHTTP binds the HTTP listener on the node IP. A taken fixed port falls
// back to a dynamic port instead of disabling the HTTP server: a co-hosted
// agent (the Python and Go agents share the fixed default listen port) holds
// the port, and Run registers the actually bound port in GCS InternalKV, so
// head-side consumers resolving the agent address get the real port.
func listenHTTP(nodeIP string, port int) (net.Listener, error) {
	lis, err := net.Listen("tcp", net.JoinHostPort(nodeIP, strconv.Itoa(port)))
	if err != nil {
		if !errors.Is(err, syscall.EADDRINUSE) {
			return nil, fmt.Errorf("listen on HTTP addr: %w", err)
		}
		// The registration is the only channel that carries the dynamic port:
		// the raylet waits for the port files only when the configured port is
		// 0, and with the fixed default configured it keeps publishing that
		// (taken) port instead.
		log.Log.Error(err, "fixed listen port taken; falling back to a dynamic port")
		lis, err = net.Listen("tcp", net.JoinHostPort(nodeIP, "0"))
		if err != nil {
			return nil, fmt.Errorf("listen on HTTP addr fallback: %w", err)
		}
	}
	return lis, nil
}

// serve serves on the bound listeners until the server is shut down. The
// loopback listener runs in the background; it only logs failures because the
// node-ip listener is the one whose errors matter.
func (s *httpServer) serve() error {
	if s.loopback != nil {
		go func() {
			if err := s.loopback.Serve(s.loopbackListener); err != nil &&
				!errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
				log.Log.Error(err, "failed to serve the dashboard agent on the loopback address")
			}
		}()
	}
	if err := s.server.Serve(s.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.closeLoopback()
		return err
	}
	return nil
}

// shutdown gracefully stops the HTTP servers.
func (s *httpServer) shutdown(ctx context.Context) error {
	s.closeLoopback()
	return s.server.Shutdown(ctx)
}

// closeLoopback releases the loopback listener. The raw listener is closed as
// well because http.Server.Close only closes listeners registered by Serve, and
// Run may abort before serve is reached.
func (s *httpServer) closeLoopback() {
	if s.loopback != nil {
		if err := s.loopback.Close(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Log.Error(err, "failed to close the dashboard agent loopback server")
		}
	}
	if s.loopbackListener != nil {
		if err := s.loopbackListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Log.Error(err, "failed to close the dashboard agent loopback listener")
		}
	}
}
