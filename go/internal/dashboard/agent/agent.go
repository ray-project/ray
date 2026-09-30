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

// Package agent provides the shared framework contract for dashboard-agent
// modules: process-level configuration, the Module interface, and the Run
// entry point that owns the HTTP/gRPC server lifecycle.
package agent

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"google.golang.org/grpc"
)

// Config is the process-level context shared by all dashboard-agent modules.
type Config struct {
	// NodeIP is the IP address this agent reports for itself.
	NodeIP string
	// NodeID uniquely identifies the Ray node this agent runs on.
	NodeID ids.NodeID
	// IsHead indicates whether this agent runs on the head node.
	IsHead bool
	// GCSAddress is the host:port of the GCS server.
	GCSAddress string
	// ClusterID identifies the cluster this agent belongs to.
	ClusterID ids.ClusterID
	// GCS is the connected GCS client used for KV registration and reads.
	GCS gcs.Client
	// LogDir is the directory where this agent writes its logs.
	LogDir string
	// TempDir is the temporary directory used by this agent.
	TempDir string
	// SessionDir is the session directory of the current Ray runtime.
	SessionDir string
	// SessionName is the name of the current Ray session.
	SessionName string
	// GRPCPort is the port the gRPC server listens on.
	GRPCPort int
	// ListenPort is the port the HTTP server listens on.
	ListenPort int
	// NodeManagerPort is the port of the node manager on this node.
	NodeManagerPort int
	// MetricsExportPort is the port where metrics are exported.
	MetricsExportPort int
	// MetricsCollectionDisabled disables metrics collection when true.
	MetricsCollectionDisabled bool
	// EventsExportAddr is the address events are exported to.
	EventsExportAddr string
	// HTTPClient is the shared HTTP client used by modules.
	HTTPClient *http.Client
}

// Module is the interface every dashboard-agent module implements.
type Module interface {
	Start(ctx context.Context) error
	RegisterHTTP(mux *http.ServeMux) error
	RegisterGRPC(s *grpc.Server) error
}

// Run owns the HTTP/gRPC server lifecycle: it builds both servers from the
// registered modules, starts every module, publishes the agent address in GCS
// InternalKV, then serves HTTP and gRPC until ctx is cancelled. An HTTP bind
// failure degrades instead of killing the agent (mirroring Python
// agent.py, which stays alive with the HTTP service disabled and skips the
// KV registration); a gRPC bind failure remains fatal.
func Run(ctx context.Context, cfg Config, mods []Module) error {
	httpSrv, httpErr := newHTTPServer(cfg, mods)
	if httpErr != nil {
		// Python keeps the agent alive with the HTTP service disabled in this
		// case and does not publish its address, because every head-side
		// consumer reaches the agent through that registration.
		log.Log.Error(httpErr, "failed to start HTTP server; " +
			"the agent will stay alive but the HTTP service will be disabled")
		httpSrv = nil
	}
	grpcSrv, err := newGRPCServer(cfg, mods)
	if err != nil {
		return fmt.Errorf("create gRPC server: %w", err)
	}

	// Start every module before serving so configuration or initialization
	// errors surface before any traffic is accepted.
	for _, m := range mods {
		if err := m.Start(ctx); err != nil {
			return fmt.Errorf("start module: %w", err)
		}
	}

	if httpSrv != nil {
		if err := registerAgentToGCS(ctx, cfg, cfg.GRPCPort, cfg.ListenPort); err != nil {
			return fmt.Errorf("register agent to GCS: %w", err)
		}
	}

	errCh := make(chan error, 2)
	if httpSrv != nil {
		go func() { errCh <- httpSrv.serve() }()
	}
	go func() { errCh <- grpcSrv.serve() }()

	select {
	case <-ctx.Done():
		log.Log.V(0).Info("dashboard agent shutting down")
		shutdownServers(httpSrv, grpcSrv)
		return nil
	case err := <-errCh:
		if err == nil {
			err = fmt.Errorf("a dashboard agent server exited unexpectedly")
		}
		shutdownServers(httpSrv, grpcSrv)
		return err
	}
}

// shutdownServers gracefully shuts down both servers, best-effort. httpSrv is
// nil when the HTTP server degraded at startup and there is nothing to stop.
func shutdownServers(httpSrv *httpServer, grpcSrv *grpcServer) {
	if httpSrv != nil {
		if err := httpSrv.shutdown(context.Background()); err != nil {
			log.Log.Error(err, "failed to shut down HTTP server")
		}
	}
	grpcSrv.stop()
}

// registerAgentToGCS publishes this agent's HTTP and gRPC addresses in GCS
// InternalKV under the dashboard namespace, mirroring the Python
// _async_register_agent_to_gcs helper. The two keys are not interchangeable:
// the node-id key maps to [ip, listen_port, grpc_port] while the ip key maps
// to [node_id, listen_port, grpc_port] — head-side consumers resolve one
// missing field through the other key (e.g. the dashboard head's log module
// reads vals[0] of the ip-keyed value as a node id).
func registerAgentToGCS(ctx context.Context, cfg Config, grpcPort, listenPort int) error {
	entries := []struct {
		key   string
		value string
	}{
		{
			key:   DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX + cfg.NodeID.Hex(),
			value: fmt.Sprintf("[%q, %d, %d]", cfg.NodeIP, listenPort, grpcPort),
		},
		{
			key:   DASHBOARD_AGENT_ADDR_IP_PREFIX + cfg.NodeIP,
			value: fmt.Sprintf("[%q, %d, %d]", cfg.NodeID.Hex(), listenPort, grpcPort),
		},
	}
	for _, e := range entries {
		if _, err := cfg.GCS.Put(ctx, KVNamespaceDashboard, e.key, []byte(e.value), true); err != nil {
			return fmt.Errorf("failed to register agent address at key %s: %w", e.key, err)
		}
	}
	return nil
}
