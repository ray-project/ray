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

// Package healthz implements the dashboard-agent health check module, a Go
// port of the Python HealthzAgent
// (python/ray/dashboard/modules/reporter/healthz_agent.py). It exposes the
// /api/healthz and /api/local_raylet_healthz HTTP endpoints used by K8s
// probes and by the head node to verify the health of this node's agent,
// raylet, and (on the head node) GCS.
//
// The module is drop-in replaceable for the Python HealthzAgent: it implements
// the same agent.Module contract and keeps the HTTP status codes, response
// bodies, and content type byte-identical to the Python implementation so
// probes see no difference during the migration.
package healthz

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
	"google.golang.org/grpc"
)

// HealthzAgent is the health check module. Like its Python counterpart it has
// no background loop: health is evaluated on each HTTP request via GCS, going
// through HealthChecker semantics (reporter/utils.py) inlined here (Python's
// HealthzAgent and HealthChecker are not otherwise coupled to the reporter).
type HealthzAgent struct {
	nodeID ids.NodeID
	isHead bool
	gcs    gcs.Client
}

// Compile-time assertion that HealthzAgent satisfies the module contract.
var _ agent.Module = (*HealthzAgent)(nil)

// New builds a HealthzAgent from the agent config. A missing GCS client is a
// configuration error and surfaces at startup, consistent with how the other
// dashboard modules validate their config.
func New(cfg agent.Config) (*HealthzAgent, error) {
	if cfg.GCS == nil {
		return nil, errors.New("gcs client is nil")
	}
	return &HealthzAgent{nodeID: cfg.NodeID, isHead: cfg.IsHead, gcs: cfg.GCS}, nil
}

// Start is a no-op: health checks run on request, and the Python HealthzAgent
// has no initialization or background task.
func (h *HealthzAgent) Start(context.Context) error { return nil }

// RegisterGRPC registers no services; the Python HealthzAgent exposes no gRPC
// API either.
func (h *HealthzAgent) RegisterGRPC(*grpc.Server) error { return nil }

// RegisterHTTP registers the two health check endpoints, mirroring the
// @routes.get decorators in healthz_agent.py.
func (h *HealthzAgent) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("/api/healthz", h.handleHealthz)
	mux.HandleFunc("/api/local_raylet_healthz", h.handleLocalRayletHealthz)
	return nil
}

// checkLocalRayletLiveness reports whether the local raylet is alive,
// mirroring HealthChecker.check_local_raylet_liveness (reporter/utils.py): an
// unknown local node id is reported as not alive.
//
// Like Python's async_check_alive([node], 0.1), a short RPC deadline is applied
// so a GCS outage surfaces as a fast "not alive" (503) instead of blocking the
// probe until the agent's GCS retry deadline (60s) elapses.
func (h *HealthzAgent) checkLocalRayletLiveness(ctx context.Context) (bool, error) {
	if h.nodeID.IsNil() {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	alive, err := h.gcs.CheckAlive(ctx, []ids.NodeID{h.nodeID})
	if err != nil {
		return false, err
	}
	if len(alive) != 1 {
		return false, errors.New("check alive returned no result for the local node")
	}
	return alive[0], nil
}

// rayletHealth mirrors HealthzAgent.raylet_health (healthz_agent.py): a dead
// raylet is an error, while a GCS call failure is not. Python only reports a
// raylet failure when the RpcError status is not UNAVAILABLE/UNKNOWN/
// DEADLINE_EXCEEDED; the Go GCS client exposes no gRPC status codes, so every
// GCS error is tolerated here. This keeps the dominant GCS-outage case
// (Probes must not flap while GCS is briefly unreachable) consistent with
// Python's intent of avoiding false positives.
func (h *HealthzAgent) rayletHealth(ctx context.Context) error {
	alive, err := h.checkLocalRayletLiveness(ctx)
	if err != nil {
		return nil
	}
	if !alive {
		return errors.New("Local Raylet failed")
	}
	return nil
}

// gcsHealth reports local GCS health, mirroring HealthzAgent.local_gcs_health:
// GCS is only checked on the head node. The Go GCS client short-circuits
// empty node id lists without a round trip (so it cannot probe reachability
// like Python's async_check_alive([])), therefore the local node id is passed
// to force a real GCS call; only a call error counts as a GCS failure,
// matching Python where a reachable GCS is healthy regardless of node states.
func (h *HealthzAgent) gcsHealth(ctx context.Context) error {
	if !h.isHead {
		return nil
	}
	_, err := h.gcs.CheckAlive(ctx, []ids.NodeID{h.nodeID})
	return err
}

// handleHealthz serves GET /api/healthz, mirroring HealthzAgent.unified_health:
// both checks are evaluated concurrently (Python runs them via asyncio.gather,
// here with two goroutines) and the response body is one "<name>: <result>"
// line per check, returned with 503 when any check failed.
//
// Concurrency is safe: both checks only read h.nodeID and h.gcs, and the
// cgo GCS client serializes on its own RWMutex. Running them concurrently
// bounds the worst-case probe latency to one CheckAlive timeout (100ms)
// instead of two serial timeouts, matching Python's overlap.
func (h *HealthzAgent) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}

	type checkResult struct {
		name string
		err  error
	}
	results := make(chan checkResult, 2)
	go func() { results <- checkResult{"raylet", h.rayletHealth(r.Context())} }()
	go func() { results <- checkResult{"gcs", h.gcsHealth(r.Context())} }()

	var rayletErr, gcsErr error
	for i := 0; i < 2; i++ {
		res := <-results
		switch res.name {
		case "raylet":
			rayletErr = res.err
		case "gcs":
			gcsErr = res.err
		}
	}

	rayletResult := "success"
	gcsResult := "success (no local gcs)"
	status := http.StatusOK
	if rayletErr != nil {
		status = http.StatusServiceUnavailable
		rayletResult = rayletErr.Error()
		log.Log.V(1).Info("health check failed", "name", "raylet", "reason", rayletErr.Error())
	}
	if gcsErr != nil {
		status = http.StatusServiceUnavailable
		gcsResult = gcsErr.Error()
		log.Log.V(1).Info("health check failed", "name", "gcs", "reason", gcsErr.Error())
	} else if h.isHead {
		gcsResult = "success"
	}

	writeText(w, status, "raylet: "+rayletResult+"\ngcs: "+gcsResult)
}

// handleLocalRayletHealthz serves GET /api/local_raylet_healthz, mirroring
// HealthzAgent.health_check: 200 "success" when the local raylet is alive,
// 503 with the failure reason otherwise.
func (h *HealthzAgent) handleLocalRayletHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	if err := h.rayletHealth(r.Context()); err != nil {
		log.Log.V(1).Info("health check failed", "name", "raylet", "reason", err.Error())
		writeText(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeText(w, http.StatusOK, "success")
}

// writeText writes a plain text response, byte-identical to how the Python
// agent serves healthz bodies: aiohttp's Response(text=...) sets Content-Type
// to content_type + "; charset=utf-8" and encodes the body as UTF-8 with no
// trailing newline.
func writeText(w http.ResponseWriter, status int, text string) {
	w.Header().Set("Content-Type", "application/text; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, text)
}

// methodNotAllowed mirrors aiohttp's HTTPMethodNotAllowed response for a GET
// route hit with another method: status 405, body "405: Method Not Allowed"
// (aiohttp web_exceptions formats "f{status}: {reason}"), Content-Type
// "text/plain; charset=utf-8", and an Allow header listing the methods aiohttp
// serves on a GET-annotated route (GET, HEAD, and OPTIONS).
func methodNotAllowed(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Allow", "GET,HEAD,OPTIONS")
	w.WriteHeader(http.StatusMethodNotAllowed)
	_, _ = io.WriteString(w, "405: Method Not Allowed")
}
