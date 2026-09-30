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

package healthz

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
)

// fakeGCS is a gcs.Client stub: it only overrides CheckAlive and satisfies the
// rest of the interface through the embedded nil gcs.Client, which the module
// under test never invokes.
type fakeGCS struct {
	gcs.Client
	alive   []bool
	err     error
	delay   time.Duration
	mu      sync.Mutex
	started int // number of CheckAlive calls currently in flight
	maxConc int // maximum concurrent CheckAlive calls observed
}

// CheckAlive records concurrency and, when delay is set, ensures calls overlap:
// it keeps a second in-flight slot busy so the test can assert the two health
// checks actually run in parallel rather than serially.
func (f *fakeGCS) CheckAlive(_ context.Context, _ []ids.NodeID) ([]bool, error) {
	f.mu.Lock()
	f.started++
	if f.started > f.maxConc {
		f.maxConc = f.started
	}
	f.mu.Unlock()

	if f.delay > 0 {
		time.Sleep(f.delay)
	}

	f.mu.Lock()
	f.started--
	f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}
	return f.alive, nil
}

// newAgent builds a HealthzAgent with a fresh NodeID over the given fake.
func newAgent(t *testing.T, isHead bool, f *fakeGCS) *HealthzAgent {
	t.Helper()
	h, err := New(agent.Config{NodeID: ids.NewNodeID(), IsHead: isHead, GCS: f})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return h
}

// serve routes one request through the registered mux, exercising the same
// route wiring the agent HTTP server uses.
func serve(t *testing.T, h *HealthzAgent, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	if err := h.RegisterHTTP(mux); err != nil {
		t.Fatalf("RegisterHTTP: %v", err)
	}
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestNewNilGCS(t *testing.T) {
	if _, err := New(agent.Config{NodeID: ids.NewNodeID()}); err == nil {
		t.Error("New with nil GCS: want an error, got nil")
	}
}

// TestModuleContract covers the parts of agent.Module that carry no behavior:
// Start and RegisterGRPC are no-ops, and HealthzAgent satisfies the interface
// at compile time.
func TestModuleContract(t *testing.T) {
	h := newAgent(t, false, &fakeGCS{alive: []bool{true}})
	if err := h.Start(context.Background()); err != nil {
		t.Errorf("Start: %v", err)
	}
	if err := h.RegisterGRPC(nil); err != nil {
		t.Errorf("RegisterGRPC: %v", err)
	}
}

func TestRegisterHTTPRoutes(t *testing.T) {
	h := newAgent(t, false, &fakeGCS{alive: []bool{true}})
	mux := http.NewServeMux()
	if err := h.RegisterHTTP(mux); err != nil {
		t.Fatalf("RegisterHTTP: %v", err)
	}
	for _, path := range []string{"/api/healthz", "/api/local_raylet_healthz"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s status = %d, want 200", path, rec.Code)
		}
	}
}

func TestCheckLocalRayletLiveness(t *testing.T) {
	t.Run("NilNodeID", func(t *testing.T) {
		h, err := New(agent.Config{NodeID: ids.NilNodeID(), GCS: &fakeGCS{alive: []bool{true}}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		alive, err := h.checkLocalRayletLiveness(context.Background())
		if err != nil {
			t.Fatalf("checkLocalRayletLiveness: %v", err)
		}
		if alive {
			t.Error("alive = true, want false for a nil node id")
		}
	})

	t.Run("Alive", func(t *testing.T) {
		h := newAgent(t, false, &fakeGCS{alive: []bool{true}})
		alive, err := h.checkLocalRayletLiveness(context.Background())
		if err != nil {
			t.Fatalf("checkLocalRayletLiveness: %v", err)
		}
		if !alive {
			t.Error("alive = false, want true")
		}
	})

	t.Run("Dead", func(t *testing.T) {
		h := newAgent(t, false, &fakeGCS{alive: []bool{false}})
		alive, err := h.checkLocalRayletLiveness(context.Background())
		if err != nil {
			t.Fatalf("checkLocalRayletLiveness: %v", err)
		}
		if alive {
			t.Error("alive = true, want false")
		}
	})

	t.Run("GCSError", func(t *testing.T) {
		h := newAgent(t, false, &fakeGCS{err: errors.New("gcs unreachable")})
		if _, err := h.checkLocalRayletLiveness(context.Background()); err == nil {
			t.Error("checkLocalRayletLiveness: want an error, got nil")
		}
	})
}

// The remaining tests lock the HTTP surface to the Python HealthzAgent
// (healthz_agent.py): status codes, exact bodies, and content type.

func TestLocalRayletHealthzOK(t *testing.T) {
	rec := serve(t, newAgent(t, false, &fakeGCS{alive: []bool{true}}), http.MethodGet, "/api/local_raylet_healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != "success" {
		t.Errorf("body = %q, want %q", got, "success")
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/text; charset=utf-8" {
		t.Errorf("content-type = %q, want %q", ct, "application/text; charset=utf-8")
	}
}

func TestLocalRayletHealthzRayletDown(t *testing.T) {
	rec := serve(t, newAgent(t, false, &fakeGCS{alive: []bool{false}}), http.MethodGet, "/api/local_raylet_healthz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Body.String(); got != "Local Raylet failed" {
		t.Errorf("body = %q, want %q", got, "Local Raylet failed")
	}
}

func TestLocalRayletHealthzNilNodeID(t *testing.T) {
	h, err := New(agent.Config{NodeID: ids.NilNodeID(), GCS: &fakeGCS{alive: []bool{true}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	rec := serve(t, h, http.MethodGet, "/api/local_raylet_healthz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Body.String(); got != "Local Raylet failed" {
		t.Errorf("body = %q, want %q", got, "Local Raylet failed")
	}
}

func TestLocalRayletHealthzGCSUnreachableIsOK(t *testing.T) {
	// A GCS call failure must not be reported as a raylet failure, matching
	// Python's leniency for UNAVAILABLE/UNKNOWN/DEADLINE_EXCEEDED RpcErrors.
	rec := serve(t, newAgent(t, false, &fakeGCS{err: errors.New("gcs unreachable")}), http.MethodGet, "/api/local_raylet_healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != "success" {
		t.Errorf("body = %q, want %q", got, "success")
	}
}

func TestHealthzNonHeadOK(t *testing.T) {
	rec := serve(t, newAgent(t, false, &fakeGCS{alive: []bool{true}}), http.MethodGet, "/api/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got, want := rec.Body.String(), "raylet: success\ngcs: success (no local gcs)"; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}

func TestHealthzHeadOK(t *testing.T) {
	rec := serve(t, newAgent(t, true, &fakeGCS{alive: []bool{true}}), http.MethodGet, "/api/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got, want := rec.Body.String(), "raylet: success\ngcs: success"; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}

func TestHealthzRayletDown(t *testing.T) {
	rec := serve(t, newAgent(t, false, &fakeGCS{alive: []bool{false}}), http.MethodGet, "/api/healthz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got, want := rec.Body.String(), "raylet: Local Raylet failed\ngcs: success (no local gcs)"; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}

func TestHealthzNonHeadGCSUnreachableIsOK(t *testing.T) {
	// On a non-head node the GCS check is skipped; a GCS call failure from the
	// raylet check is tolerated, so the endpoint stays 200 like Python.
	rec := serve(t, newAgent(t, false, &fakeGCS{err: errors.New("gcs unreachable")}), http.MethodGet, "/api/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got, want := rec.Body.String(), "raylet: success\ngcs: success (no local gcs)"; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}

func TestHealthzHeadGCSUnreachable(t *testing.T) {
	// On the head the GCS check fails when GCS cannot be reached, so the
	// endpoint reports 503 while the raylet line stays lenient, like Python.
	rec := serve(t, newAgent(t, true, &fakeGCS{err: errors.New("gcs unreachable")}), http.MethodGet, "/api/healthz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got, want := rec.Body.String(), "raylet: success\ngcs: gcs unreachable"; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}

func TestHealthzMethodNotAllowed(t *testing.T) {
	for _, path := range []string{"/api/healthz", "/api/local_raylet_healthz"} {
		rec := serve(t, newAgent(t, false, &fakeGCS{alive: []bool{true}}), http.MethodPost, path)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("POST %s status = %d, want 405", path, rec.Code)
		}
		if got, want := rec.Body.String(), "405: Method Not Allowed"; got != want {
			t.Errorf("POST %s body = %q, want %q", path, got, want)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
			t.Errorf("POST %s content-type = %q, want %q", path, ct, "text/plain; charset=utf-8")
		}
		if allow := rec.Header().Get("Allow"); allow != "GET,HEAD,OPTIONS" {
			t.Errorf("POST %s allow = %q, want %q", path, allow, "GET,HEAD,OPTIONS")
		}
	}
}

// TestHealthzCheckConcurrency locks the concurrency guarantee that motivated
// the goroutine change: on the head node the two health checks must overlap a
// slow GCS call rather than run serially. The fake GCS sleeps delay on every
// call; if the checks were serial the request would take ~2*delay, if parallel
// only ~delay.
func TestHealthzCheckConcurrency(t *testing.T) {
	const delay = 50 * time.Millisecond
	f := &fakeGCS{alive: []bool{true}, delay: delay}
	rec := serve(t, newAgent(t, true, f), http.MethodGet, "/api/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if f.maxConc < 2 {
		t.Errorf("max concurrent CheckAlive calls = %d, want >= 2 (checks must overlap)", f.maxConc)
	}
}
