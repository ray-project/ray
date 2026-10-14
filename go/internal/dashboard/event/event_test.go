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

package event

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/gcs"
)

func TestNewValidations(t *testing.T) {
	t.Run("missing log dir", func(t *testing.T) {
		if _, err := New(agent.Config{GCS: &fakeGCS{}}); err == nil {
			t.Fatal("New with empty LogDir returned no error")
		}
	})
	t.Run("missing gcs client", func(t *testing.T) {
		if _, err := New(agent.Config{LogDir: t.TempDir()}); err == nil {
			t.Fatal("New with nil GCS returned no error")
		}
	})
}

// TestStartCreatesEventsDir verifies Start creates the events directory under
// the log directory.
func TestStartCreatesEventsDir(t *testing.T) {
	dir := t.TempDir()
	e, err := New(agent.Config{LogDir: dir, GCS: &fakeGCS{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "events")); err != nil {
		t.Fatalf("events dir not created: %v", err)
	}
}

func TestModuleContract(t *testing.T) {
	e := newEventAgent(t, &fakeGCS{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := e.Start(ctx); err != nil {
		t.Errorf("Start: %v", err)
	}
	if err := e.RegisterGRPC(nil); err != nil {
		t.Errorf("RegisterGRPC: %v", err)
	}
}

// serveInternalStates routes one request through the registered mux and
// returns the recorder.
func serveInternalStates(t *testing.T, e *EventAgent, method string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	if err := e.RegisterHTTP(mux); err != nil {
		t.Fatalf("RegisterHTTP: %v", err)
	}
	req := httptest.NewRequest(method, "/api/event_internal_states", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// TestInternalStatesEndpoint204 verifies the endpoint replies 204 No Content
// before anything is reported.
func TestInternalStatesEndpoint204(t *testing.T) {
	rec := serveInternalStates(t, newEventAgent(t, &fakeGCS{}), http.MethodGet)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
}

// TestInternalStatesEndpoint200 verifies the endpoint replies 200 with the
// documented JSON fields once a report succeeded.
func TestInternalStatesEndpoint200(t *testing.T) {
	rec := &headRecorder{}
	srv := httptest.NewServer(rec.handler(0, 0))
	defer srv.Close()
	f := &fakeGCS{dashboardAddr: srv.URL}
	e := newEventAgent(t, f)

	// Report one event through the real report loop, then query the endpoint.
	cancel, done := startReportLoop(t, e)
	e.queue <- []string{`{"a":1}`}
	waitReported(t, rec, 1)

	resp := serveInternalStates(t, e, http.MethodGet)
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.Code)
	}
	if ct := resp.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want application/json", ct)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(resp.Body.Bytes(), &m); err != nil {
		t.Fatalf("response body is not valid JSON: %v", err)
	}
	for _, key := range []string{"total_events_reported", "Total_report_request", "queue_size", "total_uptime"} {
		if _, ok := m[key]; !ok {
			t.Errorf("response is missing field %q: %v", key, m)
		}
	}

	mustCancelReportLoop(t, cancel, done)
}

func TestInternalStatesMethodNotAllowed(t *testing.T) {
	rec := serveInternalStates(t, newEventAgent(t, &fakeGCS{}), http.MethodPost)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", rec.Code)
	}
}

// compile-time check that EventAgent implements the module contract; the
// concrete assertion lives at the package level in event.go.
var (
	_ agent.Module = (*EventAgent)(nil)
	_ gcs.Client   = (*fakeGCS)(nil)
)
