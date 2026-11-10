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

package agent_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/internal/dashboard/agent/job"
)

// TestRouterServesJobModule verifies the job module's routes are reachable
// through the real aiohttp-compatible router: a submit with an unparseable body
// reaches the handler (a 404 would mean the route never matched), and method
// mismatches on the job paths produce aiohttp's 405 envelope with the Allow
// header, which is only possible because every job route is method-scoped.
func TestRouterServesJobModule(t *testing.T) {
	mod, err := job.New(agent.Config{})
	if err != nil {
		t.Fatalf("job.New: %v", err)
	}
	// The routes are registered on the mux before any handler runs, so the test
	// needs no GCS client or job manager.
	h, err := agent.NewRouterForTest([]agent.Module{mod})
	if err != nil {
		t.Fatalf("NewRouterForTest: %v", err)
	}

	rec := doJobRequest(h, http.MethodPost, "http://agent/api/job_agent/jobs/", "{")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("POST /api/job_agent/jobs/ = %d, want the handler's 400 (body %q)",
			rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !strings.Contains(body, "failed to parse request body") {
		t.Errorf("body = %q, want the handler's parse error", body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q, want the handler's plain text", ct)
	}

	// A method mismatch on a job path is aiohttp's 405 envelope, not 404.
	rec = doJobRequest(h, http.MethodPut, "http://agent/api/job_agent/jobs/job-1/logs", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("PUT /api/job_agent/jobs/job-1/logs = %d, want 405", rec.Code)
	}
	if got := rec.Body.String(); got != "405: Method Not Allowed" {
		t.Errorf("body = %q, want the aiohttp 405 text", got)
	}
	if got := rec.Header().Get("Allow"); !strings.Contains(got, "GET") || !strings.Contains(got, "OPTIONS") {
		t.Errorf("Allow = %q, want it to list GET and OPTIONS", got)
	}

	// A path no job route serves keeps aiohttp's 404 envelope.
	rec = doJobRequest(h, http.MethodGet, "http://agent/api/job_agent/unknown", "")
	if rec.Code != http.StatusNotFound || rec.Body.String() != "404: Not Found" {
		t.Errorf("GET /api/job_agent/unknown = %d %q, want the aiohttp 404 envelope",
			rec.Code, rec.Body.String())
	}
}

func doJobRequest(h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}
