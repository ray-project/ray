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

package job

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ray-project/ray/go/internal/dashboard/auth"
)

// TestDoJSONCarriesAuthToken verifies the agent client attaches the bearer
// token when token authentication is enabled, matching the Python client's
// _get_headers (job_head.py:127-129). Without it every head -> agent job call
// is rejected with 401 under RAY_AUTH_MODE=token.
func TestDoJSONCarriesAuthToken(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret")

	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get(auth.AuthorizationHeaderName)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"job_id":"job1","submission_id":"sub1"}`))
	}))
	defer srv.Close()

	client := NewJobAgentSubmissionClient(srv.URL, nil)
	resp, err := client.SubmitJobInternal(context.Background(), &JobSubmitRequest{Entrypoint: "echo hi"})
	if err != nil {
		t.Fatalf("SubmitJobInternal: %v", err)
	}
	if resp.SubmissionID != "sub1" {
		t.Errorf("submission id = %q, want sub1", resp.SubmissionID)
	}
	if want := auth.BearerPrefix + "secret"; gotAuth != want {
		t.Errorf("Authorization header = %q, want %q", gotAuth, want)
	}
}

// TestDoJSONOmitsAuthHeaderWhenDisabled verifies a disabled auth mode sends no
// Authorization header, so an unauthenticated cluster does not see a stale
// credential.
func TestDoJSONOmitsAuthHeaderWhenDisabled(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "disabled")
	t.Setenv("RAY_AUTH_TOKEN", "secret")

	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get(auth.AuthorizationHeaderName)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"job_id":"job1","submission_id":"sub1"}`))
	}))
	defer srv.Close()

	client := NewJobAgentSubmissionClient(srv.URL, nil)
	if _, err := client.SubmitJobInternal(context.Background(), &JobSubmitRequest{Entrypoint: "echo hi"}); err != nil {
		t.Fatalf("SubmitJobInternal: %v", err)
	}
	if gotAuth != "" {
		t.Errorf("Authorization header = %q, want none when token auth is disabled", gotAuth)
	}
}

// TestTailJobLogsCarriesAuthToken verifies the websocket log tail dial carries
// the bearer token, matching the Python tail_job_logs ws_connect(headers=...).
func TestTailJobLogsCarriesAuthToken(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret")

	gotHeader := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader <- r.Header.Get(auth.AuthorizationHeaderName)
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		_ = ws.Write(context.Background(), websocket.MessageText, []byte("line one\n"))
		_ = ws.Close(websocket.StatusNormalClosure, "")
	}))
	defer srv.Close()

	client := NewJobAgentSubmissionClient(srv.URL, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lines, closeFn, err := client.TailJobLogs(ctx, "sub1")
	if err != nil {
		t.Fatalf("TailJobLogs: %v", err)
	}
	defer closeFn()

	select {
	case line := <-lines:
		if line != "line one\n" {
			t.Errorf("tailed line = %q, want %q", line, "line one\n")
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for a tailed log line")
	}
	if got, want := <-gotHeader, auth.BearerPrefix+"secret"; got != want {
		t.Errorf("websocket Authorization header = %q, want %q", got, want)
	}
}
