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
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusCreated, JobSubmitResponse{JobID: "j1", SubmissionID: "s1"})
	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"job_id":"j1","submission_id":"s1"}`, rec.Body.String())
}

func TestWritePlainText(t *testing.T) {
	rec := httptest.NewRecorder()
	writePlainText(rec, http.StatusBadRequest, "bad request")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.Equal(t, "11", rec.Header().Get("Content-Length"))
	assert.Equal(t, "bad request", rec.Body.String())
}

// newTestJobAgent builds an agent backed by the in-memory GCS client and a
// temporary log directory, so the routes can be exercised without a real GCS
// or a real Ray runtime.
func newTestJobAgent(t *testing.T) *JobAgent {
	t.Helper()
	fake := newFakeGCSClient()
	ja, err := New(agent.Config{GCS: fake, LogDir: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, ja.Start(t.Context()))
	// Inject the manager directly instead of going through NewJobManager: that
	// constructor caches the process-wide JOBS event logger, which would defeat
	// TestNewJobEventLoggerSinkFailure's precondition that its failing path runs
	// before any successful logger is cached.
	recoverDone := make(chan struct{})
	close(recoverDone)
	ja.jobManager = &JobManager{
		ctx:           t.Context(),
		gcsClient:     fake,
		logsDir:       ja.cfg.LogDir,
		jobInfoClient: NewJobInfoStorageClient(fake),
		logClient:     NewJobLogStorageClient(ja.cfg.LogDir),
		recoverDone:   recoverDone,
	}
	return ja
}

// newJobMux registers the job module on a plain ServeMux. The production agent
// wraps the mux in T6's aiohttp-compatible router for its 404/405 text bodies;
// the raw mux is enough here to prove the module's own route registrations.
func newJobMux(t *testing.T) *http.ServeMux {
	t.Helper()
	mux := http.NewServeMux()
	require.NoError(t, newTestJobAgent(t).RegisterHTTP(mux))
	return mux
}

func serveJobRequest(mux *http.ServeMux, method, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// TestRegisterHTTPRegistersAllJobRoutes verifies every REST route the Python
// JobAgent exposes is registered with the same method and path, including the
// trailing-slash submit endpoint and the websocket log tail. The routes are
// method-scoped so aiohttp's 405 semantics apply to method mismatches.
func TestRegisterHTTPRegistersAllJobRoutes(t *testing.T) {
	mux := newJobMux(t)

	routes := []struct {
		method  string
		target  string
		pattern string
	}{
		{
			method:  http.MethodPost,
			target:  "/api/job_agent/jobs/",
			pattern: "POST /api/job_agent/jobs/{$}",
		},
		{
			method:  http.MethodPost,
			target:  "/api/job_agent/jobs/job-1/stop",
			pattern: "POST /api/job_agent/jobs/{job_or_submission_id}/stop",
		},
		{
			method:  http.MethodDelete,
			target:  "/api/job_agent/jobs/job-1",
			pattern: "DELETE /api/job_agent/jobs/{job_or_submission_id}",
		},
		{
			method:  http.MethodGet,
			target:  "/api/job_agent/jobs/job-1/logs",
			pattern: "GET /api/job_agent/jobs/{job_or_submission_id}/logs",
		},
		{
			method:  http.MethodGet,
			target:  "/api/job_agent/jobs/job-1/logs/tail",
			pattern: "GET /api/job_agent/jobs/{job_or_submission_id}/logs/tail",
		},
	}
	for _, tc := range routes {
		t.Run(tc.method+" "+tc.target, func(t *testing.T) {
			h, pattern := mux.Handler(httptest.NewRequest(tc.method, tc.target, nil))
			require.NotNil(t, h)
			assert.Equal(t, tc.pattern, pattern)
		})
	}
}

// TestJobRoutesReachHandlers verifies the registered routes dispatch to the
// module handlers: an unknown job reaches the shared lookup and produces the
// Python agent's plain-text 404, and an unparseable submit body produces a 400
// instead of being swallowed by the router.
func TestJobRoutesReachHandlers(t *testing.T) {
	mux := newJobMux(t)

	t.Run("submit with an unparseable body is a 400", func(t *testing.T) {
		rec := serveJobRequest(mux, http.MethodPost, "/api/job_agent/jobs/", "{")
		require.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "failed to parse request body")
		assert.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))
	})

	t.Run("submit with fractional entrypoint_memory is a 400", func(t *testing.T) {
		rec := serveJobRequest(mux, http.MethodPost, "/api/job_agent/jobs/", `{"entrypoint":"echo","entrypoint_memory":1073741824.5}`)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "entrypoint_memory must be an integer, got float", rec.Body.String())
	})

	for _, tc := range []struct{ method, target string }{
		{http.MethodPost, "/api/job_agent/jobs/does-not-exist/stop"},
		{http.MethodDelete, "/api/job_agent/jobs/does-not-exist"},
		{http.MethodGet, "/api/job_agent/jobs/does-not-exist/logs"},
		// The tail route resolves the job before accepting the upgrade, so a
		// missing job is a plain-text 404 rather than a WebSocket handshake.
		{http.MethodGet, "/api/job_agent/jobs/does-not-exist/logs/tail"},
	} {
		t.Run(tc.method+" "+tc.target, func(t *testing.T) {
			rec := serveJobRequest(mux, tc.method, tc.target, "")
			require.Equal(t, http.StatusNotFound, rec.Code)
			assert.Equal(t, "Job does-not-exist does not exist", rec.Body.String())
		})
	}
}

// TestJobRoutesAreMethodScoped verifies a method mismatch on a registered path
// is a 405 with an Allow header, not a 404: the routes are registered with
// method-scoped patterns so the agent router can answer aiohttp's 405 envelope.
func TestJobRoutesAreMethodScoped(t *testing.T) {
	mux := newJobMux(t)

	rec := serveJobRequest(mux, http.MethodPut, "/api/job_agent/jobs/job-1/logs", "")
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Contains(t, rec.Header().Get("Allow"), http.MethodGet)

	rec = serveJobRequest(mux, http.MethodPost, "/api/job_agent/jobs/job-1", "")
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Contains(t, rec.Header().Get("Allow"), http.MethodDelete)
}

// startTailJobServer serves the tail route behind a real HTTP server and
// reports when the websocket handler returns, so a test can observe the handler
// exiting.
func startTailJobServer(t *testing.T, ja *JobAgent, submissionID string) (string, <-chan struct{}) {
	t.Helper()
	mux := http.NewServeMux()
	done := make(chan struct{})
	mux.HandleFunc("GET /api/job_agent/jobs/{job_or_submission_id}/logs/tail", func(w http.ResponseWriter, r *http.Request) {
		ja.handleTailJobLogs(w, r)
		close(done)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/job_agent/jobs/" + submissionID + "/logs/tail", done
}

// TestHandleTailJobLogsAnswersControlFrames pins the websocket drain: the
// handler must read the connection so coder/websocket answers pings and
// observes the peer's close instead of leaking the hijacked connection until
// the first ping timeout.
func TestHandleTailJobLogsAnswersControlFrames(t *testing.T) {
	ja := newTestJobAgent(t)
	_, err := ja.jobManager.jobInfoClient.PutInfo(t.Context(), "sub1", &JobInfo{Entrypoint: "echo", Status: JobStatusRunning}, false)
	require.NoError(t, err)

	t.Run("ping is answered while the tail is idle", func(t *testing.T) {
		wsURL, _ := startTailJobServer(t, ja, "sub1")
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, wsURL, nil)
		require.NoError(t, err)
		defer conn.CloseNow()
		// CloseRead actively reads pongs; without the server-side drain the pong
		// never arrives and Ping times out.
		pingCtx, pingCancel := context.WithTimeout(conn.CloseRead(ctx), 3*time.Second)
		defer pingCancel()
		require.NoError(t, conn.Ping(pingCtx))
	})

	t.Run("peer close ends the handler", func(t *testing.T) {
		wsURL, done := startTailJobServer(t, ja, "sub1")
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, wsURL, nil)
		require.NoError(t, err)
		defer conn.CloseNow()
		require.NoError(t, conn.Close(websocket.StatusNormalClosure, ""))
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("tail handler did not return after the peer closed the connection")
		}
	})
}
