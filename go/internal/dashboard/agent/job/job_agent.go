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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

	"github.com/coder/websocket"
	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
	"google.golang.org/grpc"
)

// JobAgent implements the dashboard-agent job module, mirroring the Python
// JobAgent (python/ray/dashboard/modules/job/job_agent.py). It serves the job
// REST API and the websocket log tail on top of the JobManager.
type JobAgent struct {
	cfg       agent.Config
	gcsClient gcs.Client
	ctx       context.Context

	mu         sync.Mutex
	jobManager *JobManager
}

var _ agent.Module = (*JobAgent)(nil)

// New builds a JobAgent from the agent config. The job manager is created
// lazily on the first request so construction neither contacts GCS nor
// initializes the runtime.
func New(cfg agent.Config) (*JobAgent, error) {
	return &JobAgent{cfg: cfg, gcsClient: cfg.GCS}, nil
}

// Start records the agent context used to build the job manager. The Python
// JobAgent has no startup step, so this is a no-op beyond the context.
func (a *JobAgent) Start(ctx context.Context) error {
	a.ctx = ctx
	return nil
}

// RegisterGRPC has no gRPC surface: the Python JobAgent only serves HTTP.
func (a *JobAgent) RegisterGRPC(_ *grpc.Server) error {
	return nil
}

// RegisterHTTP registers the five job routes of the Python agent. Every route
// is method-scoped so the agent router can answer aiohttp's 405 envelope for a
// method mismatch. The submit route ends in {$} because the Python path is
// "/api/job_agent/jobs/" and must not match nested paths.
func (a *JobAgent) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("POST /api/job_agent/jobs/{$}", a.handleSubmitJob)
	mux.HandleFunc("POST /api/job_agent/jobs/{job_or_submission_id}/stop", a.handleStopJob)
	mux.HandleFunc("DELETE /api/job_agent/jobs/{job_or_submission_id}", a.handleDeleteJob)
	mux.HandleFunc("GET /api/job_agent/jobs/{job_or_submission_id}/logs/tail", a.handleTailJobLogs)
	mux.HandleFunc("GET /api/job_agent/jobs/{job_or_submission_id}/logs", a.handleGetJobLogs)
	return nil
}

// getJobManager returns the module's job manager, building it on first use.
func (a *JobAgent) getJobManager() *JobManager {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.jobManager == nil {
		a.jobManager = NewJobManager(a.ctx, a.gcsClient, a.cfg.LogDir, a.cfg.NodeIP)
	}
	return a.jobManager
}

// handleSubmitJob handles POST /api/job_agent/jobs/.
func (a *JobAgent) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	req, err := parseAndValidateRequest(r)
	if err != nil {
		writePlainText(w, http.StatusBadRequest, err.Error())
		return
	}
	submissionID, err := a.getJobManager().submitJob(r.Context(), req)
	if err != nil {
		if _, ok := err.(*valueError); ok {
			writePlainText(w, http.StatusBadRequest, err.Error())
			return
		}
		writePlainText(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, JobSubmitResponse{JobID: submissionID, SubmissionID: submissionID})
}

// resolveJob looks up a job by job id or submission id and writes the
// 500/404 error responses on failure, returning nil when the caller should
// abort. The caller validates the job type and writes the corresponding 400
// response.
func (a *JobAgent) resolveJob(w http.ResponseWriter, r *http.Request) *JobDetails {
	jobOrSubmissionID := r.PathValue("job_or_submission_id")
	job, err := findJobByIDs(r.Context(), a.gcsClient, a.getJobManager().getJobInfoClient(), jobOrSubmissionID)
	if err != nil {
		writePlainText(w, http.StatusInternalServerError, err.Error())
		return nil
	}
	if job == nil {
		writePlainText(w, http.StatusNotFound, fmt.Sprintf("Job %s does not exist", jobOrSubmissionID))
		return nil
	}
	return job
}

// handleStopJob handles POST /api/job_agent/jobs/{job_or_submission_id}/stop.
func (a *JobAgent) handleStopJob(w http.ResponseWriter, r *http.Request) {
	job := a.resolveJob(w, r)
	if job == nil {
		return
	}
	if job.Type != JobTypeSubmission {
		writePlainText(w, http.StatusBadRequest, "Can only stop submission type jobs")
		return
	}
	stopped, err := a.getJobManager().stopJob(r.Context(), *job.SubmissionID)
	if err != nil {
		writePlainText(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, JobStopResponse{Stopped: stopped})
}

// handleDeleteJob handles DELETE /api/job_agent/jobs/{job_or_submission_id}.
func (a *JobAgent) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	job := a.resolveJob(w, r)
	if job == nil {
		return
	}
	if job.Type != JobTypeSubmission {
		writePlainText(w, http.StatusBadRequest, "Can only delete submission type jobs")
		return
	}
	deleted, err := a.getJobManager().deleteJob(r.Context(), *job.SubmissionID)
	if err != nil {
		writePlainText(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, JobDeleteResponse{Deleted: deleted})
}

// handleGetJobLogs handles GET /api/job_agent/jobs/{job_or_submission_id}/logs.
func (a *JobAgent) handleGetJobLogs(w http.ResponseWriter, r *http.Request) {
	job := a.resolveJob(w, r)
	if job == nil {
		return
	}
	if job.Type != JobTypeSubmission {
		writePlainText(w, http.StatusBadRequest, "Can only get logs of submission type jobs")
		return
	}
	logs := a.getJobManager().getJobLogs(*job.SubmissionID)
	writeJSON(w, http.StatusOK, JobLogsResponse{Logs: logs})
}

// handleTailJobLogs handles
// GET /api/job_agent/jobs/{job_or_submission_id}/logs/tail as a websocket that
// streams new driver log lines, mirroring the Python agent's tail_job_logs.
func (a *JobAgent) handleTailJobLogs(w http.ResponseWriter, r *http.Request) {
	job := a.resolveJob(w, r)
	if job == nil {
		return
	}
	if job.Type != JobTypeSubmission {
		writePlainText(w, http.StatusBadRequest, "Can only get logs of submission type jobs")
		return
	}

	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close(websocket.StatusNormalClosure, "")

	// coder/websocket processes control frames (ping, pong and close) only while
	// a read is in flight, so a write-only handler stops answering pings and
	// leaks the hijacked connection when the peer goes away. Drain the
	// connection concurrently with the write loop and stop the tail once the
	// connection is closed.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	go func() {
		defer cancel()
		for {
			if _, _, err := ws.Read(ctx); err != nil {
				return
			}
		}
	}()

	lines, err := a.getJobManager().tailJobLogs(ctx, *job.SubmissionID)
	if err != nil {
		log.Log.Error(err, "failed to tail job logs", "submission_id", *job.SubmissionID)
		return
	}
	for line := range lines {
		if err := ws.Write(ctx, websocket.MessageText, []byte(line)); err != nil {
			return
		}
	}
}

// writeJSON writes v as a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writePlainText writes an exact plain-text body with an explicit
// Content-Length, matching the Python agent's web.json_response/text responses.
func writePlainText(w http.ResponseWriter, status int, text string) {
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("Content-Length", strconv.Itoa(len(text)))
	w.WriteHeader(status)
	_, _ = io.WriteString(w, text)
}
