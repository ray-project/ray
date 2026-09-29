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

// Package state implements the StateHead dashboard module that exposes the
// /api/v0/* state APIs (actors, jobs, nodes, placement groups, workers, tasks,
// objects, runtime envs), aligned with the Python StateHead.
package state

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/job"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/log"
)

// defaultConcurrency bounds concurrent state API requests, aligned with
// RAY_STATE_SERVER_MAX_HTTP_REQUEST (100).
const defaultConcurrency = 100

// StateHead implements the /api/v0/* state APIs.
type StateHead struct {
	cfg    *head.HeadConfig
	client *head.GCSClient
	api    *head.StateAPIManager
	sem    chan struct{}
	// logs backs the /api/v0/logs routes. The Python dashboard mounts the logs
	// handlers on the StateHead module (state_head.py list_logs/get_logs), so the
	// Go StateHead holds a LogHead and registers the same routes so that
	// --modules-to-load=StateHead still serves /api/v0/logs.
	logs *log.LogHead
}

// New creates a StateHead backed by the given GCS client. The jobs list API is
// wired to the JobHead aggregation (driver + submission JobDetails), matching
// get_job_info in python/ray/util/state/state_manager.py.
func New(cfg *head.HeadConfig, client *head.GCSClient) *StateHead {
	api := head.NewStateAPIManager(client)
	info := job.NewJobInfoStorageClient(client)
	api.ListJobsFn = func(ctx context.Context) ([]map[string]interface{}, error) {
		submissionJobs, err := info.GetAllJobsOrdered(ctx)
		if err != nil {
			return nil, err
		}
		details, err := job.BuildListJobsDetails(ctx, client, submissionJobs)
		if err != nil {
			return nil, err
		}
		out := make([]map[string]interface{}, 0, len(details))
		for _, d := range details {
			out = append(out, job.JobDetailsToDict(d))
		}
		return out, nil
	}
	return &StateHead{
		cfg:    cfg,
		client: client,
		api:    api,
		sem:    make(chan struct{}, defaultConcurrency),
		logs:   log.New(cfg, client),
	}
}

// Name returns the module name.
func (s *StateHead) Name() string { return "StateHead" }

// Start has no background tasks.
func (s *StateHead) Start(ctx context.Context) error { return nil }

// Healthy reports the module health.
func (s *StateHead) Healthy() bool { return true }

// RegisterHTTP registers the /api/v0/* routes.
func (s *StateHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("GET /api/v0/actors", s.handleList(s.api.ListActors))
	mux.HandleFunc("GET /api/v0/jobs", s.handleList(s.api.ListJobs))
	mux.HandleFunc("GET /api/v0/nodes", s.handleList(s.api.ListNodes))
	mux.HandleFunc("GET /api/v0/placement_groups", s.handleList(s.api.ListPlacementGroups))
	mux.HandleFunc("GET /api/v0/workers", s.handleList(s.api.ListWorkers))
	mux.HandleFunc("GET /api/v0/tasks", s.handleList(s.api.ListTasks))
	mux.HandleFunc("GET /api/v0/tasks/summarize", s.handleSummarize(s.api.SummarizeTasks))
	mux.HandleFunc("GET /api/v0/actors/summarize", s.handleSummarize(s.api.SummarizeActors))
	mux.HandleFunc("GET /api/v0/objects/summarize", s.handleSummarize(s.api.SummarizeObjects))
	mux.HandleFunc("GET /api/v0/tasks/timeline", s.handleTaskTimeline)
	mux.HandleFunc("GET /api/v0/objects", s.handleList(s.api.ListObjects))
	mux.HandleFunc("GET /api/v0/runtime_envs", s.handleList(s.api.ListRuntimeEnvs))
	mux.HandleFunc("GET /api/v0/delay/{delay_s}", s.handleDelay)
	// The logs routes live on the StateHead in the Python dashboard; register
	// them (idempotently) through the held LogHead.
	s.logs.RegisterHTTP(mux)
	return nil
}

// handleDelay serves GET /api/v0/delay/{delay_s}, a testing helper that sleeps
// for the given number of seconds and returns an empty result, aligned with the
// delayed_response handler in state_head.py.
func (s *StateHead) handleDelay(w http.ResponseWriter, r *http.Request) {
	delayS := 10
	if v := r.PathValue("delay_s"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			delayS = n
		}
	}
	time.Sleep(time.Duration(delayS) * time.Second)
	// partial_failure_warning is always present (null), aligned with the
	// delayed_response handler in state_head.py which passes it explicitly.
	head.RESTResponse(w, http.StatusOK, "", map[string]interface{}{
		"result":                  map[string]interface{}{},
		"partial_failure_warning": nil,
	})
}

// handleTaskTimeline serves GET /api/v0/tasks/timeline, returning the raw
// chrome/perfetto tracing JSON for the job's tasks, aligned with the Python
// tasks_timeline handler (a plain JSON response, not a rest_response wrapper).
func (s *StateHead) handleTaskTimeline(w http.ResponseWriter, r *http.Request) {
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	default:
		http.Error(w, "Too many concurrent requests", http.StatusTooManyRequests)
		return
	}
	jobID := r.URL.Query().Get("job_id")
	result, err := s.api.GenerateTaskTimeline(r.Context(), jobID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if r.URL.Query().Get("download") == "1" {
		now := time.Now().Format("2006-01-02_15-04-05")
		// Python renders the missing job_id as the string "None" (f-string
		// interpolation of None), so an empty query param must match.
		if jobID == "" {
			jobID = "None"
		}
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="timeline-%s-%s.json"`, jobID, now))
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(result))
}

// handleSummarize is the shared summary-API handler, aligned with
// handle_summary_api in python/ray/dashboard/state_api_utils.py: the response
// carries {"result": <SummaryApiResponse dict>} with snake_case keys
// (do_reply uses convert_google_style=False).
func (s *StateHead) handleSummarize(fn func(context.Context, *head.SummaryApiOptions) (map[string]interface{}, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case s.sem <- struct{}{}:
			defer func() { <-s.sem }()
		default:
			http.Error(w, "Too many concurrent requests", http.StatusTooManyRequests)
			return
		}
		opt, err := head.ParseSummaryApiOptions(r)
		if err != nil {
			head.RESTResponse(w, http.StatusBadRequest, err.Error(), map[string]interface{}{"result": nil})
			return
		}
		resp, err := fn(r.Context(), opt)
		if err != nil {
			var verr *head.ValueError
			if errors.As(err, &verr) {
				head.RESTResponse(w, http.StatusBadRequest, err.Error(), map[string]interface{}{"result": nil})
				return
			}
			head.RESTResponse(w, http.StatusInternalServerError, err.Error(), map[string]interface{}{"result": nil})
			return
		}
		head.RESTResponse(w, http.StatusOK, "", map[string]interface{}{"result": resp})
	}
}

// handleList is the shared list-API handler with concurrency limiting and the
// rest_response wrapper. The data carries {"result": ListApiResponse},
// matching the Python state API response shape.
func (s *StateHead) handleList(fn func(context.Context, *head.ListApiOptions) (*head.ListApiResponse, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case s.sem <- struct{}{}:
			defer func() { <-s.sem }()
		default:
			http.Error(w, "Too many concurrent requests", http.StatusTooManyRequests)
			return
		}
		opt, err := head.ParseListApiOptions(r)
		if err != nil {
			head.RESTResponse(w, http.StatusBadRequest, err.Error(), map[string]interface{}{"result": nil})
			return
		}
		resp, err := fn(r.Context(), opt)
		if err != nil {
			// ValueError surfaces as 400 and data source failures as 500,
			// aligned with the Python handle_list_api (state_api_utils.py):
			// both carry data: {"result": null}.
			var verr *head.ValueError
			if errors.As(err, &verr) {
				head.RESTResponse(w, http.StatusBadRequest, err.Error(), map[string]interface{}{"result": nil})
				return
			}
			head.RESTResponse(w, http.StatusInternalServerError, err.Error(), map[string]interface{}{"result": nil})
			return
		}
		head.RESTResponse(w, http.StatusOK, "", map[string]interface{}{"result": resp})
	}
}
