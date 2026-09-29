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

// Package train implements the TrainHead dashboard module: cross-language
// calls into the Python Train state actors (V2 and V1) for the Train runs
// REST API. Aligned with the Python TrainHead in
// python/ray/dashboard/modules/train/train_head.py.
package train

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"sync"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/job"
	rayerrors "github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/ray-project/ray/go/proto"
)

// Cross-language actor identity constants for the TrainStateActorWrapper, the
// dashboard's proxy into the real Train state actors, centralized here so the
// name/namespace/module/className quadruples live in one place. Aligned with
// python/ray/go/dashboard/cross_language/train_wrapper.py.
const (
	// Train V2
	trainV2StateActorName      = "go_dashboard_train_v2_wrapper"
	trainV2StateActorNamespace = "_train_state_actor"
	trainV2StateActorModule    = "ray.go.dashboard.cross_language.train_wrapper"
	trainV2StateActorClass     = "TrainStateActorWrapper"
	// Train V1
	trainV1StateActorName      = "go_dashboard_train_v1_wrapper"
	trainV1StateActorNamespace = "_train_state_actor"
	trainV1StateActorModule    = "ray.go.dashboard.cross_language.train_wrapper"
	trainV1StateActorClass     = "TrainStateActorWrapper"
)

// Resource constants for the TrainStateActorWrappers, aligned with the
// original Train state actors. Both V1 and V2 live on the head node with zero
// CPU quota: V2 (get_state_actor in
// python/ray/train/v2/_internal/state/state_actor.py) uses num_cpus=0,
// resources={"node:__internal_head__": 0.001}, max_restarts=-1 and
// max_task_retries=-1; V1 (get_or_create_state_actor in
// python/ray/train/_internal/state/state_actor.py) uses num_cpus=0 (from the
// @ray.remote(num_cpus=0) decorator) plus resources={
// "node:__internal_head__": 0.001} and leaves max_restarts/max_task_retries at
// the Python default (0). The wrappers inherit the same footprint instead of
// the Go default of CPU=1.
const (
	// trainV2MaxRestarts is the V2 original actor's max_restarts=-1.
	trainV2MaxRestarts = -1
	// trainV2MaxTaskRetries is the V2 original actor's max_task_retries=-1.
	trainV2MaxTaskRetries = -1
)

// trainStateActorResources is the shared head-node, zero-CPU resource map
// ({HEAD_NODE_RESOURCE_NAME: 0.001} plus num_cpus=0) used by both V1 and V2. A
// var (not const) because Go maps are not constant expressions.
var trainStateActorResources = map[string]float64{
	"node:__internal_head__": 0.001,
	api.ResourceCPU:          0,
}

// actorStatusDead is the ActorStatus.DEAD value (python/ray/train/v2/_internal/
// state/schema.py). The GCS actor state enum string for a dead actor is "DEAD".
const actorStatusDead = "DEAD"

// TrainHead implements the Train runs API.
type TrainHead struct {
	cfg    *head.HeadConfig
	client *head.GCSClient
	info   *job.JobInfoStorageClient

	muV2         sync.Mutex
	trainV2Actor stateActor
	muV1         sync.Mutex
	trainV1Actor stateActor

	// physMu guards physicalStats, the per-node physical stats cache (node id ->
	// reporter JSON) used to enrich worker gpus/processStats, aligned with the
	// Python DataSource.node_physical_stats.
	physMu        sync.RWMutex
	physicalStats map[string]map[string]interface{}
}

// New creates a TrainHead backed by the given GCS client.
func New(cfg *head.HeadConfig, client *head.GCSClient) *TrainHead {
	return &TrainHead{
		cfg:           cfg,
		client:        client,
		info:          job.NewJobInfoStorageClient(client),
		physicalStats: map[string]map[string]interface{}{},
	}
}

// Name returns the module name.
func (h *TrainHead) Name() string { return "TrainHead" }

// Start subscribes to the node resource usage channel to cache the physical
// stats (workers/gpus/processStats) used to enrich the train worker hardware
// metrics, aligned with the Python DataSource.node_physical_stats. The
// subscription is a no-op when no GCS client is available (unit tests).
func (h *TrainHead) Start(ctx context.Context) error {
	if h.client == nil {
		return nil
	}
	usageCh, errCh := head.NewResourceUsageSubscriber(h.client).Updates(ctx)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case u, ok := <-usageCh:
				if !ok {
					return
				}
				var parsed map[string]interface{}
				if err := json.Unmarshal([]byte(u.JSON), &parsed); err != nil {
					log.Log.V(1).Info("failed to parse resource usage json", "node", u.NodeID)
					continue
				}
				h.physMu.Lock()
				h.physicalStats[u.NodeID] = parsed
				h.physMu.Unlock()
			case <-errCh:
				return
			}
		}
	}()
	return nil
}

// Healthy reports the module health.
func (h *TrainHead) Healthy() bool { return true }

// RegisterHTTP registers the train routes.
func (h *TrainHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("GET /api/train/v2/runs/v1", h.handleTrainV2Runs)
	mux.HandleFunc("GET /api/train/v2/runs", h.handleTrainV1Runs)
	return nil
}

// stateActor is the subset of the Python Train state actor surface used by the
// train routes. getTrainV2StateActor/getTrainV1StateActor return
// *api.PythonActorHandle wrapped in pythonStateActor which satisfies it; the
// interface lets tests inject a fake.
type stateActor interface {
	getTrainRuns() (map[string]interface{}, error)
	getTrainRunAttempts() (map[string]interface{}, error)
	getAllTrainRuns() (map[string]interface{}, error)
}

// pythonStateActor adapts *api.PythonActorHandle to stateActor via the
// cross-language ActorTask/Get API.
type pythonStateActor struct {
	handle *api.PythonActorHandle
}

func (a *pythonStateActor) getTrainRuns() (map[string]interface{}, error) {
	ref, err := api.ActorTask[map[string]interface{}](a.handle, "get_train_runs").Remote()
	if err != nil {
		return nil, err
	}
	return api.Get(ref)
}

func (a *pythonStateActor) getTrainRunAttempts() (map[string]interface{}, error) {
	ref, err := api.ActorTask[map[string]interface{}](a.handle, "get_train_run_attempts").Remote()
	if err != nil {
		return nil, err
	}
	return api.Get(ref)
}

func (a *pythonStateActor) getAllTrainRuns() (map[string]interface{}, error) {
	ref, err := api.ActorTask[map[string]interface{}](a.handle, "get_all_train_runs").Remote()
	if err != nil {
		return nil, err
	}
	return api.Get(ref)
}

// getTrainV2StateActorFn returns the Train V2 wrapper actor handle, or an error
// when no train instance is running. It is a package-level variable so tests
// can inject a fake actor.
//
// The wrapper is a non-detached dashboard proxy created on first use
// (Get-first, Create-fallback) and recycled with the dashboard driver. It has
// no constructor arguments: it derives its V2 identity from its own actor name
// and binds the real Train V2 state actor via ray.get_actor in __init__, so
// creation fails when no train instance is running.
var getTrainV2StateActorFn = func() (stateActor, error) {
	handle, err := api.GetPythonActorWithNamespace(
		trainV2StateActorName, trainV2StateActorNamespace,
		trainV2StateActorModule, trainV2StateActorClass)
	if err == nil {
		log.Log.Info("train: got existing v2 wrapper")
		return &pythonStateActor{handle: handle}, nil
	}
	// A not-initialized runtime (e.g. the GCS was not ready when the head
	// started) is surfaced to the caller so it can lazily initialize and retry;
	// any other error means no train instance is running and we degrade.
	if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
		return nil, err
	}
	// No wrapper yet. Only create it when the real Train V2 state actor
	// exists (train_v2_state_actor / _train_state_actor from
	// ray.train.v2._internal.state.state_actor), otherwise the wrapper's
	// __init__ dies asynchronously and the cached dead handle surfaces as
	// a misleading 503. Aligned with TrainHead.get_train_v2_state_actor
	// returning None when no train instance is running.
	if _, err := api.GetPythonActorWithNamespace(
		"train_v2_state_actor", "_train_state_actor",
		"ray.train.v2._internal.state.state_actor", "TrainStateActor"); err != nil {
		if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
			return nil, err
		}
		log.Log.Info("train: no real v2 state actor, degrade", "err", err)
		return nil, nil
	}
	log.Log.Info("train: creating v2 wrapper")
	handle, err = api.RemotePythonActor(trainV2StateActorModule, trainV2StateActorClass).
		WithName(trainV2StateActorName).WithNamespace(trainV2StateActorNamespace).
		WithResources(trainStateActorResources).
		WithMaxRestarts(trainV2MaxRestarts).
		WithMaxTaskRetries(trainV2MaxTaskRetries).
		Create()
	if err != nil {
		return nil, err
	}
	return &pythonStateActor{handle: handle}, nil
}

// getTrainV1StateActorFn returns the Train V1 wrapper actor handle, or an error
// when no train instance is running. It is a package-level variable so tests
// can inject a fake actor. Like the V2 wrapper it has no constructor arguments
// and derives its V1 identity from its own actor name.
var getTrainV1StateActorFn = func() (stateActor, error) {
	handle, err := api.GetPythonActorWithNamespace(
		trainV1StateActorName, trainV1StateActorNamespace,
		trainV1StateActorModule, trainV1StateActorClass)
	if err != nil {
		// A not-initialized runtime (e.g. the GCS was not ready when the head
		// started) is surfaced to the caller so it can lazily initialize and
		// retry; any other error means no train instance is running and we
		// degrade.
		if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
			return nil, err
		}
		// Only create the wrapper when the real Train V1 state actor exists
		// (train_state_actor / _train_state_actor from
		// ray.train._internal.state.state_actor); see getTrainV2StateActorFn.
		if _, err := api.GetPythonActorWithNamespace(
			"train_state_actor", "_train_state_actor",
			"ray.train._internal.state.state_actor", "TrainStateActor"); err != nil {
			if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
				return nil, err
			}
			return nil, nil
		}
		// The V1 wrapper aligns with the V1 original actor's resources
		// (num_cpus=0 + head-node resource); max_restarts/max_task_retries stay
		// at the Python default (0), matching get_or_create_state_actor.
		handle, err = api.RemotePythonActor(trainV1StateActorModule, trainV1StateActorClass).
			WithName(trainV1StateActorName).WithNamespace(trainV1StateActorNamespace).
			WithResources(trainStateActorResources).Create()
		if err != nil {
			return nil, err
		}
	}
	return &pythonStateActor{handle: handle}, nil
}

// getTrainV2StateActor returns the cached Train V2 state actor, aligned with
// TrainHead.get_train_v2_state_actor. It returns nil when no train instance is
// running.
func (h *TrainHead) getTrainV2StateActor() stateActor {
	h.muV2.Lock()
	defer h.muV2.Unlock()
	if h.trainV2Actor != nil {
		return h.trainV2Actor
	}
	actor, err := getTrainV2StateActorFn()
	if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
		// The Go runtime failed to initialize at startup (e.g. the GCS was not
		// ready during `ray start`). Lazily initialize it now, mirroring the
		// Python dashboard's require_initialized which ray.init()s on the first
		// request, so the head recovers the cross-language runtime without a
		// restart. EnsureInitialized is a no-op once initialized and serialized
		// against concurrent calls. Retry once after a successful init; on
		// failure degrade to nil so the handler surfaces "Train state data is
		// not available".
		if initErr := head.EnsureInitialized(h.cfg); initErr != nil {
			log.Log.Info("train: go runtime not initialized, degrade", "err", initErr)
			return nil
		}
		actor, err = getTrainV2StateActorFn()
	}
	if err != nil {
		log.Log.Info("no train v2 state actor available", "err", err)
		return nil
	}
	h.trainV2Actor = actor
	return actor
}

// getTrainV1StateActor returns the cached Train V1 state actor, aligned with
// TrainHead.get_train_stats_actor. It returns nil when no train instance is
// running.
func (h *TrainHead) getTrainV1StateActor() stateActor {
	h.muV1.Lock()
	defer h.muV1.Unlock()
	if h.trainV1Actor != nil {
		return h.trainV1Actor
	}
	actor, err := getTrainV1StateActorFn()
	if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
		// The Go runtime failed to initialize at startup (e.g. the GCS was not
		// ready during `ray start`). Lazily initialize it now, mirroring the
		// Python dashboard's require_initialized which ray.init()s on the first
		// request, so the head recovers the cross-language runtime without a
		// restart. EnsureInitialized is a no-op once initialized and serialized
		// against concurrent calls. Retry once after a successful init; on
		// failure degrade to nil so the handler surfaces "Train state data is
		// not available".
		if initErr := head.EnsureInitialized(h.cfg); initErr != nil {
			log.Log.Info("train: go runtime not initialized, degrade", "err", initErr)
			return nil
		}
		actor, err = getTrainV1StateActorFn()
	}
	if err != nil {
		log.Log.V(1).Info("no train v1 state actor available", "err", err)
		return nil
	}
	h.trainV1Actor = actor
	return actor
}

// handleTrainV2Runs serves GET /api/train/v2/runs/v1 (the Train V2 API),
// aligned with TrainHead.get_train_v2_runs.
func (h *TrainHead) handleTrainV2Runs(w http.ResponseWriter, r *http.Request) {
	stateActor := h.getTrainV2StateActor()
	if stateActor == nil {
		http.Error(w, "Train state data is not available. Please make sure Ray Train "+
			"is running and that the Train state actor is enabled by setting "+
			"the RAY_TRAIN_ENABLE_STATE_TRACKING environment variable to \"1\".",
			http.StatusInternalServerError)
		return
	}
	trainRuns, err := stateActor.getTrainRuns()
	if err != nil {
		http.Error(w, "Failed to get a response from the train stats actor. The GCS may be down, please retry later: "+err.Error(),
			http.StatusServiceUnavailable)
		return
	}
	decorated, err := h.decorateTrainV2Runs(r.Context(), stateActor, trainRuns)
	if err != nil {
		http.Error(w, "Failed to get a response from the train stats actor. The GCS may be down, please retry later: "+err.Error(),
			http.StatusServiceUnavailable)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, map[string]interface{}{"train_runs": decorated})
}

// decorateTrainV2Runs assembles the decorated V2 train runs, aligned with
// TrainHead._decorate_train_runs.
func (h *TrainHead) decorateTrainV2Runs(ctx context.Context, stateActor stateActor, trainRuns map[string]interface{}) ([]map[string]interface{}, error) {
	allTrainRunAttempts, err := stateActor.getTrainRunAttempts()
	if err != nil {
		return nil, err
	}
	runList := mapValues(trainRuns)
	jobIDs := make([]string, 0, len(runList))
	for _, run := range runList {
		if id := strFromMap(run, "job_id"); id != "" {
			jobIDs = append(jobIDs, id)
		}
	}
	jobs, err := h.findJobsByJobIDs(ctx, jobIDs)
	if err != nil {
		return nil, err
	}

	decoratedRuns := make([]map[string]interface{}, 0, len(runList))
	for _, run := range runList {
		runID := strFromMap(run, "id")
		attempts := decorateTrainRunAttempts(h, ctx, allTrainRunAttempts, runID)
		jobDetails := jobs[strFromMap(run, "job_id")]
		status, statusDetail := h.getRunStatus(ctx, run)
		decorated := copyStringMap(run)
		decorated["attempts"] = attempts
		if jobDetails != nil {
			decorated["job_details"] = jobDetails
		} else {
			decorated["job_details"] = nil
		}
		decorated["status"] = status
		decorated["status_detail"] = statusDetail
		decoratedRuns = append(decoratedRuns, decorated)
	}
	// Sort by start_time_ns descending.
	sort.Slice(decoratedRuns, func(i, j int) bool {
		return intFromMap(decoratedRuns[i], "start_time_ns") > intFromMap(decoratedRuns[j], "start_time_ns")
	})
	return decoratedRuns, nil
}

// decorateTrainRunAttempts decorates the attempts of a train run, aligned with
// TrainHead._decorate_train_run_attempts + _decorate_train_workers.
func decorateTrainRunAttempts(h *TrainHead, ctx context.Context, allTrainRunAttempts map[string]interface{}, runID string) []map[string]interface{} {
	attemptsMap, _ := allTrainRunAttempts[runID].(map[string]interface{})
	attempts := mapValues(attemptsMap)
	out := make([]map[string]interface{}, 0, len(attempts))
	for _, attempt := range attempts {
		workers := decorateTrainWorkers(h, ctx, strSliceFromMap(attempt, "workers"))
		decorated := copyStringMap(attempt)
		decorated["workers"] = workers
		out = append(out, decorated)
	}
	return out
}

// decorateTrainWorkers decorates the workers of a train run attempt, aligned
// with TrainHead._decorate_train_workers: each worker is enriched with the
// actor's status/processStats/formatted gpus when the actor is known.
func decorateTrainWorkers(h *TrainHead, ctx context.Context, workers []map[string]interface{}) []map[string]interface{} {
	actorIDs := make([]string, 0, len(workers))
	for _, worker := range workers {
		if id := strFromMap(worker, "actor_id"); id != "" {
			actorIDs = append(actorIDs, id)
		}
	}
	actorInfos := h.getActorInfos(ctx, actorIDs)
	out := make([]map[string]interface{}, 0, len(workers))
	for _, worker := range workers {
		actorID := strFromMap(worker, "actor_id")
		decorated := copyStringMap(worker)
		if actor := actorInfos[actorID]; actor != nil {
			decorated["status"] = actor["state"]
			decorated["processStats"] = actor["processStats"]
			decorated["gpus"] = formattedGPUs(worker, actor["gpus"])
		}
		out = append(out, decorated)
	}
	return out
}

// formattedGPUs ports the Python _decorate_train_workers gpu filtering: from
// the actor's gpus (already filtered to those whose processesPids contain the
// actor pid) keep only the gpus whose processesPids contains the worker pid and
// collapse each gpu's process list to a single processInfo entry, aligned with
// the Python `processInfo: [... if process["pid"] == worker.pid][0]`.
func formattedGPUs(worker map[string]interface{}, gpusVal interface{}) []interface{} {
	workerPID, ok := pidInt64(worker["pid"])
	if !ok {
		return []interface{}{}
	}
	formatted := []interface{}{}
	for _, g := range asSlice(gpusVal) {
		gpu, ok := g.(map[string]interface{})
		if !ok {
			continue
		}
		processes := asSlice(gpu["processesPids"])
		match := -1
		for i, proc := range processes {
			pm, ok := proc.(map[string]interface{})
			if !ok {
				continue
			}
			if p, ok := pidInt64(pm["pid"]); ok && p == workerPID {
				match = i
				break
			}
		}
		if match < 0 {
			continue
		}
		// Build {**gpu, "processInfo": <matching process>}.
		formattedGPU := make(map[string]interface{}, len(gpu)+1)
		for k, v := range gpu {
			formattedGPU[k] = v
		}
		formattedGPU["processInfo"] = processes[match]
		formatted = append(formatted, formattedGPU)
	}
	return formatted
}

// getRunStatus computes the V2 run status, aligned with TrainHead._get_run_status:
// a RUNNING run whose controller actor is DEAD is marked ABORTED.
func (h *TrainHead) getRunStatus(ctx context.Context, run map[string]interface{}) (string, interface{}) {
	controllerActorID := strFromMap(run, "controller_actor_id")
	actorInfos := h.getActorInfos(ctx, []string{controllerActorID})
	controllerActorInfo := actorInfos[controllerActorID]
	status := strFromMap(run, "status")
	statusDetail := run["status_detail"]
	if controllerActorInfo != nil && strFromMap(controllerActorInfo, "state") == actorStatusDead && status == "RUNNING" {
		return "ABORTED", "Terminated due to system errors or killed by the user."
	}
	return status, statusDetail
}

// handleTrainV1Runs serves GET /api/train/v2/runs (the Train V1 API), aligned
// with TrainHead.get_train_runs.
func (h *TrainHead) handleTrainV1Runs(w http.ResponseWriter, r *http.Request) {
	statsActor := h.getTrainV1StateActor()
	if statsActor == nil {
		http.Error(w, "Train state data is not available. Please make sure Ray Train "+
			"is running and that the Train state actor is enabled by setting "+
			"the RAY_TRAIN_ENABLE_STATE_TRACKING environment variable to \"1\".",
			http.StatusInternalServerError)
		return
	}
	trainRuns, err := statsActor.getAllTrainRuns()
	if err != nil {
		http.Error(w, "Failed to get a response from the train stats actor. The GCS may be down, please retry later: "+err.Error(),
			http.StatusServiceUnavailable)
		return
	}
	decorated, err := h.decorateTrainV1Runs(r.Context(), trainRuns)
	if err != nil {
		http.Error(w, "Failed to get a response from the train stats actor. The GCS may be down, please retry later: "+err.Error(),
			http.StatusServiceUnavailable)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, map[string]interface{}{"train_runs": decorated})
}

// decorateTrainV1Runs assembles the decorated V1 train runs, aligned with
// TrainHead._add_actor_status_and_update_run_status.
func (h *TrainHead) decorateTrainV1Runs(ctx context.Context, trainRuns map[string]interface{}) ([]map[string]interface{}, error) {
	runList := mapValues(trainRuns)
	decoratedRuns := make([]map[string]interface{}, 0, len(runList))
	for _, run := range runList {
		workers := decorateTrainV1Workers(h, ctx, strSliceFromMap(run, "workers"))
		decorated := copyStringMap(run)
		decorated["workers"] = workers
		// A RUNNING run whose controller actor is DEAD is marked ABORTED.
		controllerActorID := strFromMap(run, "controller_actor_id")
		actorInfos := h.getActorInfos(ctx, []string{controllerActorID})
		if actor := actorInfos[controllerActorID]; actor != nil &&
			strFromMap(actor, "state") == actorStatusDead &&
			strFromMap(run, "run_status") == "RUNNING" {
			decorated["run_status"] = "ABORTED"
			decorated["status_detail"] = "Terminated due to system errors or killed by the user."
		}
		decoratedRuns = append(decoratedRuns, decorated)
	}
	// Sort by start_time_ms descending.
	sort.Slice(decoratedRuns, func(i, j int) bool {
		return intFromMap(decoratedRuns[i], "start_time_ms") > intFromMap(decoratedRuns[j], "start_time_ms")
	})
	// Attach job details.
	jobIDs := make([]string, 0, len(decoratedRuns))
	for _, run := range decoratedRuns {
		if id := strFromMap(run, "job_id"); id != "" {
			jobIDs = append(jobIDs, id)
		}
	}
	jobs, err := h.findJobsByJobIDs(ctx, jobIDs)
	if err != nil {
		return nil, err
	}
	for _, run := range decoratedRuns {
		run["job_details"] = jobs[strFromMap(run, "job_id")]
	}
	return decoratedRuns, nil
}

// decorateTrainV1Workers decorates the V1 workers of a train run, aligned with
// the worker loop in TrainHead._add_actor_status_and_update_run_status.
func decorateTrainV1Workers(h *TrainHead, ctx context.Context, workers []map[string]interface{}) []map[string]interface{} {
	actorIDs := make([]string, 0, len(workers))
	for _, worker := range workers {
		if id := strFromMap(worker, "actor_id"); id != "" {
			actorIDs = append(actorIDs, id)
		}
	}
	actorInfos := h.getActorInfos(ctx, actorIDs)
	out := make([]map[string]interface{}, 0, len(workers))
	for _, worker := range workers {
		actorID := strFromMap(worker, "actor_id")
		decorated := copyStringMap(worker)
		if actor := actorInfos[actorID]; actor != nil {
			decorated["status"] = actor["state"]
			decorated["processStats"] = actor["processStats"]
			decorated["gpus"] = formattedGPUs(worker, actor["gpus"])
		}
		out = append(out, decorated)
	}
	return out
}

// getActorInfosFn returns a map of actor_id -> actor info for the requested
// actor ids. It is a package-level variable so tests can inject a fake. In
// production it queries the GCS actor table directly (the Go head runs all
// modules in a single process, so unlike the Python TrainHead it does not need
// an HTTP round-trip to the NodeHead module) and enriches each actor with the
// gpus/processStats pulled from the cached node physical stats by pid, aligned
// with the Python DataOrganizer._get_actor_info which the train module reads
// through /logical/actors.
var getActorInfosFn = func(h *TrainHead, ctx context.Context, actorIDs []string) (map[string]map[string]interface{}, error) {
	reply, err := h.client.GetAllActorInfo(ctx, &proto.GetAllActorInfoRequest{})
	if err != nil {
		return nil, err
	}
	infos := make(map[string]map[string]interface{}, len(actorIDs))
	for _, a := range reply.ActorTableData {
		id := hex.EncodeToString(a.ActorId)
		info := map[string]interface{}{
			"state": a.State.String(),
		}
		// Enrich gpus/processStats from the node physical stats by pid, aligned
		// with Python _get_actor_info: gpus lists the gpu dicts whose
		// processesPids contains the actor's pid, processStats is the worker
		// entry with the matching pid. pid is only usable when the actor has an
		// address (the pid is meaningless without a node).
		if a.GetPid() > 0 && a.GetAddress() != nil {
			nodeID := hex.EncodeToString(a.GetAddress().GetNodeId())
			processStats, gpus := physicalStatsForPID(h, nodeID, int64(a.GetPid()))
			info["processStats"] = processStats
			info["gpus"] = gpus
		} else {
			info["processStats"] = nil
			info["gpus"] = []interface{}{}
		}
		infos[id] = info
	}
	return infos, nil
}

// physicalStatsForPID returns the (processStats, gpus) pair for an actor pid
// from the cached node physical stats, aligned with the Python
// _get_actor_info loop: processStats is the first worker whose pid matches,
// gpus are the gpu dicts whose processesPids contains the pid.
func physicalStatsForPID(h *TrainHead, nodeID string, pid int64) (interface{}, []interface{}) {
	h.physMu.RLock()
	stats := h.physicalStats[nodeID]
	h.physMu.RUnlock()
	if stats == nil {
		return nil, []interface{}{}
	}
	var processStats interface{}
	for _, w := range asSlice(stats["workers"]) {
		worker, ok := w.(map[string]interface{})
		if !ok {
			continue
		}
		if p, ok := pidInt64(worker["pid"]); ok && p == pid {
			processStats = worker
			break
		}
	}
	gpus := []interface{}{}
	for _, g := range asSlice(stats["gpus"]) {
		gpu, ok := g.(map[string]interface{})
		if !ok {
			continue
		}
		for _, proc := range asSlice(gpu["processesPids"]) {
			pm, ok := proc.(map[string]interface{})
			if !ok {
				continue
			}
			if p, ok := pidInt64(pm["pid"]); ok && p == pid {
				gpus = append(gpus, gpu)
				break
			}
		}
	}
	return processStats, gpus
}

// asSlice returns v as a []interface{}, or nil when it is not a slice.
func asSlice(v interface{}) []interface{} {
	s, _ := v.([]interface{})
	return s
}

// pidInt64 extracts a process id from a proto uint32 or a JSON-decoded float64
// (the shape the pid takes in the physical stats) as an int64.
func pidInt64(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case uint32:
		return int64(t), true
	case float64:
		return int64(t), true
	case int64:
		return t, true
	case int:
		return int64(t), true
	}
	return 0, false
}

// getActorInfos returns the actor info map for the requested actor ids, aligned
// with TrainHead._get_actor_infos.
func (h *TrainHead) getActorInfos(ctx context.Context, actorIDs []string) map[string]map[string]interface{} {
	infos, err := getActorInfosFn(h, ctx, actorIDs)
	if err != nil {
		log.Log.Error(err, "failed to get actor infos")
		return map[string]map[string]interface{}{}
	}
	return infos
}

// findJobsByJobIDs returns the JobDetails keyed by job id for the given ids,
// aligned with find_jobs_by_job_ids in job/utils.py.
func (h *TrainHead) findJobsByJobIDs(ctx context.Context, jobIDs []string) (map[string]*job.JobDetails, error) {
	driverJobs, submissionDrivers, err := getDriverJobs(ctx, h.client)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*job.JobDetails)
	for _, jobID := range jobIDs {
		if d := driverJobs[jobID]; d != nil {
			out[jobID] = d
		}
	}
	for submissionID, driver := range submissionDrivers {
		if !containsString(jobIDs, driver.ID) {
			continue
		}
		info, err := h.info.GetInfo(ctx, submissionID)
		if err != nil {
			return nil, err
		}
		if info != nil {
			out[driver.ID] = submissionJobDetails(submissionID, info, driver)
		}
	}
	return out, nil
}

// mapValues returns the values of a string-keyed map as a slice.
func mapValues(m map[string]interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(m))
	for _, v := range m {
		if mm, ok := v.(map[string]interface{}); ok {
			out = append(out, mm)
		}
	}
	return out
}

// strSliceFromMap extracts a list of dicts field from a map.
func strSliceFromMap(m map[string]interface{}, key string) []map[string]interface{} {
	raw, ok := m[key].([]interface{})
	if !ok {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(raw))
	for _, v := range raw {
		if mm, ok := v.(map[string]interface{}); ok {
			out = append(out, mm)
		}
	}
	return out
}

// strFromMap extracts a string field from a map, defaulting to "".
func strFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// intFromMap extracts an integer field from a map, defaulting to 0.
func intFromMap(m map[string]interface{}, key string) int64 {
	switch v := m[key].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	}
	return 0
}

// copyStringMap deep-copies the top-level entries of a string-keyed map so the
// decorated run does not alias the actor-returned map.
func copyStringMap(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// containsString reports whether the slice contains the target string.
func containsString(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}

// The following helpers reimplement the job module's unexported lookup
// helpers (get_driver_jobs / find_jobs_by_job_ids in job/utils.py) against
// the shared GCS client, following the same pattern as the flow module.

// getDriverJobs queries all driver jobs and returns (driverJobs,
// submissionDrivers), reusing the job package's shared implementation.
func getDriverJobs(ctx context.Context, client *head.GCSClient) (map[string]*job.JobDetails, map[string]*job.DriverInfo, error) {
	return job.GetDriverJobs(ctx, client, "")
}

// submissionJobDetails assembles a submission JobDetails from the KV job info
// and the driver, aligned with the Python find_job_by_ids construction.
func submissionJobDetails(submissionID string, info *job.JobInfo, driver *job.DriverInfo) *job.JobDetails {
	var jobID *string
	if driver != nil {
		jobID = ptrString(driver.ID)
	}
	return &job.JobDetails{
		Type:                   job.JobTypeSubmission,
		JobID:                  jobID,
		SubmissionID:           ptrString(submissionID),
		DriverInfo:             driver,
		Status:                 info.Status,
		Entrypoint:             info.Entrypoint,
		Message:                info.Message,
		ErrorType:              info.ErrorType,
		StartTime:              info.StartTime,
		EndTime:                info.EndTime,
		Metadata:               info.Metadata,
		RuntimeEnv:             runtimeEnvFromJSON(info.RuntimeEnvJSON),
		DriverAgentHTTPAddress: info.DriverAgentHTTPAddress,
		DriverNodeID:           info.DriverNodeID,
		DriverExitCode:         info.DriverExitCode,
	}
}

// runtimeEnvFromJSON converts the runtime_env_json string stored in the KV
// into a dict, aligned with JobInfo.from_json().
func runtimeEnvFromJSON(serialized *string) map[string]interface{} {
	if serialized == nil {
		return nil
	}
	out := map[string]interface{}{}
	_ = json.Unmarshal([]byte(*serialized), &out)
	return out
}

// ptrString returns a pointer to a copy of s.
func ptrString(s string) *string { return &s }
