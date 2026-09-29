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

// Package flow implements the FlowHead dashboard module: flow job routes that
// reuse the job module's agent client plus flow plugin CRUD over the
// cross-language FlowController actor. Aligned with the Python FlowHead in
// python/ray/dashboard/modules/flow/flow_head.py.
package flow

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/job"
	rayerrors "github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// Cross-language actor identity constants for the FlowController, centralized
// here so the name/namespace/module/className quadruple lives in one place.
// Aligned with python/ray/flow/_private/constants.py.
const (
	// flowControllerName is FLOW_CONTROLLER_NAME.
	flowControllerName = "FlowController"
	// flowNamespace is FLOW_NAMESPACE.
	flowNamespace = "flow"
	// flowControllerModule is the Python module of the FlowController actor.
	flowControllerModule = "ray.flow._private.controller"
	// flowControllerClass is the Python actor class name.
	flowControllerClass = "FlowController"
)

// Flow job constants, aligned with python/ray/dashboard/modules/flow/utils.py
// and python/ray/dashboard/modules/job/common.py.
const (
	// flowJobPrefix is FLOW_JOB_PREFIX.
	flowJobPrefix = "flowsubmit_"
	// flowMetadataKey is FLOW_METADATA_KEY.
	flowMetadataKey = "job_type"
	// flowMetadataValue is FLOW_METADATA_VALUE.
	flowMetadataValue = "flow"
	// flowExecuteDriver is FLOW_EXECUTE_DRIVER (flow_head.py).
	flowExecuteDriver = "ray.flow.driver"
	// waitForSupervisorActorInterval is WAIT_FOR_SUPERVISOR_ACTOR_INTERVAL_S.
	waitForSupervisorActorInterval = time.Second
	// tryToGetAgentInfoInterval is TRY_TO_GET_AGENT_INFO_INTERVAL_SECONDS.
	tryToGetAgentInfoInterval = 500 * time.Millisecond
	// flowStartTimeout is how long the plugin handlers wait for the detached
	// FlowController to appear after submitting _start_controller, aligned with
	// the serve module's startControllerTimeout.
	flowStartTimeout = 60 * time.Second
	// flowStartPollInterval is the polling interval while waiting for the
	// controller actor to appear in GCS.
	flowStartPollInterval = 500 * time.Millisecond
	// jobIDMetadataKey is JOB_ID_METADATA_KEY from job/common.py. It remains
	// here for the flow tests which build sample driver tables keyed by it.
	jobIDMetadataKey = "job_submission_id"
	// kvNamespaceJob is KV_NAMESPACE_JOB.
	kvNamespaceJob = "job"
	// kvHeadNodeIDKey is KV_HEAD_NODE_ID_KEY.
	kvHeadNodeIDKey = "head_node_id"
	// kvNamespaceDashboard is KV_NAMESPACE_DASHBOARD.
	kvNamespaceDashboard = "dashboard"
	// dashboardAgentAddrNodeIDPrefix is DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX.
	dashboardAgentAddrNodeIDPrefix = "DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX:"
)

// errFlowDependenciesNotInstalled is the 501 error returned when the ray[flow]
// dependencies are not installed, matching the text returned by
// validate_endpoint in python/ray/dashboard/modules/flow/flow_head.py.
var errFlowDependenciesNotInstalled = errors.New("Flow dependencies are not installed. Please run `pip install \"ray[flow]\"`.")

// waitAvailableAgentTimeout is WAIT_AVAILABLE_AGENT_TIMEOUT. It is a variable
// (not a const) so tests can shorten it to exercise the timeout path quickly.
var waitAvailableAgentTimeout = 10 * time.Second

// errAgentTimeout is returned when the head node agent could not be reached
// within WAIT_AVAILABLE_AGENT_TIMEOUT, mirroring the asyncio.TimeoutError
// raised by FlowHead._get_head_node_agent which the Python submit_job maps to
// a 504 GatewayTimeout (flow_head.py:374-378).
var errAgentTimeout = errors.New("failed to get head node agent within timeout")

// flowDependencyProbeModule is the Python module whose import triggers a
// ray[flow] dependency check. Importing ray.flow.driver.utils runs
// ray/flow/driver/__init__.py first, which imports the executor that depends on
// the optional python-manifest package (the ray[flow] extra); when that is
// missing the import raises ModuleNotFoundError, mirroring the very import
// validate_endpoint performs.
const flowDependencyProbeModule = "ray.flow.driver.utils"

// checkFlowDepsFn probes whether the flow driver is importable on a Python
// worker. It is a package-level variable so tests can inject a fake without
// loading go_runtime.so. It returns ErrRuntimeNotInitialized when the Go
// runtime is not yet initialized, and errFlowDependenciesNotInstalled when the
// import fails.
var checkFlowDepsFn = func(h *FlowHead) error {
	if !api.IsInitialized() {
		// The Go runtime is not initialized (e.g. the GCS was not ready during
		// `ray start`). Surface ErrRuntimeNotInitialized so the caller lazily
		// initializes and retries; without the runtime no Python worker can be
		// scheduled to probe the import. (Submitting a task while uninitialized
		// would instead fail with submitter_not_available, which is ambiguous.)
		return rayerrors.ErrRuntimeNotInitialized
	}
	// Use RemotePython[string] (num_returns=1) rather than RemotePythonVoid:
	// generate_name returns a value, and a void call (num_returns=0) yields a
	// nil object ref that api.Get always fails on ("object reference is nil"),
	// making every probe misreport dependencies as not installed.
	ref, err := api.RemotePython[string](flowDependencyProbeModule, "generate_name", "").Call()
	if err != nil {
		return err
	}
	if _, err := api.Get(ref); err != nil {
		// The Python worker failed to import ray.flow.driver, so the ray[flow]
		// dependencies are not installed.
		return errFlowDependenciesNotInstalled
	}
	return nil
}

// FlowHead implements the flow job and plugin APIs.
type FlowHead struct {
	cfg        *head.HeadConfig
	client     *head.GCSClient
	info       *job.JobInfoStorageClient
	httpClient *http.Client

	mu     sync.Mutex
	agents map[string]*job.JobAgentSubmissionClient

	controllerMu sync.Mutex
	controller   flowController

	depsMu      sync.Mutex
	depsChecked bool
	depsOK      bool
}

// New creates a FlowHead backed by the given GCS client.
func New(cfg *head.HeadConfig, client *head.GCSClient) *FlowHead {
	return &FlowHead{
		cfg:        cfg,
		client:     client,
		info:       job.NewJobInfoStorageClient(client),
		httpClient: &http.Client{Timeout: 60 * time.Second},
		agents:     map[string]*job.JobAgentSubmissionClient{},
	}
}

// Name returns the module name.
func (h *FlowHead) Name() string { return "FlowHead" }

// Start has no background tasks.
func (h *FlowHead) Start(ctx context.Context) error { return nil }

// Healthy reports the module health.
func (h *FlowHead) Healthy() bool { return true }

// RegisterHTTP registers the flow job and plugin routes. The tail endpoint is
// registered before the logs endpoint so the more specific
// /api/flow/jobs/{id}/logs/tail path is not shadowed; net/http ServeMux
// already prefers the longer pattern but the explicit order keeps this
// obvious.
func (h *FlowHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("POST /api/flow/jobs/", h.handleSubmitJob)
	mux.HandleFunc("GET /api/flow/jobs/", h.handleListJobs)
	mux.HandleFunc("GET /api/flow/jobs/{job_or_submission_id}/logs/tail", h.handleTailJobLogs)
	mux.HandleFunc("GET /api/flow/jobs/{job_or_submission_id}/logs", h.handleGetJobLogs)
	mux.HandleFunc("GET /api/flow/jobs/{job_or_submission_id}", h.handleGetJobInfo)
	mux.HandleFunc("POST /api/flow/jobs/{job_or_submission_id}/stop", h.handleStopJob)
	mux.HandleFunc("DELETE /api/flow/jobs/{job_or_submission_id}", h.handleDeleteJob)
	mux.HandleFunc("POST /api/flow/plugins/", h.handleAddPlugin)
	mux.HandleFunc("GET /api/flow/plugins/", h.handleListPlugins)
	mux.HandleFunc("GET /api/flow/plugins/{plugin_id}", h.handleGetPluginInfo)
	mux.HandleFunc("DELETE /api/flow/plugins/{plugin_id}", h.handleDeletePlugin)
	return nil
}

// findJob resolves a job by job id or submission id, mirroring find_job_by_ids
// in job/utils.py (the job module keeps its helpers unexported, so the flow
// module reimplements the small lookup against the shared GCS client).
func (h *FlowHead) findJob(ctx context.Context, jobOrSubmissionID string) (*job.JobDetails, error) {
	driverJobs, submissionDrivers, err := getDriverJobs(ctx, h.client, jobOrSubmissionID)
	if err != nil {
		return nil, err
	}
	if jobDetails := driverJobs[jobOrSubmissionID]; jobDetails != nil {
		return jobDetails, nil
	}
	submissionID := ""
	for id, driver := range submissionDrivers {
		if driver.ID == jobOrSubmissionID {
			submissionID = id
			break
		}
	}
	if submissionID == "" {
		submissionID = jobOrSubmissionID
	}
	info, err := h.info.GetInfo(ctx, submissionID)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, nil
	}
	var driver *job.DriverInfo
	if d := submissionDrivers[submissionID]; d != nil {
		driver = d
	}
	return submissionJobDetails(submissionID, info, driver), nil
}

// getTargetAgent fetches (and caches) the JobAgentSubmissionClient for the
// head node, retrying until the agent info is available or the timeout
// elapses, aligned with FlowHead.get_target_agent/_get_head_node_agent.
func (h *FlowHead) getTargetAgent(ctx context.Context) (*job.JobAgentSubmissionClient, error) {
	deadline := time.Now().Add(waitAvailableAgentTimeout)
	for time.Now().Before(deadline) {
		agent, err := h.getHeadNodeAgentOnce(ctx)
		if err == nil {
			return agent, nil
		}
		log.Log.Error(err, "failed to get head node agent, retrying")
		select {
		case <-time.After(tryToGetAgentInfoInterval):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	// Mirror the Python _get_head_node_agent raising TimeoutError after the
	// timeout elapses: the caller maps this to a 504 GatewayTimeout.
	return nil, errAgentTimeout
}

// getHeadNodeAgentOnce resolves the head node id and its agent address, then
// builds (and caches) the agent client.
func (h *FlowHead) getHeadNodeAgentOnce(ctx context.Context) (*job.JobAgentSubmissionClient, error) {
	headNodeIDHex, err := h.info.GetHeadNodeID(ctx)
	if err != nil {
		return nil, err
	}
	if headNodeIDHex == "" {
		return nil, fmt.Errorf("head node id has not yet been persisted in GCS")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if agent := h.agents[headNodeIDHex]; agent != nil {
		return agent, nil
	}
	ip, httpPort, err := h.info.FetchAgentInfo(ctx, headNodeIDHex)
	if err != nil {
		return nil, err
	}
	agent := job.NewJobAgentSubmissionClient("http://"+buildAddress(ip, httpPort), h.httpClient)
	h.agents[headNodeIDHex] = agent
	return agent, nil
}

// getJobDriverAgentClient returns the cached agent client for the node the job
// driver runs on, or nil when the job has no driver agent address yet.
func (h *FlowHead) getJobDriverAgentClient(jobDetails *job.JobDetails) *job.JobAgentSubmissionClient {
	if jobDetails.DriverAgentHTTPAddress == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	driverNodeID := ""
	if jobDetails.DriverNodeID != nil {
		driverNodeID = *jobDetails.DriverNodeID
	}
	if agent := h.agents[driverNodeID]; agent != nil {
		return agent
	}
	agent := job.NewJobAgentSubmissionClient(*jobDetails.DriverAgentHTTPAddress, h.httpClient)
	h.agents[driverNodeID] = agent
	return agent
}

// buildAddress formats a host and port into "host:port", wrapping IPv6
// addresses in brackets, aligned with the C++ BuildAddress.
func buildAddress(host string, port int) string {
	if strings.Contains(host, ":") {
		return fmt.Sprintf("[%s]:%d", host, port)
	}
	return fmt.Sprintf("%s:%d", host, port)
}

// getDriverJobs queries all driver jobs and returns (driverJobs,
// submissionDrivers), reusing the job package's shared implementation.
func getDriverJobs(ctx context.Context, client *head.GCSClient, jobOrSubmissionID string) (map[string]*job.JobDetails, map[string]*job.DriverInfo, error) {
	return job.GetDriverJobs(ctx, client, jobOrSubmissionID)
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

// generateFlowJobID returns "flowsubmit_" + 16 secure random characters that
// exclude the confusing I/l/O/o/0, aligned with generate_flow_job_id in
// python/ray/dashboard/modules/flow/utils.py.
func generateFlowJobID() string {
	return flowJobPrefix + randomIDPart()
}

// generatePluginID returns "plugin_" + 16 secure random characters, aligned
// with generate_plugin_id in flow/utils.py.
func generatePluginID() string {
	return "plugin_" + randomIDPart()
}

// randomIDPart produces 16 characters from the secure-random alphabet
// string.ascii_letters + string.digits minus {I,l,o,O,0}.
func randomIDPart() string {
	const alphabet = "ABCDEFGHJKMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789"
	const n = 16
	out := make([]byte, n)
	randBytes := make([]byte, n)
	if _, err := rand.Read(randBytes); err != nil {
		// Fall back to a time-seeded id so the generator never blocks a request
		// on RNG failure (Python SystemRandom would raise instead).
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	for i := 0; i < n; i++ {
		out[i] = alphabet[int(randBytes[i])%len(alphabet)]
	}
	return string(out)
}

// buildJobEntrypoint transforms a flow entrypoint (JSON string) into the shell
// command that runs the flow driver, aligned with FlowHead._build_job_entrypoint.
func buildJobEntrypoint(entrypoint string) string {
	return fmt.Sprintf("python -m %s %s", flowExecuteDriver, shellQuote(entrypoint))
}

// shellQuote is a minimal POSIX single-quote quoting, equivalent to Python's
// shlex.quote for the strings passed here.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// validateFlowJSON validates the flow entrypoint JSON, aligned with
// validate_flow_json in flow/utils.py: it must be non-empty valid JSON with a
// non-empty "stages" list, every stage having name/target/type, and an
// optional strategy in ["group_by_group", "all_at_once"].
func validateFlowJSON(flowJSONStr string) error {
	if strings.TrimSpace(flowJSONStr) == "" {
		return fmt.Errorf("Input cannot be empty")
	}
	var flowData map[string]interface{}
	if err := json.Unmarshal([]byte(flowJSONStr), &flowData); err != nil {
		return fmt.Errorf("Invalid JSON format: %v", err)
	}
	stages, ok := flowData["stages"]
	if !ok {
		return fmt.Errorf("JSON must contain a 'stages' field")
	}
	stagesList, ok := stages.([]interface{})
	if !ok {
		return fmt.Errorf("The 'stages' field must be a list")
	}
	if len(stagesList) == 0 {
		return fmt.Errorf("The 'stages' list must contain at least one stage")
	}
	if strategy, ok := flowData["strategy"]; ok {
		strategyStr, ok := strategy.(string)
		if !ok {
			return fmt.Errorf("'strategy' must be a string")
		}
		if strategyStr != "group_by_group" && strategyStr != "all_at_once" {
			return fmt.Errorf("Invalid strategy '%s'. Valid strategies are: group_by_group, all_at_once", strategyStr)
		}
	}
	if input, ok := flowData["input"]; ok {
		if _, ok := input.(map[string]interface{}); !ok {
			return fmt.Errorf("'input' must be a dictionary")
		}
	}
	if name, ok := flowData["name"]; ok {
		if _, ok := name.(string); !ok {
			return fmt.Errorf("'name' must be of type str")
		}
	}
	if description, ok := flowData["description"]; ok {
		if _, ok := description.(string); !ok {
			return fmt.Errorf("'description' must be of type str")
		}
	}
	if version, ok := flowData["version"]; ok {
		if _, ok := version.(string); !ok {
			return fmt.Errorf("'version' must be of type str")
		}
	}
	if metadata, ok := flowData["metadata"]; ok {
		if _, ok := metadata.(map[string]interface{}); !ok {
			return fmt.Errorf("'metadata' must be of type dict")
		}
	}
	for i, stage := range stagesList {
		stageMap, ok := stage.(map[string]interface{})
		if !ok {
			return fmt.Errorf("Stage at index %d must be a dictionary", i)
		}
		if _, ok := stageMap["name"]; !ok {
			return fmt.Errorf("Stage at index %d must have a 'name' field", i)
		}
		if _, ok := stageMap["target"]; !ok {
			return fmt.Errorf("Stage '%s' must have a 'target' field", stageName(stageMap, i))
		}
		if _, ok := stageMap["type"]; !ok {
			return fmt.Errorf("Stage '%s' must have a 'type' field", stageName(stageMap, i))
		}
	}
	return nil
}

// stageName returns a stage's name for error messages, falling back to its
// index.
func stageName(stage map[string]interface{}, index int) string {
	if name, ok := stage["name"].(string); ok {
		return name
	}
	return fmt.Sprintf("%d", index)
}

// checkFlowDependencies verifies that the ray[flow] dependencies are installed,
// aligned with validate_endpoint in python/ray/dashboard/modules/flow/flow_head.py
// which imports ray.flow.driver before handling any flow job request and returns
// 501 "Flow dependencies are not installed" on ImportError. The result is cached
// per process: a positive probe is expensive (it submits a Python task), and
// like Python's sys.modules cache the outcome does not change at runtime.
func (h *FlowHead) checkFlowDependencies() error {
	h.depsMu.Lock()
	defer h.depsMu.Unlock()
	if h.depsChecked {
		if h.depsOK {
			return nil
		}
		return errFlowDependenciesNotInstalled
	}
	if err := checkFlowDepsFn(h); err != nil {
		if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
			// The Go runtime failed to initialize at startup (e.g. the GCS was
			// not ready during `ray start`). Lazily initialize it now and retry
			// once, mirroring the other cross-language modules. When the runtime
			// still cannot be initialized the probe cannot run: degrade to
			// proceeding (the subsequent body validation still applies), which
			// mirrors the gap between Go and Python noted below.
			if initErr := head.EnsureInitialized(h.cfg); initErr != nil {
				log.Log.Info("flow: go runtime not initialized, skip dependency check", "err", initErr)
				return nil
			}
			err = checkFlowDepsFn(h)
		}
		if err != nil {
			// The probe failed after the runtime was available: treat it as
			// dependencies-not-installed. Python returns 501 whenever it cannot
			// import ray.flow.driver.
			h.depsChecked = true
			h.depsOK = false
			return errFlowDependenciesNotInstalled
		}
	}
	h.depsChecked = true
	h.depsOK = true
	return nil
}

// handleSubmitJob serves POST /api/flow/jobs/, aligned with FlowHead.submit_job:
// it generates a flow submission id, marks the metadata job_type=flow,
// validates the flow JSON entrypoint and forwards the wrapped entrypoint to
// the head node agent.
func (h *FlowHead) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	// Dependency pre-check before body validation, aligned with Python's
	// @validate_endpoint() decorator which runs before submit_job parses the
	// body: a missing ray[flow] install returns 501 regardless of the request.
	if err := h.checkFlowDependencies(); err != nil {
		http.Error(w, err.Error(), http.StatusNotImplemented)
		return
	}
	var req job.JobSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Entrypoint == "" {
		http.Error(w, "entrypoint must be provided", http.StatusBadRequest)
		return
	}

	flowSubmissionID := generateFlowJobID()
	metadata := req.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadata[flowMetadataKey] = flowMetadataValue

	if err := validateFlowJSON(req.Entrypoint); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	jobSubmitRequest := &job.JobSubmitRequest{
		Entrypoint:          buildJobEntrypoint(req.Entrypoint),
		SubmissionID:        ptrString(flowSubmissionID),
		RuntimeEnv:          req.RuntimeEnv,
		Metadata:            metadata,
		EntrypointNumCPUs:   req.EntrypointNumCPUs,
		EntrypointNumGPUs:   req.EntrypointNumGPUs,
		EntrypointMemory:    req.EntrypointMemory,
		EntrypointResources: req.EntrypointResources,
	}

	agent, err := h.getTargetAgent(r.Context())
	if err != nil {
		// The Python submit_job maps asyncio.TimeoutError (raised when the head
		// node agent is not available within WAIT_AVAILABLE_AGENT_TIMEOUT) to a
		// 504 GatewayTimeout (flow_head.py:374-378).
		if errors.Is(err, errAgentTimeout) {
			http.Error(w, "No available agent to submit flow job, please try again later.", http.StatusGatewayTimeout)
			return
		}
		http.Error(w, "No available agent to submit flow job, please try again later.", http.StatusInternalServerError)
		return
	}
	resp, err := agent.SubmitJobInternal(r.Context(), jobSubmitRequest)
	if err != nil {
		// An HTTP request timeout (aiohttp asyncio.TimeoutError) also maps to
		// 504, aligned with the Python except asyncio.TimeoutError branch.
		if isTimeoutErr(err) {
			http.Error(w, "No available agent to submit flow job, please try again later.", http.StatusGatewayTimeout)
			return
		}
		http.Error(w, "Failed to submit flow job. Please check logs for details.", http.StatusInternalServerError)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, resp)
}

// isTimeoutErr reports whether err is a network timeout (the Go http.Client
// returns a net.Error with Timeout() true when the request deadline elapses,
// mirroring the asyncio.TimeoutError the Python submit_job catches).
func isTimeoutErr(err error) bool {
	var ne interface{ Timeout() bool }
	if errors.As(err, &ne) {
		return ne.Timeout()
	}
	return false
}

// handleStopJob serves POST /api/flow/jobs/{id}/stop, aligned with
// FlowHead.stop_job.
func (h *FlowHead) handleStopJob(w http.ResponseWriter, r *http.Request) {
	jobOrSubmissionID := r.PathValue("job_or_submission_id")
	jobDetails, err := h.findJob(r.Context(), jobOrSubmissionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if jobDetails == nil {
		http.Error(w, fmt.Sprintf("Flow job %s does not exist", jobOrSubmissionID), http.StatusNotFound)
		return
	}
	if jobDetails.Type != job.JobTypeSubmission {
		http.Error(w, "Can only stop submission type flow jobs", http.StatusBadRequest)
		return
	}
	agent, err := h.getTargetAgent(r.Context())
	if err != nil {
		http.Error(w, "Failed to stop flow job. Please check logs for details.", http.StatusInternalServerError)
		return
	}
	resp, err := agent.StopJobInternal(r.Context(), *jobDetails.SubmissionID)
	if err != nil {
		http.Error(w, "Failed to stop flow job. Please check logs for details.", http.StatusInternalServerError)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, resp)
}

// handleDeleteJob serves DELETE /api/flow/jobs/{id}, aligned with
// FlowHead.delete_job.
func (h *FlowHead) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	jobOrSubmissionID := r.PathValue("job_or_submission_id")
	jobDetails, err := h.findJob(r.Context(), jobOrSubmissionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if jobDetails == nil {
		http.Error(w, fmt.Sprintf("Flow job %s does not exist", jobOrSubmissionID), http.StatusNotFound)
		return
	}
	if jobDetails.Type != job.JobTypeSubmission {
		http.Error(w, "Can only delete submission type flow jobs", http.StatusBadRequest)
		return
	}
	agent, err := h.getTargetAgent(r.Context())
	if err != nil {
		http.Error(w, "Failed to delete flow job. Please check logs for details.", http.StatusInternalServerError)
		return
	}
	resp, err := agent.DeleteJobInternal(r.Context(), *jobDetails.SubmissionID)
	if err != nil {
		http.Error(w, "Failed to delete flow job. Please check logs for details.", http.StatusInternalServerError)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, resp)
}

// handleGetJobInfo serves GET /api/flow/jobs/{id}, aligned with
// FlowHead.get_job_info.
func (h *FlowHead) handleGetJobInfo(w http.ResponseWriter, r *http.Request) {
	jobOrSubmissionID := r.PathValue("job_or_submission_id")
	jobDetails, err := h.findJob(r.Context(), jobOrSubmissionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if jobDetails == nil {
		http.Error(w, fmt.Sprintf("Flow job %s does not exist", jobOrSubmissionID), http.StatusNotFound)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, jobDetails)
}

// handleListJobs serves GET /api/flow/jobs/, aligned with FlowHead.list_jobs:
// it filters the submission jobs to flow jobs (submission id prefix +
// job_type=flow metadata).
func (h *FlowHead) handleListJobs(w http.ResponseWriter, r *http.Request) {
	_, submissionDrivers, err := getDriverJobs(r.Context(), h.client, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	submissionJobs, err := h.info.GetAllJobs(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	items := make([]*job.JobDetails, 0)
	for submissionID, info := range submissionJobs {
		if !strings.HasPrefix(submissionID, flowJobPrefix) {
			continue
		}
		if info.Metadata == nil || info.Metadata[flowMetadataKey] != flowMetadataValue {
			continue
		}
		var driver *job.DriverInfo
		if d := submissionDrivers[submissionID]; d != nil {
			driver = d
		}
		items = append(items, submissionJobDetails(submissionID, info, driver))
	}
	head.WriteRawJSON(w, http.StatusOK, items)
}

// handleGetJobLogs serves GET /api/flow/jobs/{id}/logs, aligned with
// FlowHead.get_job_logs. When the driver agent address is not known yet, an
// empty logs response is returned.
func (h *FlowHead) handleGetJobLogs(w http.ResponseWriter, r *http.Request) {
	jobOrSubmissionID := r.PathValue("job_or_submission_id")
	jobDetails, err := h.findJob(r.Context(), jobOrSubmissionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if jobDetails == nil {
		http.Error(w, fmt.Sprintf("Flow job %s does not exist", jobOrSubmissionID), http.StatusNotFound)
		return
	}
	if jobDetails.Type != job.JobTypeSubmission {
		http.Error(w, "Can only get logs of submission type flow jobs", http.StatusBadRequest)
		return
	}
	agent := h.getJobDriverAgentClient(jobDetails)
	if agent == nil {
		head.WriteRawJSON(w, http.StatusOK, &job.JobLogsResponse{})
		return
	}
	resp, err := agent.GetJobLogsInternal(r.Context(), *jobDetails.SubmissionID)
	if err != nil {
		http.Error(w, "Failed to get job logs. Please check logs for details.", http.StatusInternalServerError)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, resp)
}

// handleTailJobLogs serves the GET /api/flow/jobs/{id}/logs/tail WebSocket,
// aligned with FlowHead.tail_job_logs. It accepts the client WebSocket, polls
// until the job's driver agent address is known (or the job goes terminal),
// then connects to the agent's tail WebSocket and forwards every text line to
// the client.
func (h *FlowHead) handleTailJobLogs(w http.ResponseWriter, r *http.Request) {
	jobOrSubmissionID := r.PathValue("job_or_submission_id")
	jobDetails, err := h.findJob(r.Context(), jobOrSubmissionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close(websocket.StatusNormalClosure, "")

	if jobDetails == nil {
		_ = ws.Close(websocket.StatusPolicyViolation, "Flow job "+jobOrSubmissionID+" does not exist")
		return
	}
	if jobDetails.Type != job.JobTypeSubmission {
		_ = ws.Close(websocket.StatusPolicyViolation, "Can only get logs of submission type flow jobs")
		return
	}

	// Poll until the driver agent http address is available. If the job is
	// terminal with no address, the supervisor actor never started: close.
	for jobDetails.DriverAgentHTTPAddress == nil {
		jobDetails, err = h.findJob(r.Context(), jobOrSubmissionID)
		if err != nil {
			return
		}
		if jobDetails == nil || (jobDetails.Status.IsTerminal() && jobDetails.DriverAgentHTTPAddress == nil) {
			return
		}
		select {
		case <-time.After(waitForSupervisorActorInterval):
		case <-r.Context().Done():
			return
		}
	}

	agent := h.getJobDriverAgentClient(jobDetails)
	if agent == nil {
		return
	}
	lines, closeTail, err := agent.TailJobLogs(r.Context(), *jobDetails.SubmissionID)
	if err != nil {
		log.Log.Error(err, "failed to tail flow job logs from agent", "submission_id", *jobDetails.SubmissionID)
		return
	}
	defer closeTail()
	ctx := r.Context()
	for line := range lines {
		if err := ws.Write(ctx, websocket.MessageText, []byte(line)); err != nil {
			return
		}
	}
}

// flowController is the subset of the FlowController actor surface used by the
// plugin routes. getFlowController returns *api.PythonActorHandle which
// satisfies it; the interface lets tests inject a fake.
type flowController interface {
	checkAlive() error
	registerPlugin(pluginID, name, path string, version, workgroup, description, metadata interface{}) (bool, error)
	listPlugins() ([]map[string]interface{}, error)
	getPlugin(pluginID string) (map[string]interface{}, error)
	deletePlugin(pluginID string) (bool, error)
}

// pythonFlowController adapts *api.PythonActorHandle to flowController via the
// cross-language ActorTask/Get API.
type pythonFlowController struct {
	handle *api.PythonActorHandle
}

func (c *pythonFlowController) checkAlive() error {
	ref, err := api.ActorTask[any](c.handle, "check_alive").Remote()
	if err != nil {
		return err
	}
	if _, err := api.Get(ref); err != nil {
		return err
	}
	return nil
}

func (c *pythonFlowController) registerPlugin(pluginID, name, path string, version, workgroup, description, metadata interface{}) (bool, error) {
	ref, err := api.ActorTask[bool](c.handle, "register_plugin",
		pluginID, name, path, version, workgroup, description, metadata).Remote()
	if err != nil {
		return false, err
	}
	return api.Get(ref)
}

func (c *pythonFlowController) listPlugins() ([]map[string]interface{}, error) {
	ref, err := api.ActorTask[[]map[string]interface{}](c.handle, "list_plugins").Remote()
	if err != nil {
		return nil, err
	}
	return api.Get(ref)
}

func (c *pythonFlowController) getPlugin(pluginID string) (map[string]interface{}, error) {
	ref, err := api.ActorTask[map[string]interface{}](c.handle, "get_plugin", pluginID).Remote()
	if err != nil {
		return nil, err
	}
	return api.Get(ref)
}

func (c *pythonFlowController) deletePlugin(pluginID string) (bool, error) {
	ref, err := api.ActorTask[bool](c.handle, "delete_plugin", pluginID).Remote()
	if err != nil {
		return false, err
	}
	return api.Get(ref)
}

// getFlowControllerFn returns the FlowController actor handle, or nil when no
// flow instance is running. It is a package-level variable so tests can inject
// a fake controller.
var getFlowControllerFn = func() (flowController, error) {
	handle, err := api.GetPythonActorWithNamespace(
		flowControllerName, flowNamespace, flowControllerModule, flowControllerClass)
	if err != nil {
		return nil, err
	}
	return &pythonFlowController{handle: handle}, nil
}

// startFlowControllerFn starts a Flow instance cross-language when none is
// running, mirroring flow_start_async in python/ray/flow/_private/api.py: it
// schedules the _start_controller function on a Python worker with num_cpus=0
// and waits for the detached FlowController named actor to appear in GCS. It is
// a package-level variable so tests can inject a fake.
var startFlowControllerFn = func(ctx context.Context, h *FlowHead) (flowController, error) {
	// _start_controller is a plain module-level function (unlike serve's, which
	// is also plain) that creates the detached FlowController actor. Schedule it
	// with num_cpus=0 like flow_start_async's ray.remote(_start_controller)
	// .options(num_cpus=0).
	ref, err := api.RemotePythonVoid("ray.flow._private.api", "_start_controller", "").
		WithResources(map[string]float64{api.ResourceCPU: 0}).
		Call()
	if err != nil {
		return nil, fmt.Errorf("failed to submit flow controller start: %w", err)
	}
	if ref != nil {
		if _, err := api.Get(ref); err != nil {
			return nil, fmt.Errorf("flow controller start failed: %w", err)
		}
	}
	// The controller is detached, so it survives the task that created it. Wait
	// for it to appear under its well-known name (FLOW_CONTROLLER_NAME in the
	// "flow" namespace).
	deadline := time.Now().Add(flowStartTimeout)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if controller, err := getFlowControllerFn(); err != nil {
			if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
				return nil, err
			}
		} else if controller != nil {
			log.Log.Info("flow: controller started cross-language")
			return controller, nil
		}
		select {
		case <-time.After(flowStartPollInterval):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("flow controller did not start within %s", flowStartTimeout)
}

// getFlowController returns the cached FlowController, validating it with
// check_alive first and re-fetching on failure, aligned with
// FlowHead.get_flow_controller. It returns nil when no flow instance is
// running.
func (h *FlowHead) getFlowController() (flowController, error) {
	h.controllerMu.Lock()
	defer h.controllerMu.Unlock()
	if h.controller != nil {
		if err := h.controller.checkAlive(); err != nil {
			// Python logs this at logger.warning; go/pkg/log exposes no Warning
			// level, so Info (visible by default, non-fatal) is used, matching
			// the serve module's "controller is dead, re-fetching" message.
			log.Log.Info("flow controller is dead, re-fetching", "err", err)
			h.controller = nil
		} else {
			return h.controller, nil
		}
	}
	controller, err := getFlowControllerFn()
	if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
		// The Go runtime failed to initialize at startup (e.g. the GCS was not
		// ready during `ray start`). Lazily initialize it now, mirroring the
		// Python dashboard's require_initialized which ray.init()s on the first
		// request, so the head recovers the cross-language runtime without a
		// restart. EnsureInitialized is a no-op once initialized and serialized
		// against concurrent calls. Retry once after a successful init; on
		// failure degrade to nil so the handler surfaces "FlowController is not
		// running" instead of a hard error.
		if initErr := head.EnsureInitialized(h.cfg); initErr != nil {
			log.Log.Info("flow: go runtime not initialized, degrade", "err", initErr)
			return nil, nil
		}
		controller, err = getFlowControllerFn()
	}
	if err != nil {
		log.Log.V(1).Info("no flow controller available", "err", err)
		return nil, nil
	}
	h.controller = controller
	return controller, nil
}

// ensureFlowController returns the controller, creating one when none is
// running, aligned with FlowHead.ensure_flow_controller which calls
// flow.start() to create a new FlowController when the cached/existing lookup
// finds none. The create failure surfaces to the caller (a 500 in the plugin
// handlers), mirroring the Python raise in ensure_flow_controller.
func (h *FlowHead) ensureFlowController(ctx context.Context) (flowController, error) {
	controller, err := h.getFlowController()
	if err != nil {
		return nil, err
	}
	if controller != nil {
		return controller, nil
	}
	log.Log.Info("flow: no FlowController, creating one", "gcs", h.cfg.GCSAddress)
	controller, err = startFlowControllerFn(ctx, h)
	if err != nil {
		log.Log.Error(err, "failed to create FlowController")
		return nil, err
	}
	h.controllerMu.Lock()
	h.controller = controller
	h.controllerMu.Unlock()
	return controller, nil
}

// handleAddPlugin serves POST /api/flow/plugins/, aligned with FlowHead.add_plugin:
// a body with a "plugins" key triggers batch registration, otherwise a single
// plugin is registered.
func (h *FlowHead) handleAddPlugin(w http.ResponseWriter, r *http.Request) {
	var data map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if plugins, ok := data["plugins"]; ok {
		h.addPluginsBatch(w, r, plugins)
		return
	}
	h.addPluginSingle(w, r, data)
}

// addPluginSingle registers a single plugin, aligned with
// FlowHead._add_plugin_single.
func (h *FlowHead) addPluginSingle(w http.ResponseWriter, r *http.Request, data map[string]interface{}) {
	name, nameOK := data["name"].(string)
	path, pathOK := data["path"].(string)
	if !nameOK || !pathOK {
		http.Error(w, "Missing required fields: name, path", http.StatusBadRequest)
		return
	}
	controller, err := h.ensureFlowController(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	pluginID, ok := data["id"].(string)
	if !ok || pluginID == "" {
		pluginID = generatePluginID()
	}
	success, err := controller.registerPlugin(pluginID, name, path,
		data["version"], data["workgroup"], data["description"], data["metadata"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !success {
		http.Error(w, "Failed to register plugin", http.StatusInternalServerError)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, map[string]string{"plugin_id": pluginID})
}

// addPluginsBatch registers multiple plugins and collects per-plugin results,
// aligned with FlowHead._add_plugins_batch.
func (h *FlowHead) addPluginsBatch(w http.ResponseWriter, r *http.Request, plugins interface{}) {
	pluginList, ok := plugins.([]interface{})
	if !ok || len(pluginList) == 0 {
		http.Error(w, "plugins list cannot be empty", http.StatusBadRequest)
		return
	}
	controller, err := h.ensureFlowController(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	results := map[string]string{}
	for _, raw := range pluginList {
		pluginData, ok := raw.(map[string]interface{})
		if !ok {
			http.Error(w, "plugin must be a dictionary", http.StatusBadRequest)
			return
		}
		pluginID, ok := pluginData["id"].(string)
		if !ok || pluginID == "" {
			pluginID = generatePluginID()
		}
		name, nameOK := pluginData["name"].(string)
		path, pathOK := pluginData["path"].(string)
		if !nameOK || !pathOK {
			results[pluginID] = "failed: Missing required fields: name, path"
			continue
		}
		success, err := controller.registerPlugin(pluginID, name, path,
			pluginData["version"], pluginData["workgroup"], pluginData["description"], pluginData["metadata"])
		if err != nil {
			results[pluginID] = "failed: " + err.Error()
			continue
		}
		if success {
			results[pluginID] = "success"
		} else {
			results[pluginID] = "failed: registration returned False"
		}
	}
	head.WriteRawJSON(w, http.StatusOK, map[string]interface{}{"results": results})
}

// pluginDetails mirrors the Python PluginDetails pydantic model
// (python/ray/flow/schema.py), aligned with FlowHead._plugin_data_to_details.
type pluginDetails struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Path        *string                `json:"path"`
	Version     *string                `json:"version"`
	Workgroup   *string                `json:"workgroup"`
	Description *string                `json:"description"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// pluginDataToDetails converts raw plugin data (a dict returned by the
// controller) into a pluginDetails, aligned with FlowHead._plugin_data_to_details.
func pluginDataToDetails(data map[string]interface{}) *pluginDetails {
	return &pluginDetails{
		ID:          strFromMap(data, "id"),
		Name:        strFromMap(data, "name"),
		Path:        optStrFromMap(data, "path"),
		Version:     optStrFromMap(data, "version"),
		Workgroup:   optStrFromMap(data, "workgroup"),
		Description: optStrFromMap(data, "description"),
		Metadata:    mapFromMap(data, "metadata"),
	}
}

// strFromMap extracts a string field, defaulting to "".
func strFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// optStrFromMap extracts an optional string field.
func optStrFromMap(m map[string]interface{}, key string) *string {
	if v, ok := m[key].(string); ok {
		return ptrString(v)
	}
	return nil
}

// mapFromMap extracts an optional dict field.
func mapFromMap(m map[string]interface{}, key string) map[string]interface{} {
	if v, ok := m[key].(map[string]interface{}); ok {
		return v
	}
	return nil
}

// handleListPlugins serves GET /api/flow/plugins/, aligned with
// FlowHead.list_plugins: no controller -> 200 empty list; otherwise it lists
// the plugins and converts each to a PluginDetails dict.
func (h *FlowHead) handleListPlugins(w http.ResponseWriter, r *http.Request) {
	controller, err := h.getFlowController()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if controller == nil {
		head.WriteRawJSON(w, http.StatusOK, []interface{}{})
		return
	}
	plugins, err := controller.listPlugins()
	if err != nil {
		// Remote call failure returns an empty list, aligned with Python.
		log.Log.V(1).Info("failed to call list_plugins", "err", err)
		head.WriteRawJSON(w, http.StatusOK, []interface{}{})
		return
	}
	items := make([]*pluginDetails, 0, len(plugins))
	for _, p := range plugins {
		items = append(items, pluginDataToDetails(p))
	}
	head.WriteRawJSON(w, http.StatusOK, items)
}

// handleDeletePlugin serves DELETE /api/flow/plugins/{plugin_id}, aligned with
// FlowHead.delete_plugin.
func (h *FlowHead) handleDeletePlugin(w http.ResponseWriter, r *http.Request) {
	pluginID := r.PathValue("plugin_id")
	controller, err := h.getFlowController()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if controller == nil {
		http.Error(w, "FlowController not available", http.StatusServiceUnavailable)
		return
	}
	deleted, err := controller.deletePlugin(pluginID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, map[string]interface{}{"deleted": deleted, "plugin_id": pluginID})
}

// handleGetPluginInfo serves GET /api/flow/plugins/{plugin_id}, aligned with
// FlowHead.get_plugin_info: no controller -> 503; found -> 200 details;
// missing or error -> 404.
func (h *FlowHead) handleGetPluginInfo(w http.ResponseWriter, r *http.Request) {
	pluginID := r.PathValue("plugin_id")
	controller, err := h.getFlowController()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if controller == nil {
		http.Error(w, "FlowController not available", http.StatusServiceUnavailable)
		return
	}
	pluginData, err := controller.getPlugin(pluginID)
	if err != nil || pluginData == nil {
		http.Error(w, "Plugin not found", http.StatusNotFound)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, pluginDataToDetails(pluginData))
}
