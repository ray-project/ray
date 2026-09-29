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

package job

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/version"
	"github.com/ray-project/ray/go/proto"
)

// Constants aligned with python/ray/dashboard/modules/job/job_head.py and
// python/ray/dashboard/consts.py.
const (
	// currentVersion is CURRENT_VERSION from
	// python/ray/dashboard/modules/version.py.
	currentVersion = "4"
	// waitAvailableAgentTimeout is WAIT_AVAILABLE_AGENT_TIMEOUT.
	waitAvailableAgentTimeout = 10 * time.Second
	// tryToGetAgentInfoInterval is TRY_TO_GET_AGENT_INFO_INTERVAL_SECONDS.
	tryToGetAgentInfoInterval = 500 * time.Millisecond
	// waitForSupervisorActorInterval is
	// JobHead.WAIT_FOR_SUPERVISOR_ACTOR_INTERVAL_S.
	waitForSupervisorActorInterval = time.Second
	// gcsRPCTimeout is GCS_RPC_TIMEOUT_SECONDS.
	gcsRPCTimeout = 60 * time.Second
	// runtimeEnvURIPinExpirationS is the default RAY_RUNTIME_ENV_URI_PIN_
	// EXPIRATION_S (10 minutes).
	runtimeEnvURIPinExpirationS = 600
	// rayClusterActivityHookEnv is RAY_CLUSTER_ACTIVITY_HOOK from
	// python/ray/dashboard/consts.py: the executable invoked by
	// /api/component_activities to fetch external component activities.
	rayClusterActivityHookEnv = "RAY_CLUSTER_ACTIVITY_HOOK"
)

// JobHead implements the Ray Jobs API routes, aligned with the Python JobHead
// in python/ray/dashboard/modules/job/job_head.py.
type JobHead struct {
	cfg        *head.HeadConfig
	client     *head.GCSClient
	info       *JobInfoStorageClient
	httpClient *http.Client

	mu     sync.Mutex
	agents map[string]*JobAgentSubmissionClient
}

// New creates a JobHead backed by the given GCS client.
func New(cfg *head.HeadConfig, client *head.GCSClient) *JobHead {
	return &JobHead{
		cfg:        cfg,
		client:     client,
		info:       NewJobInfoStorageClient(client),
		httpClient: &http.Client{Timeout: gcsRPCTimeout},
		agents:     map[string]*JobAgentSubmissionClient{},
	}
}

// Name returns the module name.
func (h *JobHead) Name() string { return "JobHead" }

// Start has no background tasks.
func (h *JobHead) Start(ctx context.Context) error { return nil }

// Healthy reports the module health.
func (h *JobHead) Healthy() bool { return true }

// RegisterHTTP registers the job routes. The tail endpoint is registered last
// so the more specific /api/jobs/{id}/logs/tail path is not shadowed by
// /api/jobs/{id}/logs; net/http ServeMux already prefers the longer pattern,
// but the explicit registration order keeps this obvious.
func (h *JobHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("GET /api/version", h.handleVersion)
	mux.HandleFunc("GET /api/packages/{protocol}/{package_name}", h.handleGetPackage)
	mux.HandleFunc("PUT /api/packages/{protocol}/{package_name}", h.handlePutPackage)
	mux.HandleFunc("POST /api/jobs/", h.handleSubmitJob)
	mux.HandleFunc("GET /api/jobs/", h.handleListJobs)
	mux.HandleFunc("GET /api/jobs/{job_or_submission_id}/logs/tail", h.handleTailJobLogs)
	mux.HandleFunc("GET /api/jobs/{job_or_submission_id}/logs", h.handleGetJobLogs)
	mux.HandleFunc("GET /api/jobs/{job_or_submission_id}", h.handleGetJobInfo)
	mux.HandleFunc("POST /api/jobs/{job_or_submission_id}/stop", h.handleStopJob)
	mux.HandleFunc("DELETE /api/jobs/{job_or_submission_id}", h.handleDeleteJob)
	mux.HandleFunc("GET /api/component_activities", h.handleComponentActivities)
	return nil
}

// handleVersion serves /api/version, aligned with JobHead.get_version.
func (h *JobHead) handleVersion(w http.ResponseWriter, r *http.Request) {
	head.WriteRawJSON(w, http.StatusOK, &VersionResponse{
		Version:     currentVersion,
		RayVersion:  version.RayVersion,
		RayCommit:   version.RayCommit,
		SessionName: h.cfg.SessionName,
	})
}

// httpURIComponentsToURI rebuilds a package URI from the protocol and package
// name path components, aligned with http_uri_components_to_uri
// (job/common.py): "{protocol}://{package_name}".
func httpURIComponentsToURI(protocol, packageName string) string {
	return protocol + "://" + packageName
}

// supportedPackageProtocols mirrors the Protocol enum members in
// python/ray/_private/runtime_env/protocol.py (ProtocolsProvider.get_protocols
// uppercased). Matching is case-insensitive because Python urlparse() lowercases
// the scheme before Protocol(scheme) validation.
var supportedPackageProtocols = []string{
	"gcs", "conda", "pip", "uv", "https", "s3", "gs", "azure", "abfss", "file",
}

// validatePackageProtocol reports whether the path protocol component is one of
// the supported runtime_env protocols. Python's parse_uri raises a ValueError
// (surfaced as a 500) for an unknown scheme; the Go handlers mirror that so an
// invalid URI is rejected instead of silently accepted.
func validatePackageProtocol(protocol string) bool {
	for _, p := range supportedPackageProtocols {
		if strings.EqualFold(protocol, p) {
			return true
		}
	}
	return false
}

// handleGetPackage serves GET /api/packages/{protocol}/{package_name}, aligned
// with JobHead.get_package: it pins the package URI and returns 200 (empty) if
// the package exists in the GCS internal KV, 404 otherwise. An unsupported
// protocol yields a 500, matching the Python ValueError from parse_uri.
func (h *JobHead) handleGetPackage(w http.ResponseWriter, r *http.Request) {
	uri := httpURIComponentsToURI(r.PathValue("protocol"), r.PathValue("package_name"))
	if !validatePackageProtocol(r.PathValue("protocol")) {
		http.Error(w, fmt.Sprintf("Invalid protocol for runtime_env URI %q", uri), http.StatusInternalServerError)
		return
	}
	if err := h.info.PinRuntimeEnvURI(r.Context(), uri, runtimeEnvURIPinExpirationS); err != nil {
		log.Log.Error(err, "failed to pin runtime env uri", "uri", uri)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	reply, err := h.client.InternalKVGet(r.Context(), "", uri)
	if err != nil {
		log.Log.Error(err, "failed to check package existence", "uri", uri)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(reply.Value) == 0 {
		http.Error(w, fmt.Sprintf("Package %s does not exist", uri), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handlePutPackage serves PUT /api/packages/{protocol}/{package_name}, aligned
// with JobHead.upload_package: it stores the request body in the GCS internal
// KV under the package URI (empty namespace). An unsupported protocol yields a
// 500, matching the Python ValueError from parse_uri.
func (h *JobHead) handlePutPackage(w http.ResponseWriter, r *http.Request) {
	uri := httpURIComponentsToURI(r.PathValue("protocol"), r.PathValue("package_name"))
	if !validatePackageProtocol(r.PathValue("protocol")) {
		log.Log.Info("invalid protocol for runtime_env URI", "uri", uri)
		http.Error(w, fmt.Sprintf("Invalid protocol for runtime_env URI %q", uri), http.StatusInternalServerError)
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		log.Log.Error(err, "failed to read package upload body", "uri", uri)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := h.client.InternalKVPut(r.Context(), "", uri, data, true); err != nil {
		log.Log.Error(err, "failed to upload package", "uri", uri)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Log.Info("uploading package", "uri", uri, "bytes", len(data))
	w.WriteHeader(http.StatusOK)
}

// getHeadNodeAgent fetches (and caches) the JobAgentSubmissionClient for the
// head node, retrying until the agent info is available or the timeout
// elapses, aligned with JobHead._get_head_node_agent.
func (h *JobHead) getHeadNodeAgent(ctx context.Context) (*JobAgentSubmissionClient, error) {
	deadline := time.Now().Add(waitAvailableAgentTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		agent, err := h.getHeadNodeAgentOnce(ctx)
		if err == nil {
			return agent, nil
		}
		lastErr = err
		log.Log.Error(err, "failed to get head node agent, retrying")
		select {
		case <-time.After(tryToGetAgentInfoInterval):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("failed to get head node agent within %s: %w", waitAvailableAgentTimeout, lastErr)
}

// getHeadNodeAgentOnce resolves the head node id, its agent address and builds
// the cached agent client, aligned with JobHead._get_head_node_agent_once.
func (h *JobHead) getHeadNodeAgentOnce(ctx context.Context) (*JobAgentSubmissionClient, error) {
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
	agent := NewJobAgentSubmissionClient("http://"+buildAddress(ip, httpPort), h.httpClient)
	h.agents[headNodeIDHex] = agent
	return agent, nil
}

// getJobDriverAgentClient returns the cached agent client for the node the job
// driver runs on, or nil when the job has no driver agent address yet, aligned
// with JobHead.get_job_driver_agent_client.
func (h *JobHead) getJobDriverAgentClient(job *JobDetails) *JobAgentSubmissionClient {
	if job.DriverAgentHTTPAddress == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	driverNodeID := ""
	if job.DriverNodeID != nil {
		driverNodeID = *job.DriverNodeID
	}
	if agent := h.agents[driverNodeID]; agent != nil {
		return agent
	}
	agent := NewJobAgentSubmissionClient(*job.DriverAgentHTTPAddress, h.httpClient)
	h.agents[driverNodeID] = agent
	return agent
}

// handleSubmitJob serves POST /api/jobs/, aligned with JobHead.submit_job.
// It validates the submit request and forwards it to the head node's agent.
func (h *JobHead) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req JobSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Log.Error(err, "failed to parse job submit request body")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Entrypoint == "" {
		log.Log.Info("job submit rejected: entrypoint missing")
		http.Error(w, "entrypoint must be provided", http.StatusBadRequest)
		return
	}
	agent, err := h.getHeadNodeAgent(r.Context())
	if err != nil {
		log.Log.Error(err, "no available agent to submit job")
		http.Error(w, "No available agent to submit job, please try again later.", http.StatusInternalServerError)
		return
	}
	resp, err := agent.SubmitJobInternal(r.Context(), &req)
	if err != nil {
		log.Log.Error(err, "failed to submit job", "entrypoint", req.Entrypoint)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, resp)
}

// handleStopJob serves POST /api/jobs/{id}/stop, aligned with JobHead.stop_job.
func (h *JobHead) handleStopJob(w http.ResponseWriter, r *http.Request) {
	jobOrSubmissionID := r.PathValue("job_or_submission_id")
	job, err := h.findJob(r.Context(), jobOrSubmissionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if job == nil {
		http.Error(w, fmt.Sprintf("Job %s does not exist", jobOrSubmissionID), http.StatusNotFound)
		return
	}
	if job.Type != JobTypeSubmission {
		http.Error(w, "Can only stop submission type jobs", http.StatusBadRequest)
		return
	}
	agent, err := h.getHeadNodeAgent(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp, err := agent.StopJobInternal(r.Context(), *job.SubmissionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, resp)
}

// handleDeleteJob serves DELETE /api/jobs/{id}, aligned with JobHead.delete_job.
func (h *JobHead) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	jobOrSubmissionID := r.PathValue("job_or_submission_id")
	job, err := h.findJob(r.Context(), jobOrSubmissionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if job == nil {
		http.Error(w, fmt.Sprintf("Job %s does not exist", jobOrSubmissionID), http.StatusNotFound)
		return
	}
	if job.Type != JobTypeSubmission {
		http.Error(w, "Can only delete submission type jobs", http.StatusBadRequest)
		return
	}
	agent, err := h.getHeadNodeAgent(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp, err := agent.DeleteJobInternal(r.Context(), *job.SubmissionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, resp)
}

// handleGetJobInfo serves GET /api/jobs/{id}, aligned with JobHead.get_job_info.
func (h *JobHead) handleGetJobInfo(w http.ResponseWriter, r *http.Request) {
	jobOrSubmissionID := r.PathValue("job_or_submission_id")
	job, err := h.findJob(r.Context(), jobOrSubmissionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if job == nil {
		http.Error(w, fmt.Sprintf("Job %s does not exist", jobOrSubmissionID), http.StatusNotFound)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, job)
}

// handleListJobs serves GET /api/jobs/, aligned with JobHead.list_jobs: it
// combines the submission jobs (from the KV) with the driver jobs (from GCS),
// submission jobs first.
func (h *JobHead) handleListJobs(w http.ResponseWriter, r *http.Request) {
	submissionJobs, err := h.info.GetAllJobsOrdered(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	items, err := BuildListJobsDetails(r.Context(), h.client, submissionJobs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, items)
}

// handleGetJobLogs serves GET /api/jobs/{id}/logs, aligned with
// JobHead.get_job_logs. When the driver agent address is not known yet, an
// empty logs response is returned.
func (h *JobHead) handleGetJobLogs(w http.ResponseWriter, r *http.Request) {
	jobOrSubmissionID := r.PathValue("job_or_submission_id")
	job, err := h.findJob(r.Context(), jobOrSubmissionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if job == nil {
		http.Error(w, fmt.Sprintf("Job %s does not exist", jobOrSubmissionID), http.StatusNotFound)
		return
	}
	if job.Type != JobTypeSubmission {
		http.Error(w, "Can only get logs of submission type jobs", http.StatusBadRequest)
		return
	}
	agent := h.getJobDriverAgentClient(job)
	if agent == nil {
		head.WriteRawJSON(w, http.StatusOK, &JobLogsResponse{})
		return
	}
	resp, err := agent.GetJobLogsInternal(r.Context(), *job.SubmissionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, resp)
}

// handleTailJobLogs serves the GET /api/jobs/{id}/logs/tail WebSocket, aligned
// with JobHead.tail_job_logs. It accepts the client WebSocket, polls until the
// job's driver agent address is known (or the job goes terminal), then connects
// to the agent's tail WebSocket and forwards every text line to the client.
func (h *JobHead) handleTailJobLogs(w http.ResponseWriter, r *http.Request) {
	jobOrSubmissionID := r.PathValue("job_or_submission_id")
	job, err := h.findJob(r.Context(), jobOrSubmissionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if job == nil {
		http.Error(w, fmt.Sprintf("Job %s does not exist", jobOrSubmissionID), http.StatusNotFound)
		return
	}
	if job.Type != JobTypeSubmission {
		http.Error(w, "Can only get logs of submission type jobs", http.StatusBadRequest)
		return
	}

	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close(websocket.StatusNormalClosure, "")

	// Poll until the driver agent http address is available. If the job is
	// terminal with no address, the supervisor actor never started: close.
	for job.DriverAgentHTTPAddress == nil {
		job, err = h.findJob(r.Context(), jobOrSubmissionID)
		if err != nil {
			return
		}
		if job == nil || (job.Status.IsTerminal() && job.DriverAgentHTTPAddress == nil) {
			return
		}
		select {
		case <-time.After(waitForSupervisorActorInterval):
		case <-r.Context().Done():
			return
		}
	}

	agent := h.getJobDriverAgentClient(job)
	if agent == nil {
		return
	}
	lines, closeTail, err := agent.TailJobLogs(r.Context(), *job.SubmissionID)
	if err != nil {
		log.Log.Error(err, "failed to tail job logs from agent", "submission_id", *job.SubmissionID)
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

// findJob resolves a job by job id or submission id, aligned with
// find_job_by_ids in job/utils.py.
func (h *JobHead) findJob(ctx context.Context, jobOrSubmissionID string) (*JobDetails, error) {
	return findJobByIDs(ctx, h.client, h.info, jobOrSubmissionID)
}

// handleComponentActivities serves /api/component_activities, aligned with
// JobHead.get_component_activities. It reports the driver activity computed
// from the GCS job table, and, when the RAY_CLUSTER_ACTIVITY_HOOK environment
// variable is set, merges in the component activities returned by the hook.
func (h *JobHead) handleComponentActivities(w http.ResponseWriter, r *http.Request) {
	timeout := 30
	if v := r.URL.Query().Get("timeout"); v != "" && isDigits(v) {
		timeout, _ = strconv.Atoi(v)
	}
	info := h.getJobActivityInfo(r.Context(), timeout)
	resp := map[string]interface{}{
		"driver": activityToMap(info),
	}
	for componentType, m := range clusterActivityHookOutput() {
		resp[componentType] = m
	}
	head.WriteRawJSON(w, http.StatusOK, resp)
}

// isDigits reports whether s consists only of ASCII digits.
func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// clusterActivityHookOutput invokes the RAY_CLUSTER_ACTIVITY_HOOK executable
// (an absolute path to a script printing a JSON object mapping component type
// to a RayActivityResponse-like dict) and returns the parsed per-component
// maps, aligned with the Python load_class(hook) call in
// JobHead.get_component_activities. Each entry is validated against the
// RayActivityResponse shape; invalid entries are replaced with an ERROR
// response. The whole hook is wrapped so a failing hook yields an
// "external_component" ERROR entry. Returns nil when the hook is unset.
func clusterActivityHookOutput() map[string]interface{} {
	hookPath := os.Getenv(rayClusterActivityHookEnv)
	if hookPath == "" {
		return nil
	}
	now := float64(time.Now().UnixNano()) / 1e9
	cmd := exec.Command(hookPath)
	out, err := cmd.Output()
	if err != nil {
		reason := fmt.Sprintf("%v", err)
		return map[string]interface{}{
			"external_component": map[string]interface{}{
				"is_active": RayActivityError,
				"reason":    reason,
				"timestamp": now,
			},
		}
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(out, &raw); err != nil {
		return map[string]interface{}{
			"external_component": map[string]interface{}{
				"is_active": RayActivityError,
				"reason":    fmt.Sprintf("%v", err),
				"timestamp": now,
			},
		}
	}
	resp := make(map[string]interface{}, len(raw))
	for componentType, v := range raw {
		m, ok := parseActivityEntry(v)
		if !ok {
			resp[componentType] = map[string]interface{}{
				"is_active": RayActivityError,
				"reason":    fmt.Sprintf("invalid activity response: %v", v),
				"timestamp": now,
			}
			continue
		}
		resp[componentType] = m
	}
	return resp
}

// parseActivityEntry converts a hook entry into the RayActivityResponse dict
// form. ok is false when the entry is not a dict with is_active and timestamp.
func parseActivityEntry(v interface{}) (map[string]interface{}, bool) {
	m, ok := v.(map[string]interface{})
	if !ok {
		return nil, false
	}
	isActive, ok := m["is_active"].(string)
	if !ok {
		return nil, false
	}
	ts, ok := m["timestamp"].(float64)
	if !ok {
		return nil, false
	}
	out := map[string]interface{}{
		"is_active": isActive,
		"timestamp": ts,
	}
	if reason, ok := m["reason"].(string); ok {
		out["reason"] = reason
	}
	if lat, ok := m["last_activity_at"].(float64); ok {
		out["last_activity_at"] = lat
	}
	return out, true
}

// activityToMap converts a RayActivityResponse to the JSON dict form. The
// last_activity_at and reason keys are always present (null when unset),
// matching the pydantic model fields.
func activityToMap(a *RayActivityResponse) map[string]interface{} {
	m := map[string]interface{}{
		"is_active":        a.IsActive,
		"timestamp":        a.Timestamp,
		"last_activity_at": nil,
		"reason":           nil,
	}
	if a.Reason != nil {
		m["reason"] = *a.Reason
	}
	if a.LastActivityAt != nil {
		m["last_activity_at"] = *a.LastActivityAt
	}
	return m
}

// getJobActivityInfo computes whether there is Ray activity from drivers,
// aligned with JobHead._get_job_activity_info: drivers in namespaces starting
// with _ray_internal_ are not considered activity; a non-dead, non-internal
// driver marks the cluster active.
func (h *JobHead) getJobActivityInfo(ctx context.Context, timeout int) *RayActivityResponse {
	now := float64(time.Now().UnixNano()) / 1e9
	try := func() *RayActivityResponse {
		ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
		req := &proto.GetAllJobInfoRequest{
			SkipSubmissionJobInfoField: boolPtr(true),
			SkipIsRunningTasksField:    boolPtr(true),
		}
		reply, err := h.client.GetAllJobInfo(ctxTimeout, req)
		if err != nil {
			return &RayActivityResponse{
				IsActive:  RayActivityError,
				Reason:    ptrString(err.Error()),
				Timestamp: now,
			}
		}
		numActiveDrivers := 0
		latestJobEndTime := float64(0)
		for _, entry := range reply.JobInfoList {
			isDead := entry.IsDead
			inInternal := entry.Config != nil && strings.HasPrefix(entry.Config.RayNamespace, rayInternalNamespacePrefix)
			if entry.EndTime > 0 && float64(entry.EndTime) > latestJobEndTime {
				latestJobEndTime = float64(entry.EndTime)
			}
			if !isDead && !inInternal {
				numActiveDrivers++
			}
		}
		// Latest job end time must be before or equal to the current timestamp.
		// Job end times may be in epoch milliseconds; convert to seconds.
		if latestJobEndTime > now {
			latestJobEndTime = latestJobEndTime / 1000
		}
		isActive := RayActivityInactive
		if numActiveDrivers > 0 {
			isActive = RayActivityActive
		}
		var reason *string
		if numActiveDrivers > 0 {
			reason = ptrString(fmt.Sprintf("Number of active drivers: %d", numActiveDrivers))
		}
		var lastActivityAt *float64
		if latestJobEndTime > 0 {
			lastActivityAt = &latestJobEndTime
		}
		return &RayActivityResponse{
			IsActive:       isActive,
			Reason:         reason,
			Timestamp:      now,
			LastActivityAt: lastActivityAt,
		}
	}
	return try()
}
