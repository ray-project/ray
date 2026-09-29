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

// Package reporter implements the ReporterHead dashboard module that exposes
// the cluster status, health check, prometheus service discovery and profiling
// routes, aligned with the Python ReportHead in
// python/ray/dashboard/modules/reporter/reporter_head.py.
package reporter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/pkg/log"
)

// KV key constants aligned with python/ray/_private/ray_constants.py,
// python/ray/dashboard/consts.py and src/ray/common/constants.h.
const (
	// kvNamespaceCluster is KV_NAMESPACE_CLUSTER.
	kvNamespaceCluster = "cluster"
	// kvNamespaceDashboard is KV_NAMESPACE_DASHBOARD.
	kvNamespaceDashboard = "dashboard"

	// clusterMetadataKey is CLUSTER_METADATA_KEY from
	// python/ray/_common/usage/usage_constants.py.
	clusterMetadataKey = "CLUSTER_METADATA"

	// debugAutoscalingStatusLegacy is DEBUG_AUTOSCALING_STATUS_LEGACY.
	debugAutoscalingStatusLegacy = "__autoscaling_status_legacy"
	// debugAutoscalingStatus is DEBUG_AUTOSCALING_STATUS.
	debugAutoscalingStatus = "__autoscaling_status"
	// debugAutoscalingError is DEBUG_AUTOSCALING_ERROR.
	debugAutoscalingError = "__autoscaling_error"

	// dashboardAgentAddrNodeIDPrefix is DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX.
	dashboardAgentAddrNodeIDPrefix = "DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX:"
	// dashboardAgentAddrIPPrefix is DASHBOARD_AGENT_ADDR_IP_PREFIX.
	dashboardAgentAddrIPPrefix = "DASHBOARD_AGENT_ADDR_IP_PREFIX:"

	// autoscalerStateNamespace is kGcsAutoscalerStateNamespace.
	autoscalerStateNamespace = "__autoscaler"
	// autoscalerV2EnabledKey is kGcsAutoscalerV2EnabledKey.
	autoscalerV2EnabledKey = "__autoscaler_v2_enabled"

	// dashboardMetricsAddress is the KV key holding the head's metrics address.
	dashboardMetricsAddress = "DashboardMetricsAddress"
	// autoscalerMetricsAddress is the KV key holding the autoscaler monitor's
	// metrics address.
	autoscalerMetricsAddress = "AutoscalerMetricsAddress"

	// gcsRPCTimeoutSeconds mirrors GCS_RPC_TIMEOUT_SECONDS in
	// python/ray/dashboard/consts.py.
	gcsRPCTimeoutSeconds = 30
	// stateAPITimeoutSeconds is the timeout used by the task lookups for
	// profiling, aligned with the ListApiOptions(timeout=10) in reporter_head.
	stateAPITimeoutSeconds = 10
	// defaultCPUDurationS is the default cpu profiling duration.
	defaultCPUDurationS = 5
	// defaultMemoryDurationS is the default memory profiling duration.
	defaultMemoryDurationS = 10
	// maxCPUDurationS is the max allowed cpu profiling duration.
	maxCPUDurationS = 60
	// defaultGPUIterations is the default number of torch optimizer steps.
	defaultGPUIterations = 4
	// defaultCPUFormat is the default cpu profiling output format.
	defaultCPUFormat = "flamegraph"
	// defaultMemoryFormat is the default memory profiling output format.
	defaultMemoryFormat = "flamegraph"

	// emojiWarning and svgStyle mirror the HTML constants in reporter_head.py.
	emojiWarning = "&#x26A0;&#xFE0F;"
	svgStyle     = "<style>\n    svg {\n        width: 100%;\n        height: 100%;\n    }\n</style>\n"

	// warningForMultiTaskInAWorker mirrors WARNING_FOR_MULTI_TASK_IN_A_WORKER.
	warningForMultiTaskInAWorker = "Warning: This task is running in a worker process that is running multiple tasks. This can happen if you are profiling a task right as it finishes or if you are using the Async Actor or Threaded Actors pattern. The information that follows may come from any of these tasks:"
)

// maxConcurrentProfiling bounds concurrent profiling RPC fan-out, mirroring
// RAY_DASHBOARD_REPORTER_HEAD_TPE_MAX_WORKERS=1 in reporter_head.py.
const maxConcurrentProfiling = 1

// ReportHead implements the reporter routes.
type ReportHead struct {
	cfg    *head.HeadConfig
	client *head.GCSClient
	api    *head.StateAPIManager

	// clusterMetadata is fetched from GCS once at Start, aligned with the
	// Python ReportHead.run() which reads it once and caches it.
	mu              sync.RWMutex
	clusterMetadata map[string]interface{}

	profSem chan struct{}
}

// New creates a ReportHead backed by the given GCS client.
func New(cfg *head.HeadConfig, client *head.GCSClient) *ReportHead {
	return &ReportHead{
		cfg:     cfg,
		client:  client,
		api:     head.NewStateAPIManager(client),
		profSem: make(chan struct{}, maxConcurrentProfiling),
	}
}

// Name returns the module name, aligned with the Python ReportHead class name.
func (r *ReportHead) Name() string { return "ReportHead" }

// Start fetches the cluster metadata once (aligned with the Python run()).
func (r *ReportHead) Start(ctx context.Context) error {
	if r.client == nil {
		return nil
	}
	metadata, err := r.getClusterMetadata(ctx)
	if err != nil {
		log.Log.Error(err, "failed to fetch cluster metadata")
		return nil
	}
	r.mu.Lock()
	r.clusterMetadata = metadata
	r.mu.Unlock()
	return nil
}

// Healthy reports the module health.
func (r *ReportHead) Healthy() bool { return true }

// RegisterHTTP registers the reporter routes.
func (r *ReportHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("GET /api/v0/cluster_metadata", r.handleClusterMetadata)
	mux.HandleFunc("GET /api/cluster_status", r.handleClusterStatus)
	mux.HandleFunc("GET /api/gcs_healthz", r.handleGCSHealthz)
	mux.HandleFunc("GET /api/prometheus/sd", r.handlePrometheusSD)
	mux.HandleFunc("GET /task/traceback", r.handleTaskTraceback)
	mux.HandleFunc("GET /task/cpu_profile", r.handleTaskCPUProfile)
	mux.HandleFunc("GET /worker/traceback", r.handleWorkerTraceback)
	mux.HandleFunc("GET /worker/cpu_profile", r.handleWorkerCPUProfile)
	mux.HandleFunc("GET /worker/gpu_profile", r.handleWorkerGPUProfile)
	mux.HandleFunc("GET /memory_profile", r.handleMemoryProfile)
	return nil
}

// getClusterMetadata reads CLUSTER_METADATA from the GCS internal KV
// (namespace "cluster") and parses it as a JSON dict.
func (r *ReportHead) getClusterMetadata(ctx context.Context) (map[string]interface{}, error) {
	reply, err := r.client.InternalKVGet(ctx, kvNamespaceCluster, clusterMetadataKey)
	if err != nil {
		return nil, err
	}
	metadata := map[string]interface{}{}
	if len(reply.Value) > 0 {
		if err := json.Unmarshal(reply.Value, &metadata); err != nil {
			return nil, err
		}
	}
	return metadata, nil
}

// handleClusterMetadata serves /api/v0/cluster_metadata. The cached metadata
// dict is expanded directly into the data (aligned with
// rest_response(**, self.cluster_metadata)).
func (r *ReportHead) handleClusterMetadata(w http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	metadata := r.clusterMetadata
	r.mu.RUnlock()
	if metadata == nil {
		// Not cached yet (Start may have failed): fetch on demand.
		m, err := r.getClusterMetadata(req.Context())
		if err != nil {
			head.RESTResponseCamel(w, http.StatusInternalServerError, err.Error(), nil)
			return
		}
		metadata = m
	}
	data := make(map[string]interface{}, len(metadata))
	for k, v := range metadata {
		data[k] = v
	}
	head.RESTResponseCamel(w, http.StatusOK, "", data)
}

// handleClusterStatus serves /api/cluster_status?format=0|1.
func (r *ReportHead) handleClusterStatus(w http.ResponseWriter, req *http.Request) {
	returnFormatted := req.URL.Query().Get("format") == "1"

	legacyReply, err := r.client.InternalKVGet(req.Context(), "", debugAutoscalingStatusLegacy)
	if err != nil {
		log.Log.Error(err, "failed to get legacy autoscaling status")
	}
	statusReply, err := r.client.InternalKVGet(req.Context(), "", debugAutoscalingStatus)
	if err != nil {
		log.Log.Error(err, "failed to get autoscaling status")
	}
	errorReply, err := r.client.InternalKVGet(req.Context(), "", debugAutoscalingError)
	if err != nil {
		log.Log.Error(err, "failed to get autoscaling error")
	}

	formattedStatus := map[string]interface{}{}
	if len(statusReply.Value) > 0 {
		_ = json.Unmarshal(statusReply.Value, &formattedStatus)
	}

	if !returnFormatted {
		head.RESTResponseCamel(w, http.StatusOK, "Got cluster status.", map[string]interface{}{
			"autoscaling_status": bytesOrNil(legacyReply.Value),
			"autoscaling_error":  bytesOrNil(errorReply.Value),
			"cluster_status":     mapOrNil(formattedStatus),
		})
		return
	}

	status := r.debugStatus(req.Context(), statusReply.Value, errorReply.Value, r.cfg.GCSAddress)
	head.RESTResponseCamel(w, http.StatusOK, "Got formatted cluster status.", map[string]interface{}{
		"cluster_status": status,
	})
}

func bytesOrNil(b []byte) interface{} {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

func mapOrNil(m map[string]interface{}) interface{} {
	if len(m) == 0 {
		return nil
	}
	return m
}

// handleGCSHealthz serves /api/gcs_healthz. It probes GCS liveness via
// CheckAlive; a successful RPC yields 200 "success", anything else 503.
func (r *ReportHead) handleGCSHealthz(w http.ResponseWriter, req *http.Request) {
	if _, err := r.client.CheckAlive(req.Context(), nil); err != nil {
		w.Header().Set("Content-Type", "application/text")
		// Mirror the Python message "Health check failed: {e}".
		http.Error(w, fmt.Sprintf("Health check failed: %v", err), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/text")
	_, _ = w.Write([]byte("success"))
}
