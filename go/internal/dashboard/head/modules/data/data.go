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

// Package data implements the DataHead dashboard module: cross-language calls
// into the Python Ray Data _StatsActor plus Prometheus metric queries for the
// datasets REST API. Aligned with the Python DataHead in
// python/ray/dashboard/modules/data/data_head.py.
package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	rayerrors "github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// Cross-language actor identity constants for the DataStatsActorWrapper, the
// dashboard's proxy into the real _StatsActor, centralized here so the
// name/namespace/module/className quadruple lives in one place. Aligned with
// python/ray/go/dashboard/cross_language/data_wrapper.py.
const (
	statsActorName      = "go_dashboard_stats_wrapper"
	statsActorNamespace = "_dataset_stats_actor"
	statsActorModule    = "ray.go.dashboard.cross_language.data_wrapper"
	statsActorClass     = "DataStatsActorWrapper"
)

// statsActorResources mirrors the original _StatsActor's num_cpus=0 (from the
// @ray.remote(num_cpus=0) decorator in python/ray/data/_internal/stats.py), so
// the wrapper does not consume a CPU quota under the Go default of CPU=1. The
// _StatsActor also uses a NodeAffinity scheduling strategy pinned to the head
// node, but the wrapper is a thin proxy so it only inherits the zero-CPU part;
// max_restarts/max_task_retries stay at the Python default (0). A var (not
// const) because Go maps are not constant expressions.
var statsActorResources = map[string]float64{api.ResourceCPU: 0}

// Prometheus environment/config constants aligned with metrics_head.py.
const (
	prometheusHostEnvVar     = "RAY_PROMETHEUS_HOST"
	defaultPrometheusHost    = "http://localhost:9090"
	prometheusHeadersEnvVar  = "RAY_PROMETHEUS_HEADERS"
	defaultPrometheusHeaders = "{}"
	// maxTimeWindow / sampleRate align with data_head.py MAX_TIME_WINDOW/SAMPLE_RATE.
	maxTimeWindow = "1h"
	sampleRate    = "1s"
)

// httpTimeout bounds the Prometheus query requests.
const httpTimeout = 10 * time.Second

// errPrometheusQueryFailed is returned when the Prometheus server responds with
// a non-200 status, mirroring the Python PrometheusQueryError (metrics_head.py)
// which the Python DataHead does not catch (only ClientConnectorError is
// swallowed) and so surfaces as a 503.
type prometheusQueryError struct {
	status  int
	message string
}

func (e *prometheusQueryError) Error() string {
	return "Error fetching data from prometheus. status: " + fmt.Sprintf("%d", e.status) + ", message: " + e.message
}

// datasetMetrics mirrors the Python DATASET_METRICS dict: each metric maps to
// the query kinds ("value" and/or "max") that are queried.
var datasetMetrics = []struct {
	metric  string
	queries []string
}{
	{"ray_data_output_rows", []string{"max"}},
	{"ray_data_spilled_bytes", []string{"max"}},
	{"ray_data_current_bytes", []string{"value", "max"}},
	{"ray_data_cpu_usage_cores", []string{"value", "max"}},
	{"ray_data_gpu_usage_cores", []string{"value", "max"}},
}

// promQueryTemplate returns the query template for a query kind, aligned with
// the PrometheusQuery enum in data_head.py. The template has three %s
// placeholders (metric, SessionName, groupby) consumed in order by
// queryPrometheus via fmt.Sprintf(template, metric, sessionName, groupBy).
func promQueryTemplate(kind string) string {
	if kind == "max" {
		return "max_over_time(sum(%s{SessionName='%s'}) by (%s))[" + maxTimeWindow + ":" + sampleRate + "]"
	}
	return "sum(%s{SessionName='%s'}) by (%s)"
}

// DataHead implements the datasets API.
type DataHead struct {
	cfg               *head.HeadConfig
	client            *head.GCSClient
	prometheusHost    string
	prometheusHeaders map[string]string
	httpClient        *http.Client
}

// New creates a DataHead backed by the given GCS client. Prometheus config is
// read from the environment at construction, aligned with the Python __init__.
func New(cfg *head.HeadConfig, client *head.GCSClient) *DataHead {
	return &DataHead{
		cfg:               cfg,
		client:            client,
		prometheusHost:    getEnv(prometheusHostEnvVar, defaultPrometheusHost),
		prometheusHeaders: parsePromHeaders(getEnv(prometheusHeadersEnvVar, defaultPrometheusHeaders)),
		httpClient:        &http.Client{Timeout: httpTimeout},
	}
}

// Name returns the module name.
func (h *DataHead) Name() string { return "DataHead" }

// Start has no background tasks.
func (h *DataHead) Start(ctx context.Context) error { return nil }

// Healthy reports the module health.
func (h *DataHead) Healthy() bool { return true }

// RegisterHTTP registers the data routes.
func (h *DataHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("GET /api/data/datasets/{job_id}", h.handleDatasets)
	return nil
}

// statsActor is the subset of the Python _StatsActor surface used by the data
// route. getStatsActor returns *api.PythonActorHandle wrapped in
// pythonStatsActor which satisfies it; the interface lets tests inject a fake.
type statsActor interface {
	getDatasets(jobID string) (map[string]interface{}, error)
}

// pythonStatsActor adapts *api.PythonActorHandle to statsActor via the
// cross-language ActorTask/Get API.
type pythonStatsActor struct {
	handle *api.PythonActorHandle
}

func (a *pythonStatsActor) getDatasets(jobID string) (map[string]interface{}, error) {
	var ref *api.ObjectRef[map[string]interface{}]
	var err error
	if jobID == "" {
		ref, err = api.ActorTask[map[string]interface{}](a.handle, "get_datasets").Remote()
	} else {
		ref, err = api.ActorTask[map[string]interface{}](a.handle, "get_datasets", jobID).Remote()
	}
	if err != nil {
		return nil, err
	}
	return api.Get(ref)
}

// getStatsActorFn returns the DataStatsActorWrapper handle, or an error when
// no datasets stats actor is running. It is a package-level variable so tests
// can inject a fake actor.
//
// The wrapper is a non-detached dashboard proxy created on first use
// (Get-first, Create-fallback) and recycled with the dashboard driver. Its
// constructor binds the real _StatsActor via ray.get_actor, so creation fails
// when no Ray Data stats actor is running and the handler degrades to empty
// datasets.
var getStatsActorFn = func() (statsActor, error) {
	handle, err := api.GetPythonActorWithNamespace(
		statsActorName, statsActorNamespace, statsActorModule, statsActorClass)
	if err == nil {
		log.Log.Info("data: got existing stats wrapper")
		return &pythonStatsActor{handle: handle}, nil
	}
	// A not-initialized runtime (e.g. the GCS was not ready when the head
	// started) is surfaced to the caller so it can lazily initialize and retry;
	// any other error means no data instance is running and we degrade.
	if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
		return nil, err
	}
	{
		// Only create the wrapper when the real _StatsActor exists
		// (datasets_stats_actor / _dataset_stats_actor from
		// ray.data._internal.stats), otherwise the wrapper's __init__ dies
		// asynchronously and the caller sees a misleading "Actor has died".
		if _, err := api.GetPythonActorWithNamespace(
			"datasets_stats_actor", "_dataset_stats_actor",
			"ray.data._internal.stats", "_StatsActor"); err != nil {
			if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
				return nil, err
			}
			log.Log.Info("data: no real stats actor, degrade", "err", err)
			return nil, nil
		}
		log.Log.Info("data: creating stats wrapper")
		handle, err = api.RemotePythonActor(statsActorModule, statsActorClass).
			WithName(statsActorName).WithNamespace(statsActorNamespace).
			WithResources(statsActorResources).Create()
		if err != nil {
			return nil, err
		}
	}
	return &pythonStatsActor{handle: handle}, nil
}

// handleDatasets serves GET /api/data/datasets/{job_id}, aligned with
// DataHead.get_datasets.
func (h *DataHead) handleDatasets(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")
	datasets, err := h.getDatasetsData(r, jobID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	head.WriteRawJSON(w, http.StatusOK, map[string]interface{}{"datasets": datasets})
}

// getDatasetsData fetches and decorates the datasets for a job, returning the
// flattened, sorted dataset list.
func (h *DataHead) getDatasetsData(r *http.Request, jobID string) ([]map[string]interface{}, error) {
	statsActor, err := getStatsActorFn()
	if errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
		// The Go runtime failed to initialize at startup (e.g. the GCS was not
		// ready during `ray start`). Lazily initialize it now, mirroring the
		// Python dashboard's require_initialized which ray.init()s on the first
		// request, so the head recovers the cross-language runtime without a
		// restart. EnsureInitialized is a no-op once initialized and serialized
		// against concurrent calls. Retry once after a successful init; on
		// failure surface 503 (aligned with the Python exception handler).
		if initErr := head.EnsureInitialized(h.cfg); initErr != nil {
			log.Log.Info("data: go runtime not initialized, degrade", "err", initErr)
			return nil, fmt.Errorf("go runtime not initialized: %w", initErr)
		}
		statsActor, err = getStatsActorFn()
	}
	if err != nil || statsActor == nil {
		// No stats actor (Ray Data not running): surface 503, aligned with the
		// Python handler which returns 503 when the stats actor is unavailable.
		log.Log.V(1).Info("no datasets stats actor available", "err", err)
		return nil, fmt.Errorf("Ray Data stats actor is not available")
	}
	datasets, err := statsActor.getDatasets(jobID)
	if err != nil {
		return nil, err
	}
	if datasets == nil {
		datasets = map[string]interface{}{}
	}

	// Initialize dataset metric values.
	for _, dsName := range stringKeys(datasets) {
		ds, _ := datasets[dsName].(map[string]interface{})
		if ds == nil {
			continue
		}
		operators, _ := ds["operators"].(map[string]interface{})
		for _, dm := range datasetMetrics {
			ds[dm.metric] = zeroMetricMap(dm.queries)
			if operators != nil {
				for _, opName := range stringKeys(operators) {
					op, _ := operators[opName].(map[string]interface{})
					if op == nil {
						continue
					}
					op[dm.metric] = zeroMetricMap(dm.queries)
				}
			}
		}
	}

	// Query Prometheus for each metric and kind, at both dataset and operator
	// level. A Prometheus connectivity failure (connection refused) only logs and
	// keeps the zero values, aligned with the Python ClientConnectorError
	// handler; a non-200 response (PrometheusQueryError) propagates and surfaces
	// as a 503, aligned with the Python outer except Exception.
	for _, dm := range datasetMetrics {
		for _, kind := range dm.queries {
			template := promQueryTemplate(kind)
			// Dataset level.
			result, err := h.queryPrometheus(r.Context(), template, dm.metric, "dataset")
			if err != nil {
				if isPrometheusQueryError(err) {
					return nil, err
				}
				log.Log.Error(err, "failed to query prometheus at dataset level",
					"metric", dm.metric, "kind", kind)
			} else {
				for _, res := range result {
					dsName := metricValue(res, "dataset")
					if _, ok := datasets[dsName]; !ok {
						continue
					}
					ds, _ := datasets[dsName].(map[string]interface{})
					if m, ok := ds[dm.metric].(map[string]interface{}); ok {
						m[kind] = resValue(res)
					}
				}
			}
			// Operator level.
			result, err = h.queryPrometheus(r.Context(), template, dm.metric, "dataset, operator")
			if err != nil {
				if isPrometheusQueryError(err) {
					return nil, err
				}
				log.Log.Error(err, "failed to query prometheus at operator level",
					"metric", dm.metric, "kind", kind)
			} else {
				for _, res := range result {
					dsName := metricValue(res, "dataset")
					opName := metricValue(res, "operator")
					ds, ok := datasets[dsName].(map[string]interface{})
					if !ok {
						continue
					}
					operators, _ := ds["operators"].(map[string]interface{})
					op, ok := operators[opName].(map[string]interface{})
					if !ok {
						continue
					}
					if m, ok := op[dm.metric].(map[string]interface{}); ok {
						m[kind] = resValue(res)
					}
				}
			}
		}
	}

	// Flatten the response.
	flatDatasets := make([]map[string]interface{}, 0, len(datasets))
	for _, dsName := range stringKeys(datasets) {
		ds, _ := datasets[dsName].(map[string]interface{})
		if ds == nil {
			continue
		}
		flatOperators := make([]map[string]interface{}, 0)
		if operators, ok := ds["operators"].(map[string]interface{}); ok {
			for _, opName := range stringKeys(operators) {
				op, _ := operators[opName].(map[string]interface{})
				if op == nil {
					continue
				}
				flat := map[string]interface{}{"operator": opName}
				for k, v := range op {
					flat[k] = v
				}
				flatOperators = append(flatOperators, flat)
			}
		}
		flat := map[string]interface{}{"dataset": dsName}
		for k, v := range ds {
			if k == "operators" {
				flat[k] = flatOperators
			} else {
				flat[k] = v
			}
		}
		flatDatasets = append(flatDatasets, flat)
	}
	// Sort by descending start_time.
	sort.Slice(flatDatasets, func(i, j int) bool {
		return numValue(flatDatasets[i]["start_time"]) > numValue(flatDatasets[j]["start_time"])
	})
	return flatDatasets, nil
}

// zeroMetricMap builds the zero-value metric dict for the given query kinds.
func zeroMetricMap(queries []string) map[string]interface{} {
	m := make(map[string]interface{}, len(queries))
	for _, q := range queries {
		m[q] = float64(0)
	}
	return m
}

// stringKeys returns the sorted keys of a string-keyed map for deterministic
// iteration.
func stringKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// queryPrometheus runs a single Prometheus instant query and returns the
// result list. aligned with DataHead._query_prometheus.
func (h *DataHead) queryPrometheus(ctx context.Context, template, metric, groupBy string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(template, metric, h.cfg.SessionName, groupBy)
	reqURL := fmt.Sprintf("%s/api/v1/query?query=%s", h.prometheusHost, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range h.prometheusHeaders {
		req.Header.Set(k, v)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		// Aligned with the Python PrometheusQueryError (metrics_head.py): a
		// non-200 response raises an error that is NOT swallowed by the inner
		// ClientConnectorError handler, so it surfaces as a 503 (unlike a
		// connection error, which keeps the zero values and returns 200).
		return nil, &prometheusQueryError{status: resp.StatusCode, message: string(body)}
	}
	var promData struct {
		Data struct {
			Result []map[string]interface{} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &promData); err != nil {
		return nil, err
	}
	return promData.Data.Result, nil
}

// metricValue extracts the metric label value from a Prometheus result.
func metricValue(res map[string]interface{}, label string) string {
	metric, _ := res["metric"].(map[string]interface{})
	if v, ok := metric[label].(string); ok {
		return v
	}
	return ""
}

// resValue extracts the sample value (res["value"][1]) from a Prometheus
// result as a string, aligned with the Python code which assigns it directly.
func resValue(res map[string]interface{}) interface{} {
	value, _ := res["value"].([]interface{})
	if len(value) >= 2 {
		return value[1]
	}
	return float64(0)
}

// numValue converts a JSON number into a float64 for sorting.
func numValue(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case json.Number:
		f, _ := n.Float64()
		return f
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case string:
		var f float64
		_, _ = fmt.Sscanf(n, "%f", &f)
		return f
	}
	return 0
}

// getEnv returns the environment variable or a default.
func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// isPrometheusQueryError reports whether err is a prometheusQueryError (a
// non-200 Prometheus response that must surface as 503, unlike a connection
// error which keeps the zero values and returns 200).
func isPrometheusQueryError(err error) bool {
	var qe *prometheusQueryError
	return errors.As(err, &qe)
}

// parsePromHeaders parses the RAY_PROMETHEUS_HEADERS JSON into a header map,
// aligned with parse_prom_headers in metrics_head.py. It accepts either an
// object {H1: V1} or an array [[H1, V1], ...]. Invalid input falls back to an
// empty map.
func parsePromHeaders(raw string) map[string]string {
	out := map[string]string{}
	if strings.TrimSpace(raw) == "" {
		return out
	}
	var asMap map[string]string
	if err := json.Unmarshal([]byte(raw), &asMap); err == nil {
		return asMap
	}
	var asList [][]string
	if err := json.Unmarshal([]byte(raw), &asList); err == nil {
		for _, pair := range asList {
			if len(pair) == 2 {
				out[pair[0]] = pair[1]
			}
		}
	}
	return out
}
