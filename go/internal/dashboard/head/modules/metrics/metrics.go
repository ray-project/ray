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

// Package metrics implements the MetricsHead dashboard module that probes the
// Grafana and Prometheus health endpoints, aligned with the Python MetricsHead
// in python/ray/dashboard/modules/metrics/metrics_head.py.
package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/head"
)

// Environment/config constants aligned with metrics_head.py.
const (
	grafanaHostEnvVar         = "RAY_GRAFANA_HOST"
	defaultGrafanaHost        = "http://localhost:3000"
	grafanaHostDisabledValue  = "DISABLED"
	grafanaHealthcheckPath    = "api/health"
	grafanaOrgIDEnvVar        = "RAY_GRAFANA_ORG_ID"
	defaultGrafanaOrgID       = "1"
	grafanaIframeHostEnvVar   = "RAY_GRAFANA_IFRAME_HOST"
	prometheusNameEnvVar      = "RAY_PROMETHEUS_NAME"
	defaultPrometheusName     = "Prometheus"
	grafanaClusterFilterEnv   = "RAY_GRAFANA_CLUSTER_FILTER"
	prometheusHostEnvVar      = "RAY_PROMETHEUS_HOST"
	defaultPrometheusHost     = "http://localhost:9090"
	prometheusHeadersEnvVar   = "RAY_PROMETHEUS_HEADERS"
	defaultPrometheusHeaders  = "{}"
	prometheusHealthcheckPath = "-/healthy"
)

// httpTimeout bounds the health probe requests.
const httpTimeout = 5 * time.Second

// MetricsHead probes Grafana and Prometheus health.
type MetricsHead struct {
	cfg               *head.HeadConfig
	grafanaHost       string
	grafanaIframeHost string
	prometheusHost    string
	prometheusName    string
	prometheusHeaders map[string]string
	grafanaOrgID      string
	// grafanaClusterFilter is a *string so an unset RAY_GRAFANA_CLUSTER_FILTER
	// encodes as JSON null in the /api/grafana_health response, aligned with
	// the Python os.environ.get(...) returning None (metrics_head.py).
	grafanaClusterFilter *string
	httpClient           *http.Client
}

// New creates a MetricsHead. Config values are read from the environment at
// construction, aligned with the Python __init__.
func New(cfg *head.HeadConfig, _ *head.GCSClient) *MetricsHead {
	m := &MetricsHead{
		cfg:               cfg,
		grafanaHost:       getEnv(grafanaHostEnvVar, defaultGrafanaHost),
		grafanaIframeHost: os.Getenv(grafanaIframeHostEnvVar),
		prometheusHost:    getEnv(prometheusHostEnvVar, defaultPrometheusHost),
		prometheusName:    getEnv(prometheusNameEnvVar, defaultPrometheusName),
		prometheusHeaders: parsePromHeaders(getEnv(prometheusHeadersEnvVar, defaultPrometheusHeaders)),
		grafanaOrgID:      getEnv(grafanaOrgIDEnvVar, defaultGrafanaOrgID),
		httpClient:        &http.Client{Timeout: httpTimeout},
	}
	if v := os.Getenv(grafanaClusterFilterEnv); v != "" {
		m.grafanaClusterFilter = &v
	}
	return m
}

// Name returns the module name.
func (m *MetricsHead) Name() string { return "MetricsHead" }

// Start has no background tasks.
func (m *MetricsHead) Start(ctx context.Context) error { return nil }

// Healthy reports the module health.
func (m *MetricsHead) Healthy() bool { return true }

// RegisterHTTP registers the metrics health routes.
func (m *MetricsHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("GET /api/grafana_health", m.handleGrafanaHealth)
	mux.HandleFunc("GET /api/prometheus_health", m.handlePrometheusHealth)
	return nil
}

// handleGrafanaHealth serves /api/grafana_health. When Grafana is disabled via
// RAY_GRAFANA_HOST=DISABLED, it returns the disabled marker; otherwise it
// probes {host}/api/health and checks the database status.
func (m *MetricsHead) handleGrafanaHealth(w http.ResponseWriter, r *http.Request) {
	if m.grafanaHost == grafanaHostDisabledValue {
		head.RESTResponseCamel(w, http.StatusOK, "Grafana disabled", map[string]interface{}{
			"grafana_host": grafanaHostDisabledValue,
		})
		return
	}
	url := m.grafanaHost + "/" + grafanaHealthcheckPath
	resp, err := m.httpClient.Get(url)
	if err != nil {
		head.RESTResponseCamel(w, http.StatusInternalServerError, "Grafana healthcheck failed", map[string]interface{}{
			"exception": err.Error(),
		})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		head.RESTResponseCamel(w, http.StatusInternalServerError, "Grafana healthcheck failed", map[string]interface{}{
			"status": resp.StatusCode,
		})
		return
	}
	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		head.RESTResponseCamel(w, http.StatusInternalServerError, "Grafana healthcheck failed", map[string]interface{}{
			"status": resp.StatusCode,
		})
		return
	}
	if db, _ := body["database"].(string); db != "ok" {
		head.RESTResponseCamel(w, http.StatusInternalServerError, "Grafana healthcheck failed. Database not ok.", map[string]interface{}{
			"status": resp.StatusCode,
			"json":   body,
		})
		return
	}
	// The iframe host defaults to the grafana host when
	// RAY_GRAFANA_IFRAME_HOST is unset, resolved at request time (aligned with
	// metrics_head.py).
	grafanaIframeHost := m.grafanaIframeHost
	if grafanaIframeHost == "" {
		grafanaIframeHost = m.grafanaHost
	}
	// grafana_cluster_filter is a *string: nil (unset env) marshals to JSON
	// null, aligned with the Python os.environ.get returning None.
	grafanaClusterFilter := interface{}(nil)
	if m.grafanaClusterFilter != nil {
		grafanaClusterFilter = *m.grafanaClusterFilter
	}
	head.RESTResponseCamel(w, http.StatusOK, "Grafana running", map[string]interface{}{
		"grafana_host":           grafanaIframeHost,
		"grafana_org_id":         m.grafanaOrgID,
		"session_name":           m.cfg.SessionName,
		"dashboard_uids":         m.dashboardUIDs(),
		"dashboard_datasource":   m.prometheusName,
		"grafana_cluster_filter": grafanaClusterFilter,
	})
}

// dashboardUIDs returns the dashboard uid map for the six Grafana dashboards
// Ray ships, aligned with metrics_head.py _create_default_grafana_configs.
// Each uid supports the RAY_GRAFANA_{name}_DASHBOARD_UID env override used by
// grafana_dashboard_factory.py.
func (m *MetricsHead) dashboardUIDs() map[string]interface{} {
	return map[string]interface{}{
		"default":          dashboardUID("default", "rayDefaultDashboard"),
		"serve":            dashboardUID("serve", "rayServeDashboard"),
		"serve_deployment": dashboardUID("serve_deployment", "rayServeDeploymentDashboard"),
		"serve_llm":        dashboardUID("serve_llm", "rayServeLlmDashboard"),
		"data":             dashboardUID("data", "rayDataDashboard"),
		"train":            dashboardUID("train", "rayTrainDashboard"),
	}
}

// dashboardUID returns the effective Grafana dashboard uid for the given name:
// the RAY_GRAFANA_{name}_DASHBOARD_UID env override when set, otherwise the
// default uid (aligned with _read_configs_for_dashboard in
// grafana_dashboard_factory.py).
func dashboardUID(name, defaultUID string) string {
	if v := os.Getenv("RAY_GRAFANA_" + strings.ToUpper(name) + "_DASHBOARD_UID"); v != "" {
		return v
	}
	return defaultUID
}

// handlePrometheusHealth serves /api/prometheus_health. It probes
// {host}/-/healthy with the configured RAY_PROMETHEUS_HEADERS and returns a
// plain rest_response on success (aligned with metrics_head.py).
func (m *MetricsHead) handlePrometheusHealth(w http.ResponseWriter, r *http.Request) {
	url := m.prometheusHost + "/" + prometheusHealthcheckPath
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	if err != nil {
		head.RESTResponseCamel(w, http.StatusInternalServerError, "prometheus healthcheck failed.", map[string]interface{}{
			"reason": err.Error(),
		})
		return
	}
	for k, v := range m.prometheusHeaders {
		req.Header.Set(k, v)
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		head.RESTResponseCamel(w, http.StatusInternalServerError, "prometheus healthcheck failed.", map[string]interface{}{
			"reason": err.Error(),
		})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		head.RESTResponseCamel(w, http.StatusInternalServerError, "prometheus healthcheck failed.", map[string]interface{}{
			"status": resp.StatusCode,
		})
		return
	}
	head.RESTResponseCamel(w, http.StatusOK, "prometheus running", nil)
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

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
