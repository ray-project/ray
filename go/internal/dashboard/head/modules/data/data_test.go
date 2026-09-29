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

package data

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/head"
)

// fakeStatsActor simulates the Ray Data _StatsActor for unit tests.
type fakeStatsActor struct {
	mu          sync.Mutex
	datasets    map[string]interface{}
	datasetsErr error
	lastJobID   string
}

func newFakeStatsActor() *fakeStatsActor {
	return &fakeStatsActor{}
}

func (a *fakeStatsActor) getDatasets(jobID string) (map[string]interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastJobID = jobID
	if a.datasetsErr != nil {
		return nil, a.datasetsErr
	}
	return a.datasets, nil
}

// setStatsActor installs a fake stats actor via the package-level getter.
func setStatsActor(t *testing.T, a statsActor) {
	old := getStatsActorFn
	getStatsActorFn = func() (statsActor, error) {
		if a == nil {
			return nil, fmt.Errorf("no stats actor")
		}
		return a, nil
	}
	t.Cleanup(func() { getStatsActorFn = old })
}

// newDataHead builds a DataHead pointing at the given Prometheus host.
func newDataHead(prometheusHost string) *DataHead {
	h := New(&head.HeadConfig{SessionName: "session_test"}, nil)
	if prometheusHost != "" {
		h.prometheusHost = prometheusHost
	}
	return h
}

// serve performs an HTTP request against the DataHead mux.
func serve(t *testing.T, h *DataHead, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	if err := h.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", path, nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

// promResult builds a single Prometheus query result entry.
func promResult(labels map[string]string, value string) map[string]interface{} {
	metric := map[string]interface{}{}
	for k, v := range labels {
		metric[k] = v
	}
	return map[string]interface{}{
		"metric": metric,
		"value":  []interface{}{float64(1726000000), value},
	}
}

// startPromServer spins up a mock Prometheus server that returns the given
// results for every query.
func startPromServer(t *testing.T, results []map[string]interface{}, queryLog *[]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if queryLog != nil {
			*queryLog = append(*queryLog, r.URL.Query().Get("query"))
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"data":   map[string]interface{}{"result": results},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestDataHeadNameAndHealth(t *testing.T) {
	h := New(&head.HeadConfig{}, nil)
	if h.Name() != "DataHead" {
		t.Fatalf("name = %q", h.Name())
	}
	if !h.Healthy() {
		t.Fatal("should be healthy")
	}
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestDatasetsNoStatsActor(t *testing.T) {
	setStatsActor(t, nil)
	h := newDataHead("")
	rr := serve(t, h, "/api/data/datasets/job1")
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 when no stats actor", rr.Code)
	}
}

func TestDatasetsDecorate(t *testing.T) {
	fa := newFakeStatsActor()
	fa.datasets = map[string]interface{}{
		"ds1": map[string]interface{}{
			"job_id": "job1", "state": "RUNNING",
			"progress": float64(50), "total": float64(100),
			"total_rows": float64(10), "start_time": float64(1000),
			"end_time": nil,
			"operators": map[string]interface{}{
				"read": map[string]interface{}{
					"state": "RUNNING", "progress": float64(25),
					"total": float64(4), "queued_blocks": float64(1),
				},
			},
		},
		"ds2": map[string]interface{}{
			"job_id": "job1", "state": "FINISHED",
			"progress": float64(100), "total": float64(100),
			"total_rows": float64(20), "start_time": float64(2000),
			"end_time":  float64(3000),
			"operators": map[string]interface{}{},
		},
	}
	setStatsActor(t, fa)

	// Mock Prometheus: ds1 gets a value for the dataset-level query of
	// ray_data_current_bytes.
	results := []map[string]interface{}{
		promResult(map[string]string{"dataset": "ds1"}, "42"),
	}
	srv := startPromServer(t, results, nil)
	h := newDataHead(srv.URL)

	rr := serve(t, h, "/api/data/datasets/job1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Datasets []map[string]interface{} `json:"datasets"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	// Sorted by start_time descending: ds2 first.
	if len(resp.Datasets) != 2 {
		t.Fatalf("datasets = %+v", resp.Datasets)
	}
	if resp.Datasets[0]["dataset"] != "ds2" || resp.Datasets[1]["dataset"] != "ds1" {
		t.Fatalf("order = %v, %v", resp.Datasets[0]["dataset"], resp.Datasets[1]["dataset"])
	}
	// The mock prometheus returns ds1 for every query, so all metrics on ds1
	// get the value where present.
	ds1 := resp.Datasets[1]
	cur, ok := ds1["ray_data_current_bytes"].(map[string]interface{})
	if !ok {
		t.Fatalf("ray_data_current_bytes = %+v", ds1["ray_data_current_bytes"])
	}
	if cur["value"] != "42" {
		t.Fatalf("current_bytes value = %v", cur["value"])
	}
	// ds2 should keep zero metrics (no prometheus result for ds2).
	ds2 := resp.Datasets[0]
	out, ok := ds2["ray_data_output_rows"].(map[string]interface{})
	if !ok || out["max"] != float64(0) {
		t.Fatalf("ds2 output_rows = %+v", ds2["ray_data_output_rows"])
	}
	// Operators should be flattened into a list.
	ops, ok := ds1["operators"].([]interface{})
	if !ok || len(ops) != 1 {
		t.Fatalf("ds1 operators = %+v", ds1["operators"])
	}
	op := ops[0].(map[string]interface{})
	if op["operator"] != "read" {
		t.Fatalf("op = %+v", op)
	}
	// The mock prometheus returns results without the operator label, so the
	// operator-level metrics keep their zero values.
	if opMetric, ok := op["ray_data_cpu_usage_cores"].(map[string]interface{}); !ok || opMetric["value"] != float64(0) {
		t.Fatalf("op cpu = %+v", op["ray_data_cpu_usage_cores"])
	}
}

// TestDatasetsOperatorPrometheus verifies that operator-level Prometheus
// results (carrying the operator label) are applied to the matching operator.
func TestDatasetsOperatorPrometheus(t *testing.T) {
	fa := newFakeStatsActor()
	fa.datasets = map[string]interface{}{
		"ds1": map[string]interface{}{
			"job_id": "job1", "state": "RUNNING",
			"progress": float64(50), "total": float64(100),
			"total_rows": float64(10), "start_time": float64(1000),
			"end_time": nil,
			"operators": map[string]interface{}{
				"read": map[string]interface{}{
					"state": "RUNNING", "progress": float64(25),
					"total": float64(4), "queued_blocks": float64(1),
				},
			},
		},
	}
	setStatsActor(t, fa)
	results := []map[string]interface{}{
		promResult(map[string]string{"dataset": "ds1", "operator": "read"}, "7"),
	}
	srv := startPromServer(t, results, nil)
	h := newDataHead(srv.URL)

	rr := serve(t, h, "/api/data/datasets/job1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Datasets []map[string]interface{} `json:"datasets"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	ds1 := resp.Datasets[0]
	ops := ds1["operators"].([]interface{})
	op := ops[0].(map[string]interface{})
	if opMetric, ok := op["ray_data_current_bytes"].(map[string]interface{}); !ok || opMetric["value"] != "7" {
		t.Fatalf("op current_bytes = %+v", op["ray_data_current_bytes"])
	}
}

func TestDatasetsActorError(t *testing.T) {
	fa := newFakeStatsActor()
	fa.datasetsErr = context.Canceled
	setStatsActor(t, fa)
	h := newDataHead("")
	rr := serve(t, h, "/api/data/datasets/job1")
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
}

// TestDatasetsPrometheusNon200 verifies the B-D1 behavior: a Prometheus
// non-200 response surfaces as a 503 (mirroring the Python PrometheusQueryError
// propagating to the outer except Exception), unlike a connection error which
// keeps the zero values and returns 200.
func TestDatasetsPrometheusNon200(t *testing.T) {
	fa := newFakeStatsActor()
	fa.datasets = map[string]interface{}{
		"ds1": map[string]interface{}{
			"job_id": "job1", "state": "RUNNING",
			"progress": float64(50), "total": float64(100),
			"total_rows": float64(10), "start_time": float64(1000),
			"end_time":  nil,
			"operators": map[string]interface{}{},
		},
	}
	setStatsActor(t, fa)

	// A mock Prometheus that returns HTTP 500 (e.g. server error).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal prometheus error", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	h := newDataHead(srv.URL)
	rr := serve(t, h, "/api/data/datasets/job1")
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "Error fetching data from prometheus") {
		t.Fatalf("body = %s", rr.Body.String())
	}
}

// TestDatasetsPrometheusConnectionError verifies that a Prometheus connection
// error keeps the zero values and returns 200, aligned with the Python
// ClientConnectorError handler.
func TestDatasetsPrometheusConnectionError(t *testing.T) {
	fa := newFakeStatsActor()
	fa.datasets = map[string]interface{}{
		"ds1": map[string]interface{}{
			"job_id": "job1", "state": "RUNNING",
			"progress": float64(50), "total": float64(100),
			"total_rows": float64(10), "start_time": float64(1000),
			"end_time":  nil,
			"operators": map[string]interface{}{},
		},
	}
	setStatsActor(t, fa)

	// A closed listener so the connection is refused.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	h := newDataHead("http://" + addr)
	rr := serve(t, h, "/api/data/datasets/job1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
}

func TestDatasetsPassesJobID(t *testing.T) {
	fa := newFakeStatsActor()
	fa.datasets = map[string]interface{}{}
	setStatsActor(t, fa)
	h := newDataHead("")
	rr := serve(t, h, "/api/data/datasets/myjob")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	fa.mu.Lock()
	jobID := fa.lastJobID
	fa.mu.Unlock()
	if jobID != "myjob" {
		t.Fatalf("job id passed to actor = %q", jobID)
	}
}

func TestPromQueryTemplate(t *testing.T) {
	got := fmt.Sprintf(promQueryTemplate("value"), "ray_data_current_bytes", "session_test", "dataset")
	want := "sum(ray_data_current_bytes{SessionName='session_test'}) by (dataset)"
	if got != want {
		t.Fatalf("value template = %q, want %q", got, want)
	}
	got = fmt.Sprintf(promQueryTemplate("max"), "ray_data_output_rows", "session_test", "dataset, operator")
	want = "max_over_time(sum(ray_data_output_rows{SessionName='session_test'}) by (dataset, operator))[1h:1s]"
	if got != want {
		t.Fatalf("max template = %q, want %q", got, want)
	}
}

func TestParsePromHeaders(t *testing.T) {
	m := parsePromHeaders(`{"H1": "V1", "H2": "V2"}`)
	if m["H1"] != "V1" || m["H2"] != "V2" {
		t.Fatalf("headers = %+v", m)
	}
	m = parsePromHeaders(`[["H1", "V1"], ["H2", "V2"]]`)
	if m["H1"] != "V1" || m["H2"] != "V2" {
		t.Fatalf("list headers = %+v", m)
	}
	m = parsePromHeaders("")
	if len(m) != 0 {
		t.Fatalf("empty headers = %+v", m)
	}
}
