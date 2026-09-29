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

package event

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/head"
)

func newTestHead() *EventHead {
	return New(&head.HeadConfig{}, nil)
}

// eventsBody JSON-encodes the given raw event strings into the JSON array
// body the agent posts (each event is a JSON-encoded string).
func eventsBody(events ...string) string {
	arr := make([]string, len(events))
	for i, e := range events {
		b, _ := json.Marshal(e)
		arr[i] = string(b)
	}
	return "[" + strings.Join(arr, ",") + "]"
}

func post(t *testing.T, e *EventHead, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	if err := e.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/report_events", strings.NewReader(body))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

func get(t *testing.T, e *EventHead, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	if err := e.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", path, nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

func TestEventHeadName(t *testing.T) {
	e := newTestHead()
	if e.Name() != "EventHead" {
		t.Fatalf("name = %q", e.Name())
	}
	if !e.Healthy() {
		t.Fatal("should be healthy")
	}
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReportEventsParsingAndDedup(t *testing.T) {
	e := newTestHead()
	ev1 := `{"event_id":"e1","timestamp":100,"severity":"INFO","source_type":"RAY","message":"hello","custom_fields":{"job_id":"job1"}}`
	ev2 := `{"event_id":"e2","timestamp":200,"severity":"WARNING","source_type":"RAY","message":"world","custom_fields":{"job_id":"job1"}}`
	// e1 repeated should dedup (no growth), plus an invalid line and an empty string.
	rr := post(t, e, eventsBody(ev1, ev2, ev1, `not json`, ``))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	// /events without job_id
	r2 := get(t, e, "/events")
	var body struct {
		Data struct {
			Events map[string][]map[string]interface{} `json:"events"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r2.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Events) != 1 {
		t.Fatalf("jobs = %d, want 1", len(body.Data.Events))
	}
	jobs, ok := body.Data.Events["job1"]
	if !ok {
		t.Fatalf("missing job1, got %v", body.Data.Events)
	}
	if len(jobs) != 2 {
		t.Fatalf("events = %d, want 2 (deduped)", len(jobs))
	}
}

// TestReportEventsObjectArrayIgnored verifies a JSON array of objects is
// accepted with 200 and the objects are skipped, aligned with Python
// report_events where parse_event_strings catches and ignores json.loads
// failures on non-string elements.
func TestReportEventsObjectArrayIgnored(t *testing.T) {
	e := newTestHead()
	rr := post(t, e, `[{"a":1}]`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (object array ignored), body=%s", rr.Code, rr.Body.String())
	}
	// No events cached from the skipped object.
	r := get(t, e, "/events")
	var body struct {
		Data struct {
			Events map[string][]interface{} `json:"events"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Events) != 0 {
		t.Fatalf("events = %v, want empty (object skipped)", body.Data.Events)
	}
}

// TestReportEventsNonListRejected verifies a non-array body returns 400,
// aligned with Python report_events raising HTTPBadRequest.
func TestReportEventsNonListRejected(t *testing.T) {
	e := newTestHead()
	rr := post(t, e, `{"a":1}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for object body", rr.Code)
	}
}

// TestReportEventsSuccessData verifies a successful report_events response
// carries data {"success": true}, aligned with the Python handler.
func TestReportEventsSuccessData(t *testing.T) {
	e := newTestHead()
	rr := post(t, e, eventsBody(`{"event_id":"s1","timestamp":100,"severity":"INFO","source_type":"RAY","message":"hi"}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		Data struct {
			Success bool `json:"success"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Data.Success {
		t.Fatalf("data.success = false, want true: %s", rr.Body.String())
	}
}

func TestEventsNumericJobIDGrouped(t *testing.T) {
	e := newTestHead()
	// Python keeps any truthy custom_fields.job_id value (including a number)
	// as the group key; only empty/None falls back to "global".
	ev := `{"event_id":"n1","timestamp":100,"severity":"INFO","source_type":"RAY","message":"num","custom_fields":{"job_id":42}}`
	post(t, e, eventsBody(ev))
	r := get(t, e, "/events")
	var body struct {
		Data struct {
			Events map[string][]map[string]interface{} `json:"events"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body.Data.Events["global"]; ok {
		t.Fatalf("numeric job_id 42 should not fall back to global, got %v", body.Data.Events)
	}
	if evs, ok := body.Data.Events["42"]; !ok || len(evs) != 1 {
		t.Fatalf("numeric job_id should group under %q, got %v", "42", body.Data.Events)
	}
}

func TestEventsGlobalJobID(t *testing.T) {
	e := newTestHead()
	ev := `{"event_id":"g1","timestamp":100,"severity":"INFO","source_type":"RAY","message":"no custom fields"}`
	post(t, e, eventsBody(ev))
	r := get(t, e, "/events")
	var body struct {
		Data struct {
			Events map[string][]map[string]interface{} `json:"events"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body.Data.Events["global"]; !ok {
		t.Fatalf("missing global job, got %v", body.Data.Events)
	}
}

func TestEventsByJobID(t *testing.T) {
	e := newTestHead()
	ev := `{"event_id":"e1","timestamp":100,"severity":"INFO","source_type":"RAY","message":"hello","custom_fields":{"job_id":"jobX"}}`
	post(t, e, eventsBody(ev))
	r := get(t, e, "/events?job_id=jobX")
	var body struct {
		Data struct {
			JobID  string                   `json:"jobId"`
			Events []map[string]interface{} `json:"events"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.JobID != "jobX" || len(body.Data.Events) != 1 {
		t.Fatalf("job_id = %q, events = %d", body.Data.JobID, len(body.Data.Events))
	}
	// /events uses the camelCase rest_response form, so nested event keys are
	// converted too (event_id -> eventId, source_type -> sourceType).
	if body.Data.Events[0]["eventId"] != "e1" {
		t.Fatalf("eventId = %v", body.Data.Events[0]["eventId"])
	}
}

func TestEventsEviction(t *testing.T) {
	// Use a small cache by lowering the constant via a direct field test:
	// verify the FIFO eviction by inserting more than max into one job through
	// the private updateEvents with an overridden limit. We temporarily patch
	// via the package-level constant by manipulating the struct directly.
	e := newTestHead()
	// Insert MAX*1.1+1 events into one job: the update triggers the eviction
	// (len > MAX*1.1) and pops down to exactly MAX, dropping the oldest.
	for i := 0; i < int(float64(maxEventsToCache)*1.1)+1; i++ {
		ev := map[string]interface{}{
			"event_id":      "a" + intToString(i),
			"timestamp":     float64(i),
			"severity":      "INFO",
			"source_type":   "RAY",
			"custom_fields": map[string]interface{}{"job_id": "jobE"},
		}
		e.updateEvents([]map[string]interface{}{ev})
	}
	e.mu.RLock()
	got := len(e.events["jobE"].order)
	_, hasOldest := e.events["jobE"].events["a0"]
	e.mu.RUnlock()
	if got != maxEventsToCache {
		t.Fatalf("cached = %d, want %d", got, maxEventsToCache)
	}
	if hasOldest {
		t.Fatal("oldest event should have been evicted")
	}
}

func TestClusterEventsListAPI(t *testing.T) {
	e := newTestHead()
	ev1 := `{"event_id":"e1","timestamp":100,"severity":"INFO","source_type":"RAY","message":"first","custom_fields":{"job_id":"job1"}}`
	ev2 := `{"event_id":"e2","timestamp":200,"severity":"WARNING","source_type":"RAY","message":"second","custom_fields":{"job_id":"job2"}}`
	ev3 := `{"event_id":"e3","timestamp":300,"severity":"ERROR","source_type":"CORE","message":"third","custom_fields":{"job_id":"job3"}}`
	post(t, e, eventsBody(ev1, ev2, ev3))

	r := get(t, e, "/api/v0/cluster_events?filter_keys=severity&filter_predicates=%3D&filter_values=ERROR&limit=10")
	if r.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", r.Code)
	}
	var body struct {
		Data struct {
			Result struct {
				Result             []map[string]interface{} `json:"result"`
				Total              int                      `json:"total"`
				NumAfterTruncation int                      `json:"num_after_truncation"`
				NumFiltered        int                      `json:"num_filtered"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Result.Result) != 1 {
		t.Fatalf("filtered = %d, want 1", len(body.Data.Result.Result))
	}
	if body.Data.Result.Result[0]["event_id"] != "e3" {
		t.Fatalf("event_id = %v", body.Data.Result.Result[0]["event_id"])
	}
	if body.Data.Result.Total != 3 || body.Data.Result.NumAfterTruncation != 3 || body.Data.Result.NumFiltered != 1 {
		t.Fatalf("counters = %+v", body.Data.Result)
	}
	// time field must be set from timestamp
	if _, ok := body.Data.Result.Result[0]["time"].(string); !ok {
		t.Fatalf("time field missing: %+v", body.Data.Result.Result[0])
	}
}

// TestClusterEventsEmptyResultIsEmptyList verifies an empty cluster_events
// result serializes as [] (not null), aligned with the Python ListApiResponse.
func TestClusterEventsEmptyResultIsEmptyList(t *testing.T) {
	e := newTestHead()
	r := get(t, e, "/api/v0/cluster_events")
	if r.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", r.Code)
	}
	var body struct {
		Data struct {
			Result struct {
				Result []map[string]interface{} `json:"result"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	// The result array must be [] (non-nil), not null.
	if body.Data.Result.Result == nil || len(body.Data.Result.Result) != 0 {
		t.Fatalf("result = %#v, want empty non-nil list", body.Data.Result.Result)
	}
}

// TestClusterEventsTrimsExtraFields verifies base output keeps only the
// ClusterEventState schema columns (severity/time/source_type/message/event_id),
// dropping label/source_hostname/source_pid/timestamp, aligned with filter_fields
// in python/ray/util/state/common.py.
func TestClusterEventsTrimsExtraFields(t *testing.T) {
	e := newTestHead()
	ev := `{"event_id":"t1","timestamp":100,"severity":"INFO","source_type":"RAY","message":"hello","label":"x","source_hostname":"h1","source_pid":123,"custom_fields":{"job_id":"jobT"}}`
	post(t, e, eventsBody(ev))

	r := get(t, e, "/api/v0/cluster_events")
	var body struct {
		Data struct {
			Result struct {
				Result []map[string]interface{} `json:"result"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Result.Result) != 1 {
		t.Fatalf("got %d events", len(body.Data.Result.Result))
	}
	entry := body.Data.Result.Result[0]
	base := map[string]bool{"severity": true, "time": true, "source_type": true, "message": true, "event_id": true}
	for k := range entry {
		if !base[k] {
			t.Fatalf("base output must only contain schema columns, got %q: %v", k, entry)
		}
	}
	if entry["event_id"] != "t1" || entry["message"] != "hello" {
		t.Fatalf("entry = %v", entry)
	}
	if _, ok := entry["time"].(string); !ok {
		t.Fatalf("time field missing: %v", entry)
	}

	// detail=true keeps custom_fields.
	r = get(t, e, "/api/v0/cluster_events?detail=true")
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body.Data.Result.Result[0]["custom_fields"].(map[string]interface{}); !ok {
		t.Fatalf("detail=true should keep custom_fields: %v", body.Data.Result.Result[0])
	}
	if _, ok := body.Data.Result.Result[0]["label"]; ok {
		t.Fatalf("detail=true should still drop label: %v", body.Data.Result.Result[0])
	}
}

func TestClusterEventsSortByTimestamp(t *testing.T) {
	e := newTestHead()
	// Insert out of order; the result must be sorted by timestamp.
	ev1 := `{"event_id":"e1","timestamp":300,"severity":"INFO","source_type":"RAY","message":"third","custom_fields":{"job_id":"j"}}`
	ev2 := `{"event_id":"e2","timestamp":100,"severity":"INFO","source_type":"RAY","message":"first","custom_fields":{"job_id":"j"}}`
	ev3 := `{"event_id":"e3","timestamp":200,"severity":"INFO","source_type":"RAY","message":"second","custom_fields":{"job_id":"j"}}`
	post(t, e, eventsBody(ev1, ev2, ev3))
	r := get(t, e, "/api/v0/cluster_events")
	var body struct {
		Data struct {
			Result struct {
				Result []map[string]interface{} `json:"result"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Result.Result) != 3 {
		t.Fatalf("got %d events", len(body.Data.Result.Result))
	}
	if body.Data.Result.Result[0]["event_id"] != "e2" ||
		body.Data.Result.Result[1]["event_id"] != "e3" ||
		body.Data.Result.Result[2]["event_id"] != "e1" {
		t.Fatalf("not sorted by timestamp: %v", body.Data.Result.Result)
	}
}

func TestClusterEventsDoesNotPolluteCache(t *testing.T) {
	e := newTestHead()
	ev := `{"event_id":"c1","timestamp":100,"severity":"INFO","source_type":"RAY","message":"hello","custom_fields":{"job_id":"jobC"}}`
	post(t, e, eventsBody(ev))

	// After a cluster_events call the returned event carries the time field...
	r := get(t, e, "/api/v0/cluster_events")
	var body struct {
		Data struct {
			Result struct {
				Result []map[string]interface{} `json:"result"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Result.Result) != 1 {
		t.Fatalf("got %d events", len(body.Data.Result.Result))
	}
	if _, ok := body.Data.Result.Result[0]["time"].(string); !ok {
		t.Fatal("cluster_events result should carry the time field")
	}

	// ...but the shared cache must NOT have been mutated (no time field).
	e.mu.RLock()
	cached := e.events["jobC"].events["c1"]
	_, polluted := cached["time"]
	e.mu.RUnlock()
	if polluted {
		t.Fatal("cache event must not gain the time field (shallow copy missing)")
	}
}

func TestClusterEventsRejectsUnknownFilter(t *testing.T) {
	e := newTestHead()
	r := get(t, e, "/api/v0/cluster_events?filter_keys=bad_col&filter_predicates=%3D&filter_values=x")
	if r.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", r.Code)
	}
}

// TestClusterEventsDetailFiltersCustomFields verifies detail=false (the
// default) drops the custom_fields column, aligned with filter_fields in
// python/ray/util/state/common.py.
func TestClusterEventsDetailFiltersCustomFields(t *testing.T) {
	e := newTestHead()
	ev := `{"event_id":"d1","timestamp":100,"severity":"INFO","source_type":"RAY","message":"hello","custom_fields":{"job_id":"jobD"}}`
	post(t, e, eventsBody(ev))

	// Default: detail=false -> no custom_fields.
	r := get(t, e, "/api/v0/cluster_events")
	var body struct {
		Data struct {
			Result struct {
				Result []map[string]interface{} `json:"result"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Result.Result) != 1 {
		t.Fatalf("got %d events", len(body.Data.Result.Result))
	}
	if _, ok := body.Data.Result.Result[0]["custom_fields"]; ok {
		t.Fatalf("detail=false should drop custom_fields: %v", body.Data.Result.Result[0])
	}

	// detail=true keeps custom_fields.
	r = get(t, e, "/api/v0/cluster_events?detail=true")
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body.Data.Result.Result[0]["custom_fields"].(map[string]interface{}); !ok {
		t.Fatalf("detail=true should keep custom_fields: %v", body.Data.Result.Result[0])
	}
}

// TestEventsUnknownJobEmptyList verifies /events?job_id=<unknown> returns an
// empty events list (not null), aligned with Python's defaultdict lookup.
func TestEventsUnknownJobEmptyList(t *testing.T) {
	e := newTestHead()
	r := get(t, e, "/events?job_id=does_not_exist")
	var body struct {
		Data struct {
			Events []interface{} `json:"events"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.Events == nil {
		t.Fatalf("events should be [] not null: %s", r.Body.String())
	}
	if len(body.Data.Events) != 0 {
		t.Fatalf("events = %v, want empty", body.Data.Events)
	}
}

// TestClusterEventsRejectsExcessiveLimit verifies a limit above
// RAY_MAX_LIMIT_FROM_API_SERVER is rejected with 400, aligned with
// options_from_req in python/ray/dashboard/state_api_utils.py.
func TestClusterEventsRejectsExcessiveLimit(t *testing.T) {
	e := newTestHead()
	r := get(t, e, "/api/v0/cluster_events?limit=10001")
	if r.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", r.Code)
	}
}

func intToString(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		return "-" + string(b)
	}
	return string(b)
}

// writeEventLog writes one event JSON line to the given file, appending a
// trailing newline.
func writeEventLog(t *testing.T, path, eventID, message string) {
	t.Helper()
	line := `{"event_id": "` + eventID + `", "source_type": "JOBS", "source_hostname": "host", "source_pid": 1, "message": "` + message + `", "timestamp": "1789712090", "custom_fields": {"submission_id": "s1"}, "severity": "INFO", "label": ""}` + "\n"
	if err := os.WriteFile(path, []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestMonitorEventsReadsEventLog verifies the events directory scanner picks up
// event_*.log files written by agents and feeds them into the cache, and that a
// second scan does not duplicate already-read lines.
func TestMonitorEventsReadsEventLog(t *testing.T) {
	dir := t.TempDir()
	eventsDir := filepath.Join(dir, "events")
	if err := os.MkdirAll(eventsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logFile := filepath.Join(eventsDir, "event_JOBS.log")
	writeEventLog(t, logFile, "evt1", "Started a ray job.")

	e := New(&head.HeadConfig{LogDir: dir}, nil)
	// startMtime far in the past so the freshly written file is monitored.
	e.scanEvents(time.Now().Add(-time.Hour).Unix())

	// The first event is cached.
	r := get(t, e, "/events")
	var body map[string]interface{}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	events := body["data"].(map[string]interface{})["events"].(map[string]interface{})
	global, ok := events["global"].([]interface{})
	if !ok || len(global) != 1 {
		t.Fatalf("global events = %v, want 1", events)
	}

	// A second scan must not re-read the already consumed line.
	e.scanEvents(time.Now().Add(-time.Hour).Unix())
	r = get(t, e, "/events")
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	events = body["data"].(map[string]interface{})["events"].(map[string]interface{})
	global, ok = events["global"].([]interface{})
	if !ok || len(global) != 1 {
		t.Fatalf("global events after rescan = %v, want still 1", events)
	}
}

// TestMonitorEventsSkipsOtherFiles verifies files not matching a known source
// type pattern (or not a .log file) are ignored.
func TestMonitorEventsSkipsOtherFiles(t *testing.T) {
	dir := t.TempDir()
	eventsDir := filepath.Join(dir, "events")
	if err := os.MkdirAll(eventsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Non-event file in the directory should be ignored.
	if err := os.WriteFile(filepath.Join(eventsDir, "dashboard.log"), []byte("not an event\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	e := New(&head.HeadConfig{LogDir: dir}, nil)
	e.scanEvents(time.Now().Add(-time.Hour).Unix())

	r := get(t, e, "/events")
	var body map[string]interface{}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	events := body["data"].(map[string]interface{})["events"].(map[string]interface{})
	if len(events) != 0 {
		t.Fatalf("events = %v, want empty", events)
	}
}

// TestStartMonitorsWithNoLogDir verifies Start tolerates a missing LogDir
// without crashing.
func TestStartMonitorsWithNoLogDir(t *testing.T) {
	e := newTestHead()
	if err := e.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}
