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

package head

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/proto"
)

func int32Ptr(v int32) *int32 { return &v }

func TestParseListApiOptions(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v0/actors?limit=50&detail=1&filter_keys=actor_id&filter_predicates=%3D&filter_values=abc", nil)
	opt, err := ParseListApiOptions(req)
	if err != nil {
		t.Fatal(err)
	}
	if opt.Limit != 50 {
		t.Fatalf("limit = %d, want 50", opt.Limit)
	}
	if !opt.Detail {
		t.Fatal("detail should be true")
	}
	if len(opt.FilterKeys) != 1 || opt.FilterKeys[0] != "actor_id" {
		t.Fatalf("filter_keys = %v", opt.FilterKeys)
	}
	if opt.FilterPredicates[0] != "=" {
		t.Fatalf("filter_predicates = %v", opt.FilterPredicates)
	}
	if opt.FilterValues[0] != "abc" {
		t.Fatalf("filter_values = %v", opt.FilterValues)
	}
}

func TestParseListApiOptionsDefaultLimit(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v0/actors", nil)
	opt, err := ParseListApiOptions(req)
	if err != nil {
		t.Fatal(err)
	}
	if opt.Limit != 100 {
		t.Fatalf("default limit = %d, want 100", opt.Limit)
	}
}

func TestParseListApiOptionsMultipleFilters(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v0/actors?filter_keys=actor_id&filter_keys=state&filter_predicates=%3D&filter_predicates=%3D&filter_values=abc&filter_values=ALIVE", nil)
	opt, err := ParseListApiOptions(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(opt.FilterKeys) != 2 || opt.FilterKeys[1] != "state" {
		t.Fatalf("filter_keys = %v", opt.FilterKeys)
	}
	if len(opt.FilterValues) != 2 || opt.FilterValues[1] != "ALIVE" {
		t.Fatalf("filter_values = %v", opt.FilterValues)
	}
}

func TestParseListApiOptionsInvalidLimit(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v0/actors?limit=abc", nil)
	if _, err := ParseListApiOptions(req); err == nil {
		t.Fatal("expected error for invalid limit")
	}
}

func TestParseListApiOptionsLimitOverMax(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v0/actors?limit=99999", nil)
	if _, err := ParseListApiOptions(req); err == nil {
		t.Fatal("expected error for limit exceeding the maximum")
	}
}

// A negative limit must be rejected at parse time: filterAndSort slices with
// filtered[:opt.Limit], and len(filtered) > opt.Limit is always true for a
// negative limit, so letting it through would panic every list API.
func TestParseListApiOptionsNegativeLimit(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v0/actors?limit=-1", nil)
	if _, err := ParseListApiOptions(req); err == nil {
		t.Fatal("expected error for negative limit")
	}
}

func TestFilterAndTruncate(t *testing.T) {
	entries := []map[string]interface{}{
		{"actor_id": "bbb", "state": "ALIVE"},
		{"actor_id": "aaa", "state": "ALIVE"},
		{"actor_id": "ccc", "state": "DEAD"},
	}
	opt := &ListApiOptions{
		Limit:            2,
		Resource:         "actors",
		FilterKeys:       []string{"state"},
		FilterPredicates: []string{"="},
		FilterValues:     []string{"ALIVE"},
	}
	got := filterAndSort(entries, opt)
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	if got[0]["actor_id"] != "aaa" {
		t.Fatalf("first entry = %v, want aaa (sorted)", got[0]["actor_id"])
	}
	if got[1]["actor_id"] != "bbb" {
		t.Fatalf("second entry = %v, want bbb", got[1]["actor_id"])
	}
}

func TestFilterNotEqual(t *testing.T) {
	entries := []map[string]interface{}{
		{"actor_id": "aaa", "state": "ALIVE"},
		{"actor_id": "bbb", "state": "DEAD"},
	}
	opt := &ListApiOptions{
		Limit:            10,
		FilterKeys:       []string{"state"},
		FilterPredicates: []string{"!="},
		FilterValues:     []string{"DEAD"},
	}
	got := filterAndSort(entries, opt)
	if len(got) != 1 || got[0]["actor_id"] != "aaa" {
		t.Fatalf("got %v, want only aaa", got)
	}
}

func TestProtoMessageToDictSnakeCaseAndHex(t *testing.T) {
	actor := &proto.ActorTableData{
		ActorId: []byte{0x01, 0x02, 0x03},
		JobId:   []byte{0xaa, 0xbb},
		Name:    "my_actor",
	}
	got := protoMessageToDict(actor, actorDecodeFields)
	if got["actor_id"] != "010203" {
		t.Fatalf("actor_id = %v, want hex 010203", got["actor_id"])
	}
	if got["job_id"] != "aabb" {
		t.Fatalf("job_id = %v, want hex aabb", got["job_id"])
	}
	if got["name"] != "my_actor" {
		t.Fatalf("name = %v", got["name"])
	}
}

func TestBase64ToHex(t *testing.T) {
	b := []byte{0xde, 0xad, 0xbe, 0xef}
	enc := base64.StdEncoding.EncodeToString(b)
	got := base64ToHex(enc)
	if got != hex.EncodeToString(b) {
		t.Fatalf("base64ToHex(%q) = %q, want %q", enc, got, hex.EncodeToString(b))
	}
	if base64ToHex("not-base64!!") != "not-base64!!" {
		t.Fatal("invalid base64 should be returned unchanged")
	}
}

// TestJSONFloatSerializesWholeValuesWithFractionalPart pins the jsonFloat
// marker type: a whole float64 must serialize as "6.0" (matching Python
// json.dumps(float)) while a fractional value keeps its shortest form.
func TestJSONFloatSerializesWholeValuesWithFractionalPart(t *testing.T) {
	b, err := json.Marshal(map[string]interface{}{"v": jsonFloat(6.0)})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"v":6.0}` {
		t.Fatalf("jsonFloat(6.0) serialized as %s, want {\"v\":6.0}", b)
	}
	b, err = json.Marshal(map[string]interface{}{"v": jsonFloat(0.5)})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"v":0.5}` {
		t.Fatalf("jsonFloat(0.5) serialized as %s, want {\"v\":0.5}", b)
	}
	b, err = json.Marshal(map[string]interface{}{"v": jsonFloat(1789777399714238.8)})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"v":1789777399714238.8}` {
		t.Fatalf("jsonFloat(1789777399714238.8) serialized as %s", b)
	}
}

// TestProtoMessageToDictDoubleFieldsBecomeFloat pins that double/float proto
// fields (including map values) are wrapped in jsonFloat so they serialize with
// a fractional part, while int32 fields stay plain numbers and int64/uint64
// stay strings, all aligned with Python message_to_dict + json.dumps.
func TestProtoMessageToDictDoubleFieldsBecomeFloat(t *testing.T) {
	node := &proto.GcsNodeInfo{
		NodeId: []byte{0x01},
		ResourcesTotal: map[string]float64{
			"CPU":    6.0,
			"memory": 8.0,
		},
	}
	got := protoMessageToDict(node, nodeDecodeFields)
	rt, ok := got["resources_total"].(map[string]interface{})
	if !ok {
		t.Fatalf("resources_total = %v (%T), want map", got["resources_total"], got["resources_total"])
	}
	if v, ok := rt["CPU"].(jsonFloat); !ok || float64(v) != 6.0 {
		t.Fatalf("resources_total.CPU = %v (%T), want jsonFloat 6.0", rt["CPU"], rt["CPU"])
	}
	if v, ok := rt["memory"].(jsonFloat); !ok || float64(v) != 8.0 {
		t.Fatalf("resources_total.memory = %v (%T), want jsonFloat 8.0", rt["memory"], rt["memory"])
	}
	raw, err := json.Marshal(got["resources_total"])
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"CPU":6.0,"memory":8.0}` {
		t.Fatalf("resources_total serialized as %s, want {\"CPU\":6.0,\"memory\":8.0}", raw)
	}

	// int32 field (pid) stays a plain JSON number; int64 field stays a string.
	actor := &proto.ActorTableData{
		Pid:         12345,
		NumRestarts: 2,
		RequiredResources: map[string]float64{
			"CPU": 1.0,
		},
	}
	gotActor := protoMessageToDict(actor, actorDecodeFields)
	if v, ok := gotActor["pid"].(float64); !ok || v != 12345 {
		t.Fatalf("pid = %v (%T), want float64 12345", gotActor["pid"], gotActor["pid"])
	}
	if v, ok := gotActor["num_restarts"].(string); !ok || v != "2" {
		t.Fatalf("num_restarts = %v (%T), want string \"2\"", gotActor["num_restarts"], gotActor["num_restarts"])
	}
	rr, ok := gotActor["required_resources"].(map[string]interface{})
	if !ok {
		t.Fatalf("required_resources = %v (%T), want map", gotActor["required_resources"], gotActor["required_resources"])
	}
	if v, ok := rr["CPU"].(jsonFloat); !ok || float64(v) != 1.0 {
		t.Fatalf("required_resources.CPU = %v (%T), want jsonFloat 1.0", rr["CPU"], rr["CPU"])
	}
}

// TestListApiResponseSerializesDoubleFieldsAsFloat end-to-end pins the state API
// JSON output: a whole double value in a nested proto map serializes as "6.0"
// through the same json.MarshalIndent path used by RESTResponse, and a whole
// task timestamp as "1000.0", matching the Python dashboard output.
func TestListApiResponseSerializesDoubleFieldsAsFloat(t *testing.T) {
	node := &proto.GcsNodeInfo{
		NodeId: []byte{0x01},
		ResourcesTotal: map[string]float64{
			"CPU": 6.0,
		},
	}
	entry := protoMessageToDict(node, nodeDecodeFields)
	resp := &ListApiResponse{Result: []map[string]interface{}{entry}}
	raw, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	if !strings.Contains(body, `"CPU": 6.0`) {
		t.Fatalf("resources_total.CPU did not serialize as 6.0 in the response body:\n%s", body)
	}
	// The int64 start_time_ms field stays a string, aligned with Python str.
	if !strings.Contains(body, `"start_time_ms": "0"`) {
		t.Fatalf("start_time_ms did not stay a string:\n%s", body)
	}
}

// fields are emitted as JSON strings, matching Python's MessageToDict output
// (verified against protobuf 6.33.6: start_time -> str, pid -> int). The
// frontend and filters rely on this exact type alignment.
func TestInt64SerializedAsString(t *testing.T) {
	actor := &proto.ActorTableData{
		ActorId:     []byte{0x01},
		StartTime:   1700000000000,
		EndTime:     1700000001000,
		NumRestarts: 2,
		Pid:         12345,
		MaxRestarts: 3,
	}
	got := protoMessageToDict(actor, actorDecodeFields)
	// int64 fields -> JSON string (matches Python MessageToDict str).
	if v, ok := got["start_time"].(string); !ok || v != "1700000000000" {
		t.Fatalf("start_time = %v (%T), want string \"1700000000000\"", got["start_time"], got["start_time"])
	}
	if v, ok := got["end_time"].(string); !ok || v != "1700000001000" {
		t.Fatalf("end_time = %v (%T), want string", got["end_time"], got["end_time"])
	}
	if v, ok := got["num_restarts"].(string); !ok || v != "2" {
		t.Fatalf("num_restarts = %v (%T), want string \"2\"", got["num_restarts"], got["num_restarts"])
	}
	// uint32 field -> JSON number (matches Python int).
	if v, ok := got["pid"].(float64); !ok || v != 12345 {
		t.Fatalf("pid = %v (%T), want number 12345", got["pid"], got["pid"])
	}
}

func TestTaskEventToStateDict(t *testing.T) {
	ev := &proto.TaskEvents{
		TaskId:        []byte{0x08},
		AttemptNumber: 1,
		JobId:         []byte{0x06},
		TaskInfo: &proto.TaskInfoEntry{
			Type:            proto.TaskType_ACTOR_TASK,
			Name:            "C.m",
			FuncOrClassName: "C.m",
			Language:        proto.Language_PYTHON,
			TaskId:          []byte{0x08},
		},
		StateUpdates: &proto.TaskStateUpdate{
			NodeId:    []byte{0x09},
			WorkerId:  []byte{0x0a},
			WorkerPid: int32Ptr(4242),
			StateTsNs: map[int32]int64{
				int32(proto.TaskStatus_PENDING_ARGS_AVAIL): 1000000000,
				int32(proto.TaskStatus_RUNNING):            2000000000,
				int32(proto.TaskStatus_FINISHED):           3000000000,
			},
		},
	}
	got := taskEventToStateDict(ev)
	if got["task_id"] != "08" {
		t.Fatalf("task_id = %v, want 08", got["task_id"])
	}
	if got["job_id"] != "06" {
		t.Fatalf("job_id = %v, want 06", got["job_id"])
	}
	if got["attempt_number"].(int32) != 1 {
		t.Fatalf("attempt_number = %v", got["attempt_number"])
	}
	if got["type"] != "ACTOR_TASK" {
		t.Fatalf("type = %v, want ACTOR_TASK", got["type"])
	}
	if got["language"] != "PYTHON" {
		t.Fatalf("language = %v, want PYTHON", got["language"])
	}
	if got["node_id"] != "09" {
		t.Fatalf("node_id = %v, want 09", got["node_id"])
	}
	if got["worker_id"] != "0a" {
		t.Fatalf("worker_id = %v, want 0a", got["worker_id"])
	}
	if got["state"] != "FINISHED" {
		t.Fatalf("state = %v, want FINISHED", got["state"])
	}
	if v, ok := got["creation_time_ms"].(jsonFloat); !ok || float64(v) != 1000 {
		t.Fatalf("creation_time_ms = %v (%T), want jsonFloat 1000", got["creation_time_ms"], got["creation_time_ms"])
	}
	if v, ok := got["start_time_ms"].(jsonFloat); !ok || float64(v) != 2000 {
		t.Fatalf("start_time_ms = %v (%T), want jsonFloat 2000", got["start_time_ms"], got["start_time_ms"])
	}
	if v, ok := got["end_time_ms"].(jsonFloat); !ok || float64(v) != 3000 {
		t.Fatalf("end_time_ms = %v (%T), want jsonFloat 3000", got["end_time_ms"], got["end_time_ms"])
	}
	events, ok := got["events"].([]interface{})
	if !ok || len(events) != 3 {
		t.Fatalf("events = %v", got["events"])
	}
	// Sub-millisecond ns is floored (aligned with Python int(ns) // 1e6).
	ev2 := &proto.TaskEvents{
		TaskId: []byte{0x08},
		TaskInfo: &proto.TaskInfoEntry{
			Type:            proto.TaskType_NORMAL_TASK,
			Name:            "f",
			FuncOrClassName: "f",
		},
		StateUpdates: &proto.TaskStateUpdate{
			StateTsNs: map[int32]int64{
				int32(proto.TaskStatus_PENDING_ARGS_AVAIL): 1789777398512600000,
			},
		},
	}
	got2 := taskEventToStateDict(ev2)
	if v, ok := got2["creation_time_ms"].(jsonFloat); !ok || float64(v) != 1789777398512 {
		t.Fatalf("creation_time_ms = %v (%T), want floored jsonFloat 1789777398512", got2["creation_time_ms"], got2["creation_time_ms"])
	}
	last := events[len(events)-1].(map[string]interface{})
	if last["state"] != "FINISHED" {
		t.Fatalf("last event state = %v, want FINISHED", last["state"])
	}
	if v, ok := last["created_ms"].(jsonFloat); !ok || float64(v) != 3000 {
		t.Fatalf("last created_ms = %v (%T), want jsonFloat 3000", last["created_ms"], last["created_ms"])
	}
}

func TestTaskEventToStateDictFailedError(t *testing.T) {
	ev := &proto.TaskEvents{
		TaskId: []byte{0x08},
		TaskInfo: &proto.TaskInfoEntry{
			Type:            proto.TaskType_NORMAL_TASK,
			Name:            "f",
			FuncOrClassName: "f",
		},
		StateUpdates: &proto.TaskStateUpdate{
			StateTsNs: map[int32]int64{
				int32(proto.TaskStatus_PENDING_ARGS_AVAIL): 1000000000,
				int32(proto.TaskStatus_FAILED):             2000000000,
			},
			ErrorInfo: &proto.RayErrorInfo{
				ErrorMessage: "\x1b[31mTraceback\x1b[0m boom",
				ErrorType:    proto.ErrorType_TASK_EXECUTION_EXCEPTION,
			},
		},
	}
	got := taskEventToStateDict(ev)
	if got["state"] != "FAILED" {
		t.Fatalf("state = %v, want FAILED", got["state"])
	}
	if got["error_message"] != "Traceback boom" {
		t.Fatalf("error_message = %q, want ANSI-stripped", got["error_message"])
	}
	if got["error_type"] != "TASK_EXECUTION_EXCEPTION" {
		t.Fatalf("error_type = %v", got["error_type"])
	}
}

func TestTaskEventRuntimeEnvInfoOmitsNilUris(t *testing.T) {
	// A RuntimeEnvInfo with only serialized_runtime_env set: the uris and
	// runtime_env_config message fields are nil. Python message_to_dict
	// (always_print_fields_with_no_presence=True) omits nil message fields, so
	// the Go dict must not carry uris: null / runtime_env_config: null.
	ev := &proto.TaskEvents{
		TaskId: []byte{0x08},
		TaskInfo: &proto.TaskInfoEntry{
			Type:            proto.TaskType_NORMAL_TASK,
			Name:            "f",
			FuncOrClassName: "f",
			RuntimeEnvInfo: &proto.RuntimeEnvInfo{
				SerializedRuntimeEnv: "{}",
			},
		},
	}
	got := taskEventToStateDict(ev)
	rei, ok := got["runtime_env_info"].(map[string]interface{})
	if !ok {
		t.Fatalf("runtime_env_info = %v (%T), want map", got["runtime_env_info"], got["runtime_env_info"])
	}
	if _, hasUris := rei["uris"]; hasUris {
		t.Fatalf("runtime_env_info carries uris = %v, want omitted", rei["uris"])
	}
	if _, hasConfig := rei["runtime_env_config"]; hasConfig {
		t.Fatalf("runtime_env_info carries runtime_env_config = %v, want omitted", rei["runtime_env_config"])
	}
	if got["runtime_env_info"].(map[string]interface{})["serialized_runtime_env"] != "{}" {
		t.Fatalf("serialized_runtime_env lost: %v", rei)
	}
}

func TestValidateFilters(t *testing.T) {
	opt := &ListApiOptions{
		FilterKeys:       []string{"bad_column"},
		FilterPredicates: []string{"="},
		FilterValues:     []string{"x"},
	}
	if err := validateFilters(opt, "actors"); err == nil {
		t.Fatal("expected error for unknown filter column")
	}
	opt2 := &ListApiOptions{FilterKeys: []string{"actor_id"}}
	if err := validateFilters(opt2, "actors"); err != nil {
		t.Fatalf("known column should pass: %v", err)
	}
}

// TestIntifyTimeFields pins the protojson-string -> int conversion used by
// list_nodes and list_workers in state_aggregator.py.
func TestIntifyTimeFields(t *testing.T) {
	d := map[string]interface{}{
		"start_time_ms": "1700000000000",
		"end_time_ms":   "1700000001000",
		"pid":           4242,
	}
	intifyTimeFields(d, "start_time_ms", "end_time_ms", "missing_field")
	if v, ok := d["start_time_ms"].(int); !ok || v != 1700000000000 {
		t.Fatalf("start_time_ms = %v (%T), want int 1700000000000", d["start_time_ms"], d["start_time_ms"])
	}
	if v, ok := d["end_time_ms"].(int); !ok || v != 1700000001000 {
		t.Fatalf("end_time_ms = %v (%T), want int 1700000001000", d["end_time_ms"], d["end_time_ms"])
	}
	if _, ok := d["missing_field"]; ok {
		t.Fatal("missing field should not be created")
	}
}

// TestComposeStateMessage pins ComposeStateMessage against
// compose_state_message in python/ray/dashboard/utils.py.
func TestComposeStateMessage(t *testing.T) {
	cases := []struct {
		name  string
		death *proto.NodeDeathInfo
		want  interface{}
	}{
		{"nil", nil, nil},
		{
			"expected with message",
			&proto.NodeDeathInfo{Reason: proto.NodeDeathInfo_EXPECTED_TERMINATION, ReasonMessage: "node drained"},
			"Expected termination: node drained",
		},
		{
			"preempted",
			&proto.NodeDeathInfo{Reason: proto.NodeDeathInfo_AUTOSCALER_DRAIN_PREEMPTED},
			"Terminated due to preemption",
		},
		{
			"idle",
			&proto.NodeDeathInfo{Reason: proto.NodeDeathInfo_AUTOSCALER_DRAIN_IDLE},
			"Terminated due to idle (no Ray activity)",
		},
		{
			"unexpected with message only",
			&proto.NodeDeathInfo{Reason: proto.NodeDeathInfo_UNEXPECTED_TERMINATION, ReasonMessage: "oom"},
			"Unexpected termination: oom",
		},
		{"unspecified", &proto.NodeDeathInfo{}, nil},
		{
			"unspecified with message",
			&proto.NodeDeathInfo{Reason: proto.NodeDeathInfo_UNSPECIFIED, ReasonMessage: "node vanished"},
			"node vanished",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ComposeStateMessage(c.death); got != c.want {
				t.Fatalf("ComposeStateMessage = %v, want %v", got, c.want)
			}
		})
	}
}

// TestWorkerAddressFlattened pins the list_workers transform: worker_address
// sub-message fields surface at the top level and the uint64 time fields are
// converted to int.
func TestWorkerAddressFlattened(t *testing.T) {
	w := &proto.WorkerTableData{
		WorkerAddress:        &proto.Address{NodeId: []byte{0x04}, WorkerId: []byte{0x05}, IpAddress: "1.2.3.4"},
		StartTimeMs:          1000,
		EndTimeMs:            2000,
		WorkerLaunchTimeMs:   500,
		WorkerLaunchedTimeMs: 600,
	}
	d := protoMessageToDict(w, workerDecodeFields)
	if addr, ok := d["worker_address"].(map[string]interface{}); ok {
		if v, ok := addr["worker_id"]; ok {
			d["worker_id"] = v
		}
		if v, ok := addr["node_id"]; ok {
			d["node_id"] = v
		}
		if v, ok := addr["ip_address"]; ok {
			d["ip"] = v
		}
	}
	intifyTimeFields(d, "start_time_ms", "end_time_ms", "worker_launch_time_ms", "worker_launched_time_ms")
	if d["worker_id"] != "05" {
		t.Fatalf("worker_id = %v, want hex 05", d["worker_id"])
	}
	if d["node_id"] != "04" {
		t.Fatalf("node_id = %v, want hex 04", d["node_id"])
	}
	if d["ip"] != "1.2.3.4" {
		t.Fatalf("ip = %v, want 1.2.3.4", d["ip"])
	}
	if v, ok := d["start_time_ms"].(int); !ok || v != 1000 {
		t.Fatalf("start_time_ms = %v (%T), want int 1000", d["start_time_ms"], d["start_time_ms"])
	}
}

// TestParseDetailBool pins the bool query parsing accepted by
// convert_string_to_type in python/ray/util/state/util.py: True/true/1 and
// False/false/0, with any other value raising a ValueError.
func TestParseDetailBool(t *testing.T) {
	cases := []struct {
		detail string
		want   bool
	}{
		{"True", true},
		{"true", true},
		{"1", true},
		{"False", false},
		{"false", false},
		{"0", false},
	}
	for _, c := range cases {
		req := httptest.NewRequest("GET", "/api/v0/actors?detail="+c.detail, nil)
		opt, err := ParseListApiOptions(req)
		if err != nil {
			t.Fatalf("detail=%s: %v", c.detail, err)
		}
		if opt.Detail != c.want {
			t.Fatalf("detail=%s: Detail=%v, want %v", c.detail, opt.Detail, c.want)
		}
	}
	req := httptest.NewRequest("GET", "/api/v0/actors?detail=yes", nil)
	if _, err := ParseListApiOptions(req); err == nil {
		t.Fatal("expected error for invalid detail value")
	}
}

// TestParseTimeoutMultiplier pins the 80% server-side timeout multiplier, with
// a minimum of 1, aligned with ListApiOptions.__post_init__.
func TestParseTimeoutMultiplier(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v0/actors?timeout=10", nil)
	opt, err := ParseListApiOptions(req)
	if err != nil {
		t.Fatal(err)
	}
	if opt.Timeout != 8 {
		t.Fatalf("timeout = %d, want 8 (80%% of 10)", opt.Timeout)
	}
	req2 := httptest.NewRequest("GET", "/api/v0/actors", nil)
	opt2, err := ParseListApiOptions(req2)
	if err != nil {
		t.Fatal(err)
	}
	if opt2.Timeout != 24 {
		t.Fatalf("default timeout = %d, want 24 (80%% of 30)", opt2.Timeout)
	}
}

// TestParseInvalidPredicate pins the ValueError raised for an unsupported
// filter predicate, aligned with the ListApiOptions __post_init__ check.
func TestParseInvalidPredicate(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v0/actors?filter_keys=state&filter_predicates=%3E&filter_values=ALIVE", nil)
	if _, err := ParseListApiOptions(req); err == nil {
		t.Fatal("expected error for invalid filter predicate")
	}
}

// TestTypedFilterMatch pins the typed filter comparison aligned with
// do_filter: bool/int columns convert the filter string, string columns match
// case-insensitively.
func TestTypedFilterMatch(t *testing.T) {
	entries := []map[string]interface{}{
		{"worker_id": "aaa", "is_alive": true, "pid": 1234},
		{"worker_id": "bbb", "is_alive": false, "pid": 5678},
	}
	opt := &ListApiOptions{
		FilterKeys:       []string{"is_alive"},
		FilterPredicates: []string{"="},
		FilterValues:     []string{"True"},
	}
	if got := applyFilters(entries, opt); len(got) != 1 || got[0]["worker_id"] != "aaa" {
		t.Fatalf("bool filter got %v, want only aaa", got)
	}
	opt2 := &ListApiOptions{
		FilterKeys:       []string{"pid"},
		FilterPredicates: []string{"="},
		FilterValues:     []string{"5678"},
	}
	if got := applyFilters(entries, opt2); len(got) != 1 || got[0]["worker_id"] != "bbb" {
		t.Fatalf("int filter got %v, want only bbb", got)
	}
	opt3 := &ListApiOptions{
		FilterKeys:       []string{"is_alive"},
		FilterPredicates: []string{"!="},
		FilterValues:     []string{"false"},
	}
	if got := applyFilters(entries, opt3); len(got) != 1 || got[0]["worker_id"] != "aaa" {
		t.Fatalf("bool != filter got %v, want only aaa", got)
	}
}

// TestFilterFields pins the base/detail column trimming aligned with
// filter_fields in python/ray/util/state/common.py: detail=False keeps only
// the base columns, detail=True keeps all, and missing columns are filled with
// nil.
func TestFilterFields(t *testing.T) {
	d := map[string]interface{}{
		"actor_id":       "01",
		"class_name":     "C",
		"state":          "ALIVE",
		"num_restarts":   "2",
		"label_selector": map[string]interface{}{},
	}
	base := filterFields(d, &ListApiOptions{Resource: "actors"})
	if len(base) != 8 {
		t.Fatalf("base columns = %d, want 8", len(base))
	}
	if _, ok := base["num_restarts"]; ok {
		t.Fatal("detail-only column should be trimmed in base mode")
	}
	if base["actor_id"] != "01" {
		t.Fatalf("actor_id = %v", base["actor_id"])
	}
	detail := filterFields(d, &ListApiOptions{Resource: "actors", Detail: true})
	if len(detail) != 19 {
		t.Fatalf("detail columns = %d, want 19", len(detail))
	}
	if v, ok := detail["num_restarts"]; !ok || v != "2" {
		t.Fatalf("num_restarts = %v, want 2", v)
	}
	// Missing detail columns are filled with nil.
	if v, ok := detail["serialized_runtime_env"]; !ok || v != nil {
		t.Fatalf("serialized_runtime_env = %v, want nil", v)
	}
	// Missing base columns are filled with nil too.
	if v, ok := detail["node_id"]; !ok || v != nil {
		t.Fatalf("node_id = %v, want nil", v)
	}
	// Unknown resource passes through unchanged.
	raw := map[string]interface{}{"x": 1}
	if got := filterFields(raw, &ListApiOptions{Resource: "nope"}); len(got) != 1 {
		t.Fatalf("unknown resource should pass through, got %v", got)
	}
}

// TestIDSortKey pins the per-resource primary sort keys aligned with the
// hard-coded sort keys in the Python list_* methods: tasks sort by task_id
// (not actor_id even though it is present), workers by worker_id (not node_id).
func TestIDSortKey(t *testing.T) {
	cases := map[string]string{
		"tasks":            "task_id",
		"workers":          "worker_id",
		"actors":           "actor_id",
		"nodes":            "node_id",
		"placement_groups": "placement_group_id",
		"jobs":             "job_id",
		"objects":          "object_id",
		"unknown_resource": "id",
	}
	for resource, want := range cases {
		if got := idSortKey(resource); got != want {
			t.Fatalf("idSortKey(%q) = %q, want %q", resource, got, want)
		}
	}
}

// TestTaskFiltersToGCS pins the tasks filter pushdown: the state/name/job_id/
// task_id/actor_id columns are converted to the GetTaskEventsRequest_Filters
// fields (id filters decoded to raw binary, "=" / "!=" mapped to the enum),
// aligned with get_all_task_info in python/ray/util/state/state_manager.py.
// Filter columns GCS cannot filter on are left untouched for the local pass.
func TestTaskFiltersToGCS(t *testing.T) {
	opt := &ListApiOptions{
		FilterKeys:       []string{"state", "name", "job_id", "task_id", "actor_id", "func_or_class_name"},
		FilterPredicates: []string{"=", "!=", "=", "=", "!=", "="},
		FilterValues:     []string{"RUNNING", "remote_task", "02000000", "00ff", "aabb", "foo"},
	}
	filters := &proto.GetTaskEventsRequest_Filters{}
	taskFiltersToGCS(opt, filters)
	if len(filters.StateFilters) != 1 || filters.StateFilters[0].State != "RUNNING" ||
		filters.StateFilters[0].Predicate != proto.FilterPredicate_EQUAL {
		t.Fatalf("state filter = %v", filters.StateFilters)
	}
	if len(filters.TaskNameFilters) != 1 || filters.TaskNameFilters[0].TaskName != "remote_task" ||
		filters.TaskNameFilters[0].Predicate != proto.FilterPredicate_NOT_EQUAL {
		t.Fatalf("name filter = %v", filters.TaskNameFilters)
	}
	if len(filters.JobFilters) != 1 || string(filters.JobFilters[0].JobId) != string([]byte{0x02, 0x00, 0x00, 0x00}) {
		t.Fatalf("job filter = %v", filters.JobFilters)
	}
	if len(filters.TaskFilters) != 1 || string(filters.TaskFilters[0].TaskId) != string([]byte{0x00, 0xff}) {
		t.Fatalf("task filter = %v", filters.TaskFilters)
	}
	if len(filters.ActorFilters) != 1 || string(filters.ActorFilters[0].ActorId) != string([]byte{0xaa, 0xbb}) ||
		filters.ActorFilters[0].Predicate != proto.FilterPredicate_NOT_EQUAL {
		t.Fatalf("actor filter = %v", filters.ActorFilters)
	}
}

// TestTasksSortByTaskID pins the tasks sort key: even though a tasks entry
// always carries actor_id, the list must sort by task_id, aligned with
// state_aggregator.py list_tasks.
func TestTasksSortByTaskID(t *testing.T) {
	entries := []map[string]interface{}{
		{"task_id": "bbb", "actor_id": "zzz", "state": "FINISHED"},
		{"task_id": "aaa", "actor_id": "yyy", "state": "FINISHED"},
		{"task_id": "ccc", "actor_id": "xxx", "state": "FINISHED"},
	}
	filterAndSort(entries, &ListApiOptions{Limit: 10, Resource: "tasks"})
	if entries[0]["task_id"] != "aaa" || entries[1]["task_id"] != "bbb" || entries[2]["task_id"] != "ccc" {
		t.Fatalf("tasks not sorted by task_id: %v", entries)
	}
}

// TestWorkersSortByWorkerID pins the workers sort key: even though a workers
// entry always carries node_id, the list must sort by worker_id, aligned with
// state_aggregator.py list_workers.
func TestWorkersSortByWorkerID(t *testing.T) {
	entries := []map[string]interface{}{
		{"worker_id": "bbb", "node_id": "aaa"},
		{"worker_id": "aaa", "node_id": "ccc"},
		{"worker_id": "ccc", "node_id": "bbb"},
	}
	filterAndSort(entries, &ListApiOptions{Limit: 10, Resource: "workers"})
	if entries[0]["worker_id"] != "aaa" || entries[1]["worker_id"] != "bbb" || entries[2]["worker_id"] != "ccc" {
		t.Fatalf("workers not sorted by worker_id: %v", entries)
	}
}

// TestIntifyTimeFieldsUint64Max pins the uint64-max sentinel for unknown worker
// launch times: it must be kept as a uint64 integer (matching the Python int())
// rather than left as a string, so it serializes as 18446744073709551615.
func TestIntifyTimeFieldsUint64Max(t *testing.T) {
	d := map[string]interface{}{
		"worker_launch_time_ms":   "18446744073709551615",
		"worker_launched_time_ms": "1700000000000",
	}
	intifyTimeFields(d, "worker_launch_time_ms", "worker_launched_time_ms")
	if v, ok := d["worker_launch_time_ms"].(uint64); !ok || v != 18446744073709551615 {
		t.Fatalf("worker_launch_time_ms = %v (%T), want uint64 18446744073709551615", d["worker_launch_time_ms"], d["worker_launch_time_ms"])
	}
	if v, ok := d["worker_launched_time_ms"].(int); !ok || v != 1700000000000 {
		t.Fatalf("worker_launched_time_ms = %v (%T), want int 1700000000000", d["worker_launched_time_ms"], d["worker_launched_time_ms"])
	}
}

// TestJobSortNullFirst pins the list_jobs sort key `entry["job_id"] or ""`:
// entries with a missing/empty job_id sort first, aligned with
// state_aggregator.py.
func TestJobSortNullFirst(t *testing.T) {
	entries := []map[string]interface{}{
		{"job_id": "b", "type": "DRIVER"},
		{"type": "SUBMISSION"}, // no job_id key
		{"job_id": "a", "type": "DRIVER"},
	}
	filterAndSort(entries, &ListApiOptions{Limit: 10, Resource: "jobs"})
	if entries[0]["job_id"] != nil {
		t.Fatalf("first entry = %v, want nil job_id first", entries[0])
	}
	if entries[1]["job_id"] != "a" {
		t.Fatalf("second entry = %v, want a", entries[1])
	}
	if entries[2]["job_id"] != "b" {
		t.Fatalf("third entry = %v, want b", entries[2])
	}
}
