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
	"encoding/json"
	"testing"
)

func TestChromeTracingDump(t *testing.T) {
	tasks := []map[string]interface{}{
		{
			"task_id":            "task1",
			"job_id":             "01000000",
			"attempt_number":     0,
			"func_or_class_name": "foo",
			"actor_id":           "",
			"profiling_data": map[string]interface{}{
				"node_ip_address": "1.2.3.4",
				"component_type":  "worker",
				"component_id":    "worker-abc",
				"events": []interface{}{
					map[string]interface{}{
						"event_name": "execute_task",
						"start_time": 1.0,
						"end_time":   3.0,
						"extra_data": map[string]interface{}{},
					},
				},
			},
		},
	}
	raw := chromeTracingDump(tasks)
	var events []map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &events); err != nil {
		t.Fatalf("chromeTracingDump() produced invalid JSON: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3 (1 complete + 1 process_name + 1 thread_name)", len(events))
	}
	complete := events[0]
	if complete["ph"] != "X" || complete["name"] != "execute_task" {
		t.Fatalf("complete event = %v, want ph=X name=execute_task", complete)
	}
	if complete["ts"] != 1000.0 || complete["dur"] != 2000.0 {
		t.Fatalf("complete event ts/dur = %v/%v, want 1000/2000", complete["ts"], complete["dur"])
	}
	if complete["cat"] != "execute_task" || complete["cname"] != "generic_work" {
		t.Fatalf("complete event cat/cname = %v/%v", complete["cat"], complete["cname"])
	}
	// args carries the propagated task fields.
	args, ok := complete["args"].(map[string]interface{})
	if !ok {
		t.Fatalf("args = %T, want map", complete["args"])
	}
	if args["task_id"] != "task1" || args["job_id"] != "01000000" {
		t.Fatalf("args = %v, want task_id/job_id propagated", args)
	}
	// Metadata events.
	var procName, threadName map[string]interface{}
	for _, e := range events[1:] {
		if e["name"] == "process_name" {
			procName = e
		}
		if e["name"] == "thread_name" {
			threadName = e
		}
	}
	if procName == nil || threadName == nil {
		t.Fatalf("missing metadata events: %v", events[1:])
	}
	if procName["args"].(map[string]interface{})["name"] != "Node 1.2.3.4" {
		t.Fatalf("process_name args = %v", procName["args"])
	}
	if threadName["args"].(map[string]interface{})["name"] != "worker:worker-abc" {
		t.Fatalf("thread_name args = %v", threadName["args"])
	}
	// process_name must have no tid (Python defaults ChromeTracingMetadataEvent
	// tid to None, serializing "tid": null), while thread_name carries its tid.
	if tid, ok := procName["tid"]; ok && tid != nil {
		t.Fatalf("process_name tid = %v, want null", procName["tid"])
	}
	if threadName["tid"] == nil {
		t.Fatalf("thread_name tid = nil, want 0")
	}
}

func TestChromeTracingDumpNoProfiling(t *testing.T) {
	tasks := []map[string]interface{}{
		{"task_id": "t1", "profiling_data": map[string]interface{}{}},
	}
	raw := chromeTracingDump(tasks)
	if raw != "[]" {
		t.Fatalf("chromeTracingDump(no profiling) = %s, want []", raw)
	}
}

func TestDefaultColorMapping(t *testing.T) {
	if got := defaultColor("worker_idle"); got != "cq_build_abandoned" {
		t.Fatalf("defaultColor(worker_idle) = %q, want cq_build_abandoned", got)
	}
	if got := defaultColor("unknown_event"); got != "generic_work" {
		t.Fatalf("defaultColor(unknown) = %q, want generic_work", got)
	}
}

// TestChromeTracingDumpMetadataOrder verifies that the metadata events follow
// first-appearance (insertion) order of nodes and workers, matching Python's
// dict iteration in chrome_tracing_dump rather than sorted order. It also
// checks that the first worker's tid of 0 serializes as 0 (not null).
func TestChromeTracingDumpMetadataOrder(t *testing.T) {
	mkTask := func(taskID, nodeIP, compID string) map[string]interface{} {
		return map[string]interface{}{
			"task_id":            taskID,
			"job_id":             "01000000",
			"attempt_number":     0,
			"func_or_class_name": "foo",
			"actor_id":           "",
			"profiling_data": map[string]interface{}{
				"node_ip_address": nodeIP,
				"component_type":  "worker",
				"component_id":    compID,
				"events": []interface{}{
					map[string]interface{}{
						"event_name": "execute_task",
						"start_time": 1.0,
						"end_time":   2.0,
						"extra_data": map[string]interface{}{},
					},
				},
			},
		}
	}
	// Insertion order deliberately differs from sorted order to prove the
	// output follows first-appearance order.
	tasks := []map[string]interface{}{
		mkTask("t1", "10.0.0.2", "worker-b"), // node 10.0.0.2, worker (0, worker-b) -> tid 0
		mkTask("t2", "10.0.0.1", "worker-a"), // node 10.0.0.1, worker (1, worker-a) -> tid 1
		mkTask("t3", "10.0.0.2", "worker-a"), // node 10.0.0.2, worker (0, worker-a) -> tid 2
	}
	raw := chromeTracingDump(tasks)
	var events []map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &events); err != nil {
		t.Fatalf("chromeTracingDump() produced invalid JSON: %v", err)
	}
	var names []string
	var procTids []interface{}
	var threadTids []interface{}
	for _, e := range events {
		switch e["name"] {
		case "process_name":
			names = append(names, "process:"+e["args"].(map[string]interface{})["name"].(string))
			procTids = append(procTids, e["tid"])
		case "thread_name":
			names = append(names, "thread:"+e["args"].(map[string]interface{})["name"].(string))
			threadTids = append(threadTids, e["tid"])
		}
	}
	// Workers appear in first-seen order: worker-b (node 10.0.0.2), then
	// worker-a on node 10.0.0.1, then worker-a on node 10.0.0.2. If the old
	// sort-by-(nodeIdx, compID) were still in effect, worker-a on node 0 would
	// come before worker-b, so this order proves insertion-order output.
	wantNames := []string{
		"process:Node 10.0.0.2",
		"process:Node 10.0.0.1",
		"thread:worker:worker-b",
		"thread:worker:worker-a",
		"thread:worker:worker-a",
	}
	if len(names) != len(wantNames) {
		t.Fatalf("metadata names = %v, want %v", names, wantNames)
	}
	for i := range wantNames {
		if names[i] != wantNames[i] {
			t.Fatalf("metadata order = %v, want %v", names, wantNames)
		}
	}
	// process_name events must serialize tid as null (Python default None).
	for i, tid := range procTids {
		if tid != nil {
			t.Fatalf("process_name[%d] tid = %v, want null", i, tid)
		}
	}
	// thread_name tids must be present, including 0 for the first worker.
	wantTids := []interface{}{float64(0), float64(1), float64(2)}
	if len(threadTids) != len(wantTids) {
		t.Fatalf("thread tids = %v, want %v", threadTids, wantTids)
	}
	for i, tid := range threadTids {
		if tid != wantTids[i] {
			t.Fatalf("thread_name[%d] tid = %v, want %v", i, tid, wantTids[i])
		}
	}
}

// TestChromeTracingDumpCname verifies the chrome tracing cname aligns with
// _default_color_mapping in python/ray/_private/profiling.py.
func TestChromeTracingDumpCname(t *testing.T) {
	tests := []struct {
		name      string
		eventName string
		cname     string
	}{
		{"deserialize_arguments", "task:deserialize_arguments", "rail_load"},
		{"execute", "task:execute", "rail_animation"},
		{"store_outputs", "task:store_outputs", "rail_idle"},
		{"worker_idle", "worker_idle", "cq_build_abandoned"},
		{"ray.get", "ray.get", "good"},
		{"ray.put", "ray.put", "terrible"},
		{"ray.wait", "ray.wait", "vsync_highlight_color"},
		{"submit_task", "submit_task", "background_memory_dump"},
		{"wait_for_function", "wait_for_function", "detailed_memory_dump"},
		{"fetch_and_run_function", "fetch_and_run_function", "detailed_memory_dump"},
		{"register_remote_function", "register_remote_function", "detailed_memory_dump"},
		{"unknown", "totally_unknown_event", "generic_work"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tasks := []map[string]interface{}{
				{
					"task_id":            "task1",
					"job_id":             "01000000",
					"attempt_number":     0,
					"func_or_class_name": "foo",
					"actor_id":           "",
					"profiling_data": map[string]interface{}{
						"node_ip_address": "1.2.3.4",
						"component_type":  "worker",
						"component_id":    "worker-abc",
						"events": []interface{}{
							map[string]interface{}{
								"event_name": tt.eventName,
								"start_time": 1.0,
								"end_time":   2.0,
								"extra_data": map[string]interface{}{},
							},
						},
					},
				},
			}
			raw := chromeTracingDump(tasks)
			var events []map[string]interface{}
			if err := json.Unmarshal([]byte(raw), &events); err != nil {
				t.Fatalf("chromeTracingDump() produced invalid JSON: %v", err)
			}
			if len(events) == 0 {
				t.Fatalf("chromeTracingDump() returned no events")
			}
			if got := events[0]["cname"]; got != tt.cname {
				t.Fatalf("cname for event %q = %v, want %q", tt.eventName, got, tt.cname)
			}
		})
	}
}

// TestChromeTracingDumpCnameOverride verifies that an explicit cname in
// extra_data is re-mapped through defaultColor, matching Python's
// _default_color_mapping[extra_data["cname"]].
func TestChromeTracingDumpCnameOverride(t *testing.T) {
	tasks := []map[string]interface{}{
		{
			"task_id":            "task1",
			"job_id":             "01000000",
			"attempt_number":     0,
			"func_or_class_name": "foo",
			"actor_id":           "",
			"profiling_data": map[string]interface{}{
				"node_ip_address": "1.2.3.4",
				"component_type":  "worker",
				"component_id":    "worker-abc",
				"events": []interface{}{
					map[string]interface{}{
						"event_name": "task:execute",
						"start_time": 1.0,
						"end_time":   2.0,
						"extra_data": map[string]interface{}{
							"cname": "task:store_outputs",
						},
					},
				},
			},
		},
	}
	raw := chromeTracingDump(tasks)
	var events []map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &events); err != nil {
		t.Fatalf("chromeTracingDump() produced invalid JSON: %v", err)
	}
	if got := events[0]["cname"]; got != "rail_idle" {
		t.Fatalf("cname override = %v, want rail_idle", got)
	}
}
