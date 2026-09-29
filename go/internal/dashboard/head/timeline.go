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
	"context"
	"encoding/json"
	"fmt"
)

// GenerateTaskTimeline builds the chrome/perfetto tracing dump for a job's
// tasks, aligned with generate_task_timeline + chrome_tracing_dump in
// python/ray/dashboard/state_aggregator.py and python/ray/_private/profiling.py.
// It returns the JSON-serialized tracing event list.
func (m *StateAPIManager) GenerateTaskTimeline(ctx context.Context, jobID string) (string, error) {
	var filters *ListApiOptions
	if jobID != "" {
		filters = &ListApiOptions{
			FilterKeys:       []string{"job_id"},
			FilterPredicates: []string{"="},
			FilterValues:     []string{jobID},
		}
	}
	// Driver tasks are excluded, matching the ListApiOptions default
	// exclude_driver=True used by generate_task_timeline in state_aggregator.py.
	opt := &ListApiOptions{
		Detail:        true,
		Limit:         10000,
		Timeout:       30,
		ExcludeDriver: true,
	}
	if filters != nil {
		opt.FilterKeys, opt.FilterPredicates, opt.FilterValues = filters.FilterKeys, filters.FilterPredicates, filters.FilterValues
	}
	listResp, err := m.ListTasks(ctx, opt)
	if err != nil {
		return "", err
	}
	return chromeTracingDump(listResp.Result), nil
}

// chromeTracingCompleteEvent mirrors ChromeTracingCompleteEvent; the JSON keys
// match the dataclass asdict output (snake_case, ph defaults to "X").
type chromeTracingCompleteEvent struct {
	Cat   string                 `json:"cat"`
	Name  string                 `json:"name"`
	Pid   int                    `json:"pid"`
	Tid   int                    `json:"tid"`
	TS    float64                `json:"ts"`
	Dur   float64                `json:"dur"`
	Cname string                 `json:"cname"`
	Args  map[string]interface{} `json:"args"`
	Ph    string                 `json:"ph"`
}

// chromeTracingMetadataEvent mirrors ChromeTracingMetadataEvent (ph = "M").
// Python's ChromeTracingMetadataEvent has tid defaulting to None, so process_name
// serializes "tid": null. A *int pointer distinguishes "unset" (nil -> null)
// from an actual tid of 0 (first worker), matching Python exactly.
type chromeTracingMetadataEvent struct {
	Name string            `json:"name"`
	Args map[string]string `json:"args"`
	Pid  int               `json:"pid"`
	Tid  *int              `json:"tid"`
	Ph   string            `json:"ph"`
}

// defaultColorMapping maps the event name to the default chrome tracing color,
// aligned with _default_color_mapping in python/ray/_private/profiling.py.
var defaultColorMapping = map[string]string{
	"worker_idle":                "cq_build_abandoned",
	"task":                       "rail_response",
	"task:deserialize_arguments": "rail_load",
	"task:execute":               "rail_animation",
	"task:store_outputs":         "rail_idle",
	"wait_for_function":          "detailed_memory_dump",
	"ray.get":                    "good",
	"ray.put":                    "terrible",
	"ray.wait":                   "vsync_highlight_color",
	"submit_task":                "background_memory_dump",
	"fetch_and_run_function":     "detailed_memory_dump",
	"register_remote_function":   "detailed_memory_dump",
}

func defaultColor(name string) string {
	if c, ok := defaultColorMapping[name]; ok {
		return c
	}
	return "generic_work"
}

// chromeTracingDump converts task states with profiling data into the chrome
// tracing event list, aligned with chrome_tracing_dump.
func chromeTracingDump(tasks []map[string]interface{}) string {
	type workerKey struct {
		nodeIdx int
		compID  string
	}
	allEvents := []interface{}{}
	nodeToIndex := map[string]int{}
	// nodeOrder tracks the first-seen order of node IPs; Python's
	// node_to_index is a dict iterated in insertion order, so the metadata
	// output must follow first-appearance order rather than sorted order.
	nodeOrder := []string{}
	nodeIdx := 0
	workerToIndex := map[workerKey]int{}
	// workerOrder tracks the first-seen order of (node, component) pairs,
	// mirroring worker_to_index insertion order in Python.
	workerOrder := []workerKey{}
	workerIdx := 0

	for _, task := range tasks {
		profData, ok := task["profiling_data"].(map[string]interface{})
		if !ok {
			continue
		}
		nodeIP, _ := profData["node_ip_address"].(string)
		componentType, _ := profData["component_type"].(string)
		if componentType != "worker" && componentType != "driver" {
			continue
		}
		componentID, _ := profData["component_id"].(string)
		component := componentType + ":" + componentID
		rawEvents, _ := profData["events"].([]interface{})
		for _, raw := range rawEvents {
			event, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			extraData, _ := event["extra_data"].(map[string]interface{})
			if extraData == nil {
				extraData = map[string]interface{}{}
			}
			extraData["task_id"] = task["task_id"]
			extraData["job_id"] = task["job_id"]
			extraData["attempt_number"] = task["attempt_number"]
			extraData["func_or_class_name"] = task["func_or_class_name"]
			extraData["actor_id"] = task["actor_id"]

			node, ok := nodeToIndex[nodeIP]
			if !ok {
				nodeToIndex[nodeIP] = nodeIdx
				nodeOrder = append(nodeOrder, nodeIP)
				node = nodeIdx
				nodeIdx++
			}
			wkey := workerKey{node, component}
			tid, ok := workerToIndex[wkey]
			if !ok {
				workerToIndex[wkey] = workerIdx
				workerOrder = append(workerOrder, wkey)
				tid = workerIdx
				workerIdx++
			}

			eventName, _ := event["event_name"].(string)
			cname := defaultColor(eventName)
			name := eventName
			if cn, ok := extraData["cname"].(string); ok {
				cname = defaultColor(cn)
			}
			if nm, ok := extraData["name"].(string); ok {
				name = nm
			}
			start, _ := toFloat64(event["start_time"])
			end, _ := toFloat64(event["end_time"])
			allEvents = append(allEvents, chromeTracingCompleteEvent{
				Cat:   eventName,
				Name:  name,
				Pid:   node,
				Tid:   tid,
				TS:    start * 1e3,
				Dur:   (end * 1e3) - (start * 1e3),
				Cname: cname,
				Args:  extraData,
				Ph:    "X",
			})
		}
	}

	// Metadata events follow first-appearance (insertion) order, matching
	// Python's dict iteration. process_name carries no tid (Python defaults
	// it to None), so Tid stays nil and serializes as "tid": null.
	for _, ip := range nodeOrder {
		allEvents = append(allEvents, chromeTracingMetadataEvent{
			Name: "process_name",
			Pid:  nodeToIndex[ip],
			Args: map[string]string{"name": fmt.Sprintf("Node %s", ip)},
			Ph:   "M",
		})
	}

	for _, w := range workerOrder {
		tid := workerToIndex[w]
		allEvents = append(allEvents, chromeTracingMetadataEvent{
			Name: "thread_name",
			Ph:   "M",
			Tid:  &tid,
			Pid:  w.nodeIdx,
			Args: map[string]string{"name": w.compID},
		})
	}

	raw, err := json.Marshal(allEvents)
	if err != nil {
		return "[]"
	}
	return string(raw)
}

// toFloat64 returns the float64 value of a JSON decoded number, defaulting to 0.
func toFloat64(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case jsonFloat:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	}
	return 0, false
}
