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
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/ray-project/ray/go/proto"
)

// taskStateDecodeFields lists the fields decoded from base64 to hex in the
// flattened TaskState dict, aligned with protobuf_to_task_state_dict in
// python/ray/util/state/common.py.
var taskStateDecodeFields = []string{
	"task_id", "job_id", "node_id", "actor_id", "parent_task_id", "worker_id", "placement_group_id",
}

// taskEventToStateDict flattens a TaskEvents proto into the TaskState dict
// shape returned by protobuf_to_task_state_dict. It mirrors the Python
// implementation: task_info/state_updates fields are lifted to the top level,
// state transitions are reconstructed from state_ts_ns into events[] with
// timestamps converted from ns to ms, and profile events' extra_data is parsed
// from JSON.
func taskEventToStateDict(ev *proto.TaskEvents) map[string]interface{} {
	out := map[string]interface{}{
		"task_id":          hexBytes(ev.TaskId),
		"attempt_number":   ev.AttemptNumber,
		"creation_time_ms": nil,
		"start_time_ms":    nil,
		"end_time_ms":      nil,
		"events":           []interface{}{},
		"state":            "NIL",
	}

	taskInfo := map[string]interface{}{}
	if ev.TaskInfo != nil {
		taskInfo = protoMessageToDict(ev.TaskInfo, taskStateDecodeFields)
	}
	// Fields lifted from task_info (aligned with the Python mappings list).
	for _, k := range []string{
		"task_id", "name", "actor_id", "type", "func_or_class_name", "language",
		"required_resources", "runtime_env_info", "parent_task_id", "placement_group_id",
		"call_site", "label_selector",
	} {
		if v, ok := taskInfo[k]; ok {
			out[k] = v
		}
	}
	// Aligned with Python protobuf_to_task_state_dict: message_to_dict emits
	// only present message fields (always_print_fields_with_no_presence only
	// affects no-presence scalar/repeated fields), so a nil message field like
	// RuntimeEnvInfo.uris is omitted entirely rather than serialized as null.
	// protojson with EmitUnpopulated emits such nil message fields as null, so
	// drop the nil-valued keys from runtime_env_info to match Python.
	if rei, ok := out["runtime_env_info"].(map[string]interface{}); ok {
		for k, v := range rei {
			if v == nil {
				delete(rei, k)
			}
		}
	}
	// task_attempt fields: task_id, attempt_number, job_id.
	if len(ev.JobId) > 0 {
		out["job_id"] = hexBytes(ev.JobId)
	}

	stateUpdates := map[string]interface{}{}
	if ev.StateUpdates != nil {
		stateUpdates = protoMessageToDict(ev.StateUpdates, taskStateDecodeFields)
	}
	for _, k := range []string{
		"node_id", "worker_id", "task_log_info", "actor_repr_name", "worker_pid", "is_debugger_paused",
	} {
		if v, ok := stateUpdates[k]; ok {
			out[k] = v
		}
	}

	// Reconstruct state transitions from state_ts_ns (aligned with the Python
	// TaskStatus iteration and the events[] construction). Timestamps are
	// float64 milliseconds, matching the Python `int(ns) // 1e6` which yields a
	// float because of the float divisor (common.py protobuf_to_task_state_dict).
	if tsMap, ok := stateUpdates["state_ts_ns"].(map[string]interface{}); ok {
		type stateEvent struct {
			name  string
			order int
			tsMS  float64
		}
		var stEvents []stateEvent
		// Iterate in TaskStatus numeric order so the events end up ordered by
		// state transition (PENDING_ARGS_AVAIL=1 ... FINISHED=11/FAILED=12).
		for _, st := range []proto.TaskStatus{
			proto.TaskStatus_PENDING_ARGS_AVAIL, proto.TaskStatus_PENDING_NODE_ASSIGNMENT,
			proto.TaskStatus_PENDING_OBJ_STORE_MEM_AVAIL, proto.TaskStatus_PENDING_ARGS_FETCH,
			proto.TaskStatus_SUBMITTED_TO_WORKER, proto.TaskStatus_PENDING_ACTOR_TASK_ARGS_FETCH,
			proto.TaskStatus_PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY, proto.TaskStatus_RUNNING,
			proto.TaskStatus_RUNNING_IN_RAY_GET, proto.TaskStatus_RUNNING_IN_RAY_WAIT,
			proto.TaskStatus_FINISHED, proto.TaskStatus_FAILED,
		} {
			key := strconvInt(int32(st))
			raw, ok := tsMap[key]
			if !ok {
				continue
			}
			ns, ok := toInt64(raw)
			if !ok {
				continue
			}
			// int(ns) // 1e6 in Python floors the millisecond value and yields a
			// float because of the float divisor; mirror that with math.Floor.
			tsMS := math.Floor(float64(ns) / 1e6)
			stEvents = append(stEvents, stateEvent{name: proto.TaskStatus_name[int32(st)], order: int(st), tsMS: tsMS})
			switch st {
			case proto.TaskStatus_PENDING_ARGS_AVAIL:
				out["creation_time_ms"] = jsonFloat(tsMS)
			case proto.TaskStatus_RUNNING:
				out["start_time_ms"] = jsonFloat(tsMS)
			case proto.TaskStatus_FINISHED, proto.TaskStatus_FAILED:
				out["end_time_ms"] = jsonFloat(tsMS)
			}
		}
		sort.SliceStable(stEvents, func(i, j int) bool { return stEvents[i].order < stEvents[j].order })
		events := make([]interface{}, 0, len(stEvents))
		for _, se := range stEvents {
			events = append(events, map[string]interface{}{
				"state":      se.name,
				"created_ms": jsonFloat(se.tsMS),
			})
		}
		if len(events) > 0 {
			out["events"] = events
			if last, ok := events[len(events)-1].(map[string]interface{}); ok {
				if st, ok := last["state"].(string); ok {
					out["state"] = st
				}
			}
		}
	}

	// FAILED error info: extract error_message/error_type from state_updates.
	if out["state"] == "FAILED" {
		if errInfo, ok := stateUpdates["error_info"].(map[string]interface{}); ok {
			if em, ok := errInfo["error_message"].(string); ok {
				out["error_message"] = removeAnsiEscapeCodes(em)
			}
			if et, ok := errInfo["error_type"]; ok {
				out["error_type"] = et
			}
		}
	}

	// Actor task name override using the actor repr name (aligned with the
	// Python actor_repr_name logic).
	if repr, ok := stateUpdates["actor_repr_name"].(string); ok && repr != "" {
		if taskType, _ := out["type"].(string); taskType == "ACTOR_TASK" {
			if name, _ := out["name"].(string); name != "" && name == asString(out["func_or_class_name"]) {
				method := name
				if i := strings.LastIndex(name, "."); i >= 0 {
					method = name[i+1:]
				}
				out["name"] = repr + "." + method
			}
		}
	}

	// Profiling data: normalize profile event times from ns to ms and parse
	// extra_data JSON (aligned with the Python profiling_data handling).
	if ev.ProfileEvents != nil {
		prof := protoMessageToDict(ev.ProfileEvents, []string{"component_id"})
		if evts, ok := prof["events"].([]interface{}); ok {
			norm := make([]interface{}, 0, len(evts))
			for _, e := range evts {
				em, ok := e.(map[string]interface{})
				if !ok {
					continue
				}
				for _, tk := range []string{"end_time", "start_time"} {
					if ns, ok := toInt64(em[tk]); ok {
						// Python does int(ns) / 1e6, a float division yielding a
						// Python float; wrap in jsonFloat so whole values keep a
						// fractional part in the JSON output.
						em[tk] = jsonFloat(float64(ns) / 1e6)
					}
				}
				if extra, ok := em["extra_data"].(string); ok && extra != "" {
					var parsed interface{}
					if err := json.Unmarshal([]byte(extra), &parsed); err == nil {
						em["extra_data"] = parsed
					}
				}
				norm = append(norm, em)
			}
			prof["events"] = norm
		}
		out["profiling_data"] = prof
	} else {
		out["profiling_data"] = map[string]interface{}{}
	}

	return out
}

// hexBytes encodes a byte slice as a lowercase hex string, returning "" for an
// empty or nil slice so optional id fields serialize as empty strings.
func hexBytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return hex.EncodeToString(b)
}

// strconvInt formats an int32 the way protobuf map keys are serialized by
// protojson (the key is emitted as a decimal string).
func strconvInt(v int32) string {
	return strconv.FormatInt(int64(v), 10)
}

// toInt64 coerces a JSON-decoded number (float64), int64, or string to int64.
func toInt64(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case float64:
		return int64(t), true
	case jsonFloat:
		return int64(t), true
	case string:
		n, err := strconv.ParseInt(t, 10, 64)
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

// removeAnsiEscapeCodes strips ANSI escape sequences from a string, aligned
// with the Python remove_ansi_escape_codes helper.
func removeAnsiEscapeCodes(s string) string {
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == 0x1b {
			// Skip until the terminating 'm' of a CSI sequence.
			for i+1 < len(s) && s[i+1] != 'm' {
				i++
			}
			i++ // skip 'm'
			continue
		}
		sb.WriteByte(s[i])
	}
	return sb.String()
}

func asString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
