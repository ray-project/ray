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

package reporter

import (
	"context"
	"encoding/json"
	"os"
	"time"
)

// noClusterStatusMessage is the placeholder when the autoscaler status is not
// available yet, aligned with debug_status in
// python/ray/autoscaler/_private/commands.py.
const noClusterStatusMessage = "No cluster status. It may take a few seconds for the Ray internal services to start up."

// autoscalerV2Enabled reports whether the autoscaler v2 is in use, aligned with
// is_autoscaler_v2 in python/ray/autoscaler/v2/utils.py: the
// RAY_enable_autoscaler_v2 env var short-circuits to true, otherwise the GCS
// internal KV key __autoscaler_v2_enabled (namespace __autoscaler) is read.
func (r *ReportHead) autoscalerV2Enabled(ctx context.Context) bool {
	if os.Getenv("RAY_enable_autoscaler_v2") == "1" {
		return true
	}
	reply, err := r.client.InternalKVGet(ctx, autoscalerStateNamespace, autoscalerV2EnabledKey)
	if err != nil {
		return false
	}
	return string(reply.Value) == "1"
}

// debugStatus formats the autoscaler debug status, aligned with debug_status in
// python/ray/autoscaler/_private/commands.py. statusJSON is the raw
// __autoscaling_status value; errorBytes the raw __autoscaling_error value.
func (r *ReportHead) debugStatus(ctx context.Context, statusJSON, errorBytes []byte, gcsAddress string) string {
	status := ""
	if r.autoscalerV2Enabled(ctx) {
		if reply := r.fetchClusterStatusV2(ctx, gcsAddress); reply != nil {
			status = formatClusterStatusV2(parseClusterStatusV2(reply))
		} else {
			status = noClusterStatusMessage
		}
	} else {
		status = debugStatusV1(statusJSON)
	}
	if len(errorBytes) > 0 {
		status += "\n" + string(errorBytes)
	}
	return status
}

// debugStatusV1 implements the v1 branch of debug_status: parse the JSON status
// dict and format load_metrics_report + autoscaler_report via format_info_string.
func debugStatusV1(statusJSON []byte) string {
	if len(statusJSON) == 0 {
		return noClusterStatusMessage
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(statusJSON, &raw); err != nil {
		return noClusterStatusMessage
	}
	lmRaw, hasLm := raw["load_metrics_report"].(map[string]interface{})
	autoRaw, hasAuto := raw["autoscaler_report"].(map[string]interface{})
	timestamp, hasTS := raw["time"].(float64)
	if !hasLm || !hasAuto || !hasTS {
		return noClusterStatusMessage
	}
	lm := loadMetricsSummaryFromMap(lmRaw)
	auto := autoscalerSummaryFromMap(autoRaw)
	reportTime := time.Unix(int64(timestamp), 0)
	gcsRequestTime, _ := raw["gcs_request_time"].(float64)
	nonTerminatedNodesTime, _ := raw["non_terminated_nodes_time"].(float64)
	return formatInfoString(lm, auto, reportTime, gcsRequestTime, nonTerminatedNodesTime, 0)
}

// loadMetricsSummary holds the fields of python LoadMetricsSummary needed by
// format_info_string.
type loadMetricsSummary struct {
	usage          map[string][2]float64
	resourceDemand []dictCount
	pgDemand       []pgDemandEntry
	requestDemand  []dictCount
	usageByNode    map[string]map[string][2]float64
}

type dictCount struct {
	bundle map[string]float64
	count  int
}

// pgDemandEntry is one entry of the pg_demand list: a deserialized PG dict
// ({"bundles": [[bundle, count], ...], "strategy": ...}) plus its frequency.
type pgDemandEntry struct {
	pg    map[string]interface{}
	count int
}

func loadMetricsSummaryFromMap(m map[string]interface{}) *loadMetricsSummary {
	lm := &loadMetricsSummary{usage: map[string][2]float64{}}
	if u, ok := m["usage"].(map[string]interface{}); ok {
		for k, v := range u {
			if arr, ok := v.([]interface{}); ok && len(arr) == 2 {
				lm.usage[k] = [2]float64{toFloat(arr[0]), toFloat(arr[1])}
			}
		}
	}
	lm.resourceDemand = dictCountsFromMap(m["resource_demand"])
	lm.pgDemand = pgDemandsFromMap(m["pg_demand"])
	lm.requestDemand = dictCountsFromMap(m["request_demand"])
	if ubn, ok := m["usage_by_node"].(map[string]interface{}); ok {
		lm.usageByNode = map[string]map[string][2]float64{}
		for node, usage := range ubn {
			if um, ok := usage.(map[string]interface{}); ok {
				entry := map[string][2]float64{}
				for k, v := range um {
					if arr, ok := v.([]interface{}); ok && len(arr) == 2 {
						entry[k] = [2]float64{toFloat(arr[0]), toFloat(arr[1])}
					}
				}
				lm.usageByNode[node] = entry
			}
		}
	}
	return lm
}

func dictCountsFromMap(v interface{}) []dictCount {
	arr, ok := v.([]interface{})
	if !ok {
		return nil
	}
	out := make([]dictCount, 0, len(arr))
	for _, e := range arr {
		pair, ok := e.([]interface{})
		if !ok || len(pair) != 2 {
			continue
		}
		bundle := map[string]float64{}
		if bm, ok := pair[0].(map[string]interface{}); ok {
			for k, val := range bm {
				bundle[k] = toFloat(val)
			}
		}
		out = append(out, dictCount{bundle: bundle, count: int(toFloat(pair[1]))})
	}
	return out
}

func pgDemandsFromMap(v interface{}) []pgDemandEntry {
	arr, ok := v.([]interface{})
	if !ok {
		return nil
	}
	out := make([]pgDemandEntry, 0, len(arr))
	for _, e := range arr {
		pair, ok := e.([]interface{})
		if !ok || len(pair) != 2 {
			continue
		}
		pg, ok := pair[0].(map[string]interface{})
		if !ok {
			continue
		}
		out = append(out, pgDemandEntry{pg: pg, count: int(toFloat(pair[1]))})
	}
	return out
}

func toFloat(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	}
	return 0
}

// autoscalerSummary holds the fields of python AutoscalerSummary needed by
// format_info_string.
type autoscalerSummary struct {
	activeNodes             map[string]int
	idleNodes               map[string]int
	pendingNodes            [][3]string // ip, node_type, status
	pendingLaunches         map[string]int
	failedNodes             [][2]string // ip, node_type
	nodeAvailabilitySummary *nodeAvailabilitySummary
	legacy                  bool
}

type nodeAvailabilitySummary struct {
	nodeAvailabilities []nodeAvailabilityRecord
}

type nodeAvailabilityRecord struct {
	nodeType                string
	isAvailable             bool
	lastCheckedTimestamp    float64
	unavailableNodeCategory string
	unavailableNodeDesc     string
}

func autoscalerSummaryFromMap(m map[string]interface{}) *autoscalerSummary {
	as := &autoscalerSummary{
		activeNodes:     map[string]int{},
		idleNodes:       map[string]int{},
		pendingLaunches: map[string]int{},
	}
	counts := func(v interface{}) map[string]int {
		out := map[string]int{}
		if cm, ok := v.(map[string]interface{}); ok {
			for k, val := range cm {
				out[k] = int(toFloat(val))
			}
		}
		return out
	}
	as.activeNodes = counts(m["active_nodes"])
	if v, ok := m["idle_nodes"].(map[string]interface{}); ok {
		as.idleNodes = counts(v)
	}
	as.pendingLaunches = counts(m["pending_launches"])
	if arr, ok := m["pending_nodes"].([]interface{}); ok {
		for _, e := range arr {
			if t, ok := e.([]interface{}); ok && len(t) == 3 {
				as.pendingNodes = append(as.pendingNodes, [3]string{asString2(t[0]), asString2(t[1]), asString2(t[2])})
			}
		}
	}
	if arr, ok := m["failed_nodes"].([]interface{}); ok {
		for _, e := range arr {
			if t, ok := e.([]interface{}); ok && len(t) == 2 {
				as.failedNodes = append(as.failedNodes, [2]string{asString2(t[0]), asString2(t[1])})
			}
		}
	}
	if nas, ok := m["node_availability_summary"].(map[string]interface{}); ok {
		as.nodeAvailabilitySummary = nodeAvailabilitySummaryFromMap(nas)
	}
	if legacy, ok := m["legacy"].(bool); ok {
		as.legacy = legacy
	}
	return as
}

func nodeAvailabilitySummaryFromMap(m map[string]interface{}) *nodeAvailabilitySummary {
	out := &nodeAvailabilitySummary{}
	if na, ok := m["node_availabilities"].(map[string]interface{}); ok {
		for nodeType, v := range na {
			rec := nodeAvailabilityRecord{nodeType: nodeType}
			if recM, ok := v.(map[string]interface{}); ok {
				rec.isAvailable, _ = recM["is_available"].(bool)
				rec.lastCheckedTimestamp, _ = recM["last_checked_timestamp"].(float64)
				if info, ok := recM["unavailable_node_information"].(map[string]interface{}); ok {
					rec.unavailableNodeCategory, _ = info["category"].(string)
					rec.unavailableNodeDesc, _ = info["description"].(string)
				}
			}
			out.nodeAvailabilities = append(out.nodeAvailabilities, rec)
		}
	}
	return out
}

func asString2(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
