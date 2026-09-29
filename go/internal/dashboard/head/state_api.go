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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ray-project/ray/go/proto"
	"google.golang.org/protobuf/encoding/protojson"
	goproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// StateAPIManager queries GCS state and aggregates it into ListApiResponse,
// aligned with the Python StateAPIManager in
// python/ray/dashboard/state_aggregator.py.
type StateAPIManager struct {
	client *GCSClient
	// ListJobsFn assembles the jobs list in the Python JobDetails form. It is
	// injected by the state module (which can depend on the job package) to
	// avoid a package cycle: head -> job would be circular since job depends
	// on head. When nil, ListJobs falls back to the raw proto conversion.
	ListJobsFn func(ctx context.Context) ([]map[string]interface{}, error)
}

// NewStateAPIManager creates a state API manager backed by the GCS client.
func NewStateAPIManager(client *GCSClient) *StateAPIManager {
	return &StateAPIManager{client: client}
}

// ListActors lists all actors.
func (m *StateAPIManager) ListActors(ctx context.Context, opt *ListApiOptions) (*ListApiResponse, error) {
	if err := validateFilters(opt, "actors"); err != nil {
		return nil, err
	}
	opt.Resource = "actors"
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(opt.Timeout)*time.Second)
	defer cancel()
	reply, err := m.client.GetAllActorInfo(ctxTimeout, &proto.GetAllActorInfoRequest{Limit: dataSourceLimitPtr()})
	if err != nil {
		return nil, fmt.Errorf("query actors from GCS: %w", err)
	}
	entries := make([]map[string]interface{}, 0, len(reply.ActorTableData))
	for _, a := range reply.ActorTableData {
		entries = append(entries, protoMessageToDict(a, actorDecodeFields))
	}
	return buildListResponse(entries, reply.Total, reply.NumFiltered, opt), nil
}

// ListJobs lists all jobs in the Python JobDetails form. When a ListJobsFn is
// injected it takes precedence, matching get_job_info in
// python/ray/util/state/state_manager.py which returns driver + submission
// JobDetails; otherwise the raw JobTableData proto conversion is returned.
func (m *StateAPIManager) ListJobs(ctx context.Context, opt *ListApiOptions) (*ListApiResponse, error) {
	if err := validateFilters(opt, "jobs"); err != nil {
		return nil, err
	}
	opt.Resource = "jobs"
	if m.ListJobsFn != nil {
		entries, err := m.ListJobsFn(ctx)
		if err != nil {
			return nil, fmt.Errorf("query jobs: %w", err)
		}
		total := len(entries)
		return buildListResponse(entries, int64(total), 0, opt), nil
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(opt.Timeout)*time.Second)
	defer cancel()
	reply, err := m.client.GetAllJobInfo(ctxTimeout, &proto.GetAllJobInfoRequest{Limit: dataSourceLimitInt32Ptr()})
	if err != nil {
		return nil, fmt.Errorf("query jobs from GCS: %w", err)
	}
	entries := make([]map[string]interface{}, 0, len(reply.JobInfoList))
	for _, j := range reply.JobInfoList {
		entries = append(entries, protoMessageToDict(j, jobDecodeFields))
	}
	return buildListResponse(entries, int64(len(entries)), 0, opt), nil
}

// ListNodes lists all nodes.
func (m *StateAPIManager) ListNodes(ctx context.Context, opt *ListApiOptions) (*ListApiResponse, error) {
	if err := validateFilters(opt, "nodes"); err != nil {
		return nil, err
	}
	opt.Resource = "nodes"
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(opt.Timeout)*time.Second)
	defer cancel()
	reply, err := m.client.GetAllNodeInfo(ctxTimeout, &proto.GetAllNodeInfoRequest{Limit: dataSourceLimitPtr()})
	if err != nil {
		return nil, fmt.Errorf("query nodes from GCS: %w", err)
	}
	entries := make([]map[string]interface{}, 0, len(reply.NodeInfoList))
	for _, n := range reply.NodeInfoList {
		d := protoMessageToDict(n, nodeDecodeFields)
		// Align with list_nodes in state_aggregator.py: expose the node
		// manager address as node_ip, convert the int64 time fields to int,
		// and compose the state_message from the death info.
		if v, ok := d["node_manager_address"]; ok {
			d["node_ip"] = v
		}
		intifyTimeFields(d, "start_time_ms", "end_time_ms")
		d["state_message"] = ComposeStateMessage(n.DeathInfo)
		entries = append(entries, d)
	}
	return buildListResponse(entries, reply.Total, reply.NumFiltered, opt), nil
}

// ListPlacementGroups lists all placement groups.
func (m *StateAPIManager) ListPlacementGroups(ctx context.Context, opt *ListApiOptions) (*ListApiResponse, error) {
	if err := validateFilters(opt, "placement_groups"); err != nil {
		return nil, err
	}
	opt.Resource = "placement_groups"
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(opt.Timeout)*time.Second)
	defer cancel()
	reply, err := m.client.GetAllPlacementGroup(ctxTimeout, &proto.GetAllPlacementGroupRequest{Limit: dataSourceLimitPtr()})
	if err != nil {
		return nil, fmt.Errorf("query placement groups from GCS: %w", err)
	}
	entries := make([]map[string]interface{}, 0, len(reply.PlacementGroupTableData))
	for _, p := range reply.PlacementGroupTableData {
		entries = append(entries, protoMessageToDict(p, pgDecodeFields))
	}
	return buildListResponse(entries, reply.Total, 0, opt), nil
}

// ListWorkers lists all workers.
func (m *StateAPIManager) ListWorkers(ctx context.Context, opt *ListApiOptions) (*ListApiResponse, error) {
	if err := validateFilters(opt, "workers"); err != nil {
		return nil, err
	}
	opt.Resource = "workers"
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(opt.Timeout)*time.Second)
	defer cancel()
	reply, err := m.client.GetAllWorkerInfo(ctxTimeout, &proto.GetAllWorkerInfoRequest{Limit: dataSourceLimitPtr()})
	if err != nil {
		return nil, fmt.Errorf("query workers from GCS: %w", err)
	}
	entries := make([]map[string]interface{}, 0, len(reply.WorkerTableData))
	for _, w := range reply.WorkerTableData {
		d := protoMessageToDict(w, workerDecodeFields)
		// Align with list_workers in state_aggregator.py: flatten the
		// worker_address sub-message into worker_id / node_id / ip, and convert
		// the uint64 time fields to int.
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
		entries = append(entries, d)
	}
	return buildListResponse(entries, reply.Total, reply.NumFiltered, opt), nil
}

// ListTasks lists task events from the GCS task info service.
func (m *StateAPIManager) ListTasks(ctx context.Context, opt *ListApiOptions) (*ListApiResponse, error) {
	if err := validateFilters(opt, "tasks"); err != nil {
		return nil, err
	}
	opt.Resource = "tasks"
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(opt.Timeout)*time.Second)
	defer cancel()
	req := &proto.GetTaskEventsRequest{Limit: dataSourceLimitPtr()}
	req.Filters = &proto.GetTaskEventsRequest_Filters{ExcludeDriver: &opt.ExcludeDriver}
	// The state/name/job_id/task_id/actor_id filters are pushed down to GCS so
	// the source returns only the matching task events, aligned with the filter
	// conversion in get_all_task_info (python/ray/util/state/state_manager.py).
	// The events GCS returns are then the filtered set, so total and
	// num_after_truncation count the post-filter events (plus the dropped status
	// events) just like Python. The remaining filter columns are still applied
	// locally in buildListResponse.
	taskFiltersToGCS(opt, req.Filters)
	reply, err := m.client.GetTaskEvents(ctxTimeout, req)
	if err != nil {
		return nil, fmt.Errorf("query tasks from GCS: %w", err)
	}
	if reply.Status != nil && reply.Status.Code != 0 {
		empty := ""
		return &ListApiResponse{
			Warnings:              &[]string{reply.Status.Message},
			PartialFailureWarning: &empty,
		}, nil
	}
	entries := make([]map[string]interface{}, 0, len(reply.EventsByTask))
	for _, t := range reply.EventsByTask {
		entries = append(entries, taskEventToStateDict(t))
	}
	// Aligned with list_tasks in state_aggregator.py: the total counts the
	// returned events plus the status events dropped on GCS, and
	// num_after_truncation is the raw event count.
	return buildListResponse(entries, int64(len(entries))+int64(reply.NumStatusTaskEventsDropped), 0, opt), nil
}

// SummarizeTasks summarizes the cluster tasks grouped by func_or_class_name,
// aligned with summarize_tasks / TaskSummaries.to_summary_by_func_name in
// python/ray/dashboard/state_aggregator.py and python/ray/util/state/common.py.
// The returned map carries the snake_case SummaryApiResponse shape used by the
// dashboard frontend (node_id_to_summary.cluster.summary).
func (m *StateAPIManager) SummarizeTasks(ctx context.Context, opt *SummaryApiOptions) (map[string]interface{}, error) {
	if opt.SummaryBy == "lineage" {
		return m.summarizeTasksByLineage(ctx, opt)
	}
	if opt.SummaryBy != "" && opt.SummaryBy != "func_name" {
		return nil, newValueError(`summary_by must be one of "func_name" or "lineage".`)
	}
	// Summary requests pull the maximum number of entries to minimize data loss,
	// aligned with RAY_MAX_LIMIT_FROM_API_SERVER in the Python implementation.
	// Driver tasks are excluded, matching the ListApiOptions default
	// exclude_driver=True used by summarize_tasks in state_aggregator.py.
	listOpt := &ListApiOptions{
		Timeout:       opt.Timeout,
		Limit:         maxListLimit,
		ExcludeDriver: true,
		FilterKeys:    opt.FilterKeys, FilterPredicates: opt.FilterPredicates, FilterValues: opt.FilterValues,
	}
	listResp, err := m.ListTasks(ctx, listOpt)
	if err != nil {
		return nil, err
	}
	summary := map[string]interface{}{}
	totalTasks, totalActorTasks, totalActorScheduled := 0, 0, 0
	for _, task := range listResp.Result {
		key := asString(task["func_or_class_name"])
		perFunc, ok := summary[key].(map[string]interface{})
		if !ok {
			perFunc = map[string]interface{}{
				"func_or_class_name": key,
				"type":               asString(task["type"]),
				"state_counts":       map[string]interface{}{},
			}
			summary[key] = perFunc
		}
		state := asString(task["state"])
		stateCounts, _ := perFunc["state_counts"].(map[string]interface{})
		count := 0
		switch t := stateCounts[state].(type) {
		case int:
			count = t
		case float64:
			count = int(t)
		}
		stateCounts[state] = count + 1
		// Count totals by task type, aligned with the Python TaskType enum check.
		switch taskTypeNum(asString(task["type"])) {
		case proto.TaskType_NORMAL_TASK:
			totalTasks++
		case proto.TaskType_ACTOR_CREATION_TASK:
			totalActorScheduled++
		case proto.TaskType_ACTOR_TASK:
			totalActorTasks++
		}
	}
	nodeToSummary := map[string]interface{}{
		"cluster": map[string]interface{}{
			"summary":               summary,
			"total_tasks":           totalTasks,
			"total_actor_tasks":     totalActorTasks,
			"total_actor_scheduled": totalActorScheduled,
			"summary_by":            "func_name",
		},
	}
	// Aligned with summarize_tasks: a missing-data warning is appended when the
	// summarized task counts fall short of num_filtered.
	var warnings []interface{}
	if totalTasks+totalActorTasks+totalActorScheduled < listResp.NumFiltered {
		warnings = []interface{}{
			"There is missing data in this aggregation. Possibly due to task data being evicted to preserve memory.",
		}
	}
	return map[string]interface{}{
		"total":                   listResp.Total,
		"result":                  map[string]interface{}{"node_id_to_summary": nodeToSummary},
		"partial_failure_warning": listResp.PartialFailureWarning,
		"warnings":                warnings,
		"num_after_truncation":    listResp.NumAfterTruncation,
		"num_filtered":            listResp.NumFiltered,
	}, nil
}

// taskFiltersToGCS converts the list option filters whose columns GCS can
// filter on (state/name/job_id/task_id/actor_id) into the
// GetTaskEventsRequest_Filters pushdown fields, aligned with the filter
// conversion in get_all_task_info of python/ray/util/state/state_manager.py:
// the hex id filters are decoded to their raw binary form and the predicate
// maps "=" / "!=" to FilterPredicate.EQUAL / NOT_EQUAL. Filter columns GCS
// cannot filter on are left to the local applyFilters pass.
func taskFiltersToGCS(opt *ListApiOptions, filters *proto.GetTaskEventsRequest_Filters) {
	for i, key := range opt.FilterKeys {
		pred := proto.FilterPredicate_EQUAL
		if i < len(opt.FilterPredicates) && opt.FilterPredicates[i] == "!=" {
			pred = proto.FilterPredicate_NOT_EQUAL
		}
		want := ""
		if i < len(opt.FilterValues) {
			want = opt.FilterValues[i]
		}
		switch key {
		case "state":
			filters.StateFilters = append(filters.StateFilters, &proto.GetTaskEventsRequest_Filters_StateFilter{
				Predicate: pred,
				State:     want,
			})
		case "name":
			filters.TaskNameFilters = append(filters.TaskNameFilters, &proto.GetTaskEventsRequest_Filters_TaskNameFilter{
				Predicate: pred,
				TaskName:  want,
			})
		case "job_id":
			filters.JobFilters = append(filters.JobFilters, &proto.GetTaskEventsRequest_Filters_JobIdFilter{
				Predicate: pred,
				JobId:     hexToBytes(want),
			})
		case "task_id":
			filters.TaskFilters = append(filters.TaskFilters, &proto.GetTaskEventsRequest_Filters_TaskIdFilter{
				Predicate: pred,
				TaskId:    hexToBytes(want),
			})
		case "actor_id":
			filters.ActorFilters = append(filters.ActorFilters, &proto.GetTaskEventsRequest_Filters_ActorIdFilter{
				Predicate: pred,
				ActorId:   hexToBytes(want),
			})
		}
	}
}

// hexToBytes decodes a hex string to its raw bytes, returning nil on a malformed
// input so the id filter is a no-op on GCS (mirroring binascii.unhexlify on a
// valid Ray hex id).
func hexToBytes(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

// driverTaskIDPrefix is the hex prefix of every driver task id. Tasks whose
// parent_task_id carries this prefix are roots of the lineage tree, aligned with
// DRIVER_TASK_ID_PREFIX in python/ray/util/state/common.py.
const driverTaskIDPrefix = "ffffffffffffffffffffffffffffffffffffffff"

// nestedTaskSummary mirrors the Python NestedTaskSummary dataclass: a node in
// the task lineage tree. type is the TaskType string, "ACTOR" for a node
// representing an actor, or "GROUP" for a merge of same-named siblings.
type nestedTaskSummary struct {
	Name        string                 `json:"name"`
	Key         string                 `json:"key"`
	Type        string                 `json:"type"`
	Timestamp   *int64                 `json:"timestamp"`
	StateCounts map[string]int         `json:"state_counts"`
	Children    []*nestedTaskSummary   `json:"children"`
	Link        *nestedTaskSummaryLink `json:"link"`
}

// MarshalJSON serializes a lineage node so that a nil Children (a leaf node
// built without siblings) is emitted as "children": [] rather than null. The
// Python NestedTaskSummary always carries an empty list (field default_factory),
// and the dashboard frontend maps over children (formatToJobProgressGroup in
// useJobProgress.ts) which would throw on null.
func (g *nestedTaskSummary) MarshalJSON() ([]byte, error) {
	type alias nestedTaskSummary
	if g.Children == nil {
		clone := *g
		clone.Children = []*nestedTaskSummary{}
		return json.Marshal((*alias)(&clone))
	}
	return json.Marshal((*alias)(g))
}

// nestedTaskSummaryLink is a reference to the task or actor backing a lineage
// node, aligned with Link in python/ray/util/state/common.py.
type nestedTaskSummaryLink struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// summarizeTasksByLineage summarizes tasks by lineage: each task is grouped
// under its parent (the actor owning actor tasks, the parent task otherwise)
// into a nested tree, aligned with TaskSummaries.to_summary_by_lineage in
// python/ray/util/state/common.py. The cluster.summary is a list (not a dict)
// of nestedTaskSummary nodes.
func (m *StateAPIManager) summarizeTasksByLineage(ctx context.Context, opt *SummaryApiOptions) (map[string]interface{}, error) {
	listOpt := &ListApiOptions{
		Timeout:       opt.Timeout,
		Limit:         maxListLimit,
		Detail:        true,
		ExcludeDriver: true,
		FilterKeys:    opt.FilterKeys, FilterPredicates: opt.FilterPredicates, FilterValues: opt.FilterValues,
	}
	taskResp, err := m.ListTasks(ctx, listOpt)
	if err != nil {
		return nil, err
	}
	// Lineage needs the actor table to resolve actor-task parents.
	actorResp, err := m.ListActors(ctx, &ListApiOptions{Timeout: opt.Timeout, Limit: maxListLimit})
	if err != nil {
		return nil, err
	}

	roots, totalTasks, totalActorTasks, totalActorScheduled := buildTaskLineage(taskResp.Result, actorResp.Result)

	var warnings []interface{}
	if totalTasks+totalActorTasks+totalActorScheduled < taskResp.NumFiltered {
		warnings = []interface{}{
			"There is missing data in this aggregation. Possibly due to task data being evicted to preserve memory.",
		}
	}
	cluster := map[string]interface{}{
		"summary":               roots,
		"total_tasks":           totalTasks,
		"total_actor_tasks":     totalActorTasks,
		"total_actor_scheduled": totalActorScheduled,
		"summary_by":            "lineage",
	}
	return map[string]interface{}{
		"total":                   taskResp.Total,
		"result":                  map[string]interface{}{"node_id_to_summary": map[string]interface{}{"cluster": cluster}},
		"partial_failure_warning": taskResp.PartialFailureWarning,
		"warnings":                warnings,
		"num_after_truncation":    taskResp.NumAfterTruncation,
		"num_filtered":            taskResp.NumFiltered,
	}, nil
}

// buildTaskLineage builds the nested lineage tree from task and actor dicts,
// aligned with TaskSummaries.to_summary_by_lineage in
// python/ray/util/state/common.py. It indexes the tasks, constructs the
// ownership tree (actor tasks hang off the actor owning them, other tasks off
// their parent task), merges same-named siblings, rolls state counts up into
// parents, and sorts each level. It returns the top-level nodes and the task
// type totals.
func buildTaskLineage(tasks, actors []map[string]interface{}) (roots []*nestedTaskSummary, totalTasks, totalActorTasks, totalActorScheduled int) {
	// Step 1: index tasks by id and map actor ids to their creation task ids.
	// A parent task may appear after its child, so the full index must exist
	// before the tree is built.
	tasksByID := map[string]map[string]interface{}{}
	creationTaskByActorID := map[string]string{}
	for _, t := range tasks {
		id := asString(t["task_id"])
		if id != "" {
			tasksByID[id] = t
		}
		if taskTypeNum(asString(t["type"])) == proto.TaskType_ACTOR_CREATION_TASK {
			if aid := asString(t["actor_id"]); aid != "" {
				creationTaskByActorID[aid] = id
			}
		}
	}
	actorByID := map[string]map[string]interface{}{}
	for _, a := range actors {
		if id := asString(a["actor_id"]); id != "" {
			actorByID[id] = a
		}
	}

	// Step 2: build the ownership tree. A nil node means data about the node or
	// one of its ancestors is missing; the subtree rooted there is dropped.
	groups := map[string]*nestedTaskSummary{}

	var taskGroup func(taskID string) *nestedTaskSummary
	var actorGroup func(actorID string) *nestedTaskSummary

	attach := func(child *nestedTaskSummary, parentID string, isRoot bool) {
		if isRoot {
			roots = append(roots, child)
			return
		}
		if parent := taskGroup(parentID); parent != nil {
			parent.Children = append(parent.Children, child)
		}
	}

	taskGroup = func(taskID string) *nestedTaskSummary {
		if g, ok := groups[taskID]; ok {
			return g
		}
		t, ok := tasksByID[taskID]
		if !ok {
			// Missing data about this parent; the whole tree is dropped at this
			// node.
			return nil
		}
		funcName := asString(t["name"])
		if funcName == "" {
			funcName = asString(t["func_or_class_name"])
		}
		g := &nestedTaskSummary{
			Name:        funcName,
			Key:         taskID,
			Type:        asString(t["type"]),
			StateCounts: map[string]int{},
		}
		if ts, ok := toInt64(t["creation_time_ms"]); ok {
			g.Timestamp = &ts
		}
		g.Link = &nestedTaskSummaryLink{Type: "task", ID: taskID}
		groups[taskID] = g

		switch taskTypeNum(g.Type) {
		case proto.TaskType_ACTOR_TASK, proto.TaskType_ACTOR_CREATION_TASK:
			// Actor tasks hang off the actor, not the parent task.
			if ag := actorGroup(asString(t["actor_id"])); ag != nil {
				ag.Children = append(ag.Children, g)
			}
		default:
			parentID := asString(t["parent_task_id"])
			attach(g, parentID, parentID == "" || strings.HasPrefix(parentID, driverTaskIDPrefix))
		}
		return g
	}

	actorGroup = func(actorID string) *nestedTaskSummary {
		if actorID == "" {
			return nil
		}
		key := "actor:" + actorID
		if g, ok := groups[key]; ok {
			return g
		}
		creationTaskID, ok := creationTaskByActorID[actorID]
		if !ok {
			// Missing data about the actor's creation task; drop the tree.
			return nil
		}
		creationTask, ok := tasksByID[creationTaskID]
		if !ok {
			return nil
		}
		actorName := ""
		if a, ok := actorByID[actorID]; ok {
			actorName = asString(a["repr_name"])
			if actorName == "" {
				actorName = asString(a["class_name"])
			}
		} else {
			// No actor info despite an existing creation task: fall back to the
			// creation task's func_or_class_name, aligned with the Python split
			// on the final ".".
			funcName := asString(creationTask["func_or_class_name"])
			if i := strings.LastIndex(funcName, "."); i >= 0 {
				actorName = funcName[:i]
			} else {
				actorName = funcName
			}
		}
		g := &nestedTaskSummary{
			Name:        actorName,
			Key:         key,
			Type:        "ACTOR",
			StateCounts: map[string]int{},
		}
		if ts, ok := toInt64(creationTask["creation_time_ms"]); ok {
			g.Timestamp = &ts
		}
		g.Link = &nestedTaskSummaryLink{Type: "actor", ID: actorID}
		groups[key] = g

		parentID := asString(creationTask["parent_task_id"])
		attach(g, parentID, parentID == "" || strings.HasPrefix(parentID, driverTaskIDPrefix))
		return g
	}

	for _, t := range tasks {
		id := asString(t["task_id"])
		g := taskGroup(id)
		if g == nil {
			continue
		}
		state := asString(t["state"])
		g.StateCounts[state]++
	}

	// Step 3: merge same-named siblings into GROUP nodes.
	roots, _ = mergeTaskSiblings(roots)

	// Step 4: roll child state counts up into parents and sort each level.
	for i := range roots {
		roots[i] = calcTaskTotal(roots[i])
	}
	sortTaskGroups(roots)

	for _, t := range tasks {
		switch taskTypeNum(asString(t["type"])) {
		case proto.TaskType_NORMAL_TASK:
			totalTasks++
		case proto.TaskType_ACTOR_CREATION_TASK:
			totalActorScheduled++
		case proto.TaskType_ACTOR_TASK:
			totalActorTasks++
		}
	}
	return roots, totalTasks, totalActorTasks, totalActorScheduled
}

// mergeTaskSiblings merges sibling nodes that share a name into a single GROUP
// node when there is more than one of them; a lone child is returned unwrapped.
// The second return value is the smallest timestamp among the siblings, aligned
// with merge_sibings_for_task_group in python/ray/util/state/common.py.
func mergeTaskSiblings(siblings []*nestedTaskSummary) ([]*nestedTaskSummary, *int64) {
	if len(siblings) == 0 {
		return siblings, nil
	}
	// Recurse first so the merge happens bottom-up.
	for _, child := range siblings {
		merged, minTS := mergeTaskSiblings(child.Children)
		child.Children = merged
		// A zero timestamp is falsy in Python and skipped, matching that here.
		if minTS != nil && *minTS != 0 && (child.Timestamp == nil || *minTS < *child.Timestamp) {
			child.Timestamp = minTS
		}
	}
	// Group by name, preserving first-seen order.
	var order []string
	groups := map[string]*nestedTaskSummary{}
	for _, child := range siblings {
		if _, ok := groups[child.Name]; !ok {
			groups[child.Name] = &nestedTaskSummary{
				Name:        child.Name,
				Key:         child.Name,
				Type:        "GROUP",
				StateCounts: map[string]int{},
			}
			order = append(order, child.Name)
		}
		groups[child.Name].Children = append(groups[child.Name].Children, child)
		// A zero child timestamp is falsy in Python and skipped, matching that here.
		if child.Timestamp != nil && *child.Timestamp != 0 &&
			(groups[child.Name].Timestamp == nil || *child.Timestamp < *groups[child.Name].Timestamp) {
			groups[child.Name].Timestamp = child.Timestamp
		}
	}
	var minTS *int64
	for _, name := range order {
		g := groups[name]
		if g.Timestamp != nil && (minTS == nil || *g.Timestamp < *minTS) {
			minTS = g.Timestamp
		}
	}
	// Keep the group only when it has more than one child; otherwise flatten.
	out := make([]*nestedTaskSummary, 0, len(order))
	for _, name := range order {
		g := groups[name]
		if len(g.Children) > 1 {
			out = append(out, g)
		} else {
			out = append(out, g.Children[0])
		}
	}
	return out, minTS
}

// calcTaskTotal rolls the state counts of every descendant up into the node and
// sorts the children by priority, aligned with calc_total_for_task_group in
// python/ray/util/state/common.py.
func calcTaskTotal(g *nestedTaskSummary) *nestedTaskSummary {
	if len(g.Children) == 0 {
		return g
	}
	for _, child := range g.Children {
		totaled := calcTaskTotal(child)
		for state, count := range totaled.StateCounts {
			g.StateCounts[state] += count
		}
	}
	sortTaskGroups(g.Children)
	return g
}

// runningTaskCount counts tasks in running states, aligned with
// get_running_tasks_count in python/ray/util/state/common.py.
func runningTaskCount(g *nestedTaskSummary) int {
	return g.StateCounts["RUNNING"] + g.StateCounts["RUNNING_IN_RAY_GET"] + g.StateCounts["RUNNING_IN_RAY_WAIT"]
}

// pendingTaskCount counts tasks in pending states, aligned with
// get_pending_tasks_count in python/ray/util/state/common.py.
func pendingTaskCount(g *nestedTaskSummary) int {
	return g.StateCounts["PENDING_ARGS_AVAIL"] +
		g.StateCounts["PENDING_NODE_ASSIGNMENT"] +
		g.StateCounts["PENDING_OBJ_STORE_MEM_AVAIL"] +
		g.StateCounts["PENDING_ARGS_FETCH"]
}

// sortTaskGroups sorts task groups by running, pending, failed, timestamp and
// actor-creation priority. Python applies a sequence of stable sorts from the
// lowest-priority key to the highest so the highest-priority key dominates;
// the same ordering is reproduced by a single sort using a compound key.
// Note: the Python sort key uses the misspelled "FAIELD" in
// to_summary_by_lineage (python/ray/util/state/common.py), so a task group
// with zero FAILED tasks sorts differently there; the Go implementation
// intentionally keeps the correct "FAILED" key.
func sortTaskGroups(groups []*nestedTaskSummary) {
	sort.SliceStable(groups, func(i, j int) bool {
		a, b := groups[i], groups[j]
		ar, br := runningTaskCount(a), runningTaskCount(b)
		if ar != br {
			return ar > br
		}
		ap, bp := pendingTaskCount(a), pendingTaskCount(b)
		if ap != bp {
			return ap > bp
		}
		af, bf := a.StateCounts["FAILED"], b.StateCounts["FAILED"]
		if af != bf {
			return af > bf
		}
		at, bt := a.Timestamp, b.Timestamp
		if at == nil && bt == nil {
			return false
		}
		if at == nil {
			return false
		}
		if bt == nil {
			return true
		}
		if *at != *bt {
			return *at < *bt
		}
		// Actor creation tasks sort above others with the same timestamp.
		return a.Type == "ACTOR_CREATION_TASK" && b.Type != "ACTOR_CREATION_TASK"
	})
}

// taskTypeNum maps a task type string to its proto enum value. Unknown or empty
// types map to NORMAL_TASK, matching the Python enum lookup which defaults to 0.
func taskTypeNum(s string) proto.TaskType {
	switch s {
	case "ACTOR_CREATION_TASK":
		return proto.TaskType_ACTOR_CREATION_TASK
	case "ACTOR_TASK":
		return proto.TaskType_ACTOR_TASK
	case "DRIVER_TASK":
		return proto.TaskType_DRIVER_TASK
	default:
		return proto.TaskType_NORMAL_TASK
	}
}

// SummarizeActors summarizes the cluster actors grouped by class_name, aligned
// with summarize_actors / ActorSummaries.to_summary in
// python/ray/dashboard/state_aggregator.py and python/ray/util/state/common.py.
// The returned map carries the snake_case SummaryApiResponse shape used by the
// dashboard frontend (node_id_to_summary.cluster.summary).
func (m *StateAPIManager) SummarizeActors(ctx context.Context, opt *SummaryApiOptions) (map[string]interface{}, error) {
	listOpt := &ListApiOptions{
		Timeout:    opt.Timeout,
		Limit:      maxListLimit,
		FilterKeys: opt.FilterKeys, FilterPredicates: opt.FilterPredicates, FilterValues: opt.FilterValues,
	}
	listResp, err := m.ListActors(ctx, listOpt)
	if err != nil {
		return nil, err
	}
	summary := map[string]interface{}{}
	totalActors := 0
	for _, actor := range listResp.Result {
		class := asString(actor["class_name"])
		perClass, ok := summary[class].(map[string]interface{})
		if !ok {
			perClass = map[string]interface{}{
				"class_name":   class,
				"state_counts": map[string]interface{}{},
			}
			summary[class] = perClass
		}
		state := asString(actor["state"])
		stateCounts, _ := perClass["state_counts"].(map[string]interface{})
		count := 0
		switch t := stateCounts[state].(type) {
		case int:
			count = t
		case float64:
			count = int(t)
		}
		stateCounts[state] = count + 1
		totalActors++
	}
	cluster := map[string]interface{}{
		"summary":      summary,
		"total_actors": totalActors,
		"summary_by":   "class",
	}
	return map[string]interface{}{
		"total":                   listResp.Total,
		"result":                  map[string]interface{}{"node_id_to_summary": map[string]interface{}{"cluster": cluster}},
		"partial_failure_warning": listResp.PartialFailureWarning,
		"warnings":                listResp.Warnings,
		"num_after_truncation":    listResp.NumAfterTruncation,
		"num_filtered":            listResp.NumFiltered,
	}, nil
}

// SummarizeObjects summarizes the cluster objects grouped by call_site, aligned
// with summarize_objects / ObjectSummaries.to_summary in
// python/ray/dashboard/state_aggregator.py and python/ray/util/state/common.py.
func (m *StateAPIManager) SummarizeObjects(ctx context.Context, opt *SummaryApiOptions) (map[string]interface{}, error) {
	listOpt := &ListApiOptions{
		Timeout:    opt.Timeout,
		Limit:      maxListLimit,
		FilterKeys: opt.FilterKeys, FilterPredicates: opt.FilterPredicates, FilterValues: opt.FilterValues,
	}
	listResp, err := m.ListObjects(ctx, listOpt)
	if err != nil {
		return nil, err
	}
	// Aligned with ObjectSummaries.to_summary: the cluster summary groups
	// objects by call_site and rolls up sizes/counts; an empty object list
	// yields an empty summary with callsite collection enabled.
	summary := map[string]interface{}{}
	totalObjects := 0
	totalSizeMB := 0.0
	callsiteEnabled := true
	keyToWorkers := map[string]map[interface{}]struct{}{}
	keyToNodes := map[string]map[interface{}]struct{}{}
	for _, object := range listResp.Result {
		key := asString(object["call_site"])
		if key == "disabled" {
			callsiteEnabled = false
		}
		perKey, ok := summary[key].(map[string]interface{})
		if !ok {
			perKey = map[string]interface{}{
				"total_objects":              0,
				"total_size_mb":              0,
				"total_num_workers":          0,
				"total_num_nodes":            0,
				"task_state_counts":          map[string]interface{}{},
				"task_attempt_number_counts": map[string]interface{}{},
				"ref_type_counts":            map[string]interface{}{},
			}
			summary[key] = perKey
			keyToWorkers[key] = map[interface{}]struct{}{}
			keyToNodes[key] = map[interface{}]struct{}{}
		}
		countBy(object, "task_status", perKey["task_state_counts"])
		countBy(object, "attempt_number", perKey["task_attempt_number_counts"])
		countBy(object, "reference_type", perKey["ref_type_counts"])
		perKey["total_objects"] = intVal(perKey["total_objects"]) + 1
		totalObjects++
		curSize, _ := toFloat64(perKey["total_size_mb"])
		sizeBytes, _ := toFloat64(object["object_size"])
		// object_size's unit is byte by default; -1 means the size is unknown.
		if sizeBytes != -1 {
			curSize += sizeBytes / (1024 * 1024)
			totalSizeMB += sizeBytes / (1024 * 1024)
		}
		perKey["total_size_mb"] = curSize
		keyToWorkers[key][object["pid"]] = struct{}{}
		keyToNodes[key][object["ip"]] = struct{}{}
	}
	for key, workers := range keyToWorkers {
		summary[key].(map[string]interface{})["total_num_workers"] = len(workers)
	}
	for key, nodes := range keyToNodes {
		summary[key].(map[string]interface{})["total_num_nodes"] = len(nodes)
	}
	cluster := map[string]interface{}{
		"summary":          summary,
		"total_objects":    totalObjects,
		"total_size_mb":    totalSizeMB,
		"callsite_enabled": callsiteEnabled,
		"summary_by":       "callsite",
	}
	return map[string]interface{}{
		"total":                   listResp.Total,
		"result":                  map[string]interface{}{"node_id_to_summary": map[string]interface{}{"cluster": cluster}},
		"partial_failure_warning": listResp.PartialFailureWarning,
		"warnings":                listResp.Warnings,
		"num_after_truncation":    listResp.NumAfterTruncation,
		"num_filtered":            listResp.NumFiltered,
	}, nil
}

// countBy increments the counter for the given object column in the counts map,
// using the column's string form as the key (aligned with the Python dict keyed
// by state/attempt/reference-type strings).
func countBy(object map[string]interface{}, col string, counts interface{}) {
	m, _ := counts.(map[string]interface{})
	if m == nil {
		return
	}
	key := fmt.Sprintf("%v", object[col])
	count := 0
	switch t := m[key].(type) {
	case int:
		count = t
	case float64:
		count = int(t)
	}
	m[key] = count + 1
}

// intVal coerces a JSON number to int.
func intVal(v interface{}) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	}
	return 0
}

// ListObjects lists all objects from the cluster, aligned with list_objects in
// python/ray/dashboard/state_aggregator.py: it queries every live raylet's
// GetObjectsInfo, folds the core worker stats through the memory table and
// returns the ObjectState entries. partial_failure_warning is null here
// (Python list_objects passes None explicitly, unlike the dataclass default ""
// of the other endpoints) unless at least one node failed to reply.
// callsiteWarning is the warning appended when object ref creation sites are
// not recorded (RAY_record_ref_creation_sites unset).
const callsiteWarning = "Callsite is not being recorded. To record callsite information for each ObjectRef created, set env variable RAY_record_ref_creation_sites=1 during `ray start` and `ray.init`."

func (m *StateAPIManager) ListObjects(ctx context.Context, opt *ListApiOptions) (*ListApiResponse, error) {
	opt.Resource = "objects"
	if err := validateFilters(opt, "objects"); err != nil {
		return nil, err
	}
	result, total, pfw, err := m.listObjects(ctx, opt)
	if err != nil {
		return nil, err
	}
	// Python's callsite_warning is always a list (empty when ref-creation
	// sites are enabled and no warning fires); keep it non-nil so the JSON
	// response carries [] instead of null for consumers that treat it as an
	// array.
	warnings := &[]string{}
	if n, _ := strconv.Atoi(os.Getenv("RAY_record_ref_creation_sites")); n == 0 {
		*warnings = []string{callsiteWarning}
	}
	// Aligned with the Python flow: num_after_truncation counts the memory
	// table entries, then filters apply, then sort by object_id and truncate.
	numAfterTruncation := len(result)
	truncated := filterAndSort(result, opt)
	if opt.Resource != "" {
		for i := range truncated {
			truncated[i] = filterFields(truncated[i], opt)
		}
	}
	return &ListApiResponse{
		Result:                truncated,
		Total:                 total,
		NumAfterTruncation:    numAfterTruncation,
		NumFiltered:           len(applyFilters(result, opt)),
		PartialFailureWarning: pfw,
		Warnings:              warnings,
	}, nil
}

// ListRuntimeEnvs lists runtime envs, aligned with list_runtime_envs in
// python/ray/dashboard/state_aggregator.py: it queries every live agent's
// runtime_env_agent_port HTTP /get_runtime_envs_info endpoint (a serialized
// GetRuntimeEnvsInfoRequest body) and folds the reply's RuntimeEnvState entries
// into the RuntimeEnvState schema. Each state's runtime_env string is
// deserialized from JSON into a dict (aligned with
// RuntimeEnv.deserialize().to_dict()).
func (m *StateAPIManager) ListRuntimeEnvs(ctx context.Context, opt *ListApiOptions) (*ListApiResponse, error) {
	opt.Resource = "runtime_envs"
	if err := validateFilters(opt, "runtime_envs"); err != nil {
		return nil, err
	}
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(opt.Timeout)*time.Second)
	defer cancel()
	// Query only the live nodes, aligned with the ("state", "=", "ALIVE")
	// source-side filter in Python list_runtime_envs. Agents advertise their
	// HTTP port on the node info; nodes without one are skipped.
	alive := proto.GcsNodeInfo_ALIVE
	allNodeReply, err := m.client.GetAllNodeInfo(ctxTimeout, &proto.GetAllNodeInfoRequest{
		Limit:       dataSourceLimitPtr(),
		StateFilter: &alive,
	})
	if err != nil {
		return nil, fmt.Errorf("query nodes from GCS: %w", err)
	}
	var nodeInfos []*proto.GcsNodeInfo
	for _, n := range allNodeReply.NodeInfoList {
		if n.GetNodeManagerAddress() != "" && n.GetRuntimeEnvAgentPort() != 0 {
			nodeInfos = append(nodeInfos, n)
		}
	}

	type agentReply struct {
		node   *proto.GcsNodeInfo
		reply  *proto.GetRuntimeEnvsInfoReply
		failed bool
	}
	replies := make([]agentReply, 0, len(nodeInfos))
	for _, n := range nodeInfos {
		reply, err := queryAgentRuntimeEnvs(ctxTimeout, n.GetNodeManagerAddress(), int(n.GetRuntimeEnvAgentPort()))
		if err != nil {
			replies = append(replies, agentReply{node: n, failed: true})
			continue
		}
		replies = append(replies, agentReply{node: n, reply: reply})
	}

	// partial_failure_warning is null unless at least one agent is unreachable,
	// aligned with the Python list_runtime_envs logic: when every queried agent
	// fails the whole call fails, otherwise a partial-failure warning is set.
	var pfw *string
	unresponsive := 0
	for _, r := range replies {
		if r.failed {
			unresponsive++
		}
	}
	if len(replies) > 0 && unresponsive > 0 {
		warningMsg := nodeQueryFailureWarning("agent", len(replies), unresponsive, "dashboard_agent.log")
		if unresponsive == len(replies) {
			return nil, fmt.Errorf("%s", warningMsg)
		}
		msg := fmt.Sprintf("The returned data may contain incomplete result. %s", warningMsg)
		pfw = &msg
	}

	// Fold the states into the RuntimeEnvState schema. total counts the sum of
	// the per-agent reply totals (the pre-truncation agent counts), aligned with
	// the Python total_runtime_envs. The slice starts non-nil so an empty result
	// serializes as [] rather than null, matching asdict(ListApiResponse(result=[])).
	entries := make([]map[string]interface{}, 0)
	totalRuntimeEnvs := int64(0)
	for _, r := range replies {
		if r.failed {
			continue
		}
		totalRuntimeEnvs += r.reply.GetTotal()
		nodeID := hex.EncodeToString(r.node.GetNodeId())
		for _, state := range r.reply.GetRuntimeEnvStates() {
			d := protoMessageToDict(state, nil)
			// Deserialize the serialized runtime_env JSON into a dict, aligned
			// with RuntimeEnv.deserialize(data["runtime_env"]).to_dict().
			if serialized, ok := d["runtime_env"].(string); ok {
				var parsed interface{}
				if err := json.Unmarshal([]byte(serialized), &parsed); err != nil {
					return nil, fmt.Errorf("deserialize runtime env %q: %w", serialized, err)
				}
				d["runtime_env"] = parsed
			}
			d["node_id"] = nodeID
			entries = append(entries, d)
		}
	}

	// num_after_truncation counts the source entries before client-side
	// filtering, then filters, sorts by creation_time_ms descending (unset /
	// nil times sort first, aligned with the Python sort_func returning
	// float("inf") for them) and truncates to the limit. buildListResponse's
	// generic id sort does not match the runtime_envs ordering, so the transform
	// is done inline.
	numAfterTruncation := len(entries)
	filtered := applyFilters(entries, opt)
	numFiltered := len(filtered)
	sort.SliceStable(filtered, func(i, j int) bool {
		ti, oki := toInt64(filtered[i]["creation_time_ms"])
		tj, okj := toInt64(filtered[j]["creation_time_ms"])
		// Missing or nil creation time sorts first in the reverse (descending)
		// order (Python float("inf") + reverse=True).
		if !oki || !okj {
			return !oki && okj
		}
		return ti > tj
	})
	if len(filtered) > opt.Limit {
		filtered = filtered[:opt.Limit]
	}
	// Trim each entry to the schema's base/detail columns, aligned with the
	// filter_fields applied in the Python do_filter.
	for i := range filtered {
		filtered[i] = filterFields(filtered[i], opt)
	}
	return &ListApiResponse{
		Result:                filtered,
		Total:                 int(totalRuntimeEnvs),
		NumAfterTruncation:    numAfterTruncation,
		NumFiltered:           numFiltered,
		PartialFailureWarning: pfw,
	}, nil
}

// queryAgentRuntimeEnvs POSTs a serialized GetRuntimeEnvsInfoRequest to the
// agent's /get_runtime_envs_info HTTP endpoint and parses the serialized
// GetRuntimeEnvsInfoReply from the response body, aligned with
// get_runtime_envs_info in python/ray/util/state/state_manager.py. The request
// and reply travel as raw protobuf bytes (content type application/octet-stream
// on the agent side).
func queryAgentRuntimeEnvs(ctx context.Context, nodeManagerAddress string, agentPort int) (*proto.GetRuntimeEnvsInfoReply, error) {
	req := &proto.GetRuntimeEnvsInfoRequest{Limit: dataSourceLimitPtr()}
	body, err := goproto.Marshal(req)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("http://%s/get_runtime_envs_info", net.JoinHostPort(nodeManagerAddress, strconv.Itoa(agentPort)))
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return nil, fmt.Errorf("agent %s replied %s", url, httpResp.Status)
	}
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}
	reply := &proto.GetRuntimeEnvsInfoReply{}
	if err := goproto.Unmarshal(respBody, reply); err != nil {
		return nil, fmt.Errorf("parse runtime env reply from %s: %w", url, err)
	}
	return reply, nil
}

// buildListResponse applies filter/sort/truncate and computes the response
// counters, aligned with the transform helpers in state_aggregator.py:
// num_after_truncation counts the entries returned from the source (before
// client-side filtering) plus any filtered on the source, while num_filtered
// counts the entries remaining after client-side filtering (before the limit
// truncation).
func buildListResponse(entries []map[string]interface{}, total, numFilteredOnSource int64, opt *ListApiOptions) *ListApiResponse {
	before := len(entries)
	numFiltered := len(applyFilters(entries, opt))
	truncated := filterAndSort(entries, opt)
	// Trim each entry to the schema's base/detail columns after filtering and
	// sorting, aligned with filter_fields applied in the Python do_filter.
	if opt.Resource != "" {
		for i := range truncated {
			truncated[i] = filterFields(truncated[i], opt)
		}
	}
	// partial_failure_warning is "" for these endpoints, matching the Python
	// dataclass default (only list_objects passes None explicitly).
	empty := ""
	return &ListApiResponse{
		Result:                truncated,
		Total:                 int(total),
		NumAfterTruncation:    before + int(numFilteredOnSource),
		NumFiltered:           numFiltered,
		PartialFailureWarning: &empty,
	}
}

// maxDataSourceLimit is the limit forwarded to the GCS data source, aligned
// with RAY_MAX_LIMIT_FROM_DATA_SOURCE in python/ray/util/state/common.py.
const maxDataSourceLimit = 10000

func dataSourceLimitPtr() *int64 {
	v := int64(maxDataSourceLimit)
	return &v
}

func dataSourceLimitInt32Ptr() *int32 {
	v := int32(maxDataSourceLimit)
	return &v
}

// fields_to_decode lists per resource, aligned with state_aggregator.py.
var (
	actorDecodeFields  = []string{"actor_id", "owner_id", "job_id", "node_id", "placement_group_id"}
	jobDecodeFields    = []string{"job_id"}
	nodeDecodeFields   = []string{"node_id"}
	pgDecodeFields     = []string{"placement_group_id", "creator_job_id", "node_id"}
	workerDecodeFields = []string{"worker_id", "node_id"}
)

// protojsonMarshalOptions emits proto field names (snake_case, matching
// preserving_proto_field_name=True in Python's message_to_dict) and always
// prints no-presence scalar fields (matching always_print_fields_with_no_presence=True).
var protojsonMarshalOptions = protojson.MarshalOptions{
	UseProtoNames:   true,
	EmitUnpopulated: true,
}

// protoMessageToDict converts a protobuf message to a map with snake_case
// keys. Byte fields listed in decodeFields are decoded from base64 (the
// protojson default for bytes) to hex strings, aligned with the fields_to_decode
// behavior of message_to_dict in python/ray/dashboard/utils.py. The values of
// double/float fields are wrapped in jsonFloat so they serialize with a
// fractional part (6.0 rather than 6), matching Python MessageToDict which
// yields Python floats for these fields and json.dumps renders a whole float as
// "6.0".
func protoMessageToDict(msg protoMessage, decodeFields []string) map[string]interface{} {
	raw, err := protojsonMarshalOptions.Marshal(msg)
	if err != nil {
		return map[string]interface{}{}
	}
	m := map[string]interface{}{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return map[string]interface{}{}
	}
	if len(decodeFields) > 0 {
		decodeBytesFields(m, decodeFields)
	}
	floatifyDoubleFields(msg, m)
	return m
}

// jsonFloat is a float64 whose JSON encoding always keeps a fractional part
// ("6.0" rather than "6"), matching Python json.dumps(float) which renders a
// whole-valued float as "6.0". encoding/json otherwise renders a whole float64
// as an integer literal, diverging from Python's float output for protobuf
// double/float fields.
type jsonFloat float64

func (f jsonFloat) MarshalJSON() ([]byte, error) {
	s := strconv.FormatFloat(float64(f), 'f', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}
	return []byte(s), nil
}

// floatifyDoubleFields wraps the values of double/float proto fields (scalar
// fields, map values, and repeated elements) in jsonFloat so they serialize
// with a fractional part, aligned with Python MessageToDict which converts
// double/float proto fields to Python floats. Fields of other proto types are
// left untouched so int32/uint32 stay integer literals, int64/uint64 stay
// strings, and enums stay name strings.
func floatifyDoubleFields(msg protoMessage, m map[string]interface{}) {
	fields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		name := string(fd.Name())
		v, ok := m[name]
		if !ok {
			continue
		}
		switch {
		case fd.IsMap():
			if k := fd.MapValue().Kind(); k == protoreflect.DoubleKind || k == protoreflect.FloatKind {
				if mv, ok := v.(map[string]interface{}); ok {
					for mk, e := range mv {
						if f, ok := e.(float64); ok {
							mv[mk] = jsonFloat(f)
						}
					}
				}
			}
		case fd.Kind() == protoreflect.DoubleKind || fd.Kind() == protoreflect.FloatKind:
			if f, ok := v.(float64); ok {
				m[name] = jsonFloat(f)
			}
		case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
			sub := msg.ProtoReflect().Get(fd)
			if fd.Cardinality() == protoreflect.Repeated {
				if lst, ok := v.([]interface{}); ok {
					subs := sub.List()
					for j, e := range lst {
						if em, ok := e.(map[string]interface{}); ok && j < subs.Len() {
							floatifyDoubleFields(subs.Get(j).Message().Interface(), em)
						}
					}
				}
			} else if em, ok := v.(map[string]interface{}); ok && sub.IsValid() {
				floatifyDoubleFields(sub.Message().Interface(), em)
			}
		}
	}
}

// protoMessage is the minimal interface shared by all generated protobuf
// messages that protojson can marshal.
type protoMessage interface {
	ProtoReflect() protoreflect.Message
}

// decodeBytesFields walks the map (recursing into nested maps and slices of
// maps) and converts base64-encoded byte fields to hex strings.
func decodeBytesFields(m map[string]interface{}, decodeFields []string) {
	for k, v := range m {
		if !stringInSlice(k, decodeFields) {
			switch t := v.(type) {
			case map[string]interface{}:
				decodeBytesFields(t, decodeFields)
			case []interface{}:
				for _, e := range t {
					if em, ok := e.(map[string]interface{}); ok {
						decodeBytesFields(em, decodeFields)
					}
				}
			}
			continue
		}
		if s, ok := v.(string); ok {
			m[k] = base64ToHex(s)
		}
	}
}

func stringInSlice(s string, slice []string) bool {
	for _, e := range slice {
		if e == s {
			return true
		}
	}
	return false
}

// base64ToHex decodes a base64 string into a lowercase hex string. On failure
// the original string is returned unchanged.
func base64ToHex(s string) string {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return s
	}
	return hex.EncodeToString(b)
}

// intifyTimeFields converts the given time fields from their protojson string
// form to int, aligned with the int() conversion applied in list_nodes and
// list_workers in python/ray/dashboard/state_aggregator.py. Fields that are
// not strings are left untouched. The protojson uint64-max sentinel used for an
// unknown worker launch time (18446744073709551615, which overflows int64) is
// converted to a uint64 so it serializes as an integer, matching the Python
// int() which also keeps the raw value.
func intifyTimeFields(d map[string]interface{}, fields ...string) {
	for _, f := range fields {
		s, ok := d[f].(string)
		if !ok {
			continue
		}
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			d[f] = int(n)
			continue
		}
		if n, err := strconv.ParseUint(s, 10, 64); err == nil {
			d[f] = n
		}
	}
}

// ComposeStateMessage mirrors compose_state_message in python/ray/dashboard/
// utils.py: it maps the node death reason to a human-readable message and
// appends the reason message when present. An unknown reason yields nil unless
// a reason message is present, in which case the message alone is returned
// (matching the Python else-branch). A nil death info yields nil. It is
// exported for reuse by the node module.
func ComposeStateMessage(death *proto.NodeDeathInfo) interface{} {
	if death == nil {
		return nil
	}
	var msg string
	switch death.Reason {
	case proto.NodeDeathInfo_EXPECTED_TERMINATION:
		msg = "Expected termination"
	case proto.NodeDeathInfo_UNEXPECTED_TERMINATION:
		msg = "Unexpected termination"
	case proto.NodeDeathInfo_AUTOSCALER_DRAIN_PREEMPTED:
		msg = "Terminated due to preemption"
	case proto.NodeDeathInfo_AUTOSCALER_DRAIN_IDLE:
		msg = "Terminated due to idle (no Ray activity)"
	}
	if death.ReasonMessage != "" {
		if msg != "" {
			msg += ": " + death.ReasonMessage
		} else {
			msg = death.ReasonMessage
		}
	}
	if msg == "" {
		return nil
	}
	return msg
}
