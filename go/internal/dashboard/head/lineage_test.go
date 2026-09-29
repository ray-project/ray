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
	"reflect"
	"testing"
)

// testTask builds a minimal TaskState dict for lineage tests. state defaults to
// RUNNING when empty.
func testTask(id, name, typ, parentID, actorID string, tsMS int64) map[string]interface{} {
	d := map[string]interface{}{
		"task_id":            id,
		"type":               typ,
		"func_or_class_name": name,
		"state":              "RUNNING",
	}
	if name != "" {
		d["name"] = name
	}
	if parentID != "" {
		d["parent_task_id"] = parentID
	}
	if actorID != "" {
		d["actor_id"] = actorID
	}
	if tsMS > 0 {
		d["creation_time_ms"] = tsMS
	}
	return d
}

func testActor(id, reprName, className string) map[string]interface{} {
	a := map[string]interface{}{"actor_id": id}
	if reprName != "" {
		a["repr_name"] = reprName
	}
	if className != "" {
		a["class_name"] = className
	}
	return a
}

// TestBuildTaskLineageSimpleChain verifies a driver -> task1 -> task2 chain
// produces a two-level tree with task1 as the root.
func TestBuildTaskLineageSimpleChain(t *testing.T) {
	tasks := []map[string]interface{}{
		testTask("t1", "f1", "NORMAL_TASK", driverTaskIDPrefix, "", 100),
		testTask("t2", "f2", "NORMAL_TASK", "t1", "", 200),
	}
	roots, total, actorTasks, actorScheduled := buildTaskLineage(tasks, nil)
	if total != 2 {
		t.Fatalf("total_tasks = %d, want 2", total)
	}
	if actorTasks != 0 || actorScheduled != 0 {
		t.Fatalf("actor counts = %d/%d, want 0/0", actorTasks, actorScheduled)
	}
	if len(roots) != 1 {
		t.Fatalf("roots = %d, want 1", len(roots))
	}
	r := roots[0]
	if r.Key != "t1" || r.Type != "NORMAL_TASK" {
		t.Fatalf("root = %s/%s, want t1/NORMAL_TASK", r.Key, r.Type)
	}
	if len(r.Children) != 1 || r.Children[0].Key != "t2" {
		t.Fatalf("root children = %d, want [t2]", len(r.Children))
	}
	// state_counts rolls up: both tasks RUNNING.
	if r.StateCounts["RUNNING"] != 2 {
		t.Fatalf("root RUNNING = %d, want 2 (rolled up)", r.StateCounts["RUNNING"])
	}
	if r.Link == nil || r.Link.Type != "task" || r.Link.ID != "t1" {
		t.Fatalf("root link = %+v, want task/t1", r.Link)
	}
}

// TestBuildTaskLineageMissingParent verifies a subtree whose parent is absent
// from the task list is dropped.
func TestBuildTaskLineageMissingParent(t *testing.T) {
	tasks := []map[string]interface{}{
		testTask("t1", "f1", "NORMAL_TASK", driverTaskIDPrefix, "", 100),
		testTask("t2", "f2", "NORMAL_TASK", "missing", "", 200),
	}
	roots, total, _, _ := buildTaskLineage(tasks, nil)
	if total != 2 {
		t.Fatalf("total_tasks = %d, want 2", total)
	}
	if len(roots) != 1 || roots[0].Key != "t1" {
		t.Fatalf("roots = %d, want only t1 (t2 subtree dropped)", len(roots))
	}
}

// TestBuildTaskLineageActorGrouping verifies actor tasks hang off the actor
// node, which in turn hangs off its creation task's parent.
func TestBuildTaskLineageActorGrouping(t *testing.T) {
	// driver -> creation task (creates actor a1) -> actor node -> actor task.
	tasks := []map[string]interface{}{
		testTask("ct", "Counter", "ACTOR_CREATION_TASK", driverTaskIDPrefix, "a1", 100),
		testTask("at", "Counter.inc", "ACTOR_TASK", "", "a1", 200),
	}
	actors := []map[string]interface{}{
		testActor("a1", "Counter(a1)", "Counter"),
	}
	roots, _, actorTasks, actorScheduled := buildTaskLineage(tasks, actors)
	if actorTasks != 1 || actorScheduled != 1 {
		t.Fatalf("actor counts = %d/%d, want 1/1", actorTasks, actorScheduled)
	}
	// The only root is the actor node: the creation task's parent is the driver,
	// and both the creation task and the actor task hang off the actor node.
	if len(roots) != 1 || roots[0].Type != "ACTOR" {
		t.Fatalf("roots = %d, want a single ACTOR root", len(roots))
	}
	actorNode := roots[0]
	if actorNode.Name != "Counter(a1)" {
		t.Fatalf("actor name = %q, want Counter(a1) from repr_name", actorNode.Name)
	}
	if actorNode.Link == nil || actorNode.Link.Type != "actor" || actorNode.Link.ID != "a1" {
		t.Fatalf("actor link = %+v, want actor/a1", actorNode.Link)
	}
	// Both the creation task and the actor task are children of the actor node.
	if len(actorNode.Children) != 2 {
		t.Fatalf("actor children = %d, want [ct, at]", len(actorNode.Children))
	}
	// State counts roll up: ct (RUNNING) + at (RUNNING) = 2 on the actor node.
	if actorNode.StateCounts["RUNNING"] != 2 {
		t.Fatalf("actor RUNNING = %d, want 2", actorNode.StateCounts["RUNNING"])
	}
}

// TestBuildTaskLineageActorFallbackName verifies the actor name falls back to
// the class_name when repr_name is absent.
func TestBuildTaskLineageActorFallbackName(t *testing.T) {
	tasks := []map[string]interface{}{
		testTask("ct", "pkg.Counter", "ACTOR_CREATION_TASK", driverTaskIDPrefix, "a1", 100),
		testTask("at", "pkg.Counter.inc", "ACTOR_TASK", "", "a1", 200),
	}
	actors := []map[string]interface{}{
		testActor("a1", "", "Counter"),
	}
	roots, _, _, _ := buildTaskLineage(tasks, actors)
	var actorNode *nestedTaskSummary
	for _, r := range roots {
		if r.Type == "ACTOR" {
			actorNode = r
		}
	}
	if actorNode == nil || actorNode.Name != "Counter" {
		t.Fatalf("actor name = %q, want Counter from class_name", actorNode.Name)
	}
}

// TestBuildTaskLineageSameNameSiblingsMerge verifies two same-named children
// under one parent merge into a single GROUP node.
func TestBuildTaskLineageSameNameSiblingsMerge(t *testing.T) {
	tasks := []map[string]interface{}{
		testTask("t1", "f1", "NORMAL_TASK", driverTaskIDPrefix, "", 100),
		testTask("t2", "worker", "NORMAL_TASK", "t1", "", 200),
		testTask("t3", "worker", "NORMAL_TASK", "t1", "", 300),
	}
	roots, _, _, _ := buildTaskLineage(tasks, nil)
	if len(roots) != 1 {
		t.Fatalf("roots = %d, want 1", len(roots))
	}
	r := roots[0]
	if len(r.Children) != 1 {
		t.Fatalf("root children = %d, want 1 GROUP", len(r.Children))
	}
	g := r.Children[0]
	if g.Type != "GROUP" || g.Name != "worker" || g.Key != "worker" {
		t.Fatalf("group = %s/%s, want GROUP/worker", g.Type, g.Name)
	}
	if len(g.Children) != 2 {
		t.Fatalf("group children = %d, want 2", len(g.Children))
	}
	// Group rolls up both children.
	if g.StateCounts["RUNNING"] != 2 {
		t.Fatalf("group RUNNING = %d, want 2", g.StateCounts["RUNNING"])
	}
	if r.StateCounts["RUNNING"] != 3 {
		t.Fatalf("root RUNNING = %d, want 3", r.StateCounts["RUNNING"])
	}
}

// TestBuildTaskLineageLoneChildNotGrouped verifies a single child is not
// wrapped in a GROUP node.
func TestBuildTaskLineageLoneChildNotGrouped(t *testing.T) {
	tasks := []map[string]interface{}{
		testTask("t1", "f1", "NORMAL_TASK", driverTaskIDPrefix, "", 100),
		testTask("t2", "only", "NORMAL_TASK", "t1", "", 200),
	}
	roots, _, _, _ := buildTaskLineage(tasks, nil)
	child := roots[0].Children[0]
	if child.Type == "GROUP" {
		t.Fatalf("single child should not be grouped, got GROUP")
	}
	if child.Key != "t2" {
		t.Fatalf("child key = %s, want t2", child.Key)
	}
}

// TestBuildTaskLineageSortOrder verifies running tasks sort above pending,
// pending above failed.
func TestBuildTaskLineageSortOrder(t *testing.T) {
	tasks := []map[string]interface{}{
		testTask("t1", "f1", "NORMAL_TASK", driverTaskIDPrefix, "", 100),
	}
	t2 := testTask("t2", "running_w", "NORMAL_TASK", "t1", "", 200)
	t2["state"] = "RUNNING"
	t3 := testTask("t3", "pending_w", "NORMAL_TASK", "t1", "", 300)
	t3["state"] = "PENDING_ARGS_AVAIL"
	t4 := testTask("t4", "failed_w", "NORMAL_TASK", "t1", "", 400)
	t4["state"] = "FAILED"
	tasks = append(tasks, t2, t3, t4)

	roots, _, _, _ := buildTaskLineage(tasks, nil)
	children := roots[0].Children
	if len(children) != 3 {
		t.Fatalf("children = %d, want 3", len(children))
	}
	if children[0].Name != "running_w" {
		t.Fatalf("first child = %s, want running_w (running first)", children[0].Name)
	}
	if children[1].Name != "pending_w" {
		t.Fatalf("second child = %s, want pending_w", children[1].Name)
	}
	if children[2].Name != "failed_w" {
		t.Fatalf("third child = %s, want failed_w", children[2].Name)
	}
}

// TestBuildTaskLineageTimestampSort verifies same-priority tasks sort by
// creation time ascending.
func TestBuildTaskLineageTimestampSort(t *testing.T) {
	tasks := []map[string]interface{}{
		testTask("t1", "f1", "NORMAL_TASK", driverTaskIDPrefix, "", 100),
		testTask("t2", "b_task", "NORMAL_TASK", "t1", "", 300),
		testTask("t3", "a_task", "NORMAL_TASK", "t1", "", 200),
	}
	roots, _, _, _ := buildTaskLineage(tasks, nil)
	children := roots[0].Children
	if len(children) != 2 {
		t.Fatalf("children = %d, want 2", len(children))
	}
	if children[0].Name != "a_task" {
		t.Fatalf("first child = %s, want a_task (earlier timestamp)", children[0].Name)
	}
}

// TestBuildTaskLineageEmpty verifies empty inputs yield no roots and zero
// totals.
func TestBuildTaskLineageEmpty(t *testing.T) {
	roots, total, actorTasks, actorScheduled := buildTaskLineage(nil, nil)
	if len(roots) != 0 {
		t.Fatalf("roots = %d, want 0", len(roots))
	}
	if total != 0 || actorTasks != 0 || actorScheduled != 0 {
		t.Fatalf("totals = %d/%d/%d, want 0/0/0", total, actorTasks, actorScheduled)
	}
}

// TestBuildTaskLineageChildBeforeParent verifies the tree builds correctly even
// when a child task appears before its parent in the input.
func TestBuildTaskLineageChildBeforeParent(t *testing.T) {
	tasks := []map[string]interface{}{
		testTask("t2", "f2", "NORMAL_TASK", "t1", "", 200),
		testTask("t1", "f1", "NORMAL_TASK", driverTaskIDPrefix, "", 100),
	}
	roots, _, _, _ := buildTaskLineage(tasks, nil)
	if len(roots) != 1 || roots[0].Key != "t1" {
		t.Fatalf("roots = %d, want [t1]", len(roots))
	}
	if len(roots[0].Children) != 1 || roots[0].Children[0].Key != "t2" {
		t.Fatalf("root children = %d, want [t2]", len(roots[0].Children))
	}
}

// TestMergeTaskSiblingsGroupAndFlatten pins mergeTaskSiblings directly: two
// same-named siblings merge, and the smaller child timestamp propagates.
func TestMergeTaskSiblingsGroupAndFlatten(t *testing.T) {
	ts200 := int64(200)
	ts300 := int64(300)
	a := &nestedTaskSummary{Name: "worker", Key: "t2", Type: "NORMAL_TASK", StateCounts: map[string]int{}, Timestamp: &ts200}
	b := &nestedTaskSummary{Name: "worker", Key: "t3", Type: "NORMAL_TASK", StateCounts: map[string]int{}, Timestamp: &ts300}
	merged, minTS := mergeTaskSiblings([]*nestedTaskSummary{a, b})
	if len(merged) != 1 {
		t.Fatalf("merged = %d, want 1", len(merged))
	}
	if merged[0].Type != "GROUP" {
		t.Fatalf("type = %s, want GROUP", merged[0].Type)
	}
	if len(merged[0].Children) != 2 {
		t.Fatalf("group children = %d, want 2", len(merged[0].Children))
	}
	if minTS == nil || *minTS != 200 {
		t.Fatalf("minTS = %v, want 200", minTS)
	}
}

// TestSummarizeTasksRejectsUnknownSummaryBy verifies an unknown summary_by
// value yields a ValueError (HTTP 400 in the handler). The check happens before
// any GCS access, so a bare manager suffices.
func TestSummarizeTasksRejectsUnknownSummaryBy(t *testing.T) {
	opt := &SummaryApiOptions{SummaryBy: "bogus"}
	_, err := (&StateAPIManager{}).SummarizeTasks(t.Context(), opt)
	if err == nil {
		t.Fatal("expected error for unknown summary_by")
	}
	if _, ok := err.(*ValueError); !ok {
		t.Fatalf("error type = %T, want *ValueError", err)
	}
}

// TestNestedTaskSummaryJSONShape pins the JSON field names of
// nestedTaskSummary so the frontend NestedJobProgress type keeps matching.
func TestNestedTaskSummaryJSONShape(t *testing.T) {
	ts := int64(100)
	g := &nestedTaskSummary{
		Name:        "f",
		Key:         "t1",
		Type:        "NORMAL_TASK",
		Timestamp:   &ts,
		StateCounts: map[string]int{"RUNNING": 1},
		Children:    []*nestedTaskSummary{},
		Link:        &nestedTaskSummaryLink{Type: "task", ID: "t1"},
	}
	raw, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]interface{}{}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"name", "key", "type", "timestamp", "state_counts", "children", "link"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("missing JSON key %q in %v", k, got)
		}
	}
	if !reflect.DeepEqual(got["state_counts"], map[string]interface{}{"RUNNING": float64(1)}) {
		t.Fatalf("state_counts = %v", got["state_counts"])
	}
	if !reflect.DeepEqual(got["children"], []interface{}{}) {
		t.Fatalf("children = %v, want []", got["children"])
	}
}

// TestNestedTaskSummaryJSONNilChildren verifies a node built without any
// children (nil slice) is emitted as "children": [] rather than null, matching
// the Python NestedTaskSummary default_factory=list. The dashboard frontend
// maps over children and would crash on null.
func TestNestedTaskSummaryJSONNilChildren(t *testing.T) {
	g := &nestedTaskSummary{
		Name:        "f",
		Key:         "t1",
		Type:        "NORMAL_TASK",
		StateCounts: map[string]int{},
	}
	raw, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]interface{}{}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got["children"], []interface{}{}) {
		t.Fatalf("children = %v, want [] (not null)", got["children"])
	}
}
