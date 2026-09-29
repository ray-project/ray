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
	"strings"
	"testing"

	"github.com/ray-project/ray/go/proto"
)

// TestIsActorHandleObjectRef pins the actor-handle detection aligned with
// _is_object_ref_actor_handle in python/ray/dashboard/memory_utils.py.
func TestIsActorHandleObjectRef(t *testing.T) {
	// 8 bytes of 'f' random, actor random bytes not all 'f' -> actor handle.
	actorHandle := strings.Repeat("f", 16) + strings.Repeat("a", 24) + strings.Repeat("0", 16)
	if !isActorHandleObjectRef(actorHandle) {
		t.Fatalf("expected actor handle for %q", actorHandle)
	}
	// Random bytes not all 'f' -> not an actor handle.
	notHandle := strings.Repeat("a", 16) + strings.Repeat("f", 24) + strings.Repeat("0", 16)
	if isActorHandleObjectRef(notHandle) {
		t.Fatalf("expected non-actor-handle for %q", notHandle)
	}
	// Too short -> not an actor handle.
	if isActorHandleObjectRef("aabb") {
		t.Fatal("short ref should not be an actor handle")
	}
}

// TestMemoryTableEntryInvalid pins the validity check aligned with
// MemoryTableEntry.is_valid: an object ref with no references at all is
// dropped.
func TestMemoryTableEntryInvalid(t *testing.T) {
	ref := &proto.ObjectRefInfo{
		ObjectId:      []byte{0x01, 0x02, 0x03},
		ObjectSize:    1024,
		CallSite:      "ray.put",
		TaskStatus:    proto.TaskStatus_RUNNING,
		AttemptNumber: 0,
	}
	if got := memoryTableEntry(ref, "1.2.3.4", false, 123); got != nil {
		t.Fatalf("ref with no references should be invalid, got %v", got)
	}
	// A nil object id (all zero bytes) is also invalid.
	ref.PinnedInMemory = true
	ref.ObjectId = []byte{0x00, 0x00, 0x00}
	if got := memoryTableEntry(ref, "1.2.3.4", false, 123); got != nil {
		t.Fatalf("nil object id should be invalid, got %v", got)
	}
}

// TestMemoryTableEntry pins the MemoryTableEntry dict shape and the field
// transformations (attempt+1, task_status NIL->"-", type Driver/Worker),
// aligned with MemoryTableEntry.as_dict in python/ray/dashboard/memory_utils.py.
func TestMemoryTableEntry(t *testing.T) {
	ref := &proto.ObjectRefInfo{
		ObjectId:       []byte{0x01, 0x02, 0x03},
		ObjectSize:     2048,
		CallSite:       "ray.put",
		TaskStatus:     proto.TaskStatus_RUNNING,
		AttemptNumber:  2,
		LocalRefCount:  1,
		PinnedInMemory: false,
	}
	got := memoryTableEntry(ref, "1.2.3.4", true, 4242)
	if got == nil {
		t.Fatal("expected a valid entry")
	}
	if got["object_ref"] != "010203" {
		t.Fatalf("object_ref = %v", got["object_ref"])
	}
	if got["pid"] != 4242 {
		t.Fatalf("pid = %v", got["pid"])
	}
	if got["node_ip_address"] != "1.2.3.4" {
		t.Fatalf("node_ip_address = %v", got["node_ip_address"])
	}
	if got["object_size"] != 2048 {
		t.Fatalf("object_size = %v", got["object_size"])
	}
	if got["attempt_number"] != 3 {
		t.Fatalf("attempt_number = %v, want 3 (attempt+1)", got["attempt_number"])
	}
	if got["task_status"] != "RUNNING" {
		t.Fatalf("task_status = %v", got["task_status"])
	}
	if got["reference_type"] != "LOCAL_REFERENCE" {
		t.Fatalf("reference_type = %v, want LOCAL_REFERENCE", got["reference_type"])
	}
	if got["type"] != "Driver" {
		t.Fatalf("type = %v, want Driver", got["type"])
	}
}

// TestMemoryTableEntryTaskStatusNIL pins the NIL -> "-" normalization inside
// memoryTableEntry.
func TestMemoryTableEntryTaskStatusNIL(t *testing.T) {
	ref := &proto.ObjectRefInfo{
		ObjectId:      []byte{0x01, 0x02, 0x03},
		ObjectSize:    100,
		TaskStatus:    proto.TaskStatus_NIL,
		LocalRefCount: 1,
	}
	got := memoryTableEntry(ref, "1.2.3.4", false, 1)
	if got["task_status"] != "-" {
		t.Fatalf("task_status = %v, want \"-\"", got["task_status"])
	}
}

// TestMemoryTableEntryActorHandle pins the ACTOR_HANDLE reference type for an
// object ref whose random bytes are all 'f' but actor random bytes are not.
func TestMemoryTableEntryActorHandle(t *testing.T) {
	objID := strings.Repeat("f", 16) + strings.Repeat("a", 24) + strings.Repeat("0", 16)
	b := make([]byte, len(objID)/2)
	for i := 0; i < len(b); i++ {
		var v byte
		for j := 0; j < 2; j++ {
			c := objID[i*2+j]
			var d byte
			switch {
			case c >= '0' && c <= '9':
				d = c - '0'
			case c >= 'a' && c <= 'f':
				d = c - 'a' + 10
			}
			v = v<<4 | d
		}
		b[i] = v
	}
	ref := &proto.ObjectRefInfo{
		ObjectId:      b,
		ObjectSize:    100,
		LocalRefCount: 1,
	}
	got := memoryTableEntry(ref, "1.2.3.4", false, 1)
	if got["reference_type"] != "ACTOR_HANDLE" {
		t.Fatalf("reference_type = %v, want ACTOR_HANDLE", got["reference_type"])
	}
}

// TestBuildObjectMemoryTable pins the full buildObjectMemoryTable transform:
// task_status "-" is normalized to "NIL", type is uppercased, and only valid
// refs are kept.
func TestBuildObjectMemoryTable(t *testing.T) {
	stats := []*proto.CoreWorkerStats{
		{
			IpAddress:  "1.2.3.4",
			Pid:        100,
			WorkerType: proto.WorkerType_DRIVER,
			ObjectRefs: []*proto.ObjectRefInfo{
				{
					ObjectId:      []byte{0x01, 0x02, 0x03},
					ObjectSize:    100,
					TaskStatus:    proto.TaskStatus_NIL,
					AttemptNumber: 0,
					LocalRefCount: 1,
				},
				{
					ObjectId:      []byte{0x0a, 0x0b},
					ObjectSize:    200,
					TaskStatus:    proto.TaskStatus_RUNNING,
					AttemptNumber: 4,
					// No references -> dropped.
				},
			},
		},
		{
			IpAddress:  "2.3.4.5",
			Pid:        200,
			WorkerType: proto.WorkerType_WORKER,
			ObjectRefs: []*proto.ObjectRefInfo{
				{
					ObjectId:              []byte{0xaa, 0xbb, 0xcc},
					ObjectSize:            300,
					TaskStatus:            proto.TaskStatus_FINISHED,
					AttemptNumber:         1,
					SubmittedTaskRefCount: 2,
				},
			},
		},
	}
	entries := buildObjectMemoryTable(stats)
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	// First entry: driver, NIL status -> "NIL", type uppercased to DRIVER.
	if entries[0]["object_id"] != "010203" {
		t.Fatalf("object_id = %v", entries[0]["object_id"])
	}
	if entries[0]["task_status"] != "NIL" {
		t.Fatalf("task_status = %v, want NIL", entries[0]["task_status"])
	}
	if entries[0]["type"] != "DRIVER" {
		t.Fatalf("type = %v, want DRIVER", entries[0]["type"])
	}
	if entries[0]["pid"] != 100 {
		t.Fatalf("pid = %v", entries[0]["pid"])
	}
	if entries[0]["ip"] != "1.2.3.4" {
		t.Fatalf("ip = %v", entries[0]["ip"])
	}
	if entries[0]["attempt_number"] != 1 {
		t.Fatalf("attempt_number = %v, want 1", entries[0]["attempt_number"])
	}
	// Second entry: worker, USED_BY_PENDING_TASK reference type.
	if entries[1]["task_status"] != "FINISHED" {
		t.Fatalf("task_status = %v", entries[1]["task_status"])
	}
	if entries[1]["type"] != "WORKER" {
		t.Fatalf("type = %v, want WORKER", entries[1]["type"])
	}
	if entries[1]["reference_type"] != "USED_BY_PENDING_TASK" {
		t.Fatalf("reference_type = %v", entries[1]["reference_type"])
	}
	if entries[1]["attempt_number"] != 2 {
		t.Fatalf("attempt_number = %v, want 2", entries[1]["attempt_number"])
	}
}

// TestNodeQueryFailureWarning pins the NODE_QUERY_FAILURE_WARNING text shape.
func TestNodeQueryFailureWarning(t *testing.T) {
	msg := nodeQueryFailureWarning("raylet", 2, 1, "raylet.out")
	if !strings.Contains(msg, "Failed to query data from raylet.") ||
		!strings.Contains(msg, "Queried 2 raylet and 1 raylet failed to reply.") {
		t.Fatalf("warning text = %q", msg)
	}
}
