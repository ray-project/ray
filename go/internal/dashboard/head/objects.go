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
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
)

// Object ref ID layout constants, aligned with the Python memory_utils actor
// handle detection: TaskID = 24 bytes (8 random + ActorID), ActorID = 16 bytes
// (12 random + JobID), JobID = 4 bytes.
const (
	objectTaskIDRandomBits = 16 // 8 bytes * 2 hex chars
	objectActorRandomBits  = 24 // 12 bytes * 2 hex chars
)

// isActorHandleObjectRef reports whether an object ref hex string is an actor
// handle, aligned with _is_object_ref_actor_handle in
// python/ray/dashboard/memory_utils.py: random bytes all 'f' but the actor
// random bytes not all 'f'.
func isActorHandleObjectRef(objectRefHex string) bool {
	if len(objectRefHex) < objectTaskIDRandomBits+objectActorRandomBits {
		return false
	}
	randomBits := objectRefHex[:objectTaskIDRandomBits]
	actorRandomBits := objectRefHex[objectTaskIDRandomBits : objectTaskIDRandomBits+objectActorRandomBits]
	return randomBits == strings.Repeat("f", objectTaskIDRandomBits) &&
		actorRandomBits != strings.Repeat("f", objectActorRandomBits)
}

// nodeQueryFailureWarning formats the partial-failure warning, aligned with
// NODE_QUERY_FAILURE_WARNING in python/ray/dashboard/state_aggregator.py.
func nodeQueryFailureWarning(kind string, total, failures int, logCommand string) string {
	return fmt.Sprintf(
		"Failed to query data from %s. Queried %d %s and %d %s failed to reply. It is due to "+
			"(1) %s is unexpectedly failed. (2) %s is overloaded. "+
			"(3) There's an unexpected network issue. Please check the %s to find the root cause.",
		kind, total, kind, failures, kind, kind, kind, logCommand,
	)
}

// listObjects queries every live raylet's GetObjectsInfo and folds the results
// through the memory table, aligned with list_objects in
// python/ray/dashboard/state_aggregator.py. Each entry is built per object ref
// (valid refs only) and transformed into the ObjectState schema.
func (m *StateAPIManager) listObjects(ctx context.Context, opt *ListApiOptions) ([]map[string]interface{}, int, *string, error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(opt.Timeout)*time.Second)
	defer cancel()
	// Filter to live nodes only, aligned with the ("state", "=", "ALIVE")
	// source-side filter in Python list_objects.
	alive := proto.GcsNodeInfo_ALIVE
	allNodeReply, err := m.client.GetAllNodeInfo(ctxTimeout, &proto.GetAllNodeInfoRequest{
		Limit:       dataSourceLimitPtr(),
		StateFilter: &alive,
	})
	if err != nil {
		return nil, 0, nil, fmt.Errorf("query nodes from GCS: %w", err)
	}

	type nodeQuery struct {
		node *proto.GcsNodeInfo
		conn *grpc.ClientConn
	}
	var queries []nodeQuery
	for _, n := range allNodeReply.NodeInfoList {
		if n.GetNodeManagerAddress() == "" || n.GetNodeManagerPort() == 0 {
			continue
		}
		queries = append(queries, nodeQuery{node: n})
	}

	// Dial each raylet and query GetObjectsInfo, aligned with the Python
	// asyncio.gather over get_object_info calls. An unreachable node yields an
	// unresponsive-node count rather than a hard error.
	type nodeReply struct {
		node   *proto.GcsNodeInfo
		reply  *proto.GetObjectsInfoReply
		failed bool
	}
	replies := make([]nodeReply, 0, len(queries))
	for i := range queries {
		n := queries[i].node
		addr := fmt.Sprintf("%s:%d", n.GetNodeManagerAddress(), n.GetNodeManagerPort())
		nodeClient, conn, err := m.client.NewNodeManagerClient(ctxTimeout, addr)
		if err != nil {
			replies = append(replies, nodeReply{node: n, failed: true})
			continue
		}
		req := &proto.GetObjectsInfoRequest{Limit: dataSourceLimitPtr()}
		reply, err := nodeClient.GetObjectsInfo(m.client.withClusterID(ctxTimeout), req)
		conn.Close()
		if err != nil {
			replies = append(replies, nodeReply{node: n, failed: true})
			continue
		}
		replies = append(replies, nodeReply{node: n, reply: reply})
	}

	// Aggregate core worker stats and build the memory table.
	var workerStats []*proto.CoreWorkerStats
	totalObjects := int64(0)
	unresponsive := 0
	for _, r := range replies {
		if r.failed {
			unresponsive++
			continue
		}
		totalObjects += r.reply.GetTotal()
		workerStats = append(workerStats, r.reply.GetCoreWorkersStats()...)
	}

	// partial_failure_warning is null unless at least one node is unreachable.
	var pfw *string
	if len(queries) > 0 && unresponsive > 0 {
		warningMsg := nodeQueryFailureWarning("raylet", len(queries), unresponsive, "raylet.out")
		if unresponsive == len(queries) {
			return nil, 0, nil, fmt.Errorf("%s", warningMsg)
		}
		msg := fmt.Sprintf("The returned data may contain incomplete result. %s", warningMsg)
		pfw = &msg
	}

	result := buildObjectMemoryTable(workerStats)
	return result, int(totalObjects), pfw, nil
}

// buildObjectMemoryTable constructs the ObjectState entries from the aggregated
// core worker stats, aligned with construct_memory_table + the per-entry
// transform in list_objects.
func buildObjectMemoryTable(workerStats []*proto.CoreWorkerStats) []map[string]interface{} {
	entries := make([]map[string]interface{}, 0, 32)
	for _, cws := range workerStats {
		pid := cws.GetPid()
		isDriver := cws.GetWorkerType() == proto.WorkerType_DRIVER
		nodeAddress := cws.GetIpAddress()
		for _, ref := range cws.GetObjectRefs() {
			entry := memoryTableEntry(ref, nodeAddress, isDriver, pid)
			if entry == nil {
				continue
			}
			// Transform to the ObjectState schema.
			data := map[string]interface{}{
				"object_id":      entry["object_ref"],
				"pid":            entry["pid"],
				"ip":             entry["node_ip_address"],
				"object_size":    entry["object_size"],
				"reference_type": entry["reference_type"],
				"call_site":      entry["call_site"],
				"task_status":    entry["task_status"],
				"attempt_number": entry["attempt_number"],
				"type":           strings.ToUpper(entry["type"].(string)),
			}
			// task_status "-" is normalized to "NIL" (Python list_objects).
			if ts, ok := data["task_status"].(string); ok && ts == "-" {
				data["task_status"] = "NIL"
			}
			entries = append(entries, data)
		}
	}
	return entries
}

// memoryTableEntry builds a single MemoryTableEntry dict from an ObjectRefInfo,
// aligned with the Python MemoryTableEntry.__init__ / as_dict. It returns nil
// for invalid refs (no references at all, or a nil object ref).
func memoryTableEntry(ref *proto.ObjectRefInfo, nodeAddress string, isDriver bool, pid uint32) map[string]interface{} {
	objectRefHex := hex.EncodeToString(ref.GetObjectId())
	// An object ref is nil when all its bytes are zero (aligned with
	// ray.ObjectRef.is_nil()).
	if strings.Trim(objectRefHex, "0") == "" {
		return nil
	}
	taskStatus := proto.TaskStatus_name[int32(ref.GetTaskStatus())]
	if taskStatus == "NIL" {
		taskStatus = "-"
	}
	callSite := ref.GetCallSite()
	if callSite == "" {
		callSite = "disabled"
	}
	containedInOwned := make([]string, 0, len(ref.GetContainedInOwned()))
	for _, c := range ref.GetContainedInOwned() {
		containedInOwned = append(containedInOwned, hex.EncodeToString(c))
	}

	// Validity check, aligned with MemoryTableEntry.is_valid: the ref must have
	// at least one of pinned/local/submitted/captured reference and a non-nil
	// object id.
	if !ref.GetPinnedInMemory() &&
		ref.GetLocalRefCount() == 0 &&
		ref.GetSubmittedTaskRefCount() == 0 &&
		len(containedInOwned) == 0 {
		return nil
	}

	referenceType := objectReferenceType(ref, containedInOwned)
	entryType := "Worker"
	if isDriver {
		entryType = "Driver"
	}
	return map[string]interface{}{
		"object_ref":               objectRefHex,
		"pid":                      int(pid),
		"node_ip_address":          nodeAddress,
		"object_size":              int(ref.GetObjectSize()),
		"reference_type":           referenceType,
		"call_site":                callSite,
		"task_status":              taskStatus,
		"attempt_number":           int(ref.GetAttemptNumber()) + 1,
		"local_ref_count":          int(ref.GetLocalRefCount()),
		"pinned_in_memory":         ref.GetPinnedInMemory(),
		"submitted_task_ref_count": int(ref.GetSubmittedTaskRefCount()),
		"contained_in_owned":       containedInOwned,
		"type":                     entryType,
	}
}

// objectReferenceType computes the reference type priority, aligned with
// _get_reference_type in python/ray/dashboard/memory_utils.py.
func objectReferenceType(ref *proto.ObjectRefInfo, containedInOwned []string) string {
	if isActorHandleObjectRef(hex.EncodeToString(ref.GetObjectId())) {
		return "ACTOR_HANDLE"
	}
	if ref.GetPinnedInMemory() {
		return "PINNED_IN_MEMORY"
	}
	if ref.GetSubmittedTaskRefCount() > 0 {
		return "USED_BY_PENDING_TASK"
	}
	if ref.GetLocalRefCount() > 0 {
		return "LOCAL_REFERENCE"
	}
	if len(containedInOwned) > 0 {
		return "CAPTURED_IN_OBJECT"
	}
	return "UNKNOWN_STATUS"
}
