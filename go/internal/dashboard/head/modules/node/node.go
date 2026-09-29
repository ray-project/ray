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

package node

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
)

// NodeHead implements the /nodes and /logical/actors routes, aligned with the
// Python NodeHead.
type NodeHead struct {
	cfg    *head.HeadConfig
	client *head.GCSClient
	ds     *DataSource
}

// New creates a NodeHead backed by the given GCS client.
func New(cfg *head.HeadConfig, client *head.GCSClient) *NodeHead {
	return &NodeHead{
		cfg:    cfg,
		client: client,
		ds: NewDataSource(client,
			head.NewNodeInfoSubscriber(client),
			head.NewActorSubscriber(client),
			head.NewResourceUsageSubscriber(client),
		),
	}
}

// Name returns the module name.
func (n *NodeHead) Name() string { return "NodeHead" }

// Start starts the DataSource subscription refresh loops.
func (n *NodeHead) Start(ctx context.Context) error { return n.ds.Start(ctx) }

// Healthy reports the module health.
func (n *NodeHead) Healthy() bool { return true }

// RegisterHTTP registers the /nodes and /logical/actors routes.
func (n *NodeHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("GET /nodes", n.handleNodes)
	mux.HandleFunc("GET /nodes/{node_id}", n.handleNodeDetail)
	mux.HandleFunc("GET /logical/actors", n.handleLogicalActors)
	mux.HandleFunc("GET /logical/actors/{actor_id}", n.handleLogicalActorDetail)
	mux.HandleFunc("GET /test/dump", n.handleDump)
	return nil
}

// handleNodes handles /nodes. view=summary returns the node summaries plus the
// per-node logical resources; view=hostNameList (case-insensitive) returns the
// alive hostnames; an unknown view returns 500, all aligned with Python
// NodeHead.get_all_nodes. The data keys are converted to google style exactly
// like the Python rest_response default (convert_google_style=True):
// host_name_list -> hostNameList, node_logical_resources -> nodeLogicalResources.
// The node order is sorted by node id so the response is deterministic.
func (n *NodeHead) handleNodes(w http.ResponseWriter, r *http.Request) {
	view := r.URL.Query().Get("view")
	if view == "summary" {
		nodes := sortedNodes(n.ds.Nodes())
		// summaries must be []interface{} (not []map[string]interface{}) so the
		// response's toGoogleStyle recurses into each entry and camel-cases
		// nested dicts such as raylet.resourcesTotal; a []map[string]interface{}
		// would fall through toGoogleStyle's `case []interface{}` and skip the
		// whole summary array, leaking snake_case keys (object_store_memory,
		// node:__internal_head__) in the response, unlike the Python
		// to_google_style which recurses into every dict.
		summaries := make([]interface{}, 0, len(nodes))
		for _, node := range nodes {
			summaries = append(summaries, n.nodeSummary(node, true))
		}
		head.RESTResponseCamel(w, http.StatusOK, "Node summary fetched.", map[string]interface{}{
			"summary":              summaries,
			"nodeLogicalResources": n.nodeLogicalResources(r.Context()),
		})
		return
	}
	if view != "" && strings.EqualFold(view, "hostNameList") {
		seen := map[string]bool{}
		hosts := []string{}
		for _, node := range sortedNodes(n.ds.Nodes()) {
			if node.State != proto.GcsNodeInfo_DEAD && !seen[node.NodeManagerHostname] {
				seen[node.NodeManagerHostname] = true
				hosts = append(hosts, node.NodeManagerHostname)
			}
		}
		head.RESTResponseCamel(w, http.StatusOK, "Node hostname list fetched.", map[string]interface{}{"hostNameList": hosts})
		return
	}
	// A missing view query parameter is None on the Python side
	// (req.query.get("view") returns None), so the unknown-view message uses
	// "None" to match the Python "Unknown view None" text. An explicit
	// ?view= (empty string) stays empty, matching Python's empty-string view.
	if !r.URL.Query().Has("view") {
		view = "None"
	}
	head.RESTResponseCamel(w, http.StatusInternalServerError, fmt.Sprintf("Unknown view %s", view), nil)
}

// sortedNodes returns the nodes sorted by their hex node id, giving the
// /nodes responses a deterministic order regardless of Go map iteration order.
func sortedNodes(nodes []*proto.GcsNodeInfo) []*proto.GcsNodeInfo {
	out := make([]*proto.GcsNodeInfo, len(nodes))
	copy(out, nodes)
	sort.Slice(out, func(i, j int) bool {
		return hex.EncodeToString(out[i].NodeId) < hex.EncodeToString(out[j].NodeId)
	})
	return out
}

// handleNodeDetail handles /nodes/{node_id}. An unknown node id yields HTTP 200
// with the same structure as the Python get_node for a missing node: the
// DataOrganizer.get_node_info lookup still builds the ray_stats object store
// fields (both zero) and the detail-only actors/workers (both empty), because
// node_physical_stats/node_stats/node are all {} there. The detail carries the
// per-node actors (aligned with Python get_node_info with get_summary=False).
func (n *NodeHead) handleNodeDetail(w http.ResponseWriter, r *http.Request) {
	nodeID := r.PathValue("node_id")
	node := n.ds.Node(nodeID)
	if node == nil {
		head.RESTResponseCamel(w, http.StatusOK, "Node details fetched.", map[string]interface{}{"detail": missingNodeDetail()})
		return
	}
	head.RESTResponseCamel(w, http.StatusOK, "Node details fetched.", map[string]interface{}{"detail": n.nodeSummary(node, false)})
}

// missingNodeDetail is the detail dict for a node id absent from the GCS cache,
// aligned with the Python DataOrganizer.get_node_info on {} inputs: the raylet
// holds only the object-store ray_stats fields and a nil stateMessage, and the
// detail-only actors/workers are empty.
func missingNodeDetail() map[string]interface{} {
	return map[string]interface{}{
		"raylet": map[string]interface{}{
			"objectStoreUsedMemory":      0,
			"objectStoreAvailableMemory": 0,
			"stateMessage":               nil,
		},
		"actors":  map[string]interface{}{},
		"workers": []interface{}{},
	}
}

// handleLogicalActors handles /logical/actors. The response data is a dict
// keyed by hex actor id (data.actors), aligned with Python
// NodeHead.get_all_actors which returns rest_response(..., actors=actors,
// convert_google_style=False). When the "ids" query parameter is present, only
// those actor ids are returned; ids missing from the cache map to null, as in
// Python DataOrganizer.get_actor_infos.
func (n *NodeHead) handleLogicalActors(w http.ResponseWriter, r *http.Request) {
	actors := map[string]interface{}{}
	if idsParam := r.URL.Query().Get("ids"); idsParam != "" {
		for _, id := range strings.Split(idsParam, ",") {
			if a := n.ds.Actor(id); a != nil {
				actors[id] = n.actorLogical(a)
			} else {
				actors[id] = nil
			}
		}
	} else {
		for _, a := range n.ds.Actors() {
			actors[hex.EncodeToString(a.ActorId)] = n.actorLogical(a)
		}
	}
	head.RESTResponse(w, http.StatusOK, "All actors fetched.", map[string]interface{}{"actors": actors})
}

// handleLogicalActorDetail handles /logical/actors/{actor_id}. An unknown actor
// id returns HTTP 200 with a null detail, aligned with the Python get_actor
// which looks the id up in the (possibly missing) cache and returns it as-is
// without a not-found status.
func (n *NodeHead) handleLogicalActorDetail(w http.ResponseWriter, r *http.Request) {
	actorID := r.PathValue("actor_id")
	actor := n.ds.Actor(actorID)
	if actor == nil {
		head.RESTResponse(w, http.StatusOK, "Actor details fetched.", map[string]interface{}{"detail": nil})
		return
	}
	head.RESTResponse(w, http.StatusOK, "Actor details fetched.", map[string]interface{}{"detail": n.actorLogical(actor)})
}

// handleDump dumps the whole DataSource cache, aligned with the Python
// /test/dump handler used for testing. Without a "key" query parameter every
// data source is returned (nodes, actors, node_physical_stats, node_stats,
// node_workers, node_actors, core_worker_stats); with a key only that data
// source is returned.
func (n *NodeHead) handleDump(w http.ResponseWriter, r *http.Request) {
	all := n.dumpAllData()
	if key := r.URL.Query().Get("key"); key != "" {
		head.RESTResponseCamel(w, http.StatusOK, fmt.Sprintf("Fetch %s from datacenter success.", key), map[string]interface{}{key: all[key]})
		return
	}
	head.RESTResponseCamel(w, http.StatusOK, "Fetch all data from datacenter success.", all)
}

// dumpAllData assembles every data source of the cache, aligned with the Python
// dump handler which serializes DataSource.__dict__.
func (n *NodeHead) dumpAllData() map[string]interface{} {
	nodes := make(map[string]interface{}, 0)
	for id, info := range n.ds.DumpNodes() {
		nodes[id] = gcsNodeInfoToMap(info)
	}
	actors := make(map[string]interface{}, 0)
	for id, a := range n.ds.DumpActors() {
		actors[id] = actorTableToDict(a)
	}
	nodePhysicalStats := make(map[string]interface{}, 0)
	for id, s := range n.ds.DumpPhysicalStats() {
		nodePhysicalStats[id] = s
	}
	nodeStats := make(map[string]interface{}, 0)
	for id, reply := range n.ds.DumpNodeStats() {
		nodeStats[id] = nodeStatsReplyToMap(reply)
	}
	nodeWorkers := make(map[string]interface{}, 0)
	nodeActors := make(map[string]interface{}, 0)
	for _, info := range n.ds.Nodes() {
		nodeID := hex.EncodeToString(info.NodeId)
		workers, _ := n.ds.NodePhysicalStats(nodeID)["workers"].([]interface{})
		nodeWorkers[nodeID] = mergeWorkerCoreStats(workers, n.ds.NodeCoreWorkerStats(nodeID))
		// The dump mirrors DataSource.node_actors which stores the raw actor
		// table (no _get_actor_info merge), unlike the /nodes detail actors.
		nodeActors[nodeID] = n.rawNodeActors(nodeID)
	}
	coreWorkerStats := make(map[string]interface{}, 0)
	for id, cs := range n.ds.DumpCoreWorkerStats() {
		coreWorkerStats[id] = coreWorkerStatsToCamelMap(cs)
	}
	return map[string]interface{}{
		"node_stats":          nodeStats,
		"node_physical_stats": nodePhysicalStats,
		"actors":              actors,
		"nodes":               nodes,
		"node_workers":        nodeWorkers,
		"node_actors":         nodeActors,
		"core_worker_stats":   coreWorkerStats,
	}
}

// nodeSummary builds the per-node summary or detail, aligned with the Python
// DataOrganizer.get_node_info: the reporter physical stats
// (cpu/mem/disk/gpus/bootTime/loadAvg/networkSpeed/cmdline/... cached from the
// GCS RAY_REPORTER resource-usage pubsub) form the base, and the raylet
// sub-message is rebuilt from the node stats (the raylet's GetNodeStats reply)
// plus the GcsNodeInfo fields, exactly like Python
// node_info["raylet"] = node_stats followed by node_info["raylet"].update(node).
// The physical-stats raylet (reporter process-monitoring fields such as
// cmdline/cpuPercent/pid/memoryInfo) is discarded, matching Python which
// overwrites it with the node_stats dict.
//
// getSummary=true builds the /nodes?view=summary entry, which drops the workers
// (aligned with get_node_info(get_summary=True) popping
// node_physical_stats["workers"]) and keeps the core worker stats under
// node_stats.coreWorkersStats. getSummary=false builds the /nodes/{node_id}
// detail, which pops node_stats.coreWorkersStats (aligned with Python
// get_node_info(get_summary=False)) and attaches the per-node actors and the
// core-worker-stats merged workers.
func (n *NodeHead) nodeSummary(info *proto.GcsNodeInfo, getSummary bool) map[string]interface{} {
	nodeID := hex.EncodeToString(info.NodeId)
	// Copy the cached physical stats so the mutations below (delete "workers"
	// in the summary view, raylet/actors/workers merge in the detail view) do
	// not leak into the DataSource cache, aligned with Python get_node_info
	// which starts from `dict(DataSource.node_physical_stats.get(node_id, {}))`.
	// Without the copy, the summary view's delete(summary, "workers") removes
	// the workers key from the cache, so a subsequent detail request (the
	// cluster page node expand) renders no workers until the reporter republishes.
	summary := map[string]interface{}{}
	if cached := n.ds.NodePhysicalStats(nodeID); cached != nil {
		for k, v := range cached {
			summary[k] = v
		}
	}
	if getSummary {
		// The summary schema has no workers, aligned with Python.
		delete(summary, "workers")
	}
	// Build the raylet sub-message from the raylet node stats (GetNodeStats
	// reply), aligned with Python node_stats_to_dict: coreWorkersStats
	// (snake_case, camel-cased for the response), numWorkers, storeStats. The
	// detail view omits coreWorkersStats entirely, like Python get_node_info
	// which pops node_stats["coreWorkersStats"] for get_summary=False.
	raylet := map[string]interface{}{}
	if reply := n.ds.NodeStatsReply(nodeID); reply != nil {
		hasCore := getSummary
		if !hasCore {
			replyWithoutCore := *reply
			replyWithoutCore.CoreWorkersStats = nil
			reply = &replyWithoutCore
		}
		if m, ok := camelKeysDeep(nodeStatsReplyToMap(reply)).(map[string]interface{}); ok {
			for k, v := range m {
				if !hasCore && k == "coreWorkersStats" {
					continue
				}
				raylet[k] = v
			}
		}
	}
	// Object-store memory stats from the node stats, aligned with Python
	// get_node_info ray_stats: objectStoreBytesAvail == total in the
	// object_manager.cc definition, so available is total minus used. The int
	// type matches Python's int(store_stats.get(...)) (a JSON number).
	used := 0
	total := 0
	if ss := n.ds.NodeStoreStats(nodeID); ss != nil {
		used = int(ss.GetObjectStoreBytesUsed())
		total = int(ss.GetObjectStoreBytesAvail())
	}
	raylet["objectStoreUsedMemory"] = used
	raylet["objectStoreAvailableMemory"] = total - used
	// Merge the GcsNodeInfo fields, aligned with get_node_info's
	// node_info["raylet"].update(node) (the message_to_dict of GcsNodeInfo with
	// always_print_fields_with_no_presence=True, so scalar fields are always
	// present).
	raylet["nodeId"] = nodeID
	raylet["nodeManagerAddress"] = info.NodeManagerAddress
	raylet["nodeManagerHostname"] = info.NodeManagerHostname
	raylet["nodeManagerPort"] = info.NodeManagerPort
	raylet["objectManagerPort"] = info.ObjectManagerPort
	raylet["metricsExportPort"] = info.MetricsExportPort
	raylet["runtimeEnvAgentPort"] = info.RuntimeEnvAgentPort
	// resourcesTotal must be a map[string]interface{} so the response's
	// toGoogleStyle recursively camel-cases its keys (e.g. object_store_memory
	// -> objectStoreMemory, node:__internal_head__ -> node:InternalHead), like
	// the Python to_google_style applied to every dict. A plain
	// map[string]float64 would fall through toGoogleStyle's default branch and
	// leak snake_case keys into the response.
	raylet["resourcesTotal"] = mapToInterface(info.ResourcesTotal)
	raylet["state"] = info.State.String()
	raylet["isHeadNode"] = info.IsHeadNode
	raylet["nodeName"] = info.NodeName
	raylet["instanceId"] = info.InstanceId
	raylet["instanceTypeName"] = info.InstanceTypeName
	raylet["nodeTypeName"] = info.NodeTypeName
	raylet["labels"] = info.Labels
	// startTimeMs/endTimeMs are uint64 fields, which Python's message_to_dict
	// serializes as strings (JSON integer precision protection); endTimeMs=0
	// becomes "0".
	raylet["startTimeMs"] = strconv.FormatUint(info.StartTimeMs, 10)
	raylet["endTimeMs"] = strconv.FormatUint(info.EndTimeMs, 10)
	raylet["rayletSocketName"] = info.RayletSocketName
	raylet["objectStoreSocketName"] = info.ObjectStoreSocketName
	if ss := info.GetStateSnapshot(); ss != nil {
		raylet["stateSnapshot"] = nodeSnapshotToMap(ss)
	}
	raylet["stateMessage"] = composeStateMessage(info)
	if death := gcsDeathInfoToMap(info.DeathInfo); death != nil {
		raylet["deathInfo"] = death
	}
	summary["raylet"] = raylet
	// The top-level identity fields from the GcsNodeInfo, aligned with Python
	// get_node_info which keeps node_physical_stats' hostname/ip (the reporter
	// reports them) and carries no top-level nodeId/state (those live under
	// raylet only).
	summary["hostname"] = info.NodeManagerHostname
	summary["ip"] = info.NodeManagerAddress
	if !getSummary {
		// Detail view: attach the per-node actors and the core-worker-stats
		// merged workers, aligned with Python get_node_info(get_summary=False).
		summary["actors"] = n.nodeActors(nodeID)
		// Merge the raylet core worker stats into the reporter physical-stats
		// workers, aligned with Python datacenter._extract_workers_for_node.
		// Without this the workers lack coreWorkerStats/language/jobId and the
		// frontend worker rows crash on coreWorkerStats.length (NodeRow.tsx).
		if workers, ok := summary["workers"].([]interface{}); ok {
			summary["workers"] = mergeWorkerCoreStats(workers, n.ds.NodeCoreWorkerStats(nodeID))
		}
	}
	return summary
}

// nodeSnapshotToMap serializes a NodeSnapshot (idle state) to a camelCase map,
// aligned with the message_to_dict shape of GcsNodeInfo.state_snapshot. The
// idle_duration_ms int64 field serializes as a string, matching Python's
// message_to_dict which emits int64 fields as strings.
func nodeSnapshotToMap(ss *proto.NodeSnapshot) map[string]interface{} {
	return map[string]interface{}{
		"state":          ss.State.String(),
		"idleDurationMs": strconv.FormatInt(ss.IdleDurationMs, 10),
		"nodeActivity":   ss.NodeActivity,
	}
}

// nodeActors returns the per-node actors keyed by hex actor id, aligned with
// Python DataSource.node_actors (built during _update_actors). Only actors
// whose address node id matches the node (and is not the nil node id) are
// grouped under it.
func (n *NodeHead) nodeActors(nodeID string) map[string]interface{} {
	out := map[string]interface{}{}
	for _, a := range n.ds.Actors() {
		if a.Address == nil || hex.EncodeToString(a.Address.NodeId) != nodeID {
			continue
		}
		out[hex.EncodeToString(a.ActorId)] = n.actorLogical(a)
	}
	return out
}

// rawNodeActors is nodeActors with the raw actor table dict (no
// _get_actor_info merge), mirroring the Python DataSource.node_actors used by
// /test/dump.
func (n *NodeHead) rawNodeActors(nodeID string) map[string]interface{} {
	out := map[string]interface{}{}
	for _, a := range n.ds.Actors() {
		if a.Address == nil || hex.EncodeToString(a.Address.NodeId) != nodeID {
			continue
		}
		out[hex.EncodeToString(a.ActorId)] = actorTableToDict(a)
	}
	return out
}

// workerDefaults match Python dashboard_consts.DEFAULT_LANGUAGE / DEFAULT_JOB_ID
// (python/ray/dashboard/consts.py), used when a worker's pid has no matching
// core worker stats.
const (
	workerDefaultLanguage = "PYTHON"
	workerDefaultJobID    = "ffff"
)

// mergeWorkerCoreStats attaches the raylet core worker stats to each worker of
// the reporter physical stats by matching pid, aligned with Python
// datacenter._extract_workers_for_node: workers get coreWorkerStats (a list
// with a single element when a match exists, else an empty list so the frontend
// can safely index it), plus language and jobId fallbacks. Workers without a
// usable pid are left untouched. The worker maps come from JSON
// deserialization, so pids are float64.
func mergeWorkerCoreStats(workers []interface{}, coreStats []*proto.CoreWorkerStats) []interface{} {
	byPid := make(map[uint32]*proto.CoreWorkerStats, len(coreStats))
	for _, cs := range coreStats {
		if cs != nil {
			byPid[cs.GetPid()] = cs
		}
	}
	out := make([]interface{}, 0, len(workers))
	for _, w := range workers {
		worker, ok := w.(map[string]interface{})
		if !ok {
			out = append(out, w)
			continue
		}
		// Copy the worker map before mutating it so the attached
		// coreWorkerStats/language/jobId do not leak into the DataSource cache
		// (the worker maps are shared references from node_physical_stats),
		// aligned with Python _extract_workers_for_node's `worker = dict(worker)`.
		copied := make(map[string]interface{}, len(worker)+3)
		for k, v := range worker {
			copied[k] = v
		}
		worker = copied
		pid, ok := worker["pid"].(float64)
		if !ok || pid < 0 || pid > math.MaxUint32 {
			out = append(out, worker)
			continue
		}
		if cs, ok := byPid[uint32(pid)]; ok {
			worker["coreWorkerStats"] = []interface{}{coreWorkerStatsToMap(cs)}
			worker["language"] = cs.GetLanguage().String()
			worker["jobId"] = hex.EncodeToString(cs.GetJobId())
		} else {
			worker["coreWorkerStats"] = []interface{}{}
			worker["language"] = workerDefaultLanguage
			worker["jobId"] = workerDefaultJobID
		}
		out = append(out, worker)
	}
	return out
}

// coreWorkerStatsToMap serializes a CoreWorkerStats proto to a snake_case map,
// aligned with Python message_to_dict (preserving_proto_field_name=True and
// always_print_fields_with_no_presence=True), with the byte fields worker_id /
// job_id / actor_id decoded to hex (the Python decode_keys set).
func coreWorkerStatsToMap(cs *proto.CoreWorkerStats) map[string]interface{} {
	raw, err := protojsonMarshalOptions.Marshal(cs)
	if err != nil {
		return map[string]interface{}{}
	}
	m := map[string]interface{}{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return map[string]interface{}{}
	}
	decodeCoreWorkerByteFields(m, cs)
	return m
}

// protojsonMarshalOptions emits proto field names (snake_case) and always prints
// no-presence scalar fields, matching Python message_to_dict defaults.
var protojsonMarshalOptions = protojson.MarshalOptions{
	UseProtoNames:   true,
	EmitUnpopulated: true,
}

// decodeCoreWorkerByteFields overwrites the base64 byte fields produced by
// protojson with hex strings taken from the raw proto, aligned with the Python
// decode_keys set for core worker stats. The language enum is pinned to its
// name string (e.g. "PYTHON"), matching Python's use_integers_for_enums=False.
func decodeCoreWorkerByteFields(m map[string]interface{}, cs *proto.CoreWorkerStats) {
	if cs.GetWorkerId() != nil {
		m["worker_id"] = hex.EncodeToString(cs.GetWorkerId())
	}
	if cs.GetJobId() != nil {
		m["job_id"] = hex.EncodeToString(cs.GetJobId())
	}
	if cs.GetActorId() != nil {
		m["actor_id"] = hex.EncodeToString(cs.GetActorId())
	}
	m["language"] = cs.GetLanguage().String()
}

// composeStateMessage builds the raylet stateMessage from the node death info,
// reusing the head package's shared implementation. The return type is
// interface{} (nil when there is no death info) so the JSON output is null,
// matching the Python compose_state_message which returns None.
func composeStateMessage(info *proto.GcsNodeInfo) interface{} {
	return head.ComposeStateMessage(info.DeathInfo)
}

// gcsDeathInfoToMap converts the node death info to a camelCase map (reason
// string plus reasonMessage), aligned with the message_to_dict shape of
// GcsNodeInfo.death_info in Python. A nil death info yields nil.
func gcsDeathInfoToMap(death *proto.NodeDeathInfo) map[string]interface{} {
	if death == nil {
		return nil
	}
	return map[string]interface{}{
		"reason":        death.Reason.String(),
		"reasonMessage": death.GetReasonMessage(),
	}
}

// actorTableToDict converts an ActorTableData to the light dict produced by the
// Python _actor_table_data_to_dict (camelCase, consumed by the frontend actor.ts
// types), keeping only the fields the dashboard needs. It is the base of
// actorLogical and is also what /test/dump reports under "actors".
func actorTableToDict(a *proto.ActorTableData) map[string]interface{} {
	entry := map[string]interface{}{
		"actorId": hex.EncodeToString(a.ActorId),
		"jobId":   hex.EncodeToString(a.JobId),
		"pid":     a.Pid,
		"state":   a.State.String(),
		"name":    a.Name,
		// num_restarts is an int64 proto field, which Python's message_to_dict
		// serializes as a string ("0"); timestamp is a double, serialized as a
		// float. Both aligned with Python _actor_table_data_to_dict.
		"numRestarts": strconv.FormatInt(a.NumRestarts, 10),
		"timestamp":   a.Timestamp,
		"className":   a.ClassName,
		"startTime":   a.StartTime,
		"endTime":     a.EndTime,
		"reprName":    a.ReprName,
		"actorClass":  a.ClassName,
		// labelSelector is always an object: the Python
		// _actor_table_data_to_dict forces dict(message.label_selector), which is
		// {} for an empty proto map, whereas a raw proto map would serialize as
		// null.
		"labelSelector": mapToInterface(a.LabelSelector),
		"exitDetail":    "-",
		// requiredResources must be a map[string]interface{} so the /test/dump
		// response's toGoogleStyle recurses into it and camel-cases its keys
		// (node:__internal_head__ -> node:InternalHead), like Python's
		// to_google_style. mapToInterface also turns an empty proto map into {}
		// rather than null, matching the Python dict conversion.
		"requiredResources": mapToInterface(a.RequiredResources),
	}
	if a.CallSite != nil {
		entry["callSite"] = *a.CallSite
	}
	if a.Address != nil {
		entry["address"] = map[string]interface{}{
			"nodeId":    hex.EncodeToString(a.Address.NodeId),
			"workerId":  hex.EncodeToString(a.Address.WorkerId),
			"ipAddress": a.Address.IpAddress,
			"port":      a.Address.Port,
		}
	}
	if a.PlacementGroupId != nil {
		entry["placementGroupId"] = hex.EncodeToString(a.PlacementGroupId)
	}
	entry["exitDetail"] = actorExitDetail(a)
	return entry
}

// actorLogical builds the full actor dict consumed by /logical/actors, aligned
// with the Python DataOrganizer._get_actor_info: the light actor dict is merged
// with the actor worker's core worker stats (indexed by worker id), then
// gpus/processStats/mem are pulled from the node physical stats by pid and the
// placement-group formatted requiredResources are normalized. All keys are
// camelCase, matching the Python actor dict which is already google-formatted
// when protobuf is converted (get_all_actors serves with convert_google_style=False).
func (n *NodeHead) actorLogical(a *proto.ActorTableData) map[string]interface{} {
	entry := actorTableToDict(a)
	var workerID, nodeID string
	if a.Address != nil {
		workerID = hex.EncodeToString(a.Address.WorkerId)
		nodeID = hex.EncodeToString(a.Address.NodeId)
	}
	if cs := n.ds.CoreWorkerStatsByWorkerID(workerID); cs != nil {
		for k, v := range coreWorkerStatsToCamelMap(cs) {
			entry[k] = v
		}
	}
	// gpus / processStats / mem come from the node physical stats by pid,
	// aligned with Python _get_actor_info (pid only when core worker stats are
	// available, i.e. non-zero).
	pid, hasPid := pidInt64(entry["pid"])
	var processStats interface{}
	gpus := []interface{}{}
	if hasPid && pid > 0 {
		if stats := n.ds.NodePhysicalStats(nodeID); stats != nil {
			for _, w := range asSlice(stats["workers"]) {
				worker, ok := w.(map[string]interface{})
				if !ok {
					continue
				}
				if p, ok := pidInt64(worker["pid"]); ok && p == pid {
					processStats = worker
					break
				}
			}
			for _, g := range asSlice(stats["gpus"]) {
				gpu, ok := g.(map[string]interface{})
				if !ok {
					continue
				}
				for _, proc := range asSlice(gpu["processesPids"]) {
					pm, ok := proc.(map[string]interface{})
					if !ok {
						continue
					}
					if p, ok := pidInt64(pm["pid"]); ok && p == pid {
						gpus = append(gpus, gpu)
						break
					}
				}
			}
		}
	}
	entry["gpus"] = gpus
	entry["processStats"] = processStats
	mem := []interface{}{}
	if stats := n.ds.NodePhysicalStats(nodeID); stats != nil {
		if m, ok := stats["mem"].([]interface{}); ok {
			mem = m
		}
	}
	entry["mem"] = mem
	entry["requiredResources"] = parsePGFormattedResources(entry["requiredResources"])
	return entry
}

// coreWorkerStatsToCamelMap serializes a CoreWorkerStats to a camelCase map with
// hex byte fields, aligned with the Python message_to_dict shape used to build
// DataSource.core_worker_stats.
func coreWorkerStatsToCamelMap(cs *proto.CoreWorkerStats) map[string]interface{} {
	return camelKeysDeep(coreWorkerStatsToMap(cs)).(map[string]interface{})
}

// camelKeysDeep recursively converts map keys to camelCase, matching the Python
// to_google_style applied to protobuf message_to_dict output.
func camelKeysDeep(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, e := range t {
			out[camelCase(k)] = camelKeysDeep(e)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, e := range t {
			out[i] = camelKeysDeep(e)
		}
		return out
	default:
		return v
	}
}

// camelCase converts a snake_case key to camelCase, matching to_camel_case in
// python/ray/dashboard/utils.py (first component kept lowercase, the rest
// title-cased; empty components from trailing underscores are skipped).
func camelCase(s string) string {
	parts := strings.Split(s, "_")
	out := parts[0]
	for _, p := range parts[1:] {
		if p == "" {
			continue
		}
		out += strings.ToUpper(p[:1]) + p[1:]
	}
	return out
}

// mapToInterface converts a map[string]V (e.g. the GcsNodeInfo resources_total
// or the ActorTableData label_selector) to a map[string]interface{} so the
// response's toGoogleStyle recurses into it and camel-cases its keys. A nil
// input yields an empty (non-nil) map, so the JSON output is {} rather than
// null, matching the Python dict(...) conversions.
func mapToInterface[V any](m map[string]V) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// asSlice returns v as a []interface{}, or nil when it is not a slice.
func asSlice(v interface{}) []interface{} {
	s, _ := v.([]interface{})
	return s
}

// pidInt64 extracts a process id from a proto uint32 or a JSON-decoded float64
// (the shape the pid takes in the physical stats) as an int64.
func pidInt64(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case uint32:
		return int64(t), true
	case float64:
		return int64(t), true
	case int64:
		return t, true
	}
	return 0, false
}

// indexedPGPattern / wildcardPGPattern match the placement-group formatted
// resource names, aligned with PLACEMENT_GROUP_INDEXED_BUNDLED_RESOURCE_PATTERN
// and PLACEMENT_GROUP_WILDCARD_RESOURCE_PATTERN in python/ray/_private/utils.py.
var (
	indexedPGPattern  = regexp.MustCompile(`(.+)_group_(\d+)_([0-9a-zA-Z]+)`)
	wildcardPGPattern = regexp.MustCompile(`(.+)_group_([0-9a-zA-Z]+)`)
)

// parsePGFormattedResources converts placement-group formatted resource names
// back to their original names, aligned with
// parse_pg_formatted_resources_to_original in python/ray/_private/utils.py. The
// "bundle" group resource (an implementation detail) is dropped.
func parsePGFormattedResources(resources interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	switch t := resources.(type) {
	case map[string]float64:
		for k, v := range t {
			if name, ok := pgResourceName(k); ok {
				out[name] = v
			}
		}
	case map[string]interface{}:
		for k, v := range t {
			f, ok := v.(float64)
			if !ok {
				continue
			}
			if name, ok := pgResourceName(k); ok {
				out[name] = f
			}
		}
	}
	return out
}

// pgResourceName reduces a placement-group formatted resource name to its
// original form (the prefix before "_group_"). It reports false for the "bundle"
// implementation-detail resource, which Python skips.
func pgResourceName(k string) (string, bool) {
	if match := indexedPGPattern.FindStringSubmatch(k); len(match) == 4 {
		if match[1] == "bundle" {
			return "", false
		}
		return match[1], true
	}
	if match := wildcardPGPattern.FindStringSubmatch(k); len(match) == 3 {
		if match[1] == "bundle" {
			return "", false
		}
		return match[1], true
	}
	return k, true
}

// gcsNodeInfoToMap serializes a GcsNodeInfo to a snake_case map with the node id
// decoded to hex, aligned with the Python _gcs_node_info_to_dict (decode_keys =
// {"nodeId"}, always_print_fields_with_no_presence=True). Used by /test/dump.
func gcsNodeInfoToMap(info *proto.GcsNodeInfo) map[string]interface{} {
	raw, err := protojsonMarshalOptions.Marshal(info)
	if err != nil {
		return map[string]interface{}{}
	}
	m := map[string]interface{}{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return map[string]interface{}{}
	}
	if info.GetNodeId() != nil {
		m["node_id"] = hex.EncodeToString(info.GetNodeId())
	}
	// Drop the death_info message field when unset: protojson with
	// EmitUnpopulated emits it as null, but Python's message_to_dict only
	// prints message fields that have presence.
	if info.GetDeathInfo() == nil {
		delete(m, "death_info")
	}
	return m
}

// nodeStatsReplyToMap serializes a GetNodeStatsReply to a snake_case map with
// the core worker stats and store stats, aligned with the Python
// node_stats_to_dict (which nests message_to_dict per core worker stat). Used by
// /test/dump.
func nodeStatsReplyToMap(reply *proto.GetNodeStatsReply) map[string]interface{} {
	out := map[string]interface{}{}
	coreWorkersStats := make([]interface{}, 0, len(reply.GetCoreWorkersStats()))
	for _, cs := range reply.GetCoreWorkersStats() {
		coreWorkersStats = append(coreWorkersStats, coreWorkerStatsToMap(cs))
	}
	out["core_workers_stats"] = coreWorkersStats
	out["num_workers"] = reply.GetNumWorkers()
	if ss := reply.GetStoreStats(); ss != nil {
		out["store_stats"] = storeStatsToMap(ss)
	}
	return out
}

// storeStatsToMap serializes an ObjectStoreStats to a snake_case map. Only
// fields with presence are emitted (protojson default), matching the Python
// node_stats_to_dict message_to_dict which does not pass
// always_print_fields_with_no_presence for the top-level reply. Used by
// /test/dump.
func storeStatsToMap(ss *proto.ObjectStoreStats) map[string]interface{} {
	raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(ss)
	if err != nil {
		return map[string]interface{}{}
	}
	m := map[string]interface{}{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return map[string]interface{}{}
	}
	return m
}

// nodeLogicalResources computes the per-node logical resource breakdown served
// under /nodes?view=summary's nodeLogicalResources, aligned with the Python
// NodeHead.get_nodes_logical_resources autoscaler v2 path (node_head.py): it
// calls the GCS GetClusterStatus RPC and, for every non-dead node, formats the
// used/total breakdown of its resources exactly like
// parse_usage(usage, verbose=True) in python/ray/autoscaler/_private/util.py
// (e.g. "1.0/6.0 CPU\n0B/3.74GiB memory\n1.02KiB/1.60GiB object_store_memory").
// It returns an empty map when the autoscaler status is unavailable, matching
// Python's {} when get_cluster_status raises or the reply has no per-node
// resource usage.
func (n *NodeHead) nodeLogicalResources(ctx context.Context) map[string]string {
	reply := n.fetchClusterStatus(ctx)
	if reply == nil {
		return map[string]string{}
	}
	out := map[string]string{}
	for _, ns := range reply.GetClusterResourceState().GetNodeStates() {
		if ns.GetStatus() == proto.NodeStatus_DEAD {
			continue
		}
		usage := map[string][2]float64{}
		for res, total := range ns.GetTotalResources() {
			e := usage[res]
			e[0] = total
			e[1] = total
			usage[res] = e
		}
		for res, avail := range ns.GetAvailableResources() {
			e := usage[res]
			e[0] -= avail
			usage[res] = e
		}
		if len(usage) == 0 {
			continue
		}
		out[hex.EncodeToString(ns.GetNodeId())] = strings.Join(parseUsageLines(usage, true), "\n")
	}
	return out
}

// fetchClusterStatus calls the GCS AutoscalerStateService.GetClusterStatus RPC,
// aligned with the Python get_cluster_status in
// python/ray/autoscaler/v2/sdk.py. It returns nil on any failure; the caller
// falls back to an empty map.
func (n *NodeHead) fetchClusterStatus(ctx context.Context) *proto.GetClusterStatusReply {
	if n.cfg == nil || n.cfg.GCSAddress == "" {
		return nil
	}
	conn, err := grpc.DialContext(ctx, n.cfg.GCSAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	defer conn.Close()
	stub := proto.NewAutoscalerStateServiceClient(conn)
	reply, err := stub.GetClusterStatus(ctx, &proto.GetClusterStatusRequest{})
	if err != nil {
		return nil
	}
	return reply
}

// parseUsageLines ports parse_usage from python/ray/autoscaler/_private/util.py
// (verbose mode). It formats each resource as "used/total name", with memory
// resources formatted through formatMemoryBytes and placement-group resources
// merged/counted like the Python implementation. The "node:" auto-added
// per-node resource is skipped.
func parseUsageLines(usage map[string][2]float64, verbose bool) []string {
	pgUsage := map[string]float64{}
	pgTotal := map[string]float64{}
	for resource, uv := range usage {
		pgResource, pgName, isCountable := parsePGResourceStr(resource)
		if pgName == "" {
			continue
		}
		if _, ok := pgUsage[pgResource]; !ok {
			pgUsage[pgResource] = 0
		}
		if isCountable {
			pgUsage[pgResource] += uv[0]
			pgTotal[pgResource] += uv[1]
		}
	}
	keys := make([]string, 0, len(usage))
	for k := range usage {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var lines []string
	for _, resource := range keys {
		if strings.Contains(resource, "node:") {
			continue
		}
		_, pgName, _ := parsePGResourceStr(resource)
		if pgName != "" {
			continue
		}
		used, total := usage[resource][0], usage[resource][1]
		pgUsed, pgTotalV := 0.0, 0.0
		usedInPG := false
		if v, ok := pgUsage[resource]; ok {
			usedInPG = true
			pgUsed = v
			pgTotalV = pgTotal[resource]
			used = used - pgTotalV + pgUsed
		}
		if resource == "memory" || resource == "object_store_memory" {
			line := fmt.Sprintf("%s/%s %s", formatMemoryBytes(used), formatMemoryBytes(total), resource)
			if usedInPG {
				line += fmt.Sprintf(" (%s used of %s reserved in placement groups)", formatMemoryBytes(pgUsed), formatMemoryBytes(pgTotalV))
			}
			lines = append(lines, line)
		} else if strings.HasPrefix(resource, "accelerator_type:") && !verbose {
			continue
		} else {
			line := fmt.Sprintf("%s/%s %s", pyFloatString(used), pyFloatString(total), resource)
			if usedInPG {
				line += fmt.Sprintf(" (%s used of %s reserved in placement groups)", pyFloatString(pgUsed), pyFloatString(pgTotalV))
			}
			lines = append(lines, line)
		}
	}
	return lines
}

// pyFloatString renders a float the way Python's str() does, so the logical
// resource strings match parse_usage exactly: integral floats keep a trailing
// ".0" (Python: str(1.0) == "1.0", str(6.0) == "6.0"), non-integral floats use
// the shortest round-trip representation.
func pyFloatString(f float64) string {
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		return s + ".0"
	}
	return s
}

// memorySuffixes is aligned with MEMORY_SUFFIXES in
// python/ray/autoscaler/_private/util.py.
var memorySuffixes = []struct {
	suffix string
	bytes  float64
}{
	{"TiB", 1 << 40},
	{"GiB", 1 << 30},
	{"MiB", 1 << 20},
	{"KiB", 1 << 10},
}

// formatMemoryBytes ports format_memory from
// python/ray/autoscaler/_private/util.py: bytes are rendered in the largest
// binary unit that fits, with two decimals (e.g. 1722123878 -> "1.60GiB"), and
// values below 1KiB as an integer byte count (e.g. 1040 -> "1040B").
func formatMemoryBytes(memBytes float64) string {
	for _, sfx := range memorySuffixes {
		if memBytes >= sfx.bytes {
			return fmt.Sprintf("%.2f%s", memBytes/sfx.bytes, sfx.suffix)
		}
	}
	return fmt.Sprintf("%dB", int(memBytes))
}

// parsePGResourceStr ports parse_placement_group_resource_str: returns
// (resourceName, pgName, isCountable). pgName empty means not a PG resource.
func parsePGResourceStr(resource string) (string, string, bool) {
	if i := strings.LastIndex(resource, "_group_"); i >= 0 {
		tail := resource[i+len("_group_"):]
		parts := strings.SplitN(tail, "_", 2)
		if len(parts) == 2 && allDigits(parts[0]) && alnumOnly(parts[1]) {
			return resource[:i], parts[1], false
		}
		if alnumOnly(parts[0]) {
			return resource[:i], parts[0], true
		}
	}
	return resource, "", true
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func alnumOnly(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
			return false
		}
	}
	return true
}

// actorExitDetail extracts the actor death detail from the death cause,
// aligned with the Python exit_detail computation.
func actorExitDetail(a *proto.ActorTableData) string {
	if a.DeathCause == nil {
		return "-"
	}
	switch c := a.DeathCause.Context.(type) {
	case *proto.ActorDeathCause_ActorDiedErrorContext:
		if c.ActorDiedErrorContext != nil {
			return c.ActorDiedErrorContext.ErrorMessage
		}
	case *proto.ActorDeathCause_RuntimeEnvFailedContext:
		if c.RuntimeEnvFailedContext != nil {
			return c.RuntimeEnvFailedContext.ErrorMessage
		}
	case *proto.ActorDeathCause_ActorUnschedulableContext:
		if c.ActorUnschedulableContext != nil {
			return c.ActorUnschedulableContext.ErrorMessage
		}
	case *proto.ActorDeathCause_CreationTaskFailureContext:
		if c.CreationTaskFailureContext != nil {
			return c.CreationTaskFailureContext.FormattedExceptionString
		}
	}
	return "-"
}
