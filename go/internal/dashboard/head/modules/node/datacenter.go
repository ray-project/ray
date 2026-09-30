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

// Package node implements the NodeHead dashboard module that exposes the
// /nodes and /logical/actors routes, aligned with the Python NodeHead in
// python/ray/dashboard/modules/node/node_head.py.
package node

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// kvNamespaceJob is KV_NAMESPACE_JOB; kvHeadNodeIDKey is KV_HEAD_NODE_ID_KEY.
// The head node id is persisted here by NodeHead (aligned with Python
// node_head.py) so JobHead can locate the head node's job agent.
const (
	kvNamespaceJob  = "job"
	kvHeadNodeIDKey = "head_node_id"

	// nodeStatsInterval is how often the per-node core worker stats are pulled
	// from each raylet's NodeManager gRPC service. Matches Python
	// NODE_STATS_UPDATE_INTERVAL_SECONDS (node_consts.py).
	nodeStatsInterval = 15 * time.Second

	// purgeInterval is how often dead-node stats are purged, aligned with the
	// Python RAY_DASHBOARD_STATS_PURGING_INTERVAL (default 600s, dashboard
	// consts.py).
	purgeInterval = 10 * time.Minute

	// grpcMaxRecvMsgSize matches the C++ max_grpc_message_size default used by
	// the GCS client (head package); node stats can carry large object refs.
	grpcMaxRecvMsgSize = 512 << 20
)

// DataSource caches cluster state (nodes, actors, per-node physical stats)
// refreshed by the GCS pubsub subscribers, aligned with the Python
// datacenter.DataSource. Map keys are the hex-encoded node/actor ids.
type DataSource struct {
	client   *head.GCSClient
	nodeSub  *head.NodeInfoSubscriber
	actorSub *head.ActorSubscriber
	usageSub *head.ResourceUsageSubscriber

	mu     sync.RWMutex
	nodes  map[string]*proto.GcsNodeInfo
	actors map[string]*proto.ActorTableData
	stats  map[string]map[string]interface{}
	// nodeStats holds each node's node manager stats pulled from its raylet,
	// aligned with Python DataSource.node_stats (node_head.py _update_node_stats).
	nodeStats map[string]*proto.GetNodeStatsReply
	// coreWorkerStats indexes the core worker stats by hex worker id, rebuilt
	// after every node stats refresh, aligned with Python
	// DataSource.core_worker_stats built by DataOrganizer.organize.
	coreWorkerStats map[string]*proto.CoreWorkerStats

	// registeredHeadNodeID is the head node id already persisted to GCS KV;
	// the node update loop writes it once when the head node becomes ALIVE
	// (aligned with Python node_head._registered_head_node_id).
	registeredHeadNodeID string
}

// NewDataSource creates a data source backed by the given GCS client and
// pubsub subscribers. Subscribers may be nil; their refresh loops are skipped.
func NewDataSource(client *head.GCSClient, nodeSub *head.NodeInfoSubscriber, actorSub *head.ActorSubscriber, usageSub *head.ResourceUsageSubscriber) *DataSource {
	return &DataSource{
		client:          client,
		nodeSub:         nodeSub,
		actorSub:        actorSub,
		usageSub:        usageSub,
		nodes:           map[string]*proto.GcsNodeInfo{},
		actors:          map[string]*proto.ActorTableData{},
		stats:           map[string]map[string]interface{}{},
		nodeStats:       map[string]*proto.GetNodeStatsReply{},
		coreWorkerStats: map[string]*proto.CoreWorkerStats{},
	}
}

// Start initializes the cache from a full GCS pull and then starts the
// subscription refresh loops. It returns after launching the loops; the loops
// run until ctx is cancelled.
func (d *DataSource) Start(ctx context.Context) error {
	// Pull the initial full state to avoid losing entries that arrive between
	// the subscription and the snapshot (TOCTOU), aligned with the Python
	// node_head._update_actors / _subscribe_for_node_updates.
	if err := d.initFromGCS(ctx); err != nil {
		log.Log.Error(err, "failed to init DataSource from GCS")
	}

	// Subscribe to node updates. A DEAD node is kept in the cache (aligned with
	// Python _update_node, which moves dead nodes to _dead_node_queue and only
	// evicts the oldest once the queue exceeds MAX_DEAD_NODES_TO_CACHE=1000);
	// this period retains all dead nodes and eviction is left for later.
	if d.nodeSub != nil {
		nodeCh, errCh := d.nodeSub.Updates(ctx)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case n, ok := <-nodeCh:
					if !ok {
						return
					}
					d.mu.Lock()
					d.nodes[hex.EncodeToString(n.NodeId)] = n
					d.mu.Unlock()
					d.persistHeadNodeID(ctx, n)
				case err := <-errCh:
					if status.Code(err) == codes.Unimplemented {
						// GCS servers built without the pubsub service report
						// Unimplemented here. The query path already serves
						// this data, so drop the realtime subscription quietly
						// instead of logging an error on every startup.
						log.Log.V(1).Info("node subscription unavailable: GCS server has no pubsub service; using query fallback")
						return
					}
					log.Log.Error(err, "node subscription error")
					return
				}
			}
		}()
	}

	// Subscribe to actor updates.
	if d.actorSub != nil {
		actorCh, errCh := d.actorSub.Updates(ctx)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case a, ok := <-actorCh:
					if !ok {
						return
					}
					// Skip updates without an actor id, aligned with the
					// Python _poll_updated_actor_table_data which filters out
					// entries whose actor_id_bytes is None.
					if len(a.ActorId) == 0 {
						continue
					}
					d.mu.Lock()
					d.actors[hex.EncodeToString(a.ActorId)] = a
					d.mu.Unlock()
				case err := <-errCh:
					if status.Code(err) == codes.Unimplemented {
						log.Log.V(1).Info("actor subscription unavailable: GCS server has no pubsub service; using query fallback")
						return
					}
					log.Log.Error(err, "actor subscription error")
					return
				}
			}
		}()
	}

	// Subscribe to resource usage updates published by the reporter every few
	// seconds; the payload is the raw reporter JSON.
	if d.usageSub != nil {
		usageCh, errCh := d.usageSub.Updates(ctx)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case u, ok := <-usageCh:
					if !ok {
						return
					}
					var parsed map[string]interface{}
					if err := json.Unmarshal([]byte(u.JSON), &parsed); err != nil {
						log.Log.Error(err, "failed to parse resource usage json", "node", u.NodeID)
						continue
					}
					d.mu.Lock()
					d.stats[u.NodeID] = parsed
					d.mu.Unlock()
				case err := <-errCh:
					if status.Code(err) == codes.Unimplemented {
						log.Log.V(1).Info("resource usage subscription unavailable: GCS server has no pubsub service; using query fallback")
						return
					}
					log.Log.Error(err, "resource usage subscription error")
					return
				}
			}
		}()
	}

	// Periodically pull the per-node core worker stats from each raylet's
	// NodeManager gRPC service (aligned with Python node_head._update_node_stats).
	// An initial pull runs before the first interval so the cache is warm for
	// early /nodes requests.
	go func() {
		d.refreshNodeStats(ctx)
		ticker := time.NewTicker(nodeStatsInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.refreshNodeStats(ctx)
			}
		}
	}()

	// Periodically purge the per-node stats of dead nodes, aligned with Python
	// DataOrganizer.purge (which removes node_stats / node_physical_stats of
	// nodes not in the alive set).
	go func() {
		ticker := time.NewTicker(purgeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.purgeDeadNodeStats()
			}
		}
	}()
	return nil
}

// refreshNodeStats pulls GetNodeStats from every ALIVE node's raylet and caches
// the full reply (core worker stats plus store stats), keyed by hex node id,
// then rebuilds the worker-id -> core worker stats index. Nodes that fail
// (unreachable or terminated) keep their previous stats, matching the Python
// behavior of collecting per-node stats with return_exceptions=True.
func (d *DataSource) refreshNodeStats(ctx context.Context) {
	d.mu.RLock()
	nodes := make([]*proto.GcsNodeInfo, 0, len(d.nodes))
	for _, n := range d.nodes {
		if n.GetState() == proto.GcsNodeInfo_ALIVE {
			nodes = append(nodes, n)
		}
	}
	d.mu.RUnlock()

	for _, n := range nodes {
		nodeID := hex.EncodeToString(n.NodeId)
		reply, err := fetchNodeStats(ctx, n)
		if err != nil {
			log.Log.V(1).Info("failed to fetch node stats", "node", nodeID, "error", err)
			continue
		}
		d.mu.Lock()
		d.nodeStats[nodeID] = reply
		d.mu.Unlock()
	}
	d.mu.Lock()
	d.rebuildCoreWorkerIndex()
	d.mu.Unlock()
}

// rebuildCoreWorkerIndex rebuilds the worker-id -> core worker stats index from
// the per-node stats cache, aligned with Python DataOrganizer.organize which
// re-derives DataSource.core_worker_stats on every update interval. Only stats
// of currently-alive workers (those present in the node physical stats workers
// list, matched by pid) are kept, matching the Python extraction loop that
// iterates over node_physical_stats workers. The caller must hold mu.
func (d *DataSource) rebuildCoreWorkerIndex() {
	// Collect the pids of currently-alive workers from the physical stats.
	alivePIDs := make(map[uint32]bool)
	for _, raw := range d.stats {
		for _, w := range asSlice(raw["workers"]) {
			worker, ok := w.(map[string]interface{})
			if !ok {
				continue
			}
			if pid, ok := pidInt64(worker["pid"]); ok {
				alivePIDs[uint32(pid)] = true
			}
		}
	}
	index := make(map[string]*proto.CoreWorkerStats, len(d.coreWorkerStats))
	for _, reply := range d.nodeStats {
		for _, cs := range reply.GetCoreWorkersStats() {
			if cs == nil || cs.GetWorkerId() == nil {
				continue
			}
			if !alivePIDs[cs.GetPid()] {
				continue
			}
			index[hex.EncodeToString(cs.GetWorkerId())] = cs
		}
	}
	d.coreWorkerStats = index
}

// purgeDeadNodeStats drops the per-node stats of nodes that are no longer
// ALIVE, aligned with Python DataOrganizer.purge: node_stats and
// node_physical_stats are removed for every node id outside the alive set.
func (d *DataSource) purgeDeadNodeStats() {
	d.mu.Lock()
	defer d.mu.Unlock()
	alive := make(map[string]bool, len(d.nodes))
	for id, n := range d.nodes {
		if n.GetState() == proto.GcsNodeInfo_ALIVE {
			alive[id] = true
		}
	}
	for id := range d.nodeStats {
		if !alive[id] {
			delete(d.nodeStats, id)
		}
	}
	for id := range d.stats {
		if !alive[id] {
			delete(d.stats, id)
		}
	}
	// Rebuild the core worker index so purged workers disappear from it too.
	d.rebuildCoreWorkerIndex()
}

// fetchNodeStats dials a node's NodeManager gRPC service and returns the full
// GetNodeStats reply. The connection is short-lived and closed after the call
// (head nodes are few, so a stub cache like Python's is unnecessary).
func fetchNodeStats(ctx context.Context, n *proto.GcsNodeInfo) (*proto.GetNodeStatsReply, error) {
	address := n.GetNodeManagerAddress() + ":" + strconv.Itoa(int(n.GetNodeManagerPort()))
	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMaxRecvMsgSize)))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := proto.NewNodeManagerServiceClient(conn)
	return client.GetNodeStats(ctx, &proto.GetNodeStatsRequest{})
}

// persistHeadNodeID writes the head node id to GCS KV (namespace "job", key
// "head_node_id") when the head node transitions to ALIVE, aligned with Python
// node_head.py (which persists it once per registered head node so JobHead /
// FlowHead can locate the head node's job agent). The write is idempotent:
// once a head node id is persisted it is not re-written.
func (d *DataSource) persistHeadNodeID(ctx context.Context, n *proto.GcsNodeInfo) {
	if d.client == nil || !n.GetIsHeadNode() || n.GetState() != proto.GcsNodeInfo_ALIVE {
		return
	}
	nodeIDHex := hex.EncodeToString(n.NodeId)
	d.mu.Lock()
	if d.registeredHeadNodeID == nodeIDHex {
		d.mu.Unlock()
		return
	}
	d.mu.Unlock()
	if _, err := d.client.InternalKVPut(ctx, kvNamespaceJob, kvHeadNodeIDKey, []byte(nodeIDHex), true); err != nil {
		log.Log.Error(err, "failed to persist head node id", "node_id", nodeIDHex)
		return
	}
	d.mu.Lock()
	d.registeredHeadNodeID = nodeIDHex
	d.mu.Unlock()
	log.Log.Info("persisted head node id", "node_id", nodeIDHex)
}

// initFromGCS performs the initial full pull of nodes and actors.
func (d *DataSource) initFromGCS(ctx context.Context) error {
	if d.client == nil {
		return nil
	}
	nodeReply, err := d.client.GetAllNodeInfo(ctx, &proto.GetAllNodeInfoRequest{})
	if err == nil {
		d.mu.Lock()
		for _, n := range nodeReply.NodeInfoList {
			d.nodes[hex.EncodeToString(n.NodeId)] = n
		}
		d.mu.Unlock()
		// Persist the head node id from the snapshot too, in case the head node
		// is already ALIVE when the subscription starts (no update event follows).
		for _, n := range nodeReply.NodeInfoList {
			d.persistHeadNodeID(ctx, n)
		}
	}
	actorReply, err := d.client.GetAllActorInfo(ctx, &proto.GetAllActorInfoRequest{})
	if err == nil {
		d.mu.Lock()
		for _, a := range actorReply.ActorTableData {
			d.actors[hex.EncodeToString(a.ActorId)] = a
		}
		d.mu.Unlock()
	}
	return err
}

// Nodes returns the cached nodes as a snapshot slice.
func (d *DataSource) Nodes() []*proto.GcsNodeInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]*proto.GcsNodeInfo, 0, len(d.nodes))
	for _, n := range d.nodes {
		out = append(out, n)
	}
	return out
}

// Actors returns the cached actors as a snapshot slice.
func (d *DataSource) Actors() []*proto.ActorTableData {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]*proto.ActorTableData, 0, len(d.actors))
	for _, a := range d.actors {
		out = append(out, a)
	}
	return out
}

// Node returns the cached node with the given hex node id, or nil.
func (d *DataSource) Node(nodeID string) *proto.GcsNodeInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.nodes[nodeID]
}

// Actor returns the cached actor with the given hex actor id, or nil.
func (d *DataSource) Actor(actorID string) *proto.ActorTableData {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.actors[actorID]
}

// NodePhysicalStats returns the cached physical stats for the node.
// TODO: consumed by the /nodes/{id} detail merge in a later task
func (d *DataSource) NodePhysicalStats(nodeID string) map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.stats[nodeID]
}

// NodeCoreWorkerStats returns the cached core worker stats for the node,
// pulled from its raylet's GetNodeStats.
func (d *DataSource) NodeCoreWorkerStats(nodeID string) []*proto.CoreWorkerStats {
	d.mu.RLock()
	defer d.mu.RUnlock()
	reply := d.nodeStats[nodeID]
	if reply == nil {
		return nil
	}
	return reply.GetCoreWorkersStats()
}

// NodeStoreStats returns the cached object store stats for the node, pulled
// from its raylet's GetNodeStats.
func (d *DataSource) NodeStoreStats(nodeID string) *proto.ObjectStoreStats {
	d.mu.RLock()
	defer d.mu.RUnlock()
	reply := d.nodeStats[nodeID]
	if reply == nil {
		return nil
	}
	return reply.GetStoreStats()
}

// CoreWorkerStatsByWorkerID returns the cached core worker stats indexed by the
// hex worker id, aligned with Python DataSource.core_worker_stats.
func (d *DataSource) CoreWorkerStatsByWorkerID(workerID string) *proto.CoreWorkerStats {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.coreWorkerStats[workerID]
}

// NodeStatsReply returns the full cached node manager stats reply for the node.
func (d *DataSource) NodeStatsReply(nodeID string) *proto.GetNodeStatsReply {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.nodeStats[nodeID]
}

// DumpNodes returns a snapshot of the nodes map (hex node id -> GcsNodeInfo),
// for the /test/dump handler.
func (d *DataSource) DumpNodes() map[string]*proto.GcsNodeInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[string]*proto.GcsNodeInfo, len(d.nodes))
	for id, n := range d.nodes {
		out[id] = n
	}
	return out
}

// DumpActors returns a snapshot of the actors map (hex actor id ->
// ActorTableData), for the /test/dump handler.
func (d *DataSource) DumpActors() map[string]*proto.ActorTableData {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[string]*proto.ActorTableData, len(d.actors))
	for id, a := range d.actors {
		out[id] = a
	}
	return out
}

// DumpPhysicalStats returns a snapshot of the per-node physical stats map, for
// the /test/dump handler.
func (d *DataSource) DumpPhysicalStats() map[string]map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[string]map[string]interface{}, len(d.stats))
	for id, s := range d.stats {
		out[id] = s
	}
	return out
}

// DumpNodeStats returns a snapshot of the per-node node manager stats replies,
// for the /test/dump handler.
func (d *DataSource) DumpNodeStats() map[string]*proto.GetNodeStatsReply {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[string]*proto.GetNodeStatsReply, len(d.nodeStats))
	for id, s := range d.nodeStats {
		out[id] = s
	}
	return out
}

// DumpCoreWorkerStats returns a snapshot of the worker-id -> core worker stats
// index, for the /test/dump handler.
func (d *DataSource) DumpCoreWorkerStats() map[string]*proto.CoreWorkerStats {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[string]*proto.CoreWorkerStats, len(d.coreWorkerStats))
	for id, cs := range d.coreWorkerStats {
		out[id] = cs
	}
	return out
}
