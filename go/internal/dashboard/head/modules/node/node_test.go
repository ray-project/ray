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
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// mockGCSServer simulates the GCS info services plus the internal pubsub
// service used by the NodeInfo/Actor/ResourceUsage subscribers.
type mockGCSServer struct {
	proto.UnimplementedActorInfoGcsServiceServer
	proto.UnimplementedNodeInfoGcsServiceServer
	proto.UnimplementedControlPlanePubSubGcsServiceServer
	proto.UnimplementedObservabilityPubSubServiceServer
	proto.UnimplementedInternalKVGcsServiceServer

	mu        sync.Mutex
	nodeInfo  []*proto.GcsNodeInfo
	actorInfo []*proto.ActorTableData
	// kvDelCalls records the namespace/key pair of every InternalKVDel call in
	// call order; kvDelFail marks the pairs whose delete returns an error.
	kvDelCalls []string
	kvDelFail  map[string]bool
	// subChannel maps subscriber id -> channel, learned from the subscribe
	// command batches, so polls can be routed per channel.
	subChannel map[string]proto.ChannelType
	// queues holds the pub messages pending per channel, consumed in FIFO order.
	queues    map[proto.ChannelType][][]*proto.PubMessage
	publisher []byte
}

func (s *mockGCSServer) GetAllActorInfo(ctx context.Context, req *proto.GetAllActorInfoRequest) (*proto.GetAllActorInfoReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &proto.GetAllActorInfoReply{ActorTableData: s.actorInfo, Total: int64(len(s.actorInfo))}, nil
}

func (s *mockGCSServer) GetAllNodeInfo(ctx context.Context, req *proto.GetAllNodeInfoRequest) (*proto.GetAllNodeInfoReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &proto.GetAllNodeInfoReply{NodeInfoList: s.nodeInfo, Total: int64(len(s.nodeInfo))}, nil
}

// GcsPublish and ReportJobError are spelled out because both embedded
// Unimplemented servers provide them, which makes the promoted method
// ambiguous and blocks both service registrations.
func (s *mockGCSServer) GcsPublish(ctx context.Context, req *proto.GcsPublishRequest) (*proto.GcsPublishReply, error) {
	return &proto.GcsPublishReply{}, nil
}

func (s *mockGCSServer) ReportJobError(ctx context.Context, req *proto.ReportJobErrorRequest) (*proto.ReportJobErrorReply, error) {
	return &proto.ReportJobErrorReply{}, nil
}

func (s *mockGCSServer) GcsSubscriberCommandBatch(ctx context.Context, req *proto.GcsSubscriberCommandBatchRequest) (*proto.GcsSubscriberCommandBatchReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subChannel == nil {
		s.subChannel = map[string]proto.ChannelType{}
	}
	for _, cmd := range req.Commands {
		if cmd.GetSubscribeMessage() != nil {
			s.subChannel[string(req.SubscriberId)] = cmd.ChannelType
		}
	}
	return &proto.GcsSubscriberCommandBatchReply{}, nil
}

func (s *mockGCSServer) GcsSubscriberPoll(ctx context.Context, req *proto.GcsSubscriberPollRequest) (*proto.GcsSubscriberPollReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reply := &proto.GcsSubscriberPollReply{PublisherId: s.publisher}
	ch := s.subChannel[string(req.SubscriberId)]
	if q := s.queues[ch]; len(q) > 0 {
		reply.PubMessages = q[0]
		s.queues[ch] = q[1:]
	}
	return reply, nil
}

// InternalKVDel records the request and fails for the pairs listed in
// kvDelFail, so the caller's "keep going after an error" path can be tested.
func (s *mockGCSServer) InternalKVDel(ctx context.Context, req *proto.InternalKVDelRequest) (*proto.InternalKVDelReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(req.Namespace) + "/" + string(req.Key)
	s.kvDelCalls = append(s.kvDelCalls, key)
	if s.kvDelFail[key] {
		return nil, errors.New("injected kv del failure")
	}
	return &proto.InternalKVDelReply{DeletedNum: 1}, nil
}

// delCalls returns a copy of the recorded InternalKVDel namespace/key pairs.
func (s *mockGCSServer) delCalls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.kvDelCalls...)
}

// enqueue sets the messages returned by the next poll of the given channel.
func (s *mockGCSServer) enqueue(ch proto.ChannelType, msgs ...*proto.PubMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queues == nil {
		s.queues = map[proto.ChannelType][][]*proto.PubMessage{}
	}
	s.queues[ch] = append(s.queues[ch], msgs)
}

// startMock starts an in-memory gRPC server serving both the info and pubsub
// services and returns a connected GCSClient plus a stop function.
func startMock(t *testing.T, mock *mockGCSServer) (*head.GCSClient, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	proto.RegisterActorInfoGcsServiceServer(srv, mock)
	proto.RegisterNodeInfoGcsServiceServer(srv, mock)
	proto.RegisterControlPlanePubSubGcsServiceServer(srv, mock)
	proto.RegisterObservabilityPubSubServiceServer(srv, mock)
	proto.RegisterInternalKVGcsServiceServer(srv, mock)
	go func() { _ = srv.Serve(ln) }()
	client, err := head.NewGCSClient(context.Background(), ln.Addr().String(), head.WithToken("test-token"))
	if err != nil {
		srv.Stop()
		t.Fatal(err)
	}
	return client, func() {
		srv.Stop()
		_ = client.Close()
	}
}

// waitFor polls cond until it returns true or times out.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestDataSourceInitFromGCS(t *testing.T) {
	mock := &mockGCSServer{
		nodeInfo: []*proto.GcsNodeInfo{
			{NodeId: []byte{0x01}, NodeManagerHostname: "node-a", NodeManagerAddress: "1.2.3.4", State: proto.GcsNodeInfo_ALIVE},
		},
		actorInfo: []*proto.ActorTableData{
			{ActorId: []byte{0x02}, JobId: []byte{0x03}, Name: "actor-a", State: proto.ActorTableData_ALIVE, ClassName: "A"},
		},
	}
	client, stop := startMock(t, mock)
	defer stop()

	ds := NewDataSource(client, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ds.Start(ctx); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	if !waitFor(t, 3*time.Second, func() bool { return len(ds.Nodes()) == 1 }) {
		t.Fatalf("Nodes() = %d, want 1 after init", len(ds.Nodes()))
	}
	nodes := ds.Nodes()
	if hex.EncodeToString(nodes[0].NodeId) != "01" {
		t.Fatalf("node id = %s, want 01", hex.EncodeToString(nodes[0].NodeId))
	}
	if nodes[0].NodeManagerHostname != "node-a" {
		t.Fatalf("hostname = %q, want node-a", nodes[0].NodeManagerHostname)
	}

	if !waitFor(t, 3*time.Second, func() bool { return len(ds.Actors()) == 1 }) {
		t.Fatalf("Actors() = %d, want 1 after init", len(ds.Actors()))
	}
	actors := ds.Actors()
	if hex.EncodeToString(actors[0].ActorId) != "02" {
		t.Fatalf("actor id = %s, want 02", hex.EncodeToString(actors[0].ActorId))
	}
}

func TestDataSourceSubscriptionRefresh(t *testing.T) {
	mock := &mockGCSServer{publisher: []byte("publisher-a")}
	client, stop := startMock(t, mock)
	defer stop()

	ds := NewDataSource(client,
		head.NewNodeInfoSubscriber(client),
		head.NewActorSubscriber(client),
		head.NewResourceUsageSubscriber(client),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ds.Start(ctx); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	// A node update is polled and upserted into the cache. The address the
	// agent registers its ip key under travels in NodeManagerAddress; the
	// DEAD update that follows drops the contact fields, and the cleanup
	// falls back to this cached address.
	nodeID := []byte{0x10}
	mock.enqueue(proto.ChannelType_GCS_NODE_INFO_CHANNEL, &proto.PubMessage{
		KeyId:      nodeID,
		SequenceId: 1,
		InnerMessage: &proto.PubMessage_NodeInfoMessage{
			NodeInfoMessage: &proto.GcsNodeInfo{NodeId: nodeID, NodeManagerHostname: "node-b", NodeManagerAddress: "10.0.0.10", State: proto.GcsNodeInfo_ALIVE},
		},
	})

	if !waitFor(t, 3*time.Second, func() bool {
		n := ds.Node("10")
		return n != nil && n.NodeManagerHostname == "node-b"
	}) {
		t.Fatal("node update was not applied to the cache")
	}

	// A DEAD node update is retained in the cache (aligned with Python, which
	// only evicts dead nodes once the queue exceeds MAX_DEAD_NODES_TO_CACHE).
	mock.enqueue(proto.ChannelType_GCS_NODE_INFO_CHANNEL, &proto.PubMessage{
		KeyId:      nodeID,
		SequenceId: 2,
		InnerMessage: &proto.PubMessage_NodeInfoMessage{
			NodeInfoMessage: &proto.GcsNodeInfo{NodeId: nodeID, State: proto.GcsNodeInfo_DEAD},
		},
	})

	if !waitFor(t, 3*time.Second, func() bool {
		n := ds.Node("10")
		return n != nil && n.State == proto.GcsNodeInfo_DEAD
	}) {
		t.Fatal("DEAD node was not retained in the cache")
	}

	// The dead node's dashboard agent addresses are removed from GCS KV.
	if !waitFor(t, 3*time.Second, func() bool { return len(mock.delCalls()) == 2 }) {
		t.Fatalf("dashboard agent address deletions = %v, want both keys", mock.delCalls())
	}

	// An actor update is upserted into the cache.
	actorID := []byte{0x20}
	mock.enqueue(proto.ChannelType_GCS_ACTOR_CHANNEL, &proto.PubMessage{
		KeyId:      actorID,
		SequenceId: 3,
		InnerMessage: &proto.PubMessage_ActorMessage{
			ActorMessage: &proto.ActorTableData{ActorId: actorID, Name: "actor-c", State: proto.ActorTableData_ALIVE},
		},
	})

	if !waitFor(t, 3*time.Second, func() bool {
		a := ds.Actor("20")
		return a != nil && a.Name == "actor-c"
	}) {
		t.Fatal("actor update was not applied to the cache")
	}

	// A resource usage update parses its JSON into the physical stats cache.
	mock.enqueue(proto.ChannelType_RAY_NODE_RESOURCE_USAGE_CHANNEL, &proto.PubMessage{
		KeyId:      []byte("RAY_REPORTER:20"),
		SequenceId: 4,
		InnerMessage: &proto.PubMessage_NodeResourceUsageMessage{
			NodeResourceUsageMessage: &proto.NodeResourceUsage{Json: `{"cpu": 0.5, "hostname": "node-c"}`},
		},
	})

	if !waitFor(t, 3*time.Second, func() bool {
		stats := ds.NodePhysicalStats("20")
		return stats != nil && stats["cpu"] == 0.5
	}) {
		t.Fatal("resource usage update was not applied to the stats cache")
	}
}

func TestModuleInterfaceAndRoutes(t *testing.T) {
	mock := &mockGCSServer{
		nodeInfo: []*proto.GcsNodeInfo{
			{NodeId: []byte{0x01}, NodeManagerHostname: "node-a", NodeManagerAddress: "1.2.3.4", State: proto.GcsNodeInfo_ALIVE, IsHeadNode: true, ResourcesTotal: map[string]float64{"CPU": 2, "object_store_memory": 256}, StartTimeMs: 1789731584838},
			{NodeId: []byte{0x02}, NodeManagerHostname: "node-b", NodeManagerAddress: "5.6.7.8", State: proto.GcsNodeInfo_DEAD},
		},
		actorInfo: []*proto.ActorTableData{
			{
				ActorId:   []byte{0x03},
				JobId:     []byte{0x04},
				Name:      "actor-a",
				ClassName: "A",
				State:     proto.ActorTableData_ALIVE,
				Address:   &proto.Address{NodeId: []byte{0x01}, WorkerId: []byte{0x05}, IpAddress: "1.3.3.7", Port: 9001},
				// The internal-head per-node resource: /test/dump (RESTResponseCamel)
				// must camel-case it to node:InternalHead while /logical/actors
				// (RESTResponse, convert_google_style=False) must keep it as
				// node:__internal_head__, aligned with Python.
				RequiredResources: map[string]float64{"node:__internal_head__": 0.001},
			},
		},
	}
	client, stop := startMock(t, mock)
	defer stop()

	cfg := &head.HeadConfig{}
	m := New(cfg, client)
	if m.Name() != "NodeHead" {
		t.Fatalf("Name() = %q, want NodeHead", m.Name())
	}
	if !m.Healthy() {
		t.Fatal("module should be healthy")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	if !waitFor(t, 3*time.Second, func() bool { return len(m.ds.Nodes()) == 2 && len(m.ds.Actors()) == 1 }) {
		t.Fatal("DataSource did not initialize")
	}

	// Seed a core worker stat for the actor's worker (0x05) so actorLogical
	// merges the full core-worker-stats field set (Python behavior). The node
	// physical stats must list that worker as alive (pid 4242) for
	// rebuildCoreWorkerIndex to keep its stat, matching the Python
	// DataOrganizer.organize loop over node_physical_stats workers.
	m.ds.mu.Lock()
	m.ds.stats["01"] = map[string]interface{}{
		"workers": []interface{}{map[string]interface{}{"pid": float64(4242)}},
	}
	m.ds.nodeStats["01"] = &proto.GetNodeStatsReply{
		CoreWorkersStats: []*proto.CoreWorkerStats{testCoreWorkerStats(4242, []byte{0x05}, []byte{0x04}, []byte{0x03}, "1.3.3.7")},
		StoreStats:       &proto.ObjectStoreStats{ObjectStoreBytesUsed: 100, ObjectStoreBytesAvail: 500},
	}
	m.ds.rebuildCoreWorkerIndex()
	m.ds.mu.Unlock()

	mux := http.NewServeMux()
	if err := m.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}

	// /nodes (summary) returns both nodes under summary.
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/nodes?view=summary", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /nodes = %d, want 200", rr.Code)
	}
	data := decodeData(t, rr)
	summary := data["summary"].([]interface{})
	if len(summary) != 2 {
		t.Fatalf("summary length = %d, want 2", len(summary))
	}
	first := summary[0].(map[string]interface{})
	// The top level carries no nodeId/state (those live under raylet only),
	// aligned with Python get_node_info.
	if _, ok := first["state"]; ok {
		t.Fatalf("first node top-level state present, want only under raylet: %v", first)
	}
	if _, ok := first["nodeId"]; ok {
		t.Fatalf("first node top-level nodeId present, want only under raylet: %v", first)
	}
	raylet, ok := first["raylet"].(map[string]interface{})
	if !ok {
		t.Fatalf("first node raylet = %v", first["raylet"])
	}
	if raylet["state"] != "ALIVE" {
		t.Fatalf("first node raylet state = %v, want ALIVE", raylet["state"])
	}
	if raylet["nodeId"] != "01" {
		t.Fatalf("first node raylet nodeId = %v, want 01", raylet["nodeId"])
	}
	// The summary array is []interface{} so toGoogleStyle recurses into it and
	// camel-cases the nested resourcesTotal keys (object_store_memory ->
	// objectStoreMemory), matching the Python to_google_style. The mock node 01
	// has ResourcesTotal {CPU:2, object_store_memory:256}.
	srt, ok := raylet["resourcesTotal"].(map[string]interface{})
	if !ok {
		t.Fatalf("summary raylet resourcesTotal = %T, want map[string]interface{}", raylet["resourcesTotal"])
	}
	if v, ok := srt["objectStoreMemory"]; !ok || v != float64(256) {
		t.Fatalf("summary raylet resourcesTotal objectStoreMemory = %v, want 256", srt)
	}
	if _, hasSnake := srt["object_store_memory"]; hasSnake {
		t.Fatalf("summary raylet resourcesTotal leaked snake_case key: %v", srt)
	}
	if first["hostname"] != "node-a" {
		t.Fatalf("first node hostname = %v, want node-a", first["hostname"])
	}
	// The node logical resources key is google style (nodeLogicalResources),
	// as the frontend useNodeList reads it.
	if _, ok := data["nodeLogicalResources"]; !ok {
		t.Fatalf("nodeLogicalResources key missing in data: %v", data)
	}

	// /nodes (hostNameList) returns only alive hostnames, case-insensitive.
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/nodes?view=hostnamelist", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /nodes hostNameList = %d, want 200", rr.Code)
	}
	hosts := decodeData(t, rr)["hostNameList"].([]interface{})
	if len(hosts) != 1 || hosts[0] != "node-a" {
		t.Fatalf("hostNameList = %v, want [node-a]", hosts)
	}

	// Unknown view returns 500 (aligned with Python).
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/nodes?view=unknown", nil))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("GET /nodes view=unknown = %d, want 500", rr.Code)
	}

	// /nodes/{node_id} returns the matching node detail.
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/nodes/01", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /nodes/01 = %d, want 200", rr.Code)
	}
	detail := decodeData(t, rr)["detail"].(map[string]interface{})
	// The top-level detail carries no nodeId/state (they live under raylet),
	// aligned with Python get_node_info.
	if _, ok := detail["nodeId"]; ok {
		t.Fatalf("detail top-level nodeId present, want only under raylet: %v", detail)
	}
	dRaylet, ok := detail["raylet"].(map[string]interface{})
	if !ok {
		t.Fatalf("detail raylet = %v", detail["raylet"])
	}
	if dRaylet["nodeId"] != "01" {
		t.Fatalf("detail raylet nodeId = %v, want 01", dRaylet["nodeId"])
	}
	// The response-level resourcesTotal keys are camelCase (object_store_memory
	// -> objectStoreMemory), matching the Python to_google_style applied by
	// rest_response.
	rt, ok := dRaylet["resourcesTotal"].(map[string]interface{})
	if !ok {
		t.Fatalf("detail raylet resourcesTotal = %T, want map[string]interface{}", dRaylet["resourcesTotal"])
	}
	if _, hasSnake := rt["object_store_memory"]; hasSnake {
		t.Fatalf("detail raylet resourcesTotal leaked snake_case key: %v", rt)
	}
	if v, ok := rt["objectStoreMemory"]; !ok || v != float64(256) {
		t.Fatalf("detail raylet resourcesTotal objectStoreMemory = %v, want 256", rt)
	}
	if v, ok := rt["CPU"]; !ok || v != float64(2) {
		t.Fatalf("detail raylet resourcesTotal CPU = %v, want 2", rt)
	}
	// The uint64 time fields serialize as strings at the response level too.
	if dRaylet["startTimeMs"] != "1789731584838" {
		t.Fatalf("detail raylet startTimeMs = %v, want string 1789731584838", dRaylet["startTimeMs"])
	}
	if dRaylet["endTimeMs"] != "0" {
		t.Fatalf("detail raylet endTimeMs = %v, want string 0", dRaylet["endTimeMs"])
	}
	// The detail view carries the per-node actors (aligned with Python
	// get_node_info with get_summary=False).
	nodeActors, ok := detail["actors"].(map[string]interface{})
	if !ok {
		t.Fatalf("detail actors = %v, want a dict", detail["actors"])
	}
	if len(nodeActors) != 1 {
		t.Fatalf("detail actors length = %d, want 1", len(nodeActors))
	}
	if actor, ok := nodeActors["03"].(map[string]interface{}); !ok || actor["actorId"] != "03" {
		t.Fatalf("detail actors[03] = %v", nodeActors["03"])
	}

	// A dead node's stateMessage is composed from its DeathInfo.
	deadNode := m.ds.Node("02")
	deadNode.DeathInfo = &proto.NodeDeathInfo{
		Reason:        proto.NodeDeathInfo_EXPECTED_TERMINATION,
		ReasonMessage: "shutdown complete",
	}
	if msg := composeStateMessage(deadNode); msg != "Expected termination: shutdown complete" {
		t.Fatalf("stateMessage = %v, want %q", msg, "Expected termination: shutdown complete")
	}

	// Unknown node id returns 200 with the aligned Python get_node_info shape:
	// raylet carries the zero object-store fields and a nil stateMessage, and
	// the detail-only actors/workers are empty.
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/nodes/ff", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /nodes/ff = %d, want 200", rr.Code)
	}
	missingDetail := decodeData(t, rr)["detail"].(map[string]interface{})
	if missingRaylet, ok := missingDetail["raylet"].(map[string]interface{}); !ok {
		t.Fatalf("missing node detail raylet = %v, want a dict", missingDetail["raylet"])
	} else {
		if missingRaylet["objectStoreUsedMemory"] != float64(0) {
			t.Fatalf("missing node objectStoreUsedMemory = %v, want 0", missingRaylet["objectStoreUsedMemory"])
		}
		if missingRaylet["objectStoreAvailableMemory"] != float64(0) {
			t.Fatalf("missing node objectStoreAvailableMemory = %v, want 0", missingRaylet["objectStoreAvailableMemory"])
		}
		if v, present := missingRaylet["stateMessage"]; !present || v != nil {
			t.Fatalf("missing node stateMessage = %v, want null", missingRaylet["stateMessage"])
		}
	}
	if actors, ok := missingDetail["actors"].(map[string]interface{}); !ok || len(actors) != 0 {
		t.Fatalf("missing node detail actors = %v, want empty dict", missingDetail["actors"])
	}
	if workers, ok := missingDetail["workers"].([]interface{}); !ok || len(workers) != 0 {
		t.Fatalf("missing node detail workers = %v, want empty list", missingDetail["workers"])
	}

	// /logical/actors returns a dict keyed by hex actor id under data.actors
	// (aligned with Python get_all_actors, consumed by useActorList).
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/logical/actors", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /logical/actors = %d, want 200", rr.Code)
	}
	actors, ok := decodeData(t, rr)["actors"].(map[string]interface{})
	if !ok {
		t.Fatalf("data.actors is not a dict: %v", decodeData(t, rr)["actors"])
	}
	if len(actors) != 1 {
		t.Fatalf("actors length = %d, want 1", len(actors))
	}
	entry, ok := actors["03"].(map[string]interface{})
	if !ok {
		t.Fatalf("actors[03] is not an object: %v", actors["03"])
	}
	if entry["actorId"] != "03" {
		t.Fatalf("actorId = %v, want 03", entry["actorId"])
	}
	if entry["actorClass"] != "A" {
		t.Fatalf("actorClass = %v, want A", entry["actorClass"])
	}
	// labelSelector is an object even when the proto map is empty, matching the
	// Python dict(message.label_selector) == {} (not null).
	if ls, ok := entry["labelSelector"].(map[string]interface{}); !ok || len(ls) != 0 {
		t.Fatalf("actor labelSelector = %v, want empty dict", entry["labelSelector"])
	}
	// The actor carries the Python _get_actor_info field set: the light actor
	// fields plus the core-worker-stats derived fields and
	// gpus/processStats/mem/requiredResources.
	for _, f := range []string{
		"actorClass", "actorId", "address", "className", "endTime", "exitDetail",
		"gpus", "ipAddress", "jobId", "labelSelector", "language", "mem", "name",
		"numExecutedTasks", "numInFlightArgPinningRequests", "numInPlasma",
		"numLocalObjects", "numObjectRefsInScope", "numOfFailedArgPinningRequests",
		"numOwnedActors", "numOwnedObjects", "numPendingTasks", "numRunningTasks",
		"objectRefs", "objectsTotal", "pid", "port", "processStats", "reprName",
		"requiredResources", "startTime", "state", "taskQueueLength", "timestamp",
		"usedObjectStoreMemory", "usedResources", "workerId",
		"workerType",
	} {
		if _, ok := entry[f]; !ok {
			t.Fatalf("actor entry missing field %s: %v", f, entry)
		}
	}
	// address sub-fields and requiredResources are present.
	addr, ok := entry["address"].(map[string]interface{})
	if !ok || addr["nodeId"] != "01" || addr["workerId"] != "05" || addr["ipAddress"] != "1.3.3.7" {
		t.Fatalf("actor address = %v", entry["address"])
	}
	if _, ok := entry["requiredResources"].(map[string]interface{}); !ok {
		t.Fatalf("actor requiredResources = %v", entry["requiredResources"])
	}
	// requiredResources keeps its raw keys under /logical/actors (RESTResponse,
	// convert_google_style=False in Python), so node:__internal_head__ must NOT
	// be camel-cased.
	if rrMap, ok := entry["requiredResources"].(map[string]interface{}); ok {
		if _, has := rrMap["node:__internal_head__"]; !has {
			t.Fatalf("actor requiredResources keys = %v, want node:__internal_head__", rrMap)
		}
		if _, has := rrMap["node:InternalHead"]; has {
			t.Fatalf("actor requiredResources should not be camel-cased under /logical/actors: %v", rrMap)
		}
	}

	// /logical/actors/{actor_id} returns the matching actor.
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/logical/actors/03", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /logical/actors/03 = %d, want 200", rr.Code)
	}
	actorDetail := decodeData(t, rr)["detail"].(map[string]interface{})
	if actorDetail["actorId"] != "03" {
		t.Fatalf("detail actorId = %v, want 03", actorDetail["actorId"])
	}

	// Unknown actor id returns 200 with a null detail (aligned with Python,
	// which looks the id up in the cache and returns it as-is).
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/logical/actors/ff", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /logical/actors/ff = %d, want 200", rr.Code)
	}
	if detail, present := decodeData(t, rr)["detail"]; !present || detail != nil {
		t.Fatalf("GET /logical/actors/ff detail = %v, want null", detail)
	}

	// /test/dump dumps the cache. The dump goes through RESTResponseCamel, so
	// the actor requiredResources keys are camel-cased (node:__internal_head__ ->
	// node:InternalHead), aligned with Python's convert_google_style=True.
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/test/dump", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /test/dump = %d, want 200", rr.Code)
	}
	dumpData := decodeData(t, rr)
	dumpActors, ok := dumpData["actors"].(map[string]interface{})
	if !ok {
		t.Fatalf("dump actors = %v", dumpData["actors"])
	}
	dumpActor, ok := dumpActors["03"].(map[string]interface{})
	if !ok {
		t.Fatalf("dump actors[03] = %v", dumpActors["03"])
	}
	dumpReq, ok := dumpActor["requiredResources"].(map[string]interface{})
	if !ok {
		t.Fatalf("dump actor requiredResources = %T, want map[string]interface{}", dumpActor["requiredResources"])
	}
	if _, has := dumpReq["node:InternalHead"]; !has {
		t.Fatalf("dump actor requiredResources keys = %v, want node:InternalHead (camel-cased)", dumpReq)
	}
	if _, has := dumpReq["node:__internal_head__"]; has {
		t.Fatalf("dump actor requiredResources should be camel-cased: %v", dumpReq)
	}
}

// TestLogicalActorsIDSFilter verifies the ?ids=a,b query filtering: only the
// requested ids are returned as a dict, and ids missing from the cache map to
// null (aligned with Python DataOrganizer.get_actor_infos).
func TestLogicalActorsIDSFilter(t *testing.T) {
	mock := &mockGCSServer{
		actorInfo: []*proto.ActorTableData{
			{ActorId: []byte{0x01}, Name: "actor-a", ClassName: "A"},
			{ActorId: []byte{0x02}, Name: "actor-b", ClassName: "B"},
		},
	}
	client, stop := startMock(t, mock)
	defer stop()

	m := New(&head.HeadConfig{}, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
	if !waitFor(t, 3*time.Second, func() bool { return len(m.ds.Actors()) == 2 }) {
		t.Fatal("DataSource did not initialize actors")
	}

	mux := http.NewServeMux()
	if err := m.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}

	// Filter to one actor; the other id (missing) maps to null.
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/logical/actors?ids=01,ff", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /logical/actors?ids= = %d, want 200", rr.Code)
	}
	actors, ok := decodeData(t, rr)["actors"].(map[string]interface{})
	if !ok {
		t.Fatalf("data.actors is not a dict: %v", decodeData(t, rr)["actors"])
	}
	if len(actors) != 2 {
		t.Fatalf("filtered actors length = %d, want 2", len(actors))
	}
	entry, ok := actors["01"].(map[string]interface{})
	if !ok || entry["name"] != "actor-a" {
		t.Fatalf("actors[01] = %v, want actor-a", actors["01"])
	}
	if v, present := actors["ff"]; !present || v != nil {
		t.Fatalf("actors[ff] = %v, want null for missing id", actors["ff"])
	}
}

// TestNodePhysicalStatsCache verifies the physical stats cache write/read path
// (consumed by the /nodes/{id} detail merge in a later task).
func TestNodePhysicalStatsCache(t *testing.T) {
	mock := &mockGCSServer{publisher: []byte("publisher-a")}
	client, stop := startMock(t, mock)
	defer stop()

	ds := NewDataSource(client, nil, nil, head.NewResourceUsageSubscriber(client))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ds.Start(ctx); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	nodeID := "30"
	mock.enqueue(proto.ChannelType_RAY_NODE_RESOURCE_USAGE_CHANNEL, &proto.PubMessage{
		KeyId:      []byte("RAY_REPORTER:" + nodeID),
		SequenceId: 1,
		InnerMessage: &proto.PubMessage_NodeResourceUsageMessage{
			NodeResourceUsageMessage: &proto.NodeResourceUsage{Json: `{"cpu": 0.25, "mem": [100, 40, 0.4]}`},
		},
	})

	if !waitFor(t, 3*time.Second, func() bool {
		stats := ds.NodePhysicalStats(nodeID)
		return stats != nil && stats["cpu"] == 0.25
	}) {
		t.Fatal("physical stats were not cached")
	}
	mem := ds.NodePhysicalStats(nodeID)["mem"].([]interface{})
	if len(mem) != 3 || mem[0].(float64) != 100 {
		t.Fatalf("mem = %v, want [100 40 0.4]", mem)
	}
	// A node with no stats returns nil.
	if ds.NodePhysicalStats("99") != nil {
		t.Fatalf("NodePhysicalStats(99) = %v, want nil", ds.NodePhysicalStats("99"))
	}
}

// decodeData decodes the RESTResponse body and returns the "data" object.
func decodeData(t *testing.T, rr *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var body struct {
		Data map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid json body: %v", err)
	}
	return body.Data
}

// TestParseUsageLines pins parseUsageLines against the Python
// parse_usage(usage, verbose=True) formatting, including the str.title-style
// float rendering ("1.0/6.0 CPU", "0B/3.74GiB memory") used by
// /nodes?view=summary's nodeLogicalResources.
func TestParseUsageLines(t *testing.T) {
	usage := map[string][2]float64{
		"CPU":                 {1.0, 6.0},
		"memory":              {0.0, 4018289050.0},
		"object_store_memory": {1040.0, 1722123878.0},
		"node:10.56.181.240":  {1.0, 1.0},
	}
	lines := parseUsageLines(usage, true)
	joined := strings.Join(lines, "\n")
	want := "1.0/6.0 CPU\n0B/3.74GiB memory\n1.02KiB/1.60GiB object_store_memory"
	if joined != want {
		t.Fatalf("parseUsageLines = %q, want %q", joined, want)
	}
}

// TestNodeSummaryMergesPhysicalStats pins the nodeSummary merge: the reporter
// physical stats form the base (cpu/mem/bootTime/loadAvg from the resource
// usage cache) and the GcsNodeInfo fields are merged under raylet.
func TestNodeSummaryMergesPhysicalStats(t *testing.T) {
	mock := &mockGCSServer{publisher: []byte("publisher-b")}
	client, stop := startMock(t, mock)
	defer stop()

	ds := NewDataSource(client, nil, nil, head.NewResourceUsageSubscriber(client))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ds.Start(ctx); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	nodeID := "40"
	mock.enqueue(proto.ChannelType_RAY_NODE_RESOURCE_USAGE_CHANNEL, &proto.PubMessage{
		KeyId:      []byte("RAY_REPORTER:" + nodeID),
		SequenceId: 1,
		InnerMessage: &proto.PubMessage_NodeResourceUsageMessage{
			NodeResourceUsageMessage: &proto.NodeResourceUsage{Json: `{"cpu": 12.5, "cpus": [6, 3], "mem": [100, 40, 60.0], "bootTime": 1700000000, "loadAvg": [[1, 2, 3], [0.1, 0.2, 0.3]], "raylet": {"pid": 1234}}`},
		},
	})
	if !waitFor(t, 3*time.Second, func() bool {
		return ds.NodePhysicalStats(nodeID) != nil
	}) {
		t.Fatal("physical stats were not cached")
	}

	h := New(&head.HeadConfig{}, client)
	h.ds = ds
	info := &proto.GcsNodeInfo{
		NodeId:              []byte{0x40},
		NodeManagerAddress:  "10.0.0.40",
		NodeManagerPort:     12345,
		IsHeadNode:          true,
		State:               proto.GcsNodeInfo_ALIVE,
		RuntimeEnvAgentPort: 8266,
		ResourcesTotal: map[string]float64{
			"CPU":                    8,
			"memory":                 1024,
			"object_store_memory":    512,
			"node:__internal_head__": 1,
		},
		StartTimeMs: 1700000000,
		EndTimeMs:   0,
	}
	summary := h.nodeSummary(info, true)
	if summary["cpu"] != 12.5 {
		t.Fatalf("cpu = %v, want 12.5", summary["cpu"])
	}
	if summary["bootTime"] != float64(1700000000) {
		t.Fatalf("bootTime = %v", summary["bootTime"])
	}
	loadAvg, ok := summary["loadAvg"].([]interface{})
	if !ok || len(loadAvg) != 2 {
		t.Fatalf("loadAvg = %v", summary["loadAvg"])
	}
	// The physical-stats raylet (process monitoring fields such as pid) is
	// discarded: Python overwrites node_info["raylet"] with the node_stats dict.
	if _, hasPid := summary["raylet"].(map[string]interface{})["pid"]; hasPid {
		t.Fatalf("raylet should not carry physical-stats pid: %v", summary["raylet"])
	}
	// The top level carries no nodeId/state (they live under raylet only).
	if _, ok := summary["state"]; ok {
		t.Fatalf("summary top-level state present, want only under raylet: %v", summary)
	}
	if _, ok := summary["nodeId"]; ok {
		t.Fatalf("summary top-level nodeId present, want only under raylet: %v", summary)
	}
	// GcsNodeInfo merged into raylet.
	raylet, ok := summary["raylet"].(map[string]interface{})
	if !ok {
		t.Fatalf("raylet = %v", summary["raylet"])
	}
	if raylet["nodeId"] != "40" || raylet["nodeManagerPort"] != int32(12345) || raylet["isHeadNode"] != true {
		t.Fatalf("raylet merged fields = %v", raylet)
	}
	// The GcsNodeInfo raylet fields use the Python field names: startTimeMs /
	// endTimeMs (not startTime/endTime), plus runtimeEnvAgentPort /
	// resourcesTotal / deathInfo. The uint64 time fields serialize as strings,
	// matching Python message_to_dict (startTimeMs=1700000000 -> "1700000000",
	// endTimeMs=0 -> "0").
	if raylet["endTimeMs"] != "0" {
		t.Fatalf("raylet endTimeMs = %v, want string 0", raylet["endTimeMs"])
	}
	if raylet["startTimeMs"] != "1700000000" {
		t.Fatalf("raylet startTimeMs = %v, want string 1700000000", raylet["startTimeMs"])
	}
	if _, hasStart := raylet["startTime"]; hasStart {
		t.Fatalf("raylet should not carry startTime (Python uses startTimeMs): %v", raylet)
	}
	if _, hasTerm := raylet["terminateTime"]; hasTerm {
		t.Fatalf("raylet should not carry terminateTime: %v", raylet)
	}
	if raylet["runtimeEnvAgentPort"] != int32(8266) {
		t.Fatalf("raylet runtimeEnvAgentPort = %v, want 8266", raylet["runtimeEnvAgentPort"])
	}
	// resourcesTotal is a map[string]interface{} so the response's toGoogleStyle
	// recurses into it and camel-cases its keys; at this layer (before the REST
	// response conversion) the keys are still the raw snake_case proto names.
	if rt, ok := raylet["resourcesTotal"].(map[string]interface{}); !ok {
		t.Fatalf("raylet resourcesTotal = %T, want map[string]interface{}", raylet["resourcesTotal"])
	} else if rt["CPU"] != float64(8) {
		t.Fatalf("raylet resourcesTotal = %v", raylet["resourcesTotal"])
	}
	// Object store fields default to 0 when node_stats is unavailable.
	if raylet["objectStoreUsedMemory"] != 0 {
		t.Fatalf("objectStoreUsedMemory = %v", raylet["objectStoreUsedMemory"])
	}

	// A death info is reflected under raylet.deathInfo.
	dead := &proto.GcsNodeInfo{
		NodeId: []byte{0x41},
		State:  proto.GcsNodeInfo_DEAD,
		DeathInfo: &proto.NodeDeathInfo{
			Reason:        proto.NodeDeathInfo_EXPECTED_TERMINATION,
			ReasonMessage: "shutdown complete",
		},
	}
	dsum := h.nodeSummary(dead, true)
	draylet := dsum["raylet"].(map[string]interface{})
	di, ok := draylet["deathInfo"].(map[string]interface{})
	if !ok || di["reason"] != "EXPECTED_TERMINATION" || di["reasonMessage"] != "shutdown complete" {
		t.Fatalf("raylet deathInfo = %v", draylet["deathInfo"])
	}
	if draylet["stateMessage"] != "Expected termination: shutdown complete" {
		t.Fatalf("death node stateMessage = %v", draylet["stateMessage"])
	}
}

// TestNodeSummaryDoesNotMutateCache verifies that nodeSummary operates on a
// copy of the cached physical stats, so the summary view's delete("workers")
// and the detail view's worker merge do not leak into the DataSource cache.
// Regression: without the copy, /nodes?view=summary removed the workers key
// from the cache, and a subsequent /nodes/{id} detail (the cluster page node
// expand) rendered no workers until the reporter republished (intermittent
// "expand shows nothing" bug).
func TestNodeSummaryDoesNotMutateCache(t *testing.T) {
	ds := &DataSource{
		stats: map[string]map[string]interface{}{
			"40": {
				"cpu":     12.5,
				"workers": []interface{}{map[string]interface{}{"pid": float64(4242), "cmdline": []interface{}{"ray::Counter"}}},
			},
		},
	}
	h := New(&head.HeadConfig{}, nil)
	h.ds = ds
	info := &proto.GcsNodeInfo{
		NodeId:              []byte{0x40},
		NodeManagerAddress:  "10.0.0.40",
		NodeManagerHostname: "node40",
		NodeManagerPort:     12345,
		IsHeadNode:          true,
		State:               proto.GcsNodeInfo_ALIVE,
	}

	// The summary view must not remove workers from the cache.
	h.nodeSummary(info, true)
	cached := ds.NodePhysicalStats("40")
	if cached == nil {
		t.Fatal("physical stats were evicted")
	}
	if _, hasWorkers := cached["workers"]; !hasWorkers {
		t.Fatalf("summary view removed workers from the cache: %v", cached)
	}

	// The detail view must carry the cached workers, and the merge must not
	// mutate the cached worker map either.
	detail := h.nodeSummary(info, false)
	workers, ok := detail["workers"].([]interface{})
	if !ok || len(workers) != 1 {
		t.Fatalf("detail workers = %v, want 1", detail["workers"])
	}
	if _, hasWorkers := cached["workers"]; !hasWorkers {
		t.Fatalf("detail view removed workers from the cache: %v", cached)
	}
	cachedWorker := cached["workers"].([]interface{})[0].(map[string]interface{})
	if _, mutated := cachedWorker["coreWorkerStats"]; mutated {
		t.Fatalf("detail merge mutated the cached worker map: %v", cachedWorker)
	}
}

// testCoreWorkerStats builds a CoreWorkerStats with all fields the frontend
// consumes (workerId/jobId/actorId/pid/ipAddress/language).
func testCoreWorkerStats(pid uint32, workerID, jobID, actorID []byte, ip string) *proto.CoreWorkerStats {
	return &proto.CoreWorkerStats{
		Pid:       pid,
		WorkerId:  workerID,
		JobId:     jobID,
		ActorId:   actorID,
		IpAddress: ip,
		Language:  proto.Language_PYTHON,
	}
}

// TestMergeWorkerCoreStatsMatch verifies a worker whose pid matches a core
// worker stat gets coreWorkerStats (with hex workerId/jobId), language, and
// jobId attached, aligned with Python _extract_workers_for_node.
func TestMergeWorkerCoreStatsMatch(t *testing.T) {
	workers := []interface{}{
		map[string]interface{}{"pid": float64(4242), "cmdline": []interface{}{"ray::test"}},
	}
	stats := []*proto.CoreWorkerStats{
		testCoreWorkerStats(4242, []byte{0x01, 0x02}, []byte{0xaa, 0xbb}, []byte{0xcc}, "10.0.0.1"),
	}
	out := mergeWorkerCoreStats(workers, stats)
	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1", len(out))
	}
	w, ok := out[0].(map[string]interface{})
	if !ok {
		t.Fatalf("worker = %T, want map", out[0])
	}
	coreStats, ok := w["coreWorkerStats"].([]interface{})
	if !ok || len(coreStats) != 1 {
		t.Fatalf("coreWorkerStats = %v, want 1-element list", w["coreWorkerStats"])
	}
	cs, ok := coreStats[0].(map[string]interface{})
	if !ok {
		t.Fatalf("coreWorkerStats[0] = %T, want map", coreStats[0])
	}
	if cs["worker_id"] != "0102" {
		t.Fatalf("worker_id = %v, want 0102", cs["worker_id"])
	}
	if cs["job_id"] != "aabb" {
		t.Fatalf("job_id = %v, want aabb", cs["job_id"])
	}
	if cs["actor_id"] != "cc" {
		t.Fatalf("actor_id = %v, want cc", cs["actor_id"])
	}
	if cs["ip_address"] != "10.0.0.1" {
		t.Fatalf("ip_address = %v, want 10.0.0.1", cs["ip_address"])
	}
	if cs["language"] != "PYTHON" {
		t.Fatalf("language = %v, want PYTHON", cs["language"])
	}
	if cs["pid"] != float64(4242) {
		t.Fatalf("pid = %v, want 4242", cs["pid"])
	}
	// Worker-level fields from the merge.
	if w["language"] != "PYTHON" {
		t.Fatalf("worker language = %v, want PYTHON", w["language"])
	}
	if w["jobId"] != "aabb" {
		t.Fatalf("worker jobId = %v, want aabb", w["jobId"])
	}
}

// TestMergeWorkerCoreStatsNoMatch verifies a worker without a matching core
// worker stat gets an empty coreWorkerStats list (so the frontend can safely
// index it) plus the Python default language/jobId.
func TestMergeWorkerCoreStatsNoMatch(t *testing.T) {
	workers := []interface{}{
		map[string]interface{}{"pid": float64(9999)},
	}
	out := mergeWorkerCoreStats(workers, nil)
	w, ok := out[0].(map[string]interface{})
	if !ok {
		t.Fatalf("worker = %T, want map", out[0])
	}
	coreStats, ok := w["coreWorkerStats"].([]interface{})
	if !ok || len(coreStats) != 0 {
		t.Fatalf("coreWorkerStats = %v, want empty list", w["coreWorkerStats"])
	}
	if w["language"] != "PYTHON" {
		t.Fatalf("language = %v, want PYTHON", w["language"])
	}
	if w["jobId"] != "ffff" {
		t.Fatalf("jobId = %v, want ffff", w["jobId"])
	}
}

// TestMergeWorkerCoreStatsNoPid verifies a worker without a usable pid is left
// untouched (no panic, no merge), matching the Python path where only workers
// with pids are merged.
func TestMergeWorkerCoreStatsNoPid(t *testing.T) {
	workers := []interface{}{
		map[string]interface{}{"name": "no-pid"},
		"not-a-map",
	}
	out := mergeWorkerCoreStats(workers, []*proto.CoreWorkerStats{
		testCoreWorkerStats(4242, []byte{0x01}, []byte{0xaa}, []byte{0xcc}, "10.0.0.1"),
	})
	if len(out) != 2 {
		t.Fatalf("len(out) = %d, want 2", len(out))
	}
	w, ok := out[0].(map[string]interface{})
	if !ok {
		t.Fatalf("worker[0] = %T, want map", out[0])
	}
	if _, exists := w["coreWorkerStats"]; exists {
		t.Fatalf("no-pid worker got coreWorkerStats = %v", w["coreWorkerStats"])
	}
	if _, ok := out[1].(string); !ok {
		t.Fatalf("worker[1] = %T, want string passthrough", out[1])
	}
}

// TestCoreWorkerStatsToMapHex pins the serialized shape of a CoreWorkerStats:
// snake_case keys, byte fields as hex, language as its enum name string,
// aligned with Python message_to_dict(decode_keys, use_integers_for_enums=False).
func TestCoreWorkerStatsToMapHex(t *testing.T) {
	cs := testCoreWorkerStats(7, []byte{0xde, 0xad}, []byte{0xbe, 0xef}, []byte{0x12}, "10.0.0.7")
	m := coreWorkerStatsToMap(cs)
	if m["worker_id"] != "dead" {
		t.Fatalf("worker_id = %v, want dead", m["worker_id"])
	}
	if m["job_id"] != "beef" {
		t.Fatalf("job_id = %v, want beef", m["job_id"])
	}
	if m["actor_id"] != "12" {
		t.Fatalf("actor_id = %v, want 12", m["actor_id"])
	}
	if m["language"] != "PYTHON" {
		t.Fatalf("language = %v, want PYTHON", m["language"])
	}
	if m["ip_address"] != "10.0.0.7" {
		t.Fatalf("ip_address = %v, want 10.0.0.7", m["ip_address"])
	}
}

// TestDumpKeyFilter verifies the /test/dump key query parameter returns only the
// requested data source, and the full dump carries every data source key.
func TestDumpKeyFilter(t *testing.T) {
	mock := &mockGCSServer{
		nodeInfo: []*proto.GcsNodeInfo{
			{NodeId: []byte{0x01}, NodeManagerHostname: "node-a", State: proto.GcsNodeInfo_ALIVE},
		},
	}
	client, stop := startMock(t, mock)
	defer stop()

	m := New(&head.HeadConfig{}, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
	if !waitFor(t, 3*time.Second, func() bool { return len(m.ds.Nodes()) == 1 }) {
		t.Fatal("DataSource did not initialize nodes")
	}

	mux := http.NewServeMux()
	if err := m.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}

	// The full dump includes every data source key (google-style after the
	// RESTResponseCamel conversion).
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/test/dump", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /test/dump = %d, want 200", rr.Code)
	}
	data := decodeData(t, rr)
	for _, k := range []string{"nodeStats", "nodePhysicalStats", "actors", "nodes", "nodeWorkers", "nodeActors", "coreWorkerStats"} {
		if _, ok := data[k]; !ok {
			t.Fatalf("dump missing key %s: %v", k, data)
		}
	}
	nodes, ok := data["nodes"].(map[string]interface{})
	if !ok || nodes["01"] == nil {
		t.Fatalf("dump nodes = %v", data["nodes"])
	}

	// The key parameter returns only that data source.
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/test/dump?key=nodes", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /test/dump?key=nodes = %d, want 200", rr.Code)
	}
	filtered := decodeData(t, rr)
	if len(filtered) != 1 {
		t.Fatalf("key-filtered dump = %v, want exactly one key", filtered)
	}
	if _, ok := filtered["nodes"]; !ok {
		t.Fatalf("key=nodes did not return nodes: %v", filtered)
	}
}

// TestPurgeDeadNodeStats verifies purgeDeadNodeStats drops the per-node stats of
// DEAD nodes and keeps those of ALIVE nodes, aligned with Python DataOrganizer.purge.
func TestPurgeDeadNodeStats(t *testing.T) {
	ds := NewDataSource(nil, nil, nil, nil)
	ds.nodes["aa"] = &proto.GcsNodeInfo{NodeId: []byte{0xaa}, State: proto.GcsNodeInfo_ALIVE}
	ds.nodes["bb"] = &proto.GcsNodeInfo{NodeId: []byte{0xbb}, State: proto.GcsNodeInfo_DEAD}
	ds.nodeStats["aa"] = &proto.GetNodeStatsReply{}
	ds.nodeStats["bb"] = &proto.GetNodeStatsReply{CoreWorkersStats: []*proto.CoreWorkerStats{
		testCoreWorkerStats(1, []byte{0x01}, []byte{0xaa}, []byte{0x11}, "10.0.0.1"),
	}}
	ds.stats["aa"] = map[string]interface{}{"cpu": 0.5}
	ds.stats["bb"] = map[string]interface{}{"cpu": 0.1}

	ds.purgeDeadNodeStats()

	if _, ok := ds.nodeStats["bb"]; ok {
		t.Fatalf("nodeStats of DEAD node bb was not purged")
	}
	if _, ok := ds.stats["bb"]; ok {
		t.Fatalf("physical stats of DEAD node bb was not purged")
	}
	if ds.nodeStats["aa"] == nil || ds.stats["aa"] == nil {
		t.Fatalf("stats of ALIVE node aa were wrongly purged")
	}
	// The core worker index drops the purged worker (worker 01 belonged to bb).
	if ds.coreWorkerStats["01"] != nil {
		t.Fatalf("core worker index still holds worker of purged node: %v", ds.coreWorkerStats)
	}
}

// TestApplyActorUpdateReplacesInsteadOfMutating pins the copy-on-write cache
// update: an existing entry is replaced with a new pointer (HTTP handlers may
// hold and read the previously published pointer without d.mu) whose state
// columns come from the update while the identity columns are preserved, and
// the previously published record is never touched.
func TestApplyActorUpdateReplacesInsteadOfMutating(t *testing.T) {
	existing := &proto.ActorTableData{
		ActorId:     []byte{0x01, 0x02},
		Name:        "actor_a",
		ClassName:   "MyClass",
		State:       proto.ActorTableData_DEPENDENCIES_UNREADY,
		NumRestarts: 3,
	}
	id := hex.EncodeToString(existing.ActorId)
	d := &DataSource{actors: map[string]*proto.ActorTableData{id: existing}}

	update := &proto.ActorTableData{
		// The GCS publishes ALIVE/DEAD updates with the identity columns empty.
		ActorId:     append([]byte(nil), existing.ActorId...),
		State:       proto.ActorTableData_ALIVE,
		NumRestarts: existing.NumRestarts + 1,
	}
	d.applyActorUpdate(update)

	got := d.Actor(id)
	if got == existing {
		t.Fatalf("the cached entry was mutated in place instead of replaced")
	}
	if got.State != proto.ActorTableData_ALIVE || got.NumRestarts != 4 {
		t.Fatalf("state columns not updated: state=%v restarts=%d", got.State, got.NumRestarts)
	}
	if got.Name != "actor_a" || got.ClassName != "MyClass" {
		t.Fatalf("identity columns wiped: name=%q class=%q", got.Name, got.ClassName)
	}
	if existing.State != proto.ActorTableData_DEPENDENCIES_UNREADY || existing.NumRestarts != 3 {
		t.Fatalf("the previously published record was mutated: state=%v restarts=%d", existing.State, existing.NumRestarts)
	}

	// A brand-new actor id is inserted as-is (the insert path of the merge).
	fresh := &proto.ActorTableData{ActorId: []byte{0x03}, Name: "actor_b", State: proto.ActorTableData_ALIVE}
	d.applyActorUpdate(fresh)
	if got := d.Actor(hex.EncodeToString(fresh.ActorId)); got != fresh {
		t.Fatalf("new entry not inserted: got %+v", got)
	}
}

func TestRemoveAgentAddrDeadNode(t *testing.T) {
	mock := &mockGCSServer{}
	client, stop := startMock(t, mock)
	defer stop()

	ds := NewDataSource(client, nil, nil, nil)
	ds.removeAgentAddr(context.Background(), &proto.GcsNodeInfo{
		NodeId:             []byte{0xab, 0xcd},
		NodeManagerAddress: "1.2.3.4",
		State:              proto.GcsNodeInfo_DEAD,
	}, nil)

	want := []string{
		"dashboard/DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX:abcd",
		"dashboard/DASHBOARD_AGENT_ADDR_IP_PREFIX:1.2.3.4",
	}
	if got := mock.delCalls(); !slices.Equal(got, want) {
		t.Fatalf("InternalKVDel calls = %v, want %v", got, want)
	}
}

// A DEAD node update carries empty contact fields (the same GCS pattern as
// the actor-table updates), so the ip key must be built from the address the
// node last advertised while ALIVE.
func TestRemoveAgentAddrUsesPreviousAddress(t *testing.T) {
	mock := &mockGCSServer{}
	client, stop := startMock(t, mock)
	defer stop()

	ds := NewDataSource(client, nil, nil, nil)
	prev := &proto.GcsNodeInfo{
		NodeId:             []byte{0xab, 0xcd},
		NodeManagerAddress: "1.2.3.4",
		State:              proto.GcsNodeInfo_ALIVE,
	}
	ds.removeAgentAddr(context.Background(), &proto.GcsNodeInfo{
		NodeId: []byte{0xab, 0xcd},
		State:  proto.GcsNodeInfo_DEAD,
	}, prev)

	want := []string{
		"dashboard/DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX:abcd",
		"dashboard/DASHBOARD_AGENT_ADDR_IP_PREFIX:1.2.3.4",
	}
	if got := mock.delCalls(); !slices.Equal(got, want) {
		t.Fatalf("InternalKVDel calls = %v, want %v", got, want)
	}
}

func TestRemoveAgentAddrNonDeadNode(t *testing.T) {
	mock := &mockGCSServer{}
	client, stop := startMock(t, mock)
	defer stop()

	ds := NewDataSource(client, nil, nil, nil)
	ds.removeAgentAddr(context.Background(), &proto.GcsNodeInfo{
		NodeId:             []byte{0xab, 0xcd},
		NodeManagerAddress: "1.2.3.4",
		State:              proto.GcsNodeInfo_ALIVE,
	}, nil)

	if got := mock.delCalls(); len(got) != 0 {
		t.Fatalf("InternalKVDel calls = %v, want none for a non-DEAD node", got)
	}
}

func TestRemoveAgentAddrNilClient(t *testing.T) {
	ds := NewDataSource(nil, nil, nil, nil)
	// A data source without a GCS client must not panic on node updates.
	ds.removeAgentAddr(context.Background(), &proto.GcsNodeInfo{
		NodeId: []byte{0xab, 0xcd},
		State:  proto.GcsNodeInfo_DEAD,
	}, nil)
}

func TestRemoveAgentAddrContinuesAfterFailure(t *testing.T) {
	mock := &mockGCSServer{kvDelFail: map[string]bool{
		"dashboard/DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX:abcd": true,
	}}
	client, stop := startMock(t, mock)
	defer stop()

	ds := NewDataSource(client, nil, nil, nil)
	ds.removeAgentAddr(context.Background(), &proto.GcsNodeInfo{
		NodeId:             []byte{0xab, 0xcd},
		NodeManagerAddress: "1.2.3.4",
		State:              proto.GcsNodeInfo_DEAD,
	}, nil)

	want := []string{
		"dashboard/DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX:abcd",
		"dashboard/DASHBOARD_AGENT_ADDR_IP_PREFIX:1.2.3.4",
	}
	if got := mock.delCalls(); !slices.Equal(got, want) {
		t.Fatalf("InternalKVDel calls = %v, want both keys even after the first fails", got)
	}
}

func TestDataSourceStartRemovesDeadAgentAddr(t *testing.T) {
	mock := &mockGCSServer{nodeInfo: []*proto.GcsNodeInfo{{
		NodeId:             []byte{0xab, 0xcd},
		NodeManagerAddress: "1.2.3.4",
		State:              proto.GcsNodeInfo_DEAD,
	}}}
	client, stop := startMock(t, mock)
	defer stop()

	ds := NewDataSource(client, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ds.Start(ctx); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	want := []string{
		"dashboard/DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX:abcd",
		"dashboard/DASHBOARD_AGENT_ADDR_IP_PREFIX:1.2.3.4",
	}
	if !waitFor(t, 3*time.Second, func() bool { return slices.Equal(mock.delCalls(), want) }) {
		t.Fatalf("InternalKVDel calls = %v, want %v", mock.delCalls(), want)
	}
}

// stubAutoscaler serves GetClusterStatus with an empty reply.
type stubAutoscaler struct {
	proto.UnimplementedAutoscalerStateServiceServer
}

func (*stubAutoscaler) GetClusterStatus(context.Context, *proto.GetClusterStatusRequest) (*proto.GetClusterStatusReply, error) {
	return &proto.GetClusterStatusReply{}, nil
}

// requireBearer rejects requests whose authorization metadata does not carry
// the expected bearer token, which is how a token-enforcing Ray server answers
// under RAY_AUTH_MODE=token (src/ray/rpc/server_call.h enforces the token; the
// Bearer header format and comparison come from
// src/ray/rpc/authentication/authentication_token.h, mirrored by
// go/pkg/gcs/dial.go). The expected value is read per request through the
// atomic pointer so a test can rotate it while the server keeps serving.
func requireBearer(expected *atomic.Pointer[string]) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if vals := md.Get("authorization"); len(vals) == 0 || vals[0] != *expected.Load() {
			return nil, status.Error(codes.Unauthenticated, "Invalid or missing authentication token")
		}
		return handler(ctx, req)
	}
}

// TestNodeHeadFetchClusterStatusPresentsTheClusterToken mirrors the reporter
// side for NodeHead: the fetch must present the cluster token under token
// mode, a rotated token must be picked up by the next fetch, and a wrong token
// must degrade to nil exactly like a failed dial.
func TestNodeHeadFetchClusterStatusPresentsTheClusterToken(t *testing.T) {
	t.Setenv("RAY_USE_TLS", "0")
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret")

	secret := "Bearer secret"
	var expected atomic.Pointer[string]
	expected.Store(&secret)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(requireBearer(&expected)))
	proto.RegisterAutoscalerStateServiceServer(srv, &stubAutoscaler{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	// A bounded context makes an unreachable server fail the test with a
	// message instead of hanging until the bazel test timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	n := &NodeHead{cfg: &head.HeadConfig{GCSAddress: lis.Addr().String()}}
	if reply := n.fetchClusterStatus(ctx); reply == nil {
		t.Fatal("fetchClusterStatus returned nil with the right token: the dial did not authenticate")
	}

	// A rotated token must be picked up by the next fetch.
	t.Setenv("RAY_AUTH_TOKEN", "rotated")
	rotated := "Bearer rotated"
	expected.Store(&rotated)

	if reply := n.fetchClusterStatus(ctx); reply == nil {
		t.Fatal("fetchClusterStatus returned nil after the token rotated: the dial did not re-resolve the token")
	}

	// A wrong token must be rejected — this guards against an interceptor
	// that accepts anything.
	t.Setenv("RAY_AUTH_TOKEN", "other")
	if reply := n.fetchClusterStatus(ctx); reply != nil {
		t.Fatal("fetchClusterStatus succeeded with a wrong token; the test server's guard is not actually checking")
	}
}

// stubNodeManagerStats answers GetNodeStats with a fixed worker count so a
// token-guarded node manager client has a call to make.
type stubNodeManagerStats struct {
	proto.UnimplementedNodeManagerServiceServer
}

func (*stubNodeManagerStats) GetNodeStats(context.Context, *proto.GetNodeStatsRequest) (*proto.GetNodeStatsReply, error) {
	return &proto.GetNodeStatsReply{NumWorkers: 3}, nil
}

// TestFetchNodeStatsPresentsTheClusterToken proves the per-node stats dial
// authenticates: under token mode a bare dial gets Unauthenticated, so worker
// stats would silently disappear from the node detail. Rotating the token
// between two calls pins the per-dial resolution.
func TestFetchNodeStatsPresentsTheClusterToken(t *testing.T) {
	t.Setenv("RAY_USE_TLS", "0")
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret")

	secret := "Bearer secret"
	var expected atomic.Pointer[string]
	expected.Store(&secret)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(requireBearer(&expected)))
	proto.RegisterNodeManagerServiceServer(srv, &stubNodeManagerStats{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	// A bounded context makes an unreachable raylet fail the test with a
	// message instead of hanging until the bazel test timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nodeInfo := &proto.GcsNodeInfo{
		NodeManagerAddress: "127.0.0.1",
		NodeManagerPort:    int32(lis.Addr().(*net.TCPAddr).Port),
	}

	reply, err := fetchNodeStats(ctx, nodeInfo)
	if err != nil {
		t.Fatalf("fetchNodeStats with token mode on: %v", err)
	}
	if got := reply.GetNumWorkers(); got != 3 {
		t.Fatalf("GetNodeStats num workers = %d, want 3", got)
	}

	// A rotated projected token must be picked up by the next dial.
	t.Setenv("RAY_AUTH_TOKEN", "rotated")
	rotated := "Bearer rotated"
	expected.Store(&rotated)

	reply, err = fetchNodeStats(ctx, nodeInfo)
	if err != nil {
		t.Fatalf("fetchNodeStats after the token rotated: %v", err)
	}
	if got := reply.GetNumWorkers(); got != 3 {
		t.Fatalf("GetNodeStats num workers after the token rotated = %d, want 3", got)
	}
}
