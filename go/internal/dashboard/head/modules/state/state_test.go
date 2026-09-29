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

package state

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
)

// mockGCSServer simulates the GCS info services with one sample entry each.
type mockGCSServer struct {
	proto.UnimplementedActorInfoGcsServiceServer
	proto.UnimplementedNodeInfoGcsServiceServer
	proto.UnimplementedWorkerInfoGcsServiceServer
	proto.UnimplementedJobInfoGcsServiceServer
	proto.UnimplementedPlacementGroupInfoGcsServiceServer
	proto.UnimplementedTaskInfoGcsServiceServer
	proto.UnimplementedInternalKVGcsServiceServer
	// actorErr, when non-nil, is returned by GetAllActorInfo to simulate a GCS
	// data source failure.
	actorErr error
}

// InternalKVKeys returns no keys so that the injected jobs list path (which
// lists submission jobs from the KV) falls through to the driver jobs.
func (s *mockGCSServer) InternalKVKeys(ctx context.Context, req *proto.InternalKVKeysRequest) (*proto.InternalKVKeysReply, error) {
	return &proto.InternalKVKeysReply{}, nil
}

func (s *mockGCSServer) GetAllActorInfo(ctx context.Context, req *proto.GetAllActorInfoRequest) (*proto.GetAllActorInfoReply, error) {
	if s.actorErr != nil {
		return nil, s.actorErr
	}
	return &proto.GetAllActorInfoReply{
		ActorTableData: []*proto.ActorTableData{
			{ActorId: []byte{0x01}, JobId: []byte{0x02}, Name: "actor-a"},
		},
		Total:       1,
		NumFiltered: 0,
	}, nil
}

func (s *mockGCSServer) GetAllNodeInfo(ctx context.Context, req *proto.GetAllNodeInfoRequest) (*proto.GetAllNodeInfoReply, error) {
	return &proto.GetAllNodeInfoReply{
		NodeInfoList: []*proto.GcsNodeInfo{
			{
				NodeId:             []byte{0x03},
				NodeManagerAddress: "1.2.3.4",
				StartTimeMs:        1000,
				DeathInfo: &proto.NodeDeathInfo{
					Reason:        proto.NodeDeathInfo_EXPECTED_TERMINATION,
					ReasonMessage: "drained",
				},
			},
		},
		Total:       1,
		NumFiltered: 0,
	}, nil
}

func (s *mockGCSServer) GetAllWorkerInfo(ctx context.Context, req *proto.GetAllWorkerInfoRequest) (*proto.GetAllWorkerInfoReply, error) {
	return &proto.GetAllWorkerInfoReply{
		WorkerTableData: []*proto.WorkerTableData{
			{
				WorkerAddress:        &proto.Address{NodeId: []byte{0x04}, WorkerId: []byte{0x05}, IpAddress: "1.2.3.4"},
				StartTimeMs:          1000,
				EndTimeMs:            2000,
				WorkerLaunchTimeMs:   500,
				WorkerLaunchedTimeMs: 600,
			},
		},
		Total:       1,
		NumFiltered: 0,
	}, nil
}

func (s *mockGCSServer) GetAllJobInfo(ctx context.Context, req *proto.GetAllJobInfoRequest) (*proto.GetAllJobInfoReply, error) {
	return &proto.GetAllJobInfoReply{
		JobInfoList: []*proto.JobTableData{
			{
				JobId:      []byte{0x06},
				Entrypoint: "python main.py",
				Config: &proto.JobConfig{
					RayNamespace: "default",
					Metadata:     map[string]string{},
				},
			},
		},
	}, nil
}

func (s *mockGCSServer) GetAllPlacementGroup(ctx context.Context, req *proto.GetAllPlacementGroupRequest) (*proto.GetAllPlacementGroupReply, error) {
	return &proto.GetAllPlacementGroupReply{
		PlacementGroupTableData: []*proto.PlacementGroupTableData{
			{PlacementGroupId: []byte{0x07}, Name: "pg-a"},
		},
		Total: 1,
	}, nil
}

func (s *mockGCSServer) GetTaskEvents(ctx context.Context, req *proto.GetTaskEventsRequest) (*proto.GetTaskEventsReply, error) {
	pid := int32(4242)
	return &proto.GetTaskEventsReply{
		EventsByTask: []*proto.TaskEvents{
			{
				TaskId:        []byte{0x08},
				AttemptNumber: 1,
				JobId:         []byte{0x06},
				TaskInfo: &proto.TaskInfoEntry{
					Type:            proto.TaskType_NORMAL_TASK,
					Name:            "f",
					FuncOrClassName: "f",
					Language:        proto.Language_PYTHON,
					TaskId:          []byte{0x08},
				},
				StateUpdates: &proto.TaskStateUpdate{
					NodeId:    []byte{0x09},
					WorkerId:  []byte{0x0a},
					WorkerPid: &pid,
					StateTsNs: map[int32]int64{
						int32(proto.TaskStatus_PENDING_ARGS_AVAIL): 1000000000,
						int32(proto.TaskStatus_FINISHED):           3000000000,
					},
				},
			},
		},
		NumTotalStored: 1,
	}, nil
}

// newMockClient spins up an in-process gRPC server and returns a GCSClient
// connected to it plus the listener for cleanup.
func newMockClient(t *testing.T) (*head.GCSClient, *grpc.Server, net.Listener) {
	t.Helper()
	return newMockClientWithMock(t, &mockGCSServer{})
}

// newMockClientWithMock is newMockClient with an injectable mock, used to
// simulate data source failures.
func newMockClientWithMock(t *testing.T, mock *mockGCSServer) (*head.GCSClient, *grpc.Server, net.Listener) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	proto.RegisterActorInfoGcsServiceServer(srv, mock)
	proto.RegisterNodeInfoGcsServiceServer(srv, mock)
	proto.RegisterWorkerInfoGcsServiceServer(srv, mock)
	proto.RegisterJobInfoGcsServiceServer(srv, mock)
	proto.RegisterPlacementGroupInfoGcsServiceServer(srv, mock)
	proto.RegisterTaskInfoGcsServiceServer(srv, mock)
	proto.RegisterInternalKVGcsServiceServer(srv, mock)
	go srv.Serve(ln)
	client, err := head.NewGCSClient(context.Background(), ln.Addr().String(),
		head.WithToken("test-token"))
	if err != nil {
		t.Fatal(err)
	}
	return client, srv, ln
}

func TestName(t *testing.T) {
	s := New(&head.HeadConfig{}, nil)
	if s.Name() != "StateHead" {
		t.Fatalf("name = %q, want StateHead", s.Name())
	}
	if !s.Healthy() {
		t.Fatal("module should be healthy")
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
}

func TestRegisterHTTPRoutes(t *testing.T) {
	client, srv, ln := newMockClient(t)
	defer ln.Close()
	defer srv.Stop()
	defer client.Close()

	s := New(&head.HeadConfig{}, client)
	mux := http.NewServeMux()
	if err := s.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	routes := []string{
		"/api/v0/actors",
		"/api/v0/jobs",
		"/api/v0/nodes",
		"/api/v0/placement_groups",
		"/api/v0/workers",
		"/api/v0/tasks",
		"/api/v0/objects",
		"/api/v0/runtime_envs",
	}
	for _, path := range routes {
		req := httptest.NewRequest("GET", path, nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", path, rr.Code)
		}
		var body map[string]interface{}
		if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
			t.Fatalf("GET %s: invalid json: %v", path, err)
		}
		if body["result"] != true {
			t.Fatalf("GET %s: result = %v, want true", path, body["result"])
		}
		data, ok := body["data"].(map[string]interface{})
		if !ok {
			t.Fatalf("GET %s: data not an object", path)
		}
		if _, ok := data["result"].(map[string]interface{}); !ok {
			t.Fatalf("GET %s: data.result is not a ListApiResponse", path)
		}
	}
	// The logs routes are mounted on the StateHead (Python mounts list_logs /
	// get_logs on StateHead), so --modules-to-load=StateHead must still serve
	// them. A bare GET /api/v0/logs without node_id/node_ip returns 400 (not
	// 404), proving the route is registered.
	req := httptest.NewRequest("GET", "/api/v0/logs", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("GET /api/v0/logs = %d, want 400 (route must be registered on StateHead)", rr.Code)
	}
	req = httptest.NewRequest("GET", "/api/v0/logs/file", nil)
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code == http.StatusNotFound {
		t.Fatalf("GET /api/v0/logs/file = 404, route must be registered on StateHead")
	}
}

func TestListActorsReturnsHexActorID(t *testing.T) {
	client, srv, ln := newMockClient(t)
	defer ln.Close()
	defer srv.Stop()
	defer client.Close()

	s := New(&head.HeadConfig{}, client)
	mux := http.NewServeMux()
	if err := s.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/api/v0/actors", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		Data struct {
			Result map[string]interface{} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	// Every ListApiResponse key is present, matching asdict(ListApiResponse)
	// in python/ray/util/state/common.py (partial_failure_warning: "" and
	// warnings: null even when unset).
	for _, k := range []string{
		"total", "num_after_truncation", "num_filtered",
		"partial_failure_warning", "warnings", "result",
	} {
		if _, ok := body.Data.Result[k]; !ok {
			t.Fatalf("ListApiResponse missing key %q", k)
		}
	}
	entries, _ := body.Data.Result["result"].([]interface{})
	if len(entries) != 1 {
		t.Fatalf("got %d actors, want 1", len(entries))
	}
	entry := entries[0].(map[string]interface{})
	if entry["actor_id"] != "01" {
		t.Fatalf("actor_id = %v, want hex 01", entry["actor_id"])
	}
	if entry["job_id"] != "02" {
		t.Fatalf("job_id = %v, want hex 02", entry["job_id"])
	}
}

func TestInvalidLimitReturnsBadRequest(t *testing.T) {
	s := New(&head.HeadConfig{}, nil)
	mux := http.NewServeMux()
	if err := s.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/api/v0/actors?limit=abc", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	// Error responses carry data: {"result": null}, aligned with the Python
	// do_reply(result=None) in state_api_utils.py.
	var body struct {
		Data map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if v, ok := body.Data["result"]; !ok || v != nil {
		t.Fatalf("data.result = %v (present=%v), want null", v, ok)
	}
}

func TestListTasksReturnsFlattenedState(t *testing.T) {
	client, srv, ln := newMockClient(t)
	defer ln.Close()
	defer srv.Stop()
	defer client.Close()

	s := New(&head.HeadConfig{}, client)
	mux := http.NewServeMux()
	if err := s.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	// events is a detail-only column in the Python StateSchema, so request the
	// detail view to pin it.
	req := httptest.NewRequest("GET", "/api/v0/tasks?detail=true", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		Data struct {
			Result struct {
				Result []map[string]interface{} `json:"result"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Result.Result) != 1 {
		t.Fatalf("got %d tasks, want 1", len(body.Data.Result.Result))
	}
	entry := body.Data.Result.Result[0]
	// Flattened TaskState fields must be at the top level (filter/sort keys).
	if entry["task_id"] != "08" {
		t.Fatalf("task_id = %v, want 08", entry["task_id"])
	}
	if entry["job_id"] != "06" {
		t.Fatalf("job_id = %v, want 06", entry["job_id"])
	}
	if entry["type"] != "NORMAL_TASK" {
		t.Fatalf("type = %v, want NORMAL_TASK", entry["type"])
	}
	if entry["state"] != "FINISHED" {
		t.Fatalf("state = %v, want FINISHED", entry["state"])
	}
	if entry["worker_pid"].(float64) != 4242 {
		t.Fatalf("worker_pid = %v, want 4242", entry["worker_pid"])
	}
	if entry["node_id"] != "09" {
		t.Fatalf("node_id = %v, want 09", entry["node_id"])
	}
	events, ok := entry["events"].([]interface{})
	if !ok || len(events) != 2 {
		t.Fatalf("events = %v, want 2 entries", entry["events"])
	}
}

// TestListJobsReturnsJobDetails pins the injected jobs list: the response
// carries the JobDetails form (type/job_id/status/entrypoint) rather than the
// raw JobTableData proto conversion, matching get_job_info in
// python/ray/util/state/state_manager.py.
func TestListJobsReturnsJobDetails(t *testing.T) {
	client, srv, ln := newMockClient(t)
	defer ln.Close()
	defer srv.Stop()
	defer client.Close()

	s := New(&head.HeadConfig{}, client)
	mux := http.NewServeMux()
	if err := s.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/api/v0/jobs", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		Data struct {
			Result struct {
				Result []map[string]interface{} `json:"result"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Result.Result) != 1 {
		t.Fatalf("got %d jobs, want 1", len(body.Data.Result.Result))
	}
	entry := body.Data.Result.Result[0]
	if entry["type"] != "DRIVER" {
		t.Fatalf("type = %v, want DRIVER", entry["type"])
	}
	if entry["job_id"] != "06" {
		t.Fatalf("job_id = %v, want hex 06", entry["job_id"])
	}
	if entry["status"] != "RUNNING" {
		t.Fatalf("status = %v, want RUNNING", entry["status"])
	}
	if entry["entrypoint"] != "python main.py" {
		t.Fatalf("entrypoint = %v", entry["entrypoint"])
	}
}

// TestListWorkersFlattened pins the list_workers transform: worker_address
// sub-message fields surface at the top level and the uint64 time fields are
// converted to int.
func TestListWorkersFlattened(t *testing.T) {
	client, srv, ln := newMockClient(t)
	defer ln.Close()
	defer srv.Stop()
	defer client.Close()

	s := New(&head.HeadConfig{}, client)
	mux := http.NewServeMux()
	if err := s.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	// start_time_ms is a detail-only column in the Python StateSchema, so
	// request the detail view to pin it.
	req := httptest.NewRequest("GET", "/api/v0/workers?detail=true", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		Data struct {
			Result struct {
				Result []map[string]interface{} `json:"result"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Result.Result) != 1 {
		t.Fatalf("got %d workers, want 1", len(body.Data.Result.Result))
	}
	entry := body.Data.Result.Result[0]
	if entry["worker_id"] != "05" {
		t.Fatalf("worker_id = %v, want hex 05", entry["worker_id"])
	}
	if entry["node_id"] != "04" {
		t.Fatalf("node_id = %v, want hex 04", entry["node_id"])
	}
	if entry["ip"] != "1.2.3.4" {
		t.Fatalf("ip = %v, want 1.2.3.4", entry["ip"])
	}
	if v, ok := entry["start_time_ms"].(float64); !ok || v != 1000 {
		t.Fatalf("start_time_ms = %v (%T), want 1000", entry["start_time_ms"], entry["start_time_ms"])
	}
}

// TestListNodesStateMessage pins the list_nodes transform: state_message is
// composed from the death info and the time fields are converted to int.
func TestListNodesStateMessage(t *testing.T) {
	client, srv, ln := newMockClient(t)
	defer ln.Close()
	defer srv.Stop()
	defer client.Close()

	s := New(&head.HeadConfig{}, client)
	mux := http.NewServeMux()
	if err := s.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	// start_time_ms is a detail-only column in the Python StateSchema, so
	// request the detail view to pin it.
	req := httptest.NewRequest("GET", "/api/v0/nodes?detail=true", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		Data struct {
			Result struct {
				Result []map[string]interface{} `json:"result"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Result.Result) != 1 {
		t.Fatalf("got %d nodes, want 1", len(body.Data.Result.Result))
	}
	entry := body.Data.Result.Result[0]
	if entry["node_ip"] != "1.2.3.4" {
		t.Fatalf("node_ip = %v, want 1.2.3.4", entry["node_ip"])
	}
	if entry["state_message"] != "Expected termination: drained" {
		t.Fatalf("state_message = %v, want 'Expected termination: drained'", entry["state_message"])
	}
	if v, ok := entry["start_time_ms"].(float64); !ok || v != 1000 {
		t.Fatalf("start_time_ms = %v (%T), want 1000", entry["start_time_ms"], entry["start_time_ms"])
	}
}

func TestListActorsRejectsUnknownFilterColumn(t *testing.T) {
	client, srv, ln := newMockClient(t)
	defer ln.Close()
	defer srv.Stop()
	defer client.Close()

	s := New(&head.HeadConfig{}, client)
	mux := http.NewServeMux()
	if err := s.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/api/v0/actors?filter_keys=bad_column&filter_predicates=%3D&filter_values=x", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for unknown filter column", rr.Code)
	}
}

// TestDataSourceFailureReturns500 pins the 500 + data:{"result":null} error
// path, aligned with the Python handle_list_api mapping DataSourceUnavailable
// to HTTPStatusCode.INTERNAL_ERROR.
func TestDataSourceFailureReturns500(t *testing.T) {
	client, srv, ln := newMockClientWithMock(t, &mockGCSServer{actorErr: errors.New("gcs unavailable")})
	defer ln.Close()
	defer srv.Stop()
	defer client.Close()

	s := New(&head.HeadConfig{}, client)
	mux := http.NewServeMux()
	if err := s.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/api/v0/actors", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
	var body struct {
		Data map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if v, ok := body.Data["result"]; !ok || v != nil {
		t.Fatalf("data.result = %v (present=%v), want null", v, ok)
	}
}
