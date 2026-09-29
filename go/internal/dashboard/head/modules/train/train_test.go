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

package train

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
)

// mockGCS simulates the GCS services used by the train module.
type mockGCS struct {
	proto.UnimplementedInternalKVGcsServiceServer
	proto.UnimplementedJobInfoGcsServiceServer
	proto.UnimplementedActorInfoGcsServiceServer

	mu        sync.Mutex
	store     map[string][]byte
	jobInfo   []*proto.JobTableData
	actorInfo []*proto.ActorTableData
}

func newMockGCS() *mockGCS {
	return &mockGCS{store: map[string][]byte{}}
}

func (s *mockGCS) nskey(ns, key []byte) string { return string(ns) + "\x00" + string(key) }

func (s *mockGCS) InternalKVGet(ctx context.Context, req *proto.InternalKVGetRequest) (*proto.InternalKVGetReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &proto.InternalKVGetReply{Value: s.store[s.nskey(req.Namespace, req.Key)]}, nil
}

func (s *mockGCS) InternalKVPut(ctx context.Context, req *proto.InternalKVPutRequest) (*proto.InternalKVPutReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.nskey(req.Namespace, req.Key)
	s.store[key] = req.Value
	return &proto.InternalKVPutReply{Added: true}, nil
}

func (s *mockGCS) GetAllJobInfo(ctx context.Context, req *proto.GetAllJobInfoRequest) (*proto.GetAllJobInfoReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &proto.GetAllJobInfoReply{JobInfoList: s.jobInfo}, nil
}

func (s *mockGCS) GetAllActorInfo(ctx context.Context, req *proto.GetAllActorInfoRequest) (*proto.GetAllActorInfoReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &proto.GetAllActorInfoReply{ActorTableData: s.actorInfo}, nil
}

// startMockGCS starts an in-memory gRPC server and returns a connected
// GCSClient plus a stop function.
func startMockGCS(t *testing.T, mock *mockGCS) (*head.GCSClient, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	proto.RegisterInternalKVGcsServiceServer(srv, mock)
	proto.RegisterJobInfoGcsServiceServer(srv, mock)
	proto.RegisterActorInfoGcsServiceServer(srv, mock)
	go func() { _ = srv.Serve(ln) }()
	client, err := head.NewGCSClient(context.Background(), ln.Addr().String(), head.WithToken("test"))
	if err != nil {
		srv.Stop()
		t.Fatal(err)
	}
	return client, func() {
		srv.Stop()
		_ = client.Close()
	}
}

// newTrainHead builds a TrainHead backed by a mock GCS client.
func newTrainHead(t *testing.T, mock *mockGCS) (*TrainHead, func()) {
	t.Helper()
	client, stop := startMockGCS(t, mock)
	return New(&head.HeadConfig{SessionName: "session_test"}, client), stop
}

// fakeStateActor simulates the Python Train state actor for unit tests.
type fakeStateActor struct {
	mu            sync.Mutex
	trainRuns     map[string]interface{}
	trainAttempts map[string]interface{}
	allTrainRuns  map[string]interface{}
	trainRunsErr  error
	attemptsErr   error
	allRunsErr    error
}

func newFakeStateActor() *fakeStateActor {
	return &fakeStateActor{}
}

func (a *fakeStateActor) getTrainRuns() (map[string]interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.trainRunsErr != nil {
		return nil, a.trainRunsErr
	}
	return a.trainRuns, nil
}

func (a *fakeStateActor) getTrainRunAttempts() (map[string]interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.attemptsErr != nil {
		return nil, a.attemptsErr
	}
	return a.trainAttempts, nil
}

func (a *fakeStateActor) getAllTrainRuns() (map[string]interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.allRunsErr != nil {
		return nil, a.allRunsErr
	}
	return a.allTrainRuns, nil
}

// setV2Actor installs a fake state actor as the Train V2 actor.
func setV2Actor(h *TrainHead, a stateActor) {
	h.muV2.Lock()
	defer h.muV2.Unlock()
	h.trainV2Actor = a
}

// setV1Actor installs a fake state actor as the Train V1 actor.
func setV1Actor(h *TrainHead, a stateActor) {
	h.muV1.Lock()
	defer h.muV1.Unlock()
	h.trainV1Actor = a
}

// serve performs an HTTP request against the TrainHead mux.
func serve(t *testing.T, h *TrainHead, method, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	if err := h.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	var reader io.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	req := httptest.NewRequest(method, path, reader)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

// sampleDriverTable builds a JobTableData for a driver job.
func sampleDriverTable(jobIDHex string, metadata map[string]string) *proto.JobTableData {
	jobIDBytes, err := hex.DecodeString(jobIDHex)
	if err != nil {
		panic("bad hex job id in test: " + err.Error())
	}
	return &proto.JobTableData{
		JobId:           jobIDBytes,
		IsDead:          false,
		DriverPid:       1234,
		Entrypoint:      "python train.py",
		StartTime:       1000,
		EndTime:         2000,
		DriverIpAddress: "10.0.0.1",
		DriverAddress:   &proto.Address{IpAddress: "10.0.0.1"},
		Config: &proto.JobConfig{
			RayNamespace: "default",
			Metadata:     metadata,
		},
	}
}

func actorTable(actorIDHex, state string) *proto.ActorTableData {
	actorIDBytes, err := hex.DecodeString(actorIDHex)
	if err != nil {
		panic("bad hex actor id: " + err.Error())
	}
	stateVal := proto.ActorTableData_ALIVE
	if state == "DEAD" {
		stateVal = proto.ActorTableData_DEAD
	}
	return &proto.ActorTableData{
		ActorId: actorIDBytes,
		State:   stateVal,
		Name:    "train_actor",
	}
}

func TestTrainHeadNameAndHealth(t *testing.T) {
	h := New(&head.HeadConfig{}, nil)
	if h.Name() != "TrainHead" {
		t.Fatalf("name = %q", h.Name())
	}
	if !h.Healthy() {
		t.Fatal("should be healthy")
	}
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestTrainV2RunsNoActor(t *testing.T) {
	mock := newMockGCS()
	h, stop := newTrainHead(t, mock)
	defer stop()
	rr := serve(t, h, "GET", "/api/train/v2/runs/v1", "")
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
	if !bytes.Contains(rr.Body.Bytes(), []byte("RAY_TRAIN_ENABLE_STATE_TRACKING")) {
		t.Fatalf("body = %s", rr.Body.String())
	}
}

func TestTrainV2RunsDecorate(t *testing.T) {
	mock := newMockGCS()
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0101", map[string]string{}),
	}
	mock.actorInfo = []*proto.ActorTableData{
		actorTable("aa01", "DEAD"),
	}
	h, stop := newTrainHead(t, mock)
	defer stop()

	// A run with a RUNNING status whose controller actor is DEAD -> ABORTED.
	fa := newFakeStateActor()
	fa.trainRuns = map[string]interface{}{
		"run1": map[string]interface{}{
			"id": "run1", "name": "train run", "job_id": "0101",
			"controller_actor_id": "aa01",
			"status":              "RUNNING", "status_detail": "",
			"start_time_ns": int64(2000), "end_time_ns": nil,
			"controller_log_file_path": "/tmp/ctrl.log",
		},
	}
	fa.trainAttempts = map[string]interface{}{
		"run1": map[string]interface{}{
			"attempt1": map[string]interface{}{
				"run_id": "run1", "attempt_id": "attempt1",
				"status": "RUNNING", "status_detail": "",
				"start_time_ns": int64(1000), "end_time_ns": nil,
				"resources": []interface{}{}, "workers": []interface{}{
					map[string]interface{}{
						"world_rank": 0, "local_rank": 0, "node_rank": 0,
						"actor_id": "aa01", "node_id": "n1", "node_ip": "10.0.0.1",
						"pid": 100, "gpu_ids": []interface{}{},
						"status": "ALIVE", "resources": map[string]interface{}{},
						"log_file_path": "/tmp/w.log",
					},
				},
			},
		},
	}
	setV2Actor(h, fa)

	rr := serve(t, h, "GET", "/api/train/v2/runs/v1", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		TrainRuns []map[string]interface{} `json:"train_runs"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.TrainRuns) != 1 {
		t.Fatalf("train_runs = %+v", resp.TrainRuns)
	}
	run := resp.TrainRuns[0]
	if run["status"] != "ABORTED" {
		t.Fatalf("status = %v, want ABORTED", run["status"])
	}
	if run["status_detail"] != "Terminated due to system errors or killed by the user." {
		t.Fatalf("status_detail = %v", run["status_detail"])
	}
	// job_details should be attached from the driver job.
	jd, ok := run["job_details"].(map[string]interface{})
	if !ok || jd["job_id"] != "0101" {
		t.Fatalf("job_details = %+v", run["job_details"])
	}
	// attempts should be decorated.
	attempts, ok := run["attempts"].([]interface{})
	if !ok || len(attempts) != 1 {
		t.Fatalf("attempts = %+v", run["attempts"])
	}
	attempt := attempts[0].(map[string]interface{})
	workers, ok := attempt["workers"].([]interface{})
	if !ok || len(workers) != 1 {
		t.Fatalf("workers = %+v", attempt["workers"])
	}
	worker := workers[0].(map[string]interface{})
	if worker["status"] != "DEAD" {
		t.Fatalf("worker status = %v, want DEAD (from actor info)", worker["status"])
	}
}

func TestTrainV2RunsActorError(t *testing.T) {
	mock := newMockGCS()
	h, stop := newTrainHead(t, mock)
	defer stop()
	fa := newFakeStateActor()
	fa.trainRunsErr = context.Canceled
	setV2Actor(h, fa)
	rr := serve(t, h, "GET", "/api/train/v2/runs/v1", "")
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
}

func TestTrainV1RunsNoActor(t *testing.T) {
	mock := newMockGCS()
	h, stop := newTrainHead(t, mock)
	defer stop()
	rr := serve(t, h, "GET", "/api/train/v2/runs", "")
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
}

func TestTrainV1RunsDecorate(t *testing.T) {
	mock := newMockGCS()
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0101", map[string]string{}),
	}
	mock.actorInfo = []*proto.ActorTableData{
		actorTable("aa01", "ALIVE"),
		actorTable("bb02", "DEAD"),
	}
	h, stop := newTrainHead(t, mock)
	defer stop()

	// Run 1 has a running controller (aa01) -> stays RUNNING.
	// Run 2 has a dead controller (bb02) and RUNNING status -> ABORTED.
	fa := newFakeStateActor()
	fa.allTrainRuns = map[string]interface{}{
		"run1": map[string]interface{}{
			"name": "r1", "id": "run1", "job_id": "0101",
			"controller_actor_id": "aa01",
			"workers": []interface{}{
				map[string]interface{}{
					"actor_id": "aa01", "world_rank": 0, "local_rank": 0, "node_rank": 0,
					"node_id": "n1", "node_ip": "10.0.0.1", "pid": 100,
					"gpu_ids": []interface{}{}, "status": "ALIVE",
					"resources": map[string]interface{}{},
				},
			},
			"datasets": []interface{}{}, "run_status": "RUNNING",
			"status_detail": "", "start_time_ms": 1000, "end_time_ms": nil,
			"resources": []interface{}{},
		},
		"run2": map[string]interface{}{
			"name": "r2", "id": "run2", "job_id": "0101",
			"controller_actor_id": "bb02",
			"workers":             []interface{}{},
			"datasets":            []interface{}{}, "run_status": "RUNNING",
			"status_detail": "", "start_time_ms": 2000, "end_time_ms": nil,
			"resources": []interface{}{},
		},
	}
	setV1Actor(h, fa)

	rr := serve(t, h, "GET", "/api/train/v2/runs", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		TrainRuns []map[string]interface{} `json:"train_runs"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.TrainRuns) != 2 {
		t.Fatalf("train_runs = %+v", resp.TrainRuns)
	}
	// Sorted by start_time_ms descending: run2 first.
	if resp.TrainRuns[0]["id"] != "run2" || resp.TrainRuns[1]["id"] != "run1" {
		t.Fatalf("order = %v, %v", resp.TrainRuns[0]["id"], resp.TrainRuns[1]["id"])
	}
	if resp.TrainRuns[0]["run_status"] != "ABORTED" {
		t.Fatalf("run2 status = %v, want ABORTED", resp.TrainRuns[0]["run_status"])
	}
	if resp.TrainRuns[1]["run_status"] != "RUNNING" {
		t.Fatalf("run1 status = %v, want RUNNING", resp.TrainRuns[1]["run_status"])
	}
	// Workers should be decorated with actor status.
	workers := resp.TrainRuns[1]["workers"].([]interface{})
	if len(workers) != 1 {
		t.Fatalf("workers = %+v", workers)
	}
	if workers[0].(map[string]interface{})["status"] != "ALIVE" {
		t.Fatalf("worker status = %v", workers[0].(map[string]interface{})["status"])
	}
	// job_details attached.
	jd, ok := resp.TrainRuns[0]["job_details"].(map[string]interface{})
	if !ok || jd["job_id"] != "0101" {
		t.Fatalf("job_details = %+v", resp.TrainRuns[0]["job_details"])
	}
}

func TestTrainV1RunsActorError(t *testing.T) {
	mock := newMockGCS()
	h, stop := newTrainHead(t, mock)
	defer stop()
	fa := newFakeStateActor()
	fa.allRunsErr = context.Canceled
	setV1Actor(h, fa)
	rr := serve(t, h, "GET", "/api/train/v2/runs", "")
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
}

// TestFormattedGPUs verifies the B-F2 gpu enrichment: from an actor's gpus
// (each with processesPids) the worker's matching gpus are kept and each is
// collapsed to a single processInfo entry matching the worker pid, aligned with
// the Python _decorate_train_workers.
func TestFormattedGPUs(t *testing.T) {
	worker := map[string]interface{}{"pid": float64(100)}
	gpus := []interface{}{
		map[string]interface{}{
			"index": 0, "name": "gpu0",
			"processesPids": []interface{}{
				map[string]interface{}{"pid": float64(50), "gpuMemoryUsage": 5},
				map[string]interface{}{"pid": float64(100), "gpuMemoryUsage": 10},
			},
		},
		map[string]interface{}{
			"index": 1, "name": "gpu1",
			"processesPids": []interface{}{
				map[string]interface{}{"pid": float64(300), "gpuMemoryUsage": 30},
			},
		},
	}
	formatted := formattedGPUs(worker, gpus)
	if len(formatted) != 1 {
		t.Fatalf("formatted = %+v, want 1 gpu", formatted)
	}
	g := formatted[0].(map[string]interface{})
	if g["index"] != 0 || g["name"] != "gpu0" {
		t.Fatalf("gpu = %+v", g)
	}
	pi, ok := g["processInfo"].(map[string]interface{})
	if !ok || pi["pid"] != float64(100) {
		t.Fatalf("processInfo = %+v, want pid 100", g["processInfo"])
	}
	// Worker without a pid -> no gpus.
	if out := formattedGPUs(map[string]interface{}{}, gpus); len(out) != 0 {
		t.Fatalf("no-pid worker gpus = %+v, want []", out)
	}
}

// TestPhysicalStatsForPID verifies the physical-stats enrichment used by
// getActorInfosFn: gpus/processStats are matched by pid from the cached node
// physical stats.
func TestPhysicalStatsForPID(t *testing.T) {
	h := New(&head.HeadConfig{}, nil)
	h.physMu.Lock()
	h.physicalStats["node1"] = map[string]interface{}{
		"workers": []interface{}{
			map[string]interface{}{"pid": float64(100), "cpuPercent": 1.5},
		},
		"gpus": []interface{}{
			map[string]interface{}{
				"index": 0,
				"processesPids": []interface{}{
					map[string]interface{}{"pid": float64(100), "gpuMemoryUsage": 10},
				},
			},
		},
	}
	h.physMu.Unlock()

	ps, gpus := physicalStatsForPID(h, "node1", 100)
	if ps == nil {
		t.Fatal("processStats = nil, want worker entry")
	}
	if len(gpus) != 1 {
		t.Fatalf("gpus = %+v, want 1", gpus)
	}
	// Unknown node -> nil/empty.
	if ps, gpus := physicalStatsForPID(h, "missing", 100); ps != nil || len(gpus) != 0 {
		t.Fatalf("unknown node ps=%v gpus=%v, want nil/empty", ps, gpus)
	}
}

// TestTrainV2RunsWorkerGPUEnrichment verifies the full B-F2 path: a worker
// whose actor has gpus in the physical stats gets gpus[].processInfo and
// processStats in the decorated response.
func TestTrainV2RunsWorkerGPUEnrichment(t *testing.T) {
	mock := newMockGCS()
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0101", map[string]string{}),
	}
	// Actor aa01 (pid 100) on node abcd. The node id must be valid hex: the
	// enrichment path keys physicalStats by hex.EncodeToString(NodeId), so a
	// pure-hex id round-trips to the same string used as the stats key.
	mock.actorInfo = []*proto.ActorTableData{{
		ActorId: mustHex(t, "aa01"),
		State:   proto.ActorTableData_ALIVE,
		Pid:     100,
		Address: &proto.Address{NodeId: mustHex(t, "abcd")},
	}}
	h, stop := newTrainHead(t, mock)
	defer stop()

	h.physMu.Lock()
	h.physicalStats["abcd"] = map[string]interface{}{
		"workers": []interface{}{
			map[string]interface{}{"pid": float64(100), "cpuPercent": 2.0},
		},
		"gpus": []interface{}{
			map[string]interface{}{
				"index": 0, "name": "gpu0",
				"processesPids": []interface{}{
					map[string]interface{}{"pid": float64(100), "gpuMemoryUsage": 20},
				},
			},
		},
	}
	h.physMu.Unlock()

	fa := newFakeStateActor()
	fa.trainRuns = map[string]interface{}{
		"run1": map[string]interface{}{
			"id": "run1", "name": "train run", "job_id": "0101",
			"controller_actor_id": "aa01",
			"status":              "RUNNING", "status_detail": "",
			"start_time_ns": int64(2000), "end_time_ns": nil,
			"controller_log_file_path": "/tmp/ctrl.log",
		},
	}
	fa.trainAttempts = map[string]interface{}{
		"run1": map[string]interface{}{
			"attempt1": map[string]interface{}{
				"run_id": "run1", "attempt_id": "attempt1",
				"status": "RUNNING", "status_detail": "",
				"start_time_ns": int64(1000), "end_time_ns": nil,
				"resources": []interface{}{}, "workers": []interface{}{
					map[string]interface{}{
						"world_rank": 0, "local_rank": 0, "node_rank": 0,
						"actor_id": "aa01", "node_id": "n1", "node_ip": "10.0.0.1",
						"pid": 100, "gpu_ids": []interface{}{},
						"status": "ALIVE", "resources": map[string]interface{}{},
						"log_file_path": "/tmp/w.log",
					},
				},
			},
		},
	}
	setV2Actor(h, fa)

	rr := serve(t, h, "GET", "/api/train/v2/runs/v1", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		TrainRuns []map[string]interface{} `json:"train_runs"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	attempts := resp.TrainRuns[0]["attempts"].([]interface{})
	workers := attempts[0].(map[string]interface{})["workers"].([]interface{})
	worker := workers[0].(map[string]interface{})
	// status from actor info.
	if worker["status"] != "ALIVE" {
		t.Fatalf("worker status = %v", worker["status"])
	}
	// processStats enriched.
	if ps, ok := worker["processStats"].(map[string]interface{}); !ok || ps["pid"] != float64(100) {
		t.Fatalf("processStats = %+v", worker["processStats"])
	}
	// gpus[0].processInfo.pid == 100.
	gpus := worker["gpus"].([]interface{})
	if len(gpus) != 1 {
		t.Fatalf("gpus = %+v, want 1", gpus)
	}
	g := gpus[0].(map[string]interface{})
	pi, ok := g["processInfo"].(map[string]interface{})
	if !ok || pi["pid"] != float64(100) {
		t.Fatalf("gpus[0].processInfo = %+v, want pid 100", g["processInfo"])
	}
}

// mustHex decodes a hex string, panicking on error (test helper).
func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
