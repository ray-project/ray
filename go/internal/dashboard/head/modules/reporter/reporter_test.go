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
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
)

// mockGCS simulates the GCS internal KV + node + task services used by the
// reporter module.
type mockGCS struct {
	proto.UnimplementedInternalKVGcsServiceServer
	proto.UnimplementedNodeInfoGcsServiceServer
	proto.UnimplementedTaskInfoGcsServiceServer
	mu    sync.Mutex
	store map[string][]byte
}

func newMockGCS() *mockGCS {
	return &mockGCS{store: map[string][]byte{}}
}

func (s *mockGCS) nskey(ns, key []byte) string { return string(ns) + "\x00" + string(key) }

func (s *mockGCS) put(ns, key string, val []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[s.nskey([]byte(ns), []byte(key))] = val
}

func (s *mockGCS) InternalKVGet(ctx context.Context, req *proto.InternalKVGetRequest) (*proto.InternalKVGetReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &proto.InternalKVGetReply{Value: s.store[s.nskey(req.Namespace, req.Key)]}, nil
}

func (s *mockGCS) CheckAlive(ctx context.Context, req *proto.CheckAliveRequest) (*proto.CheckAliveReply, error) {
	return &proto.CheckAliveReply{RayletAlive: []bool{true}}, nil
}

func (s *mockGCS) GetAllNodeInfo(ctx context.Context, req *proto.GetAllNodeInfoRequest) (*proto.GetAllNodeInfoReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &proto.GetAllNodeInfoReply{
		NodeInfoList: []*proto.GcsNodeInfo{
			{NodeId: []byte{0x01}, NodeManagerAddress: "1.2.3.4", MetricsExportPort: 8080, State: proto.GcsNodeInfo_ALIVE},
			{NodeId: []byte{0x02}, NodeManagerAddress: "5.6.7.8", MetricsExportPort: 8081, State: proto.GcsNodeInfo_DEAD},
		},
		Total: 2,
	}, nil
}

func (s *mockGCS) GetTaskEvents(ctx context.Context, req *proto.GetTaskEventsRequest) (*proto.GetTaskEventsReply, error) {
	pid := int32(1234)
	// RUNNING task state ts for the RUNNING status.
	ts := map[int32]int64{
		int32(proto.TaskStatus_PENDING_ARGS_AVAIL): 1000000000,
		int32(proto.TaskStatus_RUNNING):            2000000000,
	}
	return &proto.GetTaskEventsReply{
		EventsByTask: []*proto.TaskEvents{
			{
				TaskId:        []byte{0xaa},
				AttemptNumber: 0,
				JobId:         []byte{0xbb},
				TaskInfo: &proto.TaskInfoEntry{
					Type:            proto.TaskType_NORMAL_TASK,
					FuncOrClassName: "f",
					Language:        proto.Language_PYTHON,
				},
				StateUpdates: &proto.TaskStateUpdate{
					NodeId:    []byte{0x01},
					WorkerId:  []byte{0xcc},
					WorkerPid: &pid,
					StateTsNs: ts,
				},
			},
		},
		NumTotalStored: 1,
	}, nil
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
	proto.RegisterNodeInfoGcsServiceServer(srv, mock)
	proto.RegisterTaskInfoGcsServiceServer(srv, mock)
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

func newReportHead(t *testing.T, mock *mockGCS) (*ReportHead, *head.GCSClient, func()) {
	t.Helper()
	client, stop := startMockGCS(t, mock)
	r := New(&head.HeadConfig{GCSAddress: "127.0.0.1:9999"}, client)
	if err := r.Start(context.Background()); err != nil {
		stop()
		t.Fatal(err)
	}
	return r, client, stop
}

func serve(t *testing.T, r *ReportHead, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	if err := r.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", path, nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

func TestReportHeadName(t *testing.T) {
	r := New(&head.HeadConfig{}, nil)
	if r.Name() != "ReportHead" {
		t.Fatalf("name = %q", r.Name())
	}
	if !r.Healthy() {
		t.Fatal("should be healthy")
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestClusterMetadata(t *testing.T) {
	mock := newMockGCS()
	mock.put("cluster", "CLUSTER_METADATA", []byte(`{"ray_version":"3.0.0","python_version":"3.11","ray_init_cluster":true}`))
	r, _, stop := newReportHead(t, mock)
	defer stop()

	rr := serve(t, r, "/api/v0/cluster_metadata")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		Data struct {
			RayVersion     string `json:"rayVersion"`
			PythonVersion  string `json:"pythonVersion"`
			RayInitCluster bool   `json:"rayInitCluster"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.RayVersion != "3.0.0" || body.Data.PythonVersion != "3.11" || !body.Data.RayInitCluster {
		t.Fatalf("cluster metadata = %+v", body.Data)
	}
}

func TestClusterStatusFormat0(t *testing.T) {
	mock := newMockGCS()
	mock.put("", "__autoscaling_status_legacy", []byte("legacy status"))
	mock.put("", "__autoscaling_status", []byte(`{"load_metrics_report":{"usage":{"CPU":[1,4]}},"autoscaler_report":{"active_nodes":{"worker":2}},"time":1700000000}`))
	mock.put("", "__autoscaling_error", []byte("an error"))
	r, _, stop := newReportHead(t, mock)
	defer stop()

	rr := serve(t, r, "/api/cluster_status?format=0")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		Data struct {
			AutoscalingStatus string `json:"autoscalingStatus"`
			AutoscalingError  string `json:"autoscalingError"`
			ClusterStatus     *struct {
				LoadMetricsReport map[string]interface{} `json:"loadMetricsReport"`
			} `json:"clusterStatus"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.AutoscalingStatus != "legacy status" {
		t.Fatalf("autoscaling_status = %q", body.Data.AutoscalingStatus)
	}
	if body.Data.AutoscalingError != "an error" {
		t.Fatalf("autoscaling_error = %q", body.Data.AutoscalingError)
	}
	if body.Data.ClusterStatus == nil {
		t.Fatal("cluster_status should be the parsed dict")
	}
}

func TestClusterStatusFormat1(t *testing.T) {
	// Force v1 branch by leaving the v2 enable KV empty.
	mock := newMockGCS()
	mock.put("", "__autoscaling_status", []byte(`{"load_metrics_report":{"usage":{"CPU":[1,4]}},"autoscaler_report":{"active_nodes":{"worker":2}},"time":1700000000}`))
	mock.put("", "__autoscaling_error", []byte("boom"))
	r, _, stop := newReportHead(t, mock)
	defer stop()
	os.Setenv("RAY_enable_autoscaler_v2", "")
	defer os.Unsetenv("RAY_enable_autoscaler_v2")

	rr := serve(t, r, "/api/cluster_status?format=1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		Data struct {
			ClusterStatus string `json:"clusterStatus"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.ClusterStatus == "" {
		t.Fatal("cluster_status should be a formatted string")
	}
	if body.Data.ClusterStatus[:8] != "========" {
		t.Fatalf("cluster_status should start with the autoscaler header: %q", body.Data.ClusterStatus[:20])
	}
	if body.Data.ClusterStatus[len(body.Data.ClusterStatus)-4:] != "boom" {
		t.Fatalf("cluster_status should append the error: %q", body.Data.ClusterStatus[len(body.Data.ClusterStatus)-20:])
	}
}

func TestGCSHealthz(t *testing.T) {
	mock := newMockGCS()
	r, _, stop := newReportHead(t, mock)
	defer stop()
	rr := serve(t, r, "/api/gcs_healthz")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if rr.Body.String() != "success" {
		t.Fatalf("body = %q, want success", rr.Body.String())
	}
}

func TestPrometheusSD(t *testing.T) {
	mock := newMockGCS()
	mock.put("", "DashboardMetricsAddress", []byte("9.9.9.9:9999"))
	r, _, stop := newReportHead(t, mock)
	defer stop()

	rr := serve(t, r, "/api/prometheus/sd")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var content []struct {
		Labels  map[string]string `json:"labels"`
		Targets []string          `json:"targets"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &content); err != nil {
		t.Fatal(err)
	}
	if len(content) != 1 || content[0].Labels["job"] != "ray" {
		t.Fatalf("content = %+v", content)
	}
	found := false
	for _, tg := range content[0].Targets {
		if tg == "1.2.3.4:8080" {
			found = true
		}
		if tg == "5.6.7.8:8081" {
			t.Fatal("dead node should be excluded")
		}
	}
	if !found {
		t.Fatalf("targets = %v, missing alive node", content[0].Targets)
	}
	hasDashboard := false
	for _, tg := range content[0].Targets {
		if tg == "9.9.9.9:9999" {
			hasDashboard = true
		}
	}
	if !hasDashboard {
		t.Fatalf("targets = %v, missing dashboard metrics address", content[0].Targets)
	}
}

func TestTaskTracebackMissingParams(t *testing.T) {
	r, _, stop := newReportHead(t, newMockGCS())
	defer stop()
	rr := serve(t, r, "/task/traceback?node_id=01")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("missing task_id should be 400, got %d", rr.Code)
	}
}

func TestTaskCPUProfileDurationTooLong(t *testing.T) {
	mock := newMockGCS()
	r, _, stop := newReportHead(t, mock)
	defer stop()
	rr := serve(t, r, "/task/cpu_profile?task_id=aa&attempt_number=0&node_id=01&duration=61")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("duration>60 should be 400, got %d", rr.Code)
	}
}

func TestWorkerTracebackRequiresIPOrNode(t *testing.T) {
	r, _, stop := newReportHead(t, newMockGCS())
	defer stop()
	rr := serve(t, r, "/worker/traceback?pid=123")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("missing ip/node_id should be 400, got %d", rr.Code)
	}
}
