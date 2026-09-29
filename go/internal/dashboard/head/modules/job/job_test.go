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

package job

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
)

// mockGCS simulates the GCS services used by the job module: internal KV,
// job info and runtime env pinning.
type mockGCS struct {
	proto.UnimplementedInternalKVGcsServiceServer
	proto.UnimplementedJobInfoGcsServiceServer
	proto.UnimplementedRuntimeEnvGcsServiceServer

	mu      sync.Mutex
	store   map[string][]byte
	jobInfo []*proto.JobTableData
	pins    []string
}

func newMockGCS() *mockGCS {
	return &mockGCS{store: map[string][]byte{}}
}

func (s *mockGCS) nskey(ns, key []byte) string { return string(ns) + "\x00" + string(key) }

func (s *mockGCS) splitKey(k string) (string, string) {
	i := strings.IndexByte(k, 0)
	return k[:i], k[i+1:]
}

func (s *mockGCS) put(ns, key string, val []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[s.nskey([]byte(ns), []byte(key))] = val
}

func (s *mockGCS) get(ns, key string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store[s.nskey([]byte(ns), []byte(key))]
}

func (s *mockGCS) InternalKVGet(ctx context.Context, req *proto.InternalKVGetRequest) (*proto.InternalKVGetReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &proto.InternalKVGetReply{Value: s.store[s.nskey(req.Namespace, req.Key)]}, nil
}

func (s *mockGCS) InternalKVPut(ctx context.Context, req *proto.InternalKVPutRequest) (*proto.InternalKVPutReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.nskey(req.Namespace, req.Key)
	_, existed := s.store[key]
	if existed && !req.Overwrite {
		return &proto.InternalKVPutReply{Added: false}, nil
	}
	s.store[key] = req.Value
	return &proto.InternalKVPutReply{Added: !existed}, nil
}

func (s *mockGCS) InternalKVKeys(ctx context.Context, req *proto.InternalKVKeysRequest) (*proto.InternalKVKeysReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := string(req.Prefix)
	var keys [][]byte
	for k := range s.store {
		ns, key := s.splitKey(k)
		if ns == string(req.Namespace) && strings.HasPrefix(key, prefix) {
			keys = append(keys, []byte(key))
		}
	}
	// Return the keys in sorted order so the scan order is deterministic in
	// tests, mirroring the stable ordering the real GCS backend returns.
	sort.Slice(keys, func(i, j int) bool { return string(keys[i]) < string(keys[j]) })
	return &proto.InternalKVKeysReply{Results: keys}, nil
}

func (s *mockGCS) InternalKVDel(ctx context.Context, req *proto.InternalKVDelRequest) (*proto.InternalKVDelReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	target := s.nskey(req.Namespace, req.Key)
	var deleted int
	for k := range s.store {
		if req.DelByPrefix {
			if strings.HasPrefix(k, target) {
				delete(s.store, k)
				deleted++
			}
		} else if k == target {
			delete(s.store, k)
			deleted++
		}
	}
	return &proto.InternalKVDelReply{DeletedNum: int32(deleted)}, nil
}

func (s *mockGCS) GetAllJobInfo(ctx context.Context, req *proto.GetAllJobInfoRequest) (*proto.GetAllJobInfoReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &proto.GetAllJobInfoReply{JobInfoList: s.jobInfo}, nil
}

func (s *mockGCS) PinRuntimeEnvURI(ctx context.Context, req *proto.PinRuntimeEnvURIRequest) (*proto.PinRuntimeEnvURIReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pins = append(s.pins, req.Uri)
	return &proto.PinRuntimeEnvURIReply{}, nil
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
	proto.RegisterRuntimeEnvGcsServiceServer(srv, mock)
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

// newJobHead builds a JobHead backed by a mock GCS client.
func newJobHead(t *testing.T, mock *mockGCS) (*JobHead, func()) {
	t.Helper()
	client, stop := startMockGCS(t, mock)
	j := New(&head.HeadConfig{SessionName: "session_test"}, client)
	return j, stop
}

// serve performs an HTTP request against the JobHead mux.
func serve(t *testing.T, j *JobHead, method, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	if err := j.RegisterHTTP(mux); err != nil {
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

// sampleDriverTable builds a JobTableData for a driver job. jobID is the hex
// job id, matching how the GCS stores and the head hex-encodes job ids.
func sampleDriverTable(jobIDHex, ns string, metadata map[string]string, isDead bool) *proto.JobTableData {
	jobIDBytes, err := hex.DecodeString(jobIDHex)
	if err != nil {
		panic("bad hex job id in test: " + err.Error())
	}
	return &proto.JobTableData{
		JobId:           jobIDBytes,
		IsDead:          isDead,
		DriverPid:       1234,
		Entrypoint:      "python main.py",
		StartTime:       1000,
		EndTime:         2000,
		DriverIpAddress: "10.0.0.1",
		DriverAddress:   &proto.Address{IpAddress: "10.0.0.1"},
		Config: &proto.JobConfig{
			RayNamespace: ns,
			Metadata:     metadata,
			RuntimeEnvInfo: &proto.RuntimeEnvInfo{
				SerializedRuntimeEnv: `{"working_dir":"gcs://x.zip"}`,
			},
		},
	}
}

func TestJobInfoStorageClient(t *testing.T) {
	mock := newMockGCS()
	client, stop := startMockGCS(t, mock)
	defer stop()
	info := NewJobInfoStorageClient(client)
	ctx := context.Background()

	if _, err := info.PutInfo(ctx, "job_001", &JobInfo{
		Status:     JobStatusPending,
		Entrypoint: "echo hi",
	}, true); err != nil {
		t.Fatalf("PutInfo: %v", err)
	}

	got, err := info.GetInfo(ctx, "job_001")
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if got == nil || got.Status != JobStatusPending || got.Entrypoint != "echo hi" {
		t.Fatalf("GetInfo = %+v", got)
	}

	// Missing submission id returns nil.
	missing, err := info.GetInfo(ctx, "job_missing")
	if err != nil {
		t.Fatalf("GetInfo missing: %v", err)
	}
	if missing != nil {
		t.Fatalf("GetInfo missing = %+v, want nil", missing)
	}

	all, err := info.GetAllJobs(ctx)
	if err != nil {
		t.Fatalf("GetAllJobs: %v", err)
	}
	if len(all) != 1 || all["job_001"] == nil {
		t.Fatalf("GetAllJobs = %+v", all)
	}

	// Verify the raw KV key format is _ray_internal_job_info_{submission_id}.
	raw := mock.get(kvNamespaceJob, jobDataKey("job_001"))
	if raw == nil {
		t.Fatal("raw KV entry missing")
	}

	// Overwrite=false on an existing key adds nothing.
	added, err := info.PutInfo(ctx, "job_001", &JobInfo{Status: JobStatusRunning, Entrypoint: "x"}, false)
	if err != nil {
		t.Fatalf("PutInfo no-overwrite: %v", err)
	}
	if added {
		t.Fatal("PutInfo overwrite=false should not add")
	}
	got2, _ := info.GetInfo(ctx, "job_001")
	if got2.Status != JobStatusPending {
		t.Fatalf("job status should be unchanged, got %s", got2.Status)
	}

	if err := info.DeleteInfo(ctx, "job_001"); err != nil {
		t.Fatalf("DeleteInfo: %v", err)
	}
	gone, _ := info.GetInfo(ctx, "job_001")
	if gone != nil {
		t.Fatal("job should be deleted")
	}
}

func TestGetHeadNodeID(t *testing.T) {
	mock := newMockGCS()
	mock.put(kvNamespaceJob, kvHeadNodeIDKey, []byte("aabbcc"))
	client, stop := startMockGCS(t, mock)
	defer stop()
	info := NewJobInfoStorageClient(client)

	got, err := info.GetHeadNodeID(context.Background())
	if err != nil {
		t.Fatalf("GetHeadNodeID: %v", err)
	}
	if got != "aabbcc" {
		t.Fatalf("head node id = %q", got)
	}
}

func TestFetchAgentInfo(t *testing.T) {
	mock := newMockGCS()
	mock.put(kvNamespaceDashboard, dashboardAgentAddrNodeIDPrefix+"aabbcc", []byte(`["1.2.3.4",8266,8267]`))
	client, stop := startMockGCS(t, mock)
	defer stop()
	info := NewJobInfoStorageClient(client)

	ip, port, err := info.FetchAgentInfo(context.Background(), "aabbcc")
	if err != nil {
		t.Fatalf("FetchAgentInfo: %v", err)
	}
	if ip != "1.2.3.4" || port != 8266 {
		t.Fatalf("agent info = %s:%d", ip, port)
	}

	if _, _, err := info.FetchAgentInfo(context.Background(), "deadbeef"); err == nil {
		t.Fatal("missing agent info should error")
	}
}

func TestGetDriverJobs(t *testing.T) {
	mock := newMockGCS()
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0101", "default", map[string]string{"k": "v"}, false),
		// Submission driver: metadata carries job_submission_id.
		sampleDriverTable("0102", "default", map[string]string{jobIDMetadataKey: "sub_1"}, false),
		// Internal namespace is skipped.
		sampleDriverTable("0103", "_ray_internal_dashboard", nil, false),
	}
	client, stop := startMockGCS(t, mock)
	defer stop()

	jobs, subs, err := GetDriverJobs(context.Background(), client, "")
	if err != nil {
		t.Fatalf("getDriverJobs: %v", err)
	}
	if len(jobs) != 1 || jobs["0101"] == nil {
		t.Fatalf("driver jobs = %+v", jobs)
	}
	if len(subs) != 1 || subs["sub_1"] == nil || subs["sub_1"].ID != "0102" {
		t.Fatalf("submission drivers = %+v", subs)
	}
	jd := jobs["0101"]
	if jd.Type != JobTypeDriver || jd.Status != JobStatusRunning {
		t.Fatalf("driver job = %+v", jd)
	}
	if jd.RuntimeEnv["working_dir"] != "gcs://x.zip" {
		t.Fatalf("runtime_env = %+v", jd.RuntimeEnv)
	}
	if jd.DriverInfo == nil || jd.DriverInfo.NodeIPAddress != "10.0.0.1" || jd.DriverInfo.PID != "1234" {
		t.Fatalf("driver info = %+v", jd.DriverInfo)
	}

	// Dead driver maps to SUCCEEDED.
	mock.jobInfo = []*proto.JobTableData{sampleDriverTable("0104", "default", nil, true)}
	jobs, _, _ = GetDriverJobs(context.Background(), client, "")
	if jobs["0104"].Status != JobStatusSucceeded {
		t.Fatalf("dead driver status = %s", jobs["0104"].Status)
	}
}

func TestFindJobByIDs(t *testing.T) {
	mock := newMockGCS()
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0101", "default", nil, false),
		sampleDriverTable("0102", "default", map[string]string{jobIDMetadataKey: "sub_1"}, false),
	}
	client, stop := startMockGCS(t, mock)
	defer stop()
	info := NewJobInfoStorageClient(client)
	ctx := context.Background()
	// Put the submission job info in the KV.
	if _, err := info.PutInfo(ctx, "sub_1", &JobInfo{
		Status:                 JobStatusRunning,
		Entrypoint:             "echo sub",
		Message:                ptrString("running"),
		DriverAgentHTTPAddress: ptrString("http://1.2.3.4:8266"),
		DriverNodeID:           ptrString("node1"),
	}, true); err != nil {
		t.Fatal(err)
	}

	// Found by driver job id.
	job, err := findJobByIDs(ctx, client, info, "0101")
	if err != nil || job == nil || job.Type != JobTypeDriver {
		t.Fatalf("find by job id: job=%+v err=%v", job, err)
	}

	// Found by submission id.
	job, err = findJobByIDs(ctx, client, info, "sub_1")
	if err != nil || job == nil {
		t.Fatalf("find by submission id: job=%+v err=%v", job, err)
	}
	if job.Type != JobTypeSubmission || job.SubmissionID == nil || *job.SubmissionID != "sub_1" {
		t.Fatalf("submission job = %+v", job)
	}
	if job.JobID == nil || *job.JobID != "0102" {
		t.Fatalf("submission job job_id = %v", job.JobID)
	}
	if job.DriverInfo == nil || job.DriverInfo.ID != "0102" {
		t.Fatalf("submission driver = %+v", job.DriverInfo)
	}
	if job.DriverAgentHTTPAddress == nil || *job.DriverAgentHTTPAddress != "http://1.2.3.4:8266" {
		t.Fatalf("agent addr = %v", job.DriverAgentHTTPAddress)
	}

	// Found by driver id of a submission job.
	job, err = findJobByIDs(ctx, client, info, "0102")
	if err != nil || job == nil || job.Type != JobTypeSubmission {
		t.Fatalf("find by submission driver id: job=%+v err=%v", job, err)
	}

	// Missing.
	job, err = findJobByIDs(ctx, client, info, "nope")
	if err != nil || job != nil {
		t.Fatalf("missing job: job=%+v err=%v", job, err)
	}
}

func TestJobHeadNameAndHealth(t *testing.T) {
	j := New(&head.HeadConfig{}, nil)
	if j.Name() != "JobHead" {
		t.Fatalf("name = %q", j.Name())
	}
	if !j.Healthy() {
		t.Fatal("should be healthy")
	}
	if err := j.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestVersionEndpoint(t *testing.T) {
	j := New(&head.HeadConfig{SessionName: "sess_abc"}, nil)
	rr := serve(t, j, "GET", "/api/version", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	var v struct {
		Version     string `json:"version"`
		RayVersion  string `json:"ray_version"`
		RayCommit   string `json:"ray_commit"`
		SessionName string `json:"session_name"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if v.Version != "4" || v.RayVersion != "3.0.0.dev0" || v.RayCommit == "" || v.SessionName != "sess_abc" {
		t.Fatalf("version = %+v", v)
	}
	// Bare JSON: no result/msg/data wrapper.
	if strings.Contains(rr.Body.String(), `"result"`) {
		t.Fatalf("version body should be bare JSON, got %s", rr.Body.String())
	}
}

// startMockAgent spins up a mock JobAgent HTTP server. The handler receives
// the request and can inject a JSON response or an error status.
func startMockAgent(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(srv.Close)
	return srv
}

func TestSubmitJobForwardsToAgent(t *testing.T) {
	mock := newMockGCS()
	mock.put(kvNamespaceJob, kvHeadNodeIDKey, []byte("head01"))
	mock.put(kvNamespaceDashboard, dashboardAgentAddrNodeIDPrefix+"head01", []byte(`["127.0.0.1",9999,9998]`))
	j, stop := newJobHead(t, mock)
	defer stop()

	var gotBody map[string]interface{}
	var gotPath string
	agent := startMockAgent(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"job_id":"sub_1","submission_id":"sub_1"}`))
	})
	// Point the head node agent at the mock: overwrite the KV address port.
	mock.put(kvNamespaceDashboard, dashboardAgentAddrNodeIDPrefix+"head01",
		[]byte(`["127.0.0.1",`+agentPort(agent)+`,9998]`))

	body := `{"entrypoint":"python run.py","runtime_env":{"env_vars":{"A":"B"}}}`
	rr := serve(t, j, "POST", "/api/jobs/", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if gotPath != "/api/job_agent/jobs/" {
		t.Fatalf("agent path = %q", gotPath)
	}
	if gotBody["entrypoint"] != "python run.py" {
		t.Fatalf("agent body = %+v", gotBody)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["submission_id"] != "sub_1" {
		t.Fatalf("submit response = %+v", resp)
	}
}

func agentPort(srv *httptest.Server) string {
	addr := srv.Listener.Addr().String()
	return addr[strings.LastIndex(addr, ":")+1:]
}

func TestSubmitJobBadEntrypoint(t *testing.T) {
	mock := newMockGCS()
	j, stop := newJobHead(t, mock)
	defer stop()
	rr := serve(t, j, "POST", "/api/jobs/", `{"entrypoint":""}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

// TestSubmitJobTypeValidation verifies malformed field types are rejected with
// 400, aligned with JobSubmitRequest.__post_init__ validation: a non-string
// entrypoint and a non-dict runtime_env must both be rejected.
func TestSubmitJobTypeValidation(t *testing.T) {
	mock := newMockGCS()
	j, stop := newJobHead(t, mock)
	defer stop()
	for _, body := range []string{
		`{"entrypoint":123}`,
		`{"entrypoint":"x","runtime_env":"not-a-dict"}`,
		`{"entrypoint":"x","metadata":"not-a-dict"}`,
	} {
		rr := serve(t, j, "POST", "/api/jobs/", body)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("body %s: status = %d, want 400", body, rr.Code)
		}
	}
}

func TestStopDeleteGetLogsRoutes(t *testing.T) {
	mock := newMockGCS()
	// Driver jobs + submission driver + KV job info for sub_1.
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0101", "default", nil, false),
		sampleDriverTable("0102", "default", map[string]string{jobIDMetadataKey: "sub_1"}, false),
	}
	j, stop := newJobHead(t, mock)
	defer stop()

	// Mock agent: stop/delete/logs each return the right JSON.
	agent := startMockAgent(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/stop"):
			_, _ = w.Write([]byte(`{"stopped":true}`))
		case r.Method == http.MethodDelete:
			_, _ = w.Write([]byte(`{"deleted":true}`))
		case strings.HasSuffix(r.URL.Path, "/logs"):
			_, _ = w.Write([]byte(`{"logs":"hello log"}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	})
	// Wire the head-node agent address to the mock so stop/delete forward there.
	mock.put(kvNamespaceJob, kvHeadNodeIDKey, []byte("head01"))
	mock.put(kvNamespaceDashboard, dashboardAgentAddrNodeIDPrefix+"head01", []byte(`["127.0.0.1",`+agentPort(agent)+`,1]`))
	// Point the job's driver agent address at the mock agent so the logs route
	// uses it.
	if _, err := j.info.PutInfo(context.Background(), "sub_1", &JobInfo{
		Status: JobStatusRunning, Entrypoint: "echo",
		DriverAgentHTTPAddress: ptrString("http://" + agent.Listener.Addr().String()),
	}, true); err != nil {
		t.Fatal(err)
	}

	// Stop.
	rr := serve(t, j, "POST", "/api/jobs/sub_1/stop", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("stop status = %d body=%s", rr.Code, rr.Body.String())
	}
	var stopResp map[string]interface{}
	_ = json.Unmarshal(rr.Body.Bytes(), &stopResp)
	if stopResp["stopped"] != true {
		t.Fatalf("stop response = %+v", stopResp)
	}

	// Delete.
	rr = serve(t, j, "DELETE", "/api/jobs/sub_1", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("delete status = %d body=%s", rr.Code, rr.Body.String())
	}
	var delResp map[string]interface{}
	_ = json.Unmarshal(rr.Body.Bytes(), &delResp)
	if delResp["deleted"] != true {
		t.Fatalf("delete response = %+v", delResp)
	}

	// Get job info.
	rr = serve(t, j, "GET", "/api/jobs/sub_1", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("get status = %d body=%s", rr.Code, rr.Body.String())
	}
	var jd JobDetails
	if err := json.Unmarshal(rr.Body.Bytes(), &jd); err != nil {
		t.Fatal(err)
	}
	if jd.Type != JobTypeSubmission || jd.SubmissionID == nil || *jd.SubmissionID != "sub_1" {
		t.Fatalf("job details = %+v", jd)
	}

	// Logs via the driver agent client (driver_agent_http_address in the KV).
	rr = serve(t, j, "GET", "/api/jobs/sub_1/logs", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("logs status = %d body=%s", rr.Code, rr.Body.String())
	}
	var logs JobLogsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &logs); err != nil {
		t.Fatal(err)
	}
	if logs.Logs != "hello log" {
		t.Fatalf("logs = %+v", logs)
	}
}

func TestJobErrorPaths(t *testing.T) {
	mock := newMockGCS()
	j, stop := newJobHead(t, mock)
	defer stop()

	// Missing job -> 404.
	for _, tc := range []struct{ method, path string }{
		{"GET", "/api/jobs/nope"},
		{"POST", "/api/jobs/nope/stop"},
		{"DELETE", "/api/jobs/nope"},
		{"GET", "/api/jobs/nope/logs"},
	} {
		rr := serve(t, j, tc.method, tc.path, "")
		if rr.Code != http.StatusNotFound {
			t.Fatalf("%s %s status = %d, want 404", tc.method, tc.path, rr.Code)
		}
	}

	// Driver job (non-submission) -> 400 for stop/delete/logs.
	mock.jobInfo = []*proto.JobTableData{sampleDriverTable("0101", "default", nil, false)}
	for _, tc := range []struct{ method, path string }{
		{"POST", "/api/jobs/0101/stop"},
		{"DELETE", "/api/jobs/0101"},
		{"GET", "/api/jobs/0101/logs"},
	} {
		rr := serve(t, j, tc.method, tc.path, "")
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("%s %s status = %d, want 400", tc.method, tc.path, rr.Code)
		}
	}
}

func TestListJobs(t *testing.T) {
	mock := newMockGCS()
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0101", "default", nil, false),
		sampleDriverTable("0102", "default", map[string]string{jobIDMetadataKey: "sub_1"}, false),
		sampleDriverTable("0103", "_ray_internal_dashboard", nil, false),
	}
	j, stop := newJobHead(t, mock)
	defer stop()
	if _, err := j.info.PutInfo(context.Background(), "sub_1", &JobInfo{
		Status: JobStatusRunning, Entrypoint: "echo",
	}, true); err != nil {
		t.Fatal(err)
	}

	rr := serve(t, j, "GET", "/api/jobs/", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var items []map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &items); err != nil {
		t.Fatal(err)
	}
	// sub_1 submission + d1 driver. The internal d3 driver is excluded.
	if len(items) != 2 {
		t.Fatalf("list jobs = %+v", items)
	}
	if items[0]["type"] != "SUBMISSION" || items[1]["type"] != "DRIVER" {
		t.Fatalf("job order/types = %s, %s", items[0]["type"], items[1]["type"])
	}
}

// TestListJobsPreservesKVScanOrder pins the submission job order to the GCS
// internal KV scan order (not a submission-id sort), matching the Python
// dashboard list_jobs which iterates the dict built from
// async_internal_kv_keys in order. The mock KV returns keys in sorted order.
func TestListJobsPreservesKVScanOrder(t *testing.T) {
	mock := newMockGCS()
	j, stop := newJobHead(t, mock)
	defer stop()
	// Write the submission infos in a deliberately non-sorted order; the keys
	// are _ray_internal_job_info_<id>, so the KV scan order is id-lexicographic.
	for _, id := range []string{"job_c", "job_a", "job_b"} {
		if _, err := j.info.PutInfo(context.Background(), id, &JobInfo{
			Status: JobStatusRunning, Entrypoint: "echo",
		}, true); err != nil {
			t.Fatal(err)
		}
	}

	rr := serve(t, j, "GET", "/api/jobs/", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var items []map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("list jobs = %+v", items)
	}
	want := []string{"job_a", "job_b", "job_c"}
	for i, id := range want {
		if items[i]["submission_id"] != id {
			t.Fatalf("items[%d].submission_id = %v, want %s (KV scan order)", i, items[i]["submission_id"], id)
		}
	}
}

// TestTailJobLogsWebSocket spins up a mock agent WebSocket server that emits
// log lines and verifies the JobHead forwards them to the client WebSocket.
func TestTailJobLogsWebSocket(t *testing.T) {
	mock := newMockGCS()
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0102", "default", map[string]string{jobIDMetadataKey: "sub_1"}, false),
	}
	j, stop := newJobHead(t, mock)
	defer stop()
	// The KV job info carries the driver agent address, so the polling loop
	// resolves it immediately (no need to wait for the supervisor actor).
	if _, err := j.info.PutInfo(context.Background(), "sub_1", &JobInfo{
		Status: JobStatusRunning, Entrypoint: "echo",
		DriverAgentHTTPAddress: ptrString("http://agent-addr"),
		DriverNodeID:           ptrString("nodeX"),
	}, true); err != nil {
		t.Fatal(err)
	}

	// Mock agent WebSocket server: emits two text lines then closes.
	agentSent := make(chan struct{})
	var agentSrv *httptest.Server
	agentSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/job_agent/jobs/sub_1/logs/tail" {
			t.Errorf("unexpected agent path %s", r.URL.Path)
			return
		}
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		_ = ws.Write(context.Background(), websocket.MessageText, []byte("line one\n"))
		_ = ws.Write(context.Background(), websocket.MessageText, []byte("line two\n"))
		_ = ws.Close(websocket.StatusNormalClosure, "")
		close(agentSent)
	}))
	defer agentSrv.Close()

	// Route the job's driver node to the mock agent. The JobHead agent cache
	// maps driver node id -> agent client. To avoid polluting the cache, we
	// pre-seed the agent client through getJobDriverAgentClient by setting the
	// KV driver_agent_http_address to the mock agent.
	if _, err := j.info.PutInfo(context.Background(), "sub_1", &JobInfo{
		Status: JobStatusRunning, Entrypoint: "echo",
		DriverAgentHTTPAddress: ptrString("http://" + agentSrv.Listener.Addr().String()),
		DriverNodeID:           ptrString("nodeX"),
	}, true); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	if err := j.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	// Use a real HTTP server so the WebSocket upgrade works.
	headSrv := httptest.NewServer(mux)
	defer headSrv.Close()

	wsURL := "ws" + strings.TrimPrefix(headSrv.URL, "http") + "/api/jobs/sub_1/logs/tail"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial head ws: %v", err)
	}
	defer conn.CloseNow()

	var got []string
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			break
		}
		if typ == websocket.MessageText {
			got = append(got, string(data))
		}
	}
	if len(got) != 2 || got[0] != "line one\n" || got[1] != "line two\n" {
		t.Fatalf("forwarded lines = %q", got)
	}
	<-agentSent
}

// TestTailJobLogsTerminalJobNoAgent verifies the WebSocket closes promptly when
// the job is terminal and no driver agent address is known.
func TestTailJobLogsTerminalJobNoAgent(t *testing.T) {
	mock := newMockGCS()
	j, stop := newJobHead(t, mock)
	defer stop()
	if _, err := j.info.PutInfo(context.Background(), "sub_term", &JobInfo{
		Status: JobStatusFailed, Entrypoint: "echo",
	}, true); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	if err := j.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	headSrv := httptest.NewServer(mux)
	defer headSrv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(headSrv.URL, "http") + "/api/jobs/sub_term/logs/tail"
	start := time.Now()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	// Server closes the WS when it detects the terminal job without an agent.
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("expected the server to close the WS")
	}
	if time.Since(start) > 4*time.Second {
		t.Fatal("terminal job without agent should close the WS quickly")
	}
}

func TestComponentActivities(t *testing.T) {
	mock := newMockGCS()
	// One active non-internal driver, one internal, one dead.
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0101", "default", nil, false),
		sampleDriverTable("0102", "_ray_internal_dashboard", nil, false),
		sampleDriverTable("0103", "default", nil, true),
	}
	j, stop := newJobHead(t, mock)
	defer stop()

	rr := serve(t, j, "GET", "/api/component_activities", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		Driver struct {
			IsActive string  `json:"is_active"`
			Reason   *string `json:"reason"`
		} `json:"driver"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Driver.IsActive != string(RayActivityActive) {
		t.Fatalf("is_active = %s", body.Driver.IsActive)
	}
	if body.Driver.Reason == nil || *body.Driver.Reason != "Number of active drivers: 1" {
		t.Fatalf("reason = %v", body.Driver.Reason)
	}

	// All dead/internal -> inactive.
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0102", "_ray_internal_dashboard", nil, false),
		sampleDriverTable("0103", "default", nil, true),
	}
	rr = serve(t, j, "GET", "/api/component_activities", "")
	_ = json.Unmarshal(rr.Body.Bytes(), &body)
	if body.Driver.IsActive != string(RayActivityInactive) {
		t.Fatalf("is_active = %s, want INACTIVE", body.Driver.IsActive)
	}
}

// TestComponentActivitiesLastActivityAtKey verifies the last_activity_at key is
// always present in the driver response (null when no job has finished), aligned
// with the Python RayActivityResponse pydantic model which always emits the
// field.
func TestComponentActivitiesLastActivityAtKey(t *testing.T) {
	mock := newMockGCS()
	// A running non-internal driver with EndTime=0: no finished job, so
	// last_activity_at is null but the key is present.
	noEnd := sampleDriverTable("0101", "default", nil, false)
	noEnd.EndTime = 0
	mock.jobInfo = []*proto.JobTableData{noEnd}
	j, stop := newJobHead(t, mock)
	defer stop()

	rr := serve(t, j, "GET", "/api/component_activities", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		Driver map[string]interface{} `json:"driver"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body.Driver["last_activity_at"]; !ok {
		t.Fatalf("last_activity_at key missing: %v", body.Driver)
	}
	if v := body.Driver["last_activity_at"]; v != nil {
		t.Fatalf("last_activity_at = %v, want null", v)
	}

	// A finished non-internal driver sets last_activity_at to its end time.
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0103", "default", nil, true),
	}
	rr = serve(t, j, "GET", "/api/component_activities", "")
	_ = json.Unmarshal(rr.Body.Bytes(), &body)
	if v, ok := body.Driver["last_activity_at"].(float64); !ok || v <= 0 {
		t.Fatalf("last_activity_at = %v, want a timestamp", body.Driver["last_activity_at"])
	}
}

// TestComponentActivitiesTimeoutQuery verifies the timeout query parameter is
// accepted and the driver activity is still computed, aligned with
// JobHead.get_component_activities.
func TestComponentActivitiesTimeoutQuery(t *testing.T) {
	mock := newMockGCS()
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0101", "default", nil, false),
	}
	j, stop := newJobHead(t, mock)
	defer stop()
	rr := serve(t, j, "GET", "/api/component_activities?timeout=5", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		Driver struct {
			IsActive string `json:"is_active"`
		} `json:"driver"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Driver.IsActive != string(RayActivityActive) {
		t.Fatalf("is_active = %s", body.Driver.IsActive)
	}
}

// TestComponentActivitiesHook verifies the RAY_CLUSTER_ACTIVITY_HOOK executable
// is invoked and its JSON output merged into the response, aligned with
// JobHead.get_component_activities.
func TestComponentActivitiesHook(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	script := filepath.Join(t.TempDir(), "hook.sh")
	hookBody := `{"test_component": {"is_active": "ACTIVE", "reason": "hook ran", "timestamp": 123.0}}`
	if err := os.WriteFile(script, []byte("#!/bin/sh\ncat <<'EOF'\n"+hookBody+"\nEOF\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(rayClusterActivityHookEnv, script)

	mock := newMockGCS()
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0102", "_ray_internal_dashboard", nil, false),
	}
	j, stop := newJobHead(t, mock)
	defer stop()
	rr := serve(t, j, "GET", "/api/component_activities", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		Driver            map[string]interface{} `json:"driver"`
		TestComponent     map[string]interface{} `json:"test_component"`
		ExternalComponent map[string]interface{} `json:"external_component"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.TestComponent["is_active"] != "ACTIVE" {
		t.Fatalf("test_component = %v", body.TestComponent)
	}
	if body.ExternalComponent != nil {
		t.Fatalf("external_component should be absent, got %v", body.ExternalComponent)
	}
}

func TestPackageEndpoints(t *testing.T) {
	mock := newMockGCS()
	j, stop := newJobHead(t, mock)
	defer stop()

	// PUT stores the package.
	rr := serve(t, j, "PUT", "/api/packages/gcs/my_pkg.zip", "packagedata")
	if rr.Code != http.StatusOK {
		t.Fatalf("put status = %d body=%s", rr.Code, rr.Body.String())
	}
	if got := mock.get("", "gcs://my_pkg.zip"); string(got) != "packagedata" {
		t.Fatalf("stored package = %q", got)
	}

	// GET returns 200 for an existing package.
	rr = serve(t, j, "GET", "/api/packages/gcs/my_pkg.zip", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("get status = %d", rr.Code)
	}

	// GET returns 404 for a missing package.
	rr = serve(t, j, "GET", "/api/packages/gcs/missing.zip", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("missing package status = %d", rr.Code)
	}
}

// TestPackageEndpointsInvalidProtocol verifies an unsupported URI scheme is
// rejected with 500 on both GET and PUT, matching the Python ValueError raised
// by parse_uri (which surfaces as an HTTP 500).
func TestPackageEndpointsInvalidProtocol(t *testing.T) {
	mock := newMockGCS()
	j, stop := newJobHead(t, mock)
	defer stop()

	for _, method := range []string{"GET", "PUT"} {
		rr := serve(t, j, method, "/api/packages/uri/my_pkg.zip", "packagedata")
		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("%s invalid protocol status = %d, want 500", method, rr.Code)
		}
		if !strings.Contains(rr.Body.String(), "Invalid protocol for runtime_env URI") {
			t.Fatalf("%s body = %q", method, rr.Body.String())
		}
	}
	if got := mock.get("", "uri://my_pkg.zip"); len(got) != 0 {
		t.Fatalf("invalid protocol must not be stored, got %q", got)
	}
}
