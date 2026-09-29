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

package flow

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/internal/dashboard/head/modules/job"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
)

// mockGCS simulates the GCS services used by the flow module.
type mockGCS struct {
	proto.UnimplementedInternalKVGcsServiceServer
	proto.UnimplementedJobInfoGcsServiceServer

	mu      sync.Mutex
	store   map[string][]byte
	jobInfo []*proto.JobTableData
}

func newMockGCS() *mockGCS { return &mockGCS{store: map[string][]byte{}} }

func (s *mockGCS) nskey(ns, key []byte) string { return string(ns) + "\x00" + string(key) }

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

func (s *mockGCS) splitKey(k string) (string, string) {
	i := strings.IndexByte(k, 0)
	return k[:i], k[i+1:]
}

func (s *mockGCS) GetAllJobInfo(ctx context.Context, req *proto.GetAllJobInfoRequest) (*proto.GetAllJobInfoReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &proto.GetAllJobInfoReply{JobInfoList: s.jobInfo}, nil
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

// newFlowHead builds a FlowHead backed by a mock GCS client.
func newFlowHead(t *testing.T, mock *mockGCS) (*FlowHead, func()) {
	t.Helper()
	client, stop := startMockGCS(t, mock)
	f := New(&head.HeadConfig{SessionName: "session_test"}, client)
	return f, stop
}

// serve performs an HTTP request against the FlowHead mux.
func serve(t *testing.T, f *FlowHead, method, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	if err := f.RegisterHTTP(mux); err != nil {
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

func agentPort(srv *httptest.Server) string {
	addr := srv.Listener.Addr().String()
	return addr[strings.LastIndex(addr, ":")+1:]
}

func TestFlowHeadNameAndHealth(t *testing.T) {
	f := New(&head.HeadConfig{}, nil)
	if f.Name() != "FlowHead" {
		t.Fatalf("name = %q", f.Name())
	}
	if !f.Healthy() {
		t.Fatal("should be healthy")
	}
	if err := f.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestGenerateIDs(t *testing.T) {
	for i := 0; i < 100; i++ {
		id := generateFlowJobID()
		if !strings.HasPrefix(id, "flowsubmit_") || len(id) != len("flowsubmit_")+16 {
			t.Fatalf("flow job id = %q", id)
		}
		for _, c := range id[len("flowsubmit_"):] {
			if strings.ContainsRune("IlOo0", c) {
				t.Fatalf("flow job id %q contains confusing char %c", id, c)
			}
		}
		pid := generatePluginID()
		if !strings.HasPrefix(pid, "plugin_") || len(pid) != len("plugin_")+16 {
			t.Fatalf("plugin id = %q", pid)
		}
	}
}

func TestBuildJobEntrypoint(t *testing.T) {
	got := buildJobEntrypoint(`{"stages":[]}`)
	want := "python -m ray.flow.driver '{\"stages\":[]}'"
	if got != want {
		t.Fatalf("entrypoint = %q, want %q", got, want)
	}
	// shlex-like quoting of embedded quotes.
	got = buildJobEntrypoint("it's")
	if !strings.Contains(got, `'it'\''s'`) {
		t.Fatalf("entrypoint with quote = %q", got)
	}
}

func TestValidateFlowJSON(t *testing.T) {
	valid := `{"strategy":"group_by_group","stages":[{"name":"a","target":"t","type":"python"}]}`
	if err := validateFlowJSON(valid); err != nil {
		t.Fatalf("valid json rejected: %v", err)
	}
	for name, bad := range map[string]string{
		"empty":           "",
		"bad json":        "not-json",
		"no stages":       `{"strategy":"all_at_once"}`,
		"stages dict":     `{"stages":{"a":1}}`,
		"empty list":      `{"stages":[]}`,
		"bad strategy":    `{"strategy":"parallel","stages":[{"name":"a","target":"t","type":"python"}]}`,
		"stage no name":   `{"stages":[{"target":"t","type":"python"}]}`,
		"stage no target": `{"stages":[{"name":"a","type":"python"}]}`,
		"stage no type":   `{"stages":[{"name":"a","target":"t"}]}`,
		"stage not dict":  `{"stages":["a"]}`,
	} {
		if err := validateFlowJSON(bad); err == nil {
			t.Fatalf("invalid json accepted: %s", name)
		}
	}
}

func TestPluginDataToDetails(t *testing.T) {
	d := pluginDataToDetails(map[string]interface{}{
		"id": "plugin_1", "name": "n", "path": "p", "version": "v",
		"workgroup": "wg", "description": "d", "metadata": map[string]interface{}{"k": "v"},
	})
	if d.ID != "plugin_1" || d.Name != "n" || d.Path == nil || *d.Path != "p" {
		t.Fatalf("details = %+v", d)
	}
	if d.Metadata["k"] != "v" {
		t.Fatalf("metadata = %+v", d.Metadata)
	}
	// Missing optional fields are nil.
	empty := pluginDataToDetails(map[string]interface{}{})
	if empty.ID != "" || empty.Path != nil || empty.Metadata != nil {
		t.Fatalf("empty details = %+v", empty)
	}
}

// startMockAgent spins up a mock JobAgent HTTP server.
func startMockAgent(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(srv.Close)
	return srv
}

// setFlowDepsInstalled installs a fake dependency probe reporting ray[flow]
// as installed (or not), so submit_job tests do not touch the cross-language
// runtime.
func setFlowDepsInstalled(t *testing.T, j *FlowHead, installed bool) {
	t.Helper()
	orig := checkFlowDepsFn
	checkFlowDepsFn = func(h *FlowHead) error {
		if !installed {
			return errFlowDependenciesNotInstalled
		}
		return nil
	}
	t.Cleanup(func() { checkFlowDepsFn = orig })
	j.depsMu.Lock()
	defer j.depsMu.Unlock()
	j.depsChecked = false
}

func TestSubmitFlowJobForwardsToAgent(t *testing.T) {
	mock := newMockGCS()
	mock.put(kvNamespaceJob, kvHeadNodeIDKey, []byte("head01"))
	j, stop := newFlowHead(t, mock)
	defer stop()
	setFlowDepsInstalled(t, j, true)

	var gotBody map[string]interface{}
	var gotPath string
	agent := startMockAgent(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"job_id":"sub_1","submission_id":"sub_1"}`))
	})
	mock.put(kvNamespaceDashboard, dashboardAgentAddrNodeIDPrefix+"head01",
		[]byte(`["127.0.0.1",`+agentPort(agent)+`,9998]`))

	body := `{"entrypoint":"{\"stages\":[{\"name\":\"a\",\"target\":\"t\",\"type\":\"python\"}]}","runtime_env":{"env_vars":{"A":"B"}}}`
	rr := serve(t, j, "POST", "/api/flow/jobs/", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if gotPath != "/api/job_agent/jobs/" {
		t.Fatalf("agent path = %q", gotPath)
	}
	// The entrypoint is wrapped with the flow driver.
	if ep, ok := gotBody["entrypoint"].(string); !ok || !strings.HasPrefix(ep, "python -m ray.flow.driver ") {
		t.Fatalf("agent entrypoint = %v", gotBody["entrypoint"])
	}
	// submission_id is generated with the flowsubmit_ prefix.
	sid, _ := gotBody["submission_id"].(string)
	if !strings.HasPrefix(sid, "flowsubmit_") {
		t.Fatalf("submission_id = %q", sid)
	}
	// metadata carries job_type=flow.
	md, _ := gotBody["metadata"].(map[string]interface{})
	if md["job_type"] != "flow" {
		t.Fatalf("metadata = %+v", md)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["submission_id"] != "sub_1" {
		t.Fatalf("submit response = %+v", resp)
	}
}

func TestSubmitFlowJobBadRequest(t *testing.T) {
	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()
	setFlowDepsInstalled(t, j, true)
	// Invalid flow JSON -> 400.
	rr := serve(t, j, "POST", "/api/flow/jobs/", `{"entrypoint":"not-json"}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	// Missing entrypoint -> 400.
	rr = serve(t, j, "POST", "/api/flow/jobs/", `{}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

// TestSubmitFlowJobDependenciesNotInstalled verifies the 501 returned when the
// ray[flow] dependencies are missing, aligned with validate_endpoint in
// python/ray/dashboard/modules/flow/flow_head.py: the dependency pre-check runs
// before body validation, so even an empty body yields 501 with the Python
// message.
func TestSubmitFlowJobDependenciesNotInstalled(t *testing.T) {
	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()
	setFlowDepsInstalled(t, j, false)

	rr := serve(t, j, "POST", "/api/flow/jobs/", ``)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rr.Code)
	}
	want := "Flow dependencies are not installed. Please run `pip install \"ray[flow]\"`."
	if body := strings.TrimSpace(rr.Body.String()); body != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
	// The pre-check short-circuits even a well-formed body.
	rr = serve(t, j, "POST", "/api/flow/jobs/", `{"entrypoint":"{\"stages\":[]}"}`)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rr.Code)
	}
	// A second request hits the cached negative result.
	rr = serve(t, j, "POST", "/api/flow/jobs/", ``)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("cached status = %d, want 501", rr.Code)
	}
}

func TestStopDeleteGetLogsRoutes(t *testing.T) {
	mock := newMockGCS()
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0101", "default", nil, false),
		sampleDriverTable("0102", "default", map[string]string{jobIDMetadataKey: "flowsubmit_abc"}, false),
	}
	j, stop := newFlowHead(t, mock)
	defer stop()

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
	mock.put(kvNamespaceJob, kvHeadNodeIDKey, []byte("head01"))
	mock.put(kvNamespaceDashboard, dashboardAgentAddrNodeIDPrefix+"head01", []byte(`["127.0.0.1",`+agentPort(agent)+`,1]`))
	if _, err := j.info.PutInfo(context.Background(), "flowsubmit_abc", &job.JobInfo{
		Status: job.JobStatusRunning, Entrypoint: "echo",
		Metadata:               map[string]string{flowMetadataKey: flowMetadataValue},
		DriverAgentHTTPAddress: ptrString("http://" + agent.Listener.Addr().String()),
	}, true); err != nil {
		t.Fatal(err)
	}

	// Stop.
	rr := serve(t, j, "POST", "/api/flow/jobs/flowsubmit_abc/stop", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("stop status = %d body=%s", rr.Code, rr.Body.String())
	}
	var stopResp map[string]interface{}
	_ = json.Unmarshal(rr.Body.Bytes(), &stopResp)
	if stopResp["stopped"] != true {
		t.Fatalf("stop response = %+v", stopResp)
	}

	// Delete.
	rr = serve(t, j, "DELETE", "/api/flow/jobs/flowsubmit_abc", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("delete status = %d body=%s", rr.Code, rr.Body.String())
	}
	var delResp map[string]interface{}
	_ = json.Unmarshal(rr.Body.Bytes(), &delResp)
	if delResp["deleted"] != true {
		t.Fatalf("delete response = %+v", delResp)
	}

	// Get job info.
	rr = serve(t, j, "GET", "/api/flow/jobs/flowsubmit_abc", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("get status = %d body=%s", rr.Code, rr.Body.String())
	}
	var jd job.JobDetails
	if err := json.Unmarshal(rr.Body.Bytes(), &jd); err != nil {
		t.Fatal(err)
	}
	if jd.Type != job.JobTypeSubmission || jd.SubmissionID == nil || *jd.SubmissionID != "flowsubmit_abc" {
		t.Fatalf("job details = %+v", jd)
	}

	// Logs via the driver agent client.
	rr = serve(t, j, "GET", "/api/flow/jobs/flowsubmit_abc/logs", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("logs status = %d body=%s", rr.Code, rr.Body.String())
	}
	var logs job.JobLogsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &logs); err != nil {
		t.Fatal(err)
	}
	if logs.Logs != "hello log" {
		t.Fatalf("logs = %+v", logs)
	}
}

func TestFlowJobErrorPaths(t *testing.T) {
	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()

	for _, tc := range []struct{ method, path string }{
		{"GET", "/api/flow/jobs/nope"},
		{"POST", "/api/flow/jobs/nope/stop"},
		{"DELETE", "/api/flow/jobs/nope"},
		{"GET", "/api/flow/jobs/nope/logs"},
	} {
		rr := serve(t, j, tc.method, tc.path, "")
		if rr.Code != http.StatusNotFound {
			t.Fatalf("%s %s status = %d, want 404", tc.method, tc.path, rr.Code)
		}
	}

	// Driver job (non-submission) -> 400 for stop/delete/logs.
	mock.jobInfo = []*proto.JobTableData{sampleDriverTable("0101", "default", nil, false)}
	for _, tc := range []struct{ method, path string }{
		{"POST", "/api/flow/jobs/0101/stop"},
		{"DELETE", "/api/flow/jobs/0101"},
		{"GET", "/api/flow/jobs/0101/logs"},
	} {
		rr := serve(t, j, tc.method, tc.path, "")
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("%s %s status = %d, want 400", tc.method, tc.path, rr.Code)
		}
	}
}

func TestListFlowJobs(t *testing.T) {
	mock := newMockGCS()
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0101", "default", nil, false),
		sampleDriverTable("0102", "default", map[string]string{jobIDMetadataKey: "flowsubmit_a"}, false),
	}
	j, stop := newFlowHead(t, mock)
	defer stop()
	// A flow job and a non-flow submission job; only the flow one is listed.
	if _, err := j.info.PutInfo(context.Background(), "flowsubmit_a", &job.JobInfo{
		Status: job.JobStatusRunning, Entrypoint: "echo",
		Metadata: map[string]string{flowMetadataKey: flowMetadataValue},
	}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := j.info.PutInfo(context.Background(), "regular_job", &job.JobInfo{
		Status: job.JobStatusRunning, Entrypoint: "echo",
	}, true); err != nil {
		t.Fatal(err)
	}

	rr := serve(t, j, "GET", "/api/flow/jobs/", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var items []map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("list jobs = %+v", items)
	}
	if items[0]["type"] != "SUBMISSION" || items[0]["submission_id"] != "flowsubmit_a" {
		t.Fatalf("flow job = %+v", items[0])
	}
}

// fakeController is an in-memory flowController implementing the plugin CRUD
// surface, used to test the plugin routes without a live actor.
type fakeController struct {
	mu       sync.Mutex
	alive    bool
	plugins  map[string]map[string]interface{}
	regCalls []struct{ id, name, path string }
}

func newFakeController() *fakeController {
	return &fakeController{alive: true, plugins: map[string]map[string]interface{}{}}
}

func (c *fakeController) checkAlive() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.alive {
		return errDead
	}
	return nil
}

func (c *fakeController) registerPlugin(pluginID, name, path string, version, workgroup, description, metadata interface{}) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.alive {
		return false, errDead
	}
	c.plugins[pluginID] = map[string]interface{}{
		"id": pluginID, "name": name, "path": path,
		"version": version, "workgroup": workgroup, "description": description, "metadata": metadata,
	}
	c.regCalls = append(c.regCalls, struct{ id, name, path string }{pluginID, name, path})
	return true, nil
}

func (c *fakeController) listPlugins() ([]map[string]interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.alive {
		return nil, errDead
	}
	out := make([]map[string]interface{}, 0, len(c.plugins))
	for _, p := range c.plugins {
		out = append(out, p)
	}
	return out, nil
}

func (c *fakeController) getPlugin(pluginID string) (map[string]interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.alive {
		return nil, errDead
	}
	p, ok := c.plugins[pluginID]
	if !ok {
		return nil, nil
	}
	return p, nil
}

func (c *fakeController) deletePlugin(pluginID string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.alive {
		return false, errDead
	}
	if _, ok := c.plugins[pluginID]; !ok {
		return false, nil
	}
	delete(c.plugins, pluginID)
	return true, nil
}

var errDead = context.Canceled

// setController installs a fake controller into the FlowHead cache.
func setController(f *FlowHead, c flowController) {
	f.controllerMu.Lock()
	defer f.controllerMu.Unlock()
	f.controller = c
}

func TestPluginAddSingle(t *testing.T) {
	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()
	fc := newFakeController()
	setController(j, fc)

	rr := serve(t, j, "POST", "/api/flow/plugins/",
		`{"id":"plugin_1","name":"n","path":"p","version":"v","workgroup":"wg","metadata":{"k":"v"}}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["plugin_id"] != "plugin_1" {
		t.Fatalf("response = %+v", resp)
	}
	fc.mu.Lock()
	got := fc.plugins["plugin_1"]
	fc.mu.Unlock()
	if got["name"] != "n" || got["path"] != "p" || got["version"] != "v" {
		t.Fatalf("stored plugin = %+v", got)
	}
}

func TestPluginAddSingleMissingFields(t *testing.T) {
	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()
	setController(j, newFakeController())

	rr := serve(t, j, "POST", "/api/flow/plugins/", `{"name":"only-name"}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	rr = serve(t, j, "POST", "/api/flow/plugins/", `{"path":"only-path"}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestPluginAddBatch(t *testing.T) {
	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()
	fc := newFakeController()
	setController(j, fc)

	rr := serve(t, j, "POST", "/api/flow/plugins/",
		`{"plugins":[{"id":"p1","name":"a","path":"/a"},{"id":"p2","name":"b","path":"/b"}]}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Results map[string]string `json:"results"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Results["p1"] != "success" || resp.Results["p2"] != "success" {
		t.Fatalf("results = %+v", resp.Results)
	}

	// Empty plugins list -> 400.
	rr = serve(t, j, "POST", "/api/flow/plugins/", `{"plugins":[]}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("empty batch status = %d, want 400", rr.Code)
	}
}

func TestPluginList(t *testing.T) {
	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()
	fc := newFakeController()
	if _, err := fc.registerPlugin("p1", "n", "/p", "v", nil, "desc", nil); err != nil {
		t.Fatal(err)
	}
	setController(j, fc)

	rr := serve(t, j, "GET", "/api/flow/plugins/", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var items []map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0]["id"] != "p1" || items[0]["name"] != "n" {
		t.Fatalf("plugins = %+v", items)
	}
}

func TestPluginListNoController(t *testing.T) {
	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()
	// No controller installed -> empty list 200.
	rr := serve(t, j, "GET", "/api/flow/plugins/", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if strings.TrimSpace(rr.Body.String()) != "[]" {
		t.Fatalf("body = %s, want []", rr.Body.String())
	}
}

func TestPluginDeleteAndGet(t *testing.T) {
	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()
	fc := newFakeController()
	if _, err := fc.registerPlugin("p1", "n", "/p", nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	setController(j, fc)

	// Get existing.
	rr := serve(t, j, "GET", "/api/flow/plugins/p1", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("get status = %d body=%s", rr.Code, rr.Body.String())
	}
	var d map[string]interface{}
	_ = json.Unmarshal(rr.Body.Bytes(), &d)
	if d["id"] != "p1" || d["name"] != "n" {
		t.Fatalf("plugin details = %+v", d)
	}

	// Get missing -> 404.
	rr = serve(t, j, "GET", "/api/flow/plugins/missing", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("missing get status = %d, want 404", rr.Code)
	}

	// Delete existing.
	rr = serve(t, j, "DELETE", "/api/flow/plugins/p1", "")
	if rr.Code != http.StatusOK {
		t.Fatalf("delete status = %d body=%s", rr.Code, rr.Body.String())
	}
	var del map[string]interface{}
	_ = json.Unmarshal(rr.Body.Bytes(), &del)
	if del["deleted"] != true || del["plugin_id"] != "p1" {
		t.Fatalf("delete response = %+v", del)
	}
}

func TestPluginControllerUnavailable(t *testing.T) {
	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()
	// No controller: delete/get -> 503.
	rr := serve(t, j, "DELETE", "/api/flow/plugins/p1", "")
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("delete status = %d, want 503", rr.Code)
	}
	rr = serve(t, j, "GET", "/api/flow/plugins/p1", "")
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("get status = %d, want 503", rr.Code)
	}
	// Single add without a controller: ensure_flow_controller tries to create
	// one; the production startFlowControllerFn fails without a runtime and the
	// handler returns 500, aligned with the Python raise in ensure_flow_controller.
	rr = serve(t, j, "POST", "/api/flow/plugins/", `{"name":"n","path":"/p"}`)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("add status = %d, want 500", rr.Code)
	}
}

// TestPluginAddSingleAutoCreatesController verifies the B-D3 behavior: when no
// FlowController is running, add_plugin auto-creates one (via
// startFlowControllerFn, mirroring flow.start()) and then registers the plugin,
// like the Python ensure_flow_controller.
func TestPluginAddSingleAutoCreatesController(t *testing.T) {
	origStart := startFlowControllerFn
	fc := newFakeController()
	startFlowControllerFn = func(ctx context.Context, h *FlowHead) (flowController, error) {
		return fc, nil
	}
	defer func() { startFlowControllerFn = origStart }()

	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()

	rr := serve(t, j, "POST", "/api/flow/plugins/", `{"name":"n","path":"/p"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]interface{}
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["plugin_id"] == "" {
		t.Fatalf("response = %+v", resp)
	}
	// The controller must have been cached on the head for subsequent calls.
	j.controllerMu.Lock()
	cached := j.controller
	j.controllerMu.Unlock()
	if cached != fc {
		t.Fatalf("controller not cached after auto-create")
	}
	// A second add reuses the cached controller (no second create).
	rr = serve(t, j, "POST", "/api/flow/plugins/", `{"name":"n2","path":"/p2"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("second add status = %d body=%s", rr.Code, rr.Body.String())
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if len(fc.plugins) != 2 {
		t.Fatalf("plugins = %+v, want 2 registered", fc.plugins)
	}
}

// TestPluginAddBatchAutoCreatesController verifies the batch path also
// auto-creates the controller when none is running.
func TestPluginAddBatchAutoCreatesController(t *testing.T) {
	origStart := startFlowControllerFn
	fc := newFakeController()
	startFlowControllerFn = func(ctx context.Context, h *FlowHead) (flowController, error) {
		return fc, nil
	}
	defer func() { startFlowControllerFn = origStart }()

	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()

	rr := serve(t, j, "POST", "/api/flow/plugins/",
		`{"plugins":[{"id":"p1","name":"a","path":"/a"},{"id":"p2","name":"b","path":"/b"}]}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Results map[string]string `json:"results"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Results["p1"] != "success" || resp.Results["p2"] != "success" {
		t.Fatalf("results = %+v", resp.Results)
	}
}

// TestSubmitFlowJobAgentTimeout verifies the B-D4 behavior: when the head node
// agent is unavailable within WAIT_AVAILABLE_AGENT_TIMEOUT, the handler returns
// 504 GatewayTimeout instead of 500, aligned with the Python submit_job mapping
// asyncio.TimeoutError to 504.
func TestSubmitFlowJobAgentTimeout(t *testing.T) {
	// No head node id in the KV: getTargetAgent retries until the timeout then
	// returns errAgentTimeout. Shorten the retry interval via a direct call to
	// the handler with a pre-drained agent cache is not possible, so verify the
	// error mapping path directly through the handler with a fast timeout by
	// injecting an empty head-node-id KV (no agent ever resolves) and relying on
	// the package timeout of 10s would be slow. Instead, test the handler's
	// branch by making getTargetAgent return errAgentTimeout directly through
	// the timeout path: we can't inject getTargetAgent, so exercise
	// ensureFlowController's dependency-free submit with a mock that never
	// resolves is 10s. Use a small trick: replace the flow submit body with a
	// valid entrypoint so the handler reaches getTargetAgent, and confirm the
	// timeout returns 504 by shortening waitAvailableAgentTimeout temporarily.
	old := waitAvailableAgentTimeout
	waitAvailableAgentTimeout = 20 * time.Millisecond
	defer func() { waitAvailableAgentTimeout = old }()

	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()

	// checkFlowDependencies probes via RemotePythonVoid which fails without a
	// runtime, so override it to pass.
	origDeps := checkFlowDepsFn
	checkFlowDepsFn = func(h *FlowHead) error { return nil }
	defer func() { checkFlowDepsFn = origDeps }()

	body := `{"entrypoint":"{\"stages\":[{\"name\":\"s\",\"target\":\"t\",\"type\":\"python\"}]}"}`
	rr := serve(t, j, "POST", "/api/flow/jobs/", body)
	if rr.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504, body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "No available agent to submit flow job") {
		t.Fatalf("body = %s", rr.Body.String())
	}
}

// TestTailFlowJobLogsWebSocket spins up a mock agent WebSocket server that
// emits log lines and verifies the FlowHead forwards them to the client.
func TestTailFlowJobLogsWebSocket(t *testing.T) {
	mock := newMockGCS()
	mock.jobInfo = []*proto.JobTableData{
		sampleDriverTable("0102", "default", map[string]string{jobIDMetadataKey: "flowsubmit_ws"}, false),
	}
	j, stop := newFlowHead(t, mock)
	defer stop()

	agentSent := make(chan struct{})
	agentSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/job_agent/jobs/flowsubmit_ws/logs/tail" {
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

	if _, err := j.info.PutInfo(context.Background(), "flowsubmit_ws", &job.JobInfo{
		Status: job.JobStatusRunning, Entrypoint: "echo",
		Metadata:               map[string]string{flowMetadataKey: flowMetadataValue},
		DriverAgentHTTPAddress: ptrString("http://" + agentSrv.Listener.Addr().String()),
		DriverNodeID:           ptrString("nodeX"),
	}, true); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	if err := j.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	headSrv := httptest.NewServer(mux)
	defer headSrv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(headSrv.URL, "http") + "/api/flow/jobs/flowsubmit_ws/logs/tail"
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

// TestTailFlowJobLogsTerminalJobNoAgent verifies the WebSocket closes promptly
// when the job is terminal and no driver agent address is known.
func TestTailFlowJobLogsTerminalJobNoAgent(t *testing.T) {
	mock := newMockGCS()
	j, stop := newFlowHead(t, mock)
	defer stop()
	if _, err := j.info.PutInfo(context.Background(), "flowsubmit_term", &job.JobInfo{
		Status: job.JobStatusFailed, Entrypoint: "echo",
		Metadata: map[string]string{flowMetadataKey: flowMetadataValue},
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
	wsURL := "ws" + strings.TrimPrefix(headSrv.URL, "http") + "/api/flow/jobs/flowsubmit_term/logs/tail"
	start := time.Now()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("expected the server to close the WS")
	}
	if time.Since(start) > 4*time.Second {
		t.Fatal("terminal job without agent should close the WS quickly")
	}
}
