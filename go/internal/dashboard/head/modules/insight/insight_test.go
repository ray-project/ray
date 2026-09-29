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

package insight

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
)

// mockGCS simulates the GCS internal KV service used by InsightHead.
type mockGCS struct {
	proto.UnimplementedInternalKVGcsServiceServer

	mu    sync.Mutex
	store map[string][]byte
}

func newMockGCS() *mockGCS { return &mockGCS{store: map[string][]byte{}} }

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

// kvServer is implemented by the mock GCS services that serve the internal KV
// RPCs used by InsightHead.
type kvServer interface {
	proto.InternalKVGcsServiceServer
}

// startMockGCS starts an in-memory gRPC server and returns a connected
// GCSClient plus a stop function.
func startMockGCS(t *testing.T, mock kvServer) (*head.GCSClient, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	proto.RegisterInternalKVGcsServiceServer(srv, mock)
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

// mockInsightServer records the requests it receives and echoes back a
// configurable body, used as the fake flow insight monitor service.
type mockInsightServer struct {
	mu        sync.Mutex
	requests  []*http.Request
	reqBodies []string
	status    int
	body      string
	header    http.Header
	pingAlive bool
}

func newMockInsightServer() *mockInsightServer {
	return &mockInsightServer{status: http.StatusOK, body: `{"hello":"world"}`, pingAlive: true}
}

func (s *mockInsightServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqBody, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	s.requests = append(s.requests, r)
	// Record the body only for proxied data requests (the health-check ping
	// carries no body and would otherwise shift the indices).
	if r.URL.Path != "/ping" {
		s.reqBodies = append(s.reqBodies, string(reqBody))
	}
	pingAlive := s.pingAlive
	status := s.status
	body := s.body
	hdr := s.header
	s.mu.Unlock()

	if r.URL.Path == "/ping" {
		if !pingAlive {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"result":false}`))
			return
		}
		_, _ = w.Write([]byte(`{"result":true}`))
		return
	}
	if hdr != nil {
		for k, vs := range hdr {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

// dataRequests filters out the health-check ping requests and returns only the
// proxied data requests received by the mock insight server.
func dataRequests(rs []*http.Request) []*http.Request {
	out := make([]*http.Request, 0, len(rs))
	for _, r := range rs {
		if r.URL.Path != "/ping" {
			out = append(out, r)
		}
	}
	return out
}

// newInsightHead builds an InsightHead backed by a mock GCS client whose KV
// store maps the insight monitor address to the given httptest server.
func newInsightHead(t *testing.T, mock *mockGCS, insightAddr string) (*InsightHead, func()) {
	t.Helper()
	if insightAddr != "" {
		mock.put(insightKVNamespace, insightKVKey, []byte(insightAddr))
	}
	client, stop := startMockGCS(t, mock)
	return New(&head.HeadConfig{}, client), stop
}

// proxyRequest performs a request against the InsightHead mux.
func proxyRequest(t *testing.T, h *InsightHead, method, path, body string, hdrs map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	if err := h.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	for k, v := range hdrs {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

// decodeRest decodes a RESTResponse body into its data object.
func decodeRest(t *testing.T, rr *httptest.ResponseRecorder) (bool, string) {
	t.Helper()
	var body struct {
		Result bool   `json:"result"`
		Msg    string `json:"msg"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid json body: %v", err)
	}
	return body.Result, body.Msg
}

func TestNameStartHealthy(t *testing.T) {
	mock := newMockGCS()
	client, stop := startMockGCS(t, mock)
	defer stop()

	h := New(&head.HeadConfig{}, client)
	if h.Name() != "InsightHead" {
		t.Fatalf("Name() = %q, want InsightHead", h.Name())
	}
	if !h.Healthy() {
		t.Fatal("module should be healthy")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := h.Start(ctx); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
}

func TestProxyAddressMissing(t *testing.T) {
	mock := newMockGCS()
	h, stop := newInsightHead(t, mock, "")
	defer stop()

	rr := proxyRequest(t, h, "GET", "/insight/get_call_graph_data", "", nil)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("GET /insight without address = %d, want 500", rr.Code)
	}
	result, msg := decodeRest(t, rr)
	if result {
		t.Fatalf("result = true, want false")
	}
	if !strings.Contains(msg, "InsightMonitor address not found in KV store") {
		t.Fatalf("msg = %q, want address-not-found message", msg)
	}

	// POST behaves identically.
	rr = proxyRequest(t, h, "POST", "/insight/emit", `{"x":1}`, nil)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("POST /insight without address = %d, want 500", rr.Code)
	}
}

func TestProxyForwardGET(t *testing.T) {
	insight := newMockInsightServer()
	srv := httptest.NewServer(insight)
	defer srv.Close()

	mock := newMockGCS()
	h, stop := newInsightHead(t, mock, strings.TrimPrefix(srv.URL, "http://"))
	defer stop()

	rr := proxyRequest(t, h, "GET", "/insight/get_call_graph_data?flow_id=abc&stack_mode=false", "", map[string]string{
		"X-Custom-Header": "custom-value",
		"Accept":          "application/json",
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /insight = %d, want 200", rr.Code)
	}
	if rr.Body.String() != `{"hello":"world"}` {
		t.Fatalf("body = %q, want echoed body", rr.Body.String())
	}

	insight.mu.Lock()
	defer insight.mu.Unlock()
	dataReqs := dataRequests(insight.requests)
	if len(dataReqs) != 1 {
		t.Fatalf("received %d data requests, want 1", len(dataReqs))
	}
	req := dataReqs[0]
	if req.URL.Path != "/get_call_graph_data" {
		t.Fatalf("proxied path = %q, want /get_call_graph_data", req.URL.Path)
	}
	if req.URL.RawQuery != "flow_id=abc&stack_mode=false" {
		t.Fatalf("proxied query = %q, want flow_id=abc&stack_mode=false", req.URL.RawQuery)
	}
	if req.Header.Get("X-Custom-Header") != "custom-value" {
		t.Fatalf("X-Custom-Header not forwarded: %q", req.Header.Get("X-Custom-Header"))
	}
}

func TestProxyForwardPOSTBodyAndHeaders(t *testing.T) {
	insight := newMockInsightServer()
	srv := httptest.NewServer(insight)
	defer srv.Close()

	mock := newMockGCS()
	h, stop := newInsightHead(t, mock, strings.TrimPrefix(srv.URL, "http://"))
	defer stop()

	body := `{"flow_id":"abc","label":"test"}`
	rr := proxyRequest(t, h, "POST", "/insight/create_snapshot", body, map[string]string{
		"Content-Type": "application/json",
		"X-Request-Id": "req-1",
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("POST /insight = %d, want 200", rr.Code)
	}

	insight.mu.Lock()
	defer insight.mu.Unlock()
	dataReqs := dataRequests(insight.requests)
	if len(dataReqs) != 1 {
		t.Fatalf("received %d data requests, want 1", len(dataReqs))
	}
	req := dataReqs[0]
	if req.Method != http.MethodPost {
		t.Fatalf("proxied method = %s, want POST", req.Method)
	}
	if req.URL.Path != "/create_snapshot" {
		t.Fatalf("proxied path = %q, want /create_snapshot", req.URL.Path)
	}
	if got := insight.reqBodies[0]; got != body {
		t.Fatalf("proxied body = %q, want %q", got, body)
	}
	if len(insight.reqBodies) != 1 {
		t.Fatalf("recorded %d data bodies, want 1", len(insight.reqBodies))
	}
	if req.Header.Get("X-Request-Id") != "req-1" {
		t.Fatalf("X-Request-Id not forwarded: %q", req.Header.Get("X-Request-Id"))
	}
}

func TestProxyResponseHeadersAndStatusPassthrough(t *testing.T) {
	insight := newMockInsightServer()
	insight.status = http.StatusAccepted
	insight.body = `partial`
	insight.header = http.Header{"X-Upstream": []string{"echo"}, "Content-Type": []string{"text/plain"}}
	srv := httptest.NewServer(insight)
	defer srv.Close()

	mock := newMockGCS()
	h, stop := newInsightHead(t, mock, strings.TrimPrefix(srv.URL, "http://"))
	defer stop()

	rr := proxyRequest(t, h, "GET", "/insight/some/path", "", nil)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("GET /insight = %d, want 202 passthrough", rr.Code)
	}
	if rr.Body.String() != "partial" {
		t.Fatalf("body = %q, want passthrough body", rr.Body.String())
	}
	if rr.Header().Get("X-Upstream") != "echo" {
		t.Fatalf("X-Upstream = %q, want passthrough header", rr.Header().Get("X-Upstream"))
	}
}

func TestProxyForwardFailure(t *testing.T) {
	// Reserve a port and close it so nothing listens there.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	_ = ln.Close()

	mock := newMockGCS()
	h, stop := newInsightHead(t, mock, deadAddr)
	defer stop()

	rr := proxyRequest(t, h, "GET", "/insight/get_call_graph_data", "", nil)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("GET /insight to dead target = %d, want 500", rr.Code)
	}
	result, msg := decodeRest(t, rr)
	if result {
		t.Fatalf("result = true, want false")
	}
	if !strings.Contains(msg, "Error proxying request to insight monitor") {
		t.Fatalf("msg = %q, want proxy error message", msg)
	}
}

func TestProxyHealthCheckStaleAddress(t *testing.T) {
	// First KV read returns an unreachable address; the health check fails and
	// the re-read returns nothing, so the handler 500s with the address-not-found
	// message (aligned with the Python is_insight_server_alive re-read path).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	_ = ln.Close()

	seq := &sequenceMockGCS{seq: [][]byte{[]byte(deadAddr), nil}}
	client, stop := startMockGCS(t, seq)
	defer stop()

	h := New(&head.HeadConfig{}, client)
	rr := proxyRequest(t, h, "GET", "/insight/get_call_graph_data", "", nil)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("GET /insight with stale dead address = %d, want 500", rr.Code)
	}
	result, msg := decodeRest(t, rr)
	if result {
		t.Fatalf("result = true, want false")
	}
	if !strings.Contains(msg, "InsightMonitor address not found in KV store") {
		t.Fatalf("msg = %q, want address-not-found message", msg)
	}
}

// sequenceMockGCS returns a different KV value on each InternalKVGet call,
// used to exercise the health-check re-read path where the address changes.
type sequenceMockGCS struct {
	proto.UnimplementedInternalKVGcsServiceServer

	mu    sync.Mutex
	reads int
	seq   [][]byte
}

func (s *sequenceMockGCS) InternalKVGet(ctx context.Context, req *proto.InternalKVGetRequest) (*proto.InternalKVGetReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var val []byte
	if s.reads < len(s.seq) {
		val = s.seq[s.reads]
	}
	s.reads++
	return &proto.InternalKVGetReply{Value: val}, nil
}

func TestProxyHealthCheckRecoversAddress(t *testing.T) {
	// First KV read returns a dead address; the health check fails, the KV is
	// re-read and returns the live address, and the request is proxied there.
	insight := newMockInsightServer()
	srv := httptest.NewServer(insight)
	defer srv.Close()
	liveAddr := strings.TrimPrefix(srv.URL, "http://")

	seq := &sequenceMockGCS{seq: [][]byte{[]byte("127.0.0.1:1"), []byte(liveAddr)}}
	client, stop := startMockGCS(t, seq)
	defer stop()

	h := New(&head.HeadConfig{}, client)
	rr := proxyRequest(t, h, "GET", "/insight/get_call_graph_data", "", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /insight after recovery = %d, want 200", rr.Code)
	}
	if rr.Body.String() != `{"hello":"world"}` {
		t.Fatalf("body = %q, want echoed body", rr.Body.String())
	}
	seq.mu.Lock()
	defer seq.mu.Unlock()
	if seq.reads != 2 {
		t.Fatalf("KV reads = %d, want 2 (initial + re-read)", seq.reads)
	}
}
