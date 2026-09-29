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

package log

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
)

// mockKV implements the internal KV GCS service with an empty store, so agent
// address lookups fail with a clean error instead of a nil-client panic.
type mockKV struct {
	proto.UnimplementedInternalKVGcsServiceServer
	store map[string][]byte
}

func (s *mockKV) InternalKVGet(ctx context.Context, req *proto.InternalKVGetRequest) (*proto.InternalKVGetReply, error) {
	if s.store == nil {
		return &proto.InternalKVGetReply{}, nil
	}
	return &proto.InternalKVGetReply{Value: s.store[string(req.Namespace)+"\x00"+string(req.Key)]}, nil
}

// mockLogAgent implements the LogService with a StreamLog that fails with a
// log_grpc_status initial metadata error, mirroring log_agent.py sending
// send_initial_metadata([[LOG_GRPC_ERROR, str(e)]]) for a missing file.
type mockLogAgent struct {
	proto.UnimplementedLogServiceServer
	fileMissing bool
}

func (s *mockLogAgent) StreamLog(req *proto.StreamLogRequest, stream proto.LogService_StreamLogServer) error {
	if s.fileMissing {
		return stream.SendHeader(map[string][]string{logGRPCError: {"A file is not found at: /tmp/ray/session_1/logs/" + req.LogFileName}})
	}
	return nil
}

// startMockGCS starts an in-memory gRPC server exposing the internal KV service
// and returns a connected GCSClient.
func startMockGCS(t *testing.T) (*head.GCSClient, func()) {
	t.Helper()
	return startMockGCSWithAgent(t, &mockKV{}, nil)
}

// startMockGCSWithAgent starts an in-memory gRPC server exposing the internal
// KV service (with the given store) and, when agent is non-nil, the LogService,
// returning a connected GCSClient.
func startMockGCSWithAgent(t *testing.T, kv *mockKV, agent *mockLogAgent) (*head.GCSClient, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	proto.RegisterInternalKVGcsServiceServer(srv, kv)
	if agent != nil {
		proto.RegisterLogServiceServer(srv, agent)
	}
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

func TestCategorizeLogFiles(t *testing.T) {
	files := []string{
		"worker-abc-01000000-12345.out",
		"worker-abc-01000000-12345.err",
		"core-worker-xyz.log",
		"core-driver-abc.log",
		"raylet.out",
		"gcs_server.out",
		"log_monitor.log",
		"monitor.out",
		"agent.out",
		"dashboard.log",
		"unknown.log",
	}
	got := categorizeLogFiles(files)
	want := map[string][]string{
		"worker_out":  {"worker-abc-01000000-12345.out"},
		"worker_err":  {"worker-abc-01000000-12345.err"},
		"core_worker": {"core-worker-xyz.log"},
		"driver":      {"core-driver-abc.log"},
		"raylet":      {"raylet.out"},
		"gcs_server":  {"gcs_server.out"},
		"internal":    {"log_monitor.log", "unknown.log"},
		"autoscaler":  {"monitor.out"},
		"agent":       {"agent.out"},
		"dashboard":   {"dashboard.log"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("categorizeLogFiles() = %v, want %v", got, want)
	}
}

func TestCategorizeLogFilesEmpty(t *testing.T) {
	got := categorizeLogFiles(nil)
	if len(got) != 0 {
		t.Fatalf("categorizeLogFiles(nil) = %v, want empty", got)
	}
}

func TestStripANSI(t *testing.T) {
	// ESC[31m = red foreground, ESC[0m = reset, ESC[2K = clear line.
	input := []byte("\x1b[31mred\x1b[0m plain \x1b[2Kdone\n")
	got := stripANSI(input)
	want := "red plain done\n"
	if string(got) != want {
		t.Fatalf("stripANSI() = %q, want %q", got, want)
	}
}

func TestStripANSINoEscape(t *testing.T) {
	input := []byte("plain text without escapes\n")
	got := stripANSI(input)
	if !bytes.Equal(got, input) {
		t.Fatalf("stripANSI() = %q, want %q", got, input)
	}
}

// newTestHead builds a LogHead backed by a mock GCS whose internal KV is empty
// (agent address lookups fail cleanly, which exercises the error paths below).
func newTestHead(t *testing.T) (*LogHead, func()) {
	t.Helper()
	client, stop := startMockGCS(t)
	return New(&head.HeadConfig{}, client), stop
}

// serveGet performs a GET against the LogHead mux.
func serveGet(t *testing.T, l *LogHead, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	if err := l.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", path, nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

// TestGetLogsRejectsInvalidMediaType verifies a media_type other than file or
// stream returns 500, aligned with the Python get_logs handler: the
// GetLogOptions ValueError ("Invalid media type: ...") raised in __post_init__
// is not caught by the handler and bubbles up as an unhandled exception, which
// aiohttp turns into HTTP 500.
func TestGetLogsRejectsInvalidMediaType(t *testing.T) {
	l, stop := newTestHead(t)
	defer stop()
	rr := serveGet(t, l, "/api/v0/logs/bad_type")
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Invalid media type: bad_type") {
		t.Fatalf("body = %q, want Invalid media type message", rr.Body.String())
	}
}

// TestGetLogsDefaultDownloadFilename verifies the Content-Disposition header
// falls back to DEFAULT_DOWNLOAD_FILENAME ("file.txt") when download_filename
// is not specified, aligned with GetLogOptions.download_filename. The header is
// set before the agent gRPC call, so the response is a 500 from the missing
// agent but the header must already be set.
func TestGetLogsDefaultDownloadFilename(t *testing.T) {
	l, stop := newTestHead(t)
	defer stop()
	rr := serveGet(t, l, "/api/v0/logs/file?node_id=abc&filename=x.out")
	cd := rr.Header().Get("Content-Disposition")
	if cd != `attachment; filename="file.txt"` {
		t.Fatalf("Content-Disposition = %q, want file.txt default", cd)
	}

	rr = serveGet(t, l, "/api/v0/logs/file?node_id=abc&filename=x.out&download_filename=custom.txt")
	cd = rr.Header().Get("Content-Disposition")
	if cd != `attachment; filename="custom.txt"` {
		t.Fatalf("Content-Disposition = %q, want custom.txt", cd)
	}
}

// TestGetLogsStreamMediaTypeAccepted verifies media_type=stream is accepted
// (it passes the media-type validation and proceeds to the agent lookup, which
// fails with 500 rather than 400), aligned with GetLogOptions.
func TestGetLogsStreamMediaTypeAccepted(t *testing.T) {
	l, stop := newTestHead(t)
	defer stop()
	rr := serveGet(t, l, "/api/v0/logs/stream?node_id=abc&filename=x.out")
	if rr.Code == http.StatusBadRequest {
		t.Fatalf("stream media type should not be rejected as invalid: %s", rr.Body.String())
	}
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (agent lookup failure)", rr.Code)
	}
}

// TestGetLogsMissingFileIs500 verifies a missing log file returns 500 with the
// agent's error text, aligned with the Python stream_logs path: log_agent.py
// sends the error through the log_grpc_status initial metadata and
// state_manager.stream_log raises it, which the get_logs handler turns into
// HTTPInternalServerError (500).
func TestGetLogsMissingFileIs500(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	agent := &mockLogAgent{fileMissing: true}
	proto.RegisterLogServiceServer(srv, agent)
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()

	kv := &mockKV{store: map[string][]byte{
		kvNamespaceDashboard + "\x00" + dashboardAgentAddrNodeIDPrefix + "node01": []byte(`["127.0.0.1",8265,` + strconv.Itoa(ln.Addr().(*net.TCPAddr).Port) + `]`),
	}}
	client, stop := startMockGCSWithAgent(t, kv, agent)
	defer stop()
	l := New(&head.HeadConfig{}, client)

	rr := serveGet(t, l, "/api/v0/logs/file?node_id=node01&filename=missing.out")
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500, body = %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "A file is not found at:") {
		t.Fatalf("body = %s, want file-not-found message", rr.Body.String())
	}
}

// TestListLogsUnresolvableIPIs404 verifies a node_ip that cannot be resolved
// returns 404, aligned with the Python list_logs handler.
func TestListLogsUnresolvableIPIs404(t *testing.T) {
	l, stop := newTestHead(t)
	defer stop()
	rr := serveGet(t, l, "/api/v0/logs?node_ip=10.0.0.99")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404, body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "Cannot find matching node_id") {
		t.Fatalf("body = %s", rr.Body.String())
	}
}

// TestListLogsMissingNodeIs400 verifies neither node_id nor node_ip returns
// 400, aligned with the Python list_logs handler.
func TestListLogsMissingNodeIs400(t *testing.T) {
	l, stop := newTestHead(t)
	defer stop()
	rr := serveGet(t, l, "/api/v0/logs")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	// The error body nests the None result under data.result, matching Python
	// do_reply(result=None) -> data: {"result": null} (not data: null).
	body := rr.Body.String()
	if !strings.Contains(body, `"data": {`) || !strings.Contains(body, `"result": null`) {
		t.Fatalf("body = %s, want data.result null", body)
	}
	if strings.Contains(body, `"data": null`) {
		t.Fatalf("body = %s, data must not be null", body)
	}
}

// TestRegisterHTTPIdempotent verifies that registering the log routes twice
// (once via the StateHead-held LogHead and once via the standalone LogHead
// module) does not panic the ServeMux: the StateHead mounts the logs handlers
// on its own module in the Python dashboard, so a Go StateHead also holds a
// LogHead and both may register in the default all-modules run.
func TestRegisterHTTPIdempotent(t *testing.T) {
	l, stop := newTestHead(t)
	defer stop()
	mux := http.NewServeMux()
	if err := l.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	// A second registration must be ignored, not panic.
	if err := l.RegisterHTTP(mux); err != nil {
		t.Fatal(err)
	}
	// The first handler is still the one serving.
	rr := serveGet(t, l, "/api/v0/logs")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (neither node_id nor node_ip)", rr.Code)
	}
}
