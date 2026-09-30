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
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// writeLogTree creates the given files (relative to dir), creating parent
// directories as needed, plus an empty directory to exercise listing.
func writeLogTree(t *testing.T, dir string, files ...string) {
	t.Helper()
	for _, f := range files {
		p := filepath.Join(dir, f)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(p), err)
		}
		if err := os.WriteFile(p, []byte("content-"+f+"\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
}

func TestResolveFilename(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	outFile := filepath.Join(outside, "out.log")
	if err := os.WriteFile(outFile, []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeLogTree(t, root, "a.log", "sub/b.log")
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outFile, filepath.Join(root, "sub", "link.log")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "emptydir"), 0o755); err != nil {
		t.Fatal(err)
	}

	t.Run("relative inside", func(t *testing.T) {
		got, err := resolveFilename(root, "a.log")
		if err != nil {
			t.Fatalf("resolveFilename: %v", err)
		}
		if want, _ := filepath.EvalSymlinks(filepath.Join(root, "a.log")); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
	t.Run("nested inside", func(t *testing.T) {
		got, err := resolveFilename(root, "sub/b.log")
		if err != nil {
			t.Fatalf("resolveFilename: %v", err)
		}
		if want, _ := filepath.EvalSymlinks(filepath.Join(root, "sub/b.log")); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
	t.Run("absolute inside", func(t *testing.T) {
		if _, err := resolveFilename(root, filepath.Join(root, "a.log")); err != nil {
			t.Errorf("absolute inside: %v", err)
		}
	})
	t.Run("symlink to outside is allowed by design", func(t *testing.T) {
		// Python deliberately allows relative paths whose symlinks point
		// outside root_log_dir; containment is checked before resolution.
		got, err := resolveFilename(root, "sub/link.log")
		if err != nil {
			t.Fatalf("resolveFilename: %v", err)
		}
		resolvedOutside, _ := filepath.EvalSymlinks(outFile)
		if got != resolvedOutside {
			t.Errorf("got %q, want %q", got, resolvedOutside)
		}
	})
	t.Run("directory is rejected", func(t *testing.T) {
		if _, err := resolveFilename(root, "emptydir"); err == nil {
			t.Error("directory resolved without error")
		}
	})
	t.Run("missing file is rejected", func(t *testing.T) {
		if _, err := resolveFilename(root, "nope.log"); err == nil {
			t.Error("missing file resolved without error")
		}
	})
	for _, name := range []string{"..", "../outside", "sub/../../x", "/etc/passwd"} {
		name := name
		t.Run("traversal "+name, func(t *testing.T) {
			if _, err := resolveFilename(root, name); err == nil {
				t.Errorf("resolveFilename(%q) returned no error", name)
			}
		})
	}
	t.Run("absolute outside is rejected", func(t *testing.T) {
		if _, err := resolveFilename(root, outFile); err == nil {
			t.Errorf("absolute outside path resolved without error: %s", outFile)
		}
	})
}

func TestNewValidatesLogDir(t *testing.T) {
	if _, err := New(agent.Config{LogDir: ""}); err == nil {
		t.Error("New with empty LogDir returned no error")
	}
	if _, err := New(agent.Config{LogDir: filepath.Join(t.TempDir(), "missing")}); err == nil {
		t.Error("New with missing LogDir returned no error")
	}
	file := filepath.Join(t.TempDir(), "plain.log")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(agent.Config{LogDir: file}); err == nil {
		t.Error("New with a file as LogDir returned no error")
	}
	la, err := New(agent.Config{LogDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if la == nil {
		t.Fatal("New returned nil module")
	}
}

func TestRegisterGRPC(t *testing.T) {
	la := &LogAgentModule{logDir: t.TempDir()}
	srv := grpc.NewServer()
	if err := la.RegisterGRPC(srv); err != nil {
		t.Fatalf("RegisterGRPC: %v", err)
	}
	if _, ok := srv.GetServiceInfo()["ray.rpc.LogService"]; !ok {
		t.Error("ray.rpc.LogService not registered on gRPC server")
	}
}

func TestListLogs(t *testing.T) {
	dir := t.TempDir()
	writeLogTree(t, dir, "a.log", "b.err", "sub/c.log", "sub/d.out")
	if err := os.Mkdir(filepath.Join(dir, "emptydir"), 0o755); err != nil {
		t.Fatal(err)
	}
	la := &LogAgentModule{logDir: dir}
	ctx := context.Background()

	t.Run("star lists files and directories", func(t *testing.T) {
		rep, err := la.ListLogs(ctx, &proto.ListLogsRequest{GlobFilter: "*"})
		if err != nil {
			t.Fatalf("ListLogs: %v", err)
		}
		got := rep.GetLogFiles()
		sort.Strings(got)
		want := []string{"a.log", "b.err", "emptydir/", "sub/"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("LogFiles = %v, want %v", got, want)
		}
	})
	t.Run("extension filter", func(t *testing.T) {
		rep, err := la.ListLogs(ctx, &proto.ListLogsRequest{GlobFilter: "*.log"})
		if err != nil {
			t.Fatalf("ListLogs: %v", err)
		}
		if got := rep.GetLogFiles(); !reflect.DeepEqual(got, []string{"a.log"}) {
			t.Errorf("LogFiles = %v, want [a.log]", got)
		}
	})
	t.Run("nested glob", func(t *testing.T) {
		rep, err := la.ListLogs(ctx, &proto.ListLogsRequest{GlobFilter: "sub/*"})
		if err != nil {
			t.Fatalf("ListLogs: %v", err)
		}
		got := rep.GetLogFiles()
		sort.Strings(got)
		if !reflect.DeepEqual(got, []string{"sub/c.log", "sub/d.out"}) {
			t.Errorf("LogFiles = %v, want [sub/c.log sub/d.out]", got)
		}
	})
	t.Run("no match returns empty", func(t *testing.T) {
		rep, err := la.ListLogs(ctx, &proto.ListLogsRequest{GlobFilter: "*.none"})
		if err != nil {
			t.Fatalf("ListLogs: %v", err)
		}
		if got := rep.GetLogFiles(); len(got) != 0 {
			t.Errorf("LogFiles = %v, want empty", got)
		}
	})
}

func TestListLogsMissingDir(t *testing.T) {
	la := &LogAgentModule{logDir: filepath.Join(t.TempDir(), "missing")}
	_, err := la.ListLogs(context.Background(), &proto.ListLogsRequest{GlobFilter: "*"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("ListLogs error code = %v, want NotFound", status.Code(err))
	}
}

func TestListLogsInvalidGlob(t *testing.T) {
	la := &LogAgentModule{logDir: t.TempDir()}
	_, err := la.ListLogs(context.Background(), &proto.ListLogsRequest{GlobFilter: "["})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListLogs error code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestHTTPServeLogs(t *testing.T) {
	dir := t.TempDir()
	writeLogTree(t, dir, "a.log", "sub/b.log")
	if err := os.Mkdir(filepath.Join(dir, "emptydir"), 0o755); err != nil {
		t.Fatal(err)
	}
	la := &LogAgentModule{logDir: dir}
	mux := http.NewServeMux()
	if err := la.RegisterHTTP(mux); err != nil {
		t.Fatalf("RegisterHTTP: %v", err)
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	t.Run("serve file", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/logs/a.log")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
		if string(body) != "content-a.log\n" {
			t.Errorf("body = %q", body)
		}
	})
	t.Run("serve nested file", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/logs/sub/b.log")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || string(body) != "content-sub/b.log\n" {
			t.Errorf("nested: status=%d body=%q", resp.StatusCode, body)
		}
	})
	t.Run("directory listing", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/logs/emptydir/")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200", resp.StatusCode)
		}
	})
	t.Run("missing file is 404", func(t *testing.T) {
		resp, err := http.Get(srv.URL + "/logs/nope.log")
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("status = %d, want 404", resp.StatusCode)
		}
	})
}

func TestHTTPTraversalRejected(t *testing.T) {
	la := &LogAgentModule{logDir: t.TempDir()}
	for _, name := range []string{"../secret", "sub/../../x", "/etc/passwd"} {
		req := httptest.NewRequest(http.MethodGet, "http://agent/logs/"+name, nil)
		rec := httptest.NewRecorder()
		la.handleLogs(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("handleLogs(%q) status = %d, want 400", name, rec.Code)
		}
	}
}

func i32p(v int32) *int32     { return &v }
func f32p(v float32) *float32 { return &v }

// fakeStreamLogServer is an in-memory LogService_StreamLogServer that records
// the streamed replies and the initial metadata header sent by the handler.
type fakeStreamLogServer struct {
	ctx    context.Context
	mu     sync.Mutex
	header metadata.MD
	data   bytes.Buffer
}

func (s *fakeStreamLogServer) Send(r *proto.StreamLogReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Write(r.GetData())
	return nil
}

func (s *fakeStreamLogServer) SendHeader(md metadata.MD) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.header = md
	return nil
}

func (s *fakeStreamLogServer) SetHeader(metadata.MD) error { return nil }
func (s *fakeStreamLogServer) SetTrailer(metadata.MD)      {}
func (s *fakeStreamLogServer) Context() context.Context    { return s.ctx }
func (s *fakeStreamLogServer) SendMsg(interface{}) error   { return nil }
func (s *fakeStreamLogServer) RecvMsg(interface{}) error   { return nil }

func (s *fakeStreamLogServer) received() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.String()
}

func (s *fakeStreamLogServer) headerValue(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	vals := s.header.Get(key)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func TestStreamLog(t *testing.T) {
	dir := t.TempDir()
	content := "line1\nline2\nline3\nline4\nline5\n"
	if err := os.WriteFile(filepath.Join(dir, "a.log"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	la := &LogAgentModule{logDir: dir}

	t.Run("default tails up to 1000 lines", func(t *testing.T) {
		s := &fakeStreamLogServer{ctx: context.Background()}
		if err := la.StreamLog(&proto.StreamLogRequest{LogFileName: "a.log"}, s); err != nil {
			t.Fatalf("StreamLog: %v", err)
		}
		if got := s.received(); got != content {
			t.Errorf("data = %q, want %q", got, content)
		}
	})

	t.Run("lines zero uses default tail", func(t *testing.T) {
		s := &fakeStreamLogServer{ctx: context.Background()}
		req := &proto.StreamLogRequest{LogFileName: "a.log", Lines: i32p(0)}
		if err := la.StreamLog(req, s); err != nil {
			t.Fatalf("StreamLog: %v", err)
		}
		if got := s.received(); got != content {
			t.Errorf("data = %q, want %q", got, content)
		}
	})

	t.Run("lines two tails last two lines", func(t *testing.T) {
		s := &fakeStreamLogServer{ctx: context.Background()}
		req := &proto.StreamLogRequest{LogFileName: "a.log", Lines: i32p(2)}
		if err := la.StreamLog(req, s); err != nil {
			t.Fatalf("StreamLog: %v", err)
		}
		if got := s.received(); got != "line4\nline5\n" {
			t.Errorf("data = %q, want %q", got, "line4\nline5\n")
		}
	})

	t.Run("minus one lines streams from head", func(t *testing.T) {
		s := &fakeStreamLogServer{ctx: context.Background()}
		req := &proto.StreamLogRequest{LogFileName: "a.log", Lines: i32p(-1)}
		if err := la.StreamLog(req, s); err != nil {
			t.Fatalf("StreamLog: %v", err)
		}
		if got := s.received(); got != content {
			t.Errorf("data = %q, want %q", got, content)
		}
	})

	t.Run("start offset honored with minus one lines", func(t *testing.T) {
		s := &fakeStreamLogServer{ctx: context.Background()}
		req := &proto.StreamLogRequest{LogFileName: "a.log", Lines: i32p(-1), StartOffset: i32p(12)}
		if err := la.StreamLog(req, s); err != nil {
			t.Fatalf("StreamLog: %v", err)
		}
		if got := s.received(); got != "line3\nline4\nline5\n" {
			t.Errorf("data = %q, want %q", got, "line3\nline4\nline5\n")
		}
	})

	t.Run("start offset beyond content yields empty stream", func(t *testing.T) {
		s := &fakeStreamLogServer{ctx: context.Background()}
		req := &proto.StreamLogRequest{LogFileName: "a.log", Lines: i32p(-1), StartOffset: i32p(1000)}
		if err := la.StreamLog(req, s); err != nil {
			t.Fatalf("StreamLog: %v", err)
		}
		if got := s.received(); got != "" {
			t.Errorf("data = %q, want empty", got)
		}
	})

	t.Run("end offset truncates the stream", func(t *testing.T) {
		s := &fakeStreamLogServer{ctx: context.Background()}
		req := &proto.StreamLogRequest{LogFileName: "a.log", Lines: i32p(-1), EndOffset: i32p(6)}
		if err := la.StreamLog(req, s); err != nil {
			t.Fatalf("StreamLog: %v", err)
		}
		if got := s.received(); got != "line1\n" {
			t.Errorf("data = %q, want %q", got, "line1\n")
		}
	})

	t.Run("unresolvable file reports header error and succeeds", func(t *testing.T) {
		s := &fakeStreamLogServer{ctx: context.Background()}
		req := &proto.StreamLogRequest{LogFileName: "nope.log"}
		if err := la.StreamLog(req, s); err != nil {
			t.Fatalf("StreamLog: %v", err)
		}
		if got := s.headerValue(logGRPCError); got == "" {
			t.Error("expected log_grpc_status header, got none")
		}
		if got := s.received(); got != "" {
			t.Errorf("data = %q, want empty", got)
		}
	})

	t.Run("traversal is rejected through header", func(t *testing.T) {
		s := &fakeStreamLogServer{ctx: context.Background()}
		req := &proto.StreamLogRequest{LogFileName: "../outside.log"}
		if err := la.StreamLog(req, s); err != nil {
			t.Fatalf("StreamLog: %v", err)
		}
		if got := s.headerValue(logGRPCError); got == "" {
			t.Error("expected log_grpc_status header, got none")
		}
	})

	t.Run("nil request is invalid", func(t *testing.T) {
		s := &fakeStreamLogServer{ctx: context.Background()}
		if err := la.StreamLog(nil, s); status.Code(err) != codes.InvalidArgument {
			t.Errorf("error code = %v, want InvalidArgument", status.Code(err))
		}
	})

	t.Run("keep alive follows appended content until cancel", func(t *testing.T) {
		file := filepath.Join(dir, "follow.log")
		if err := os.WriteFile(file, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := &fakeStreamLogServer{ctx: ctx}
		req := &proto.StreamLogRequest{
			LogFileName: "follow.log",
			KeepAlive:   true,
			Interval:    f32p(0.05),
			Lines:       i32p(-1),
		}
		done := make(chan error, 1)
		go func() { done <- la.StreamLog(req, s) }()

		time.Sleep(300 * time.Millisecond)
		if err := os.WriteFile(file, []byte("appended\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for !bytes.Contains([]byte(s.received()), []byte("appended\n")) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
			if err := s.headerValue(logGRPCError); err != "" {
				t.Fatalf("unexpected error header: %s", err)
			}
		}
		if got := s.received(); got != "appended\n" {
			t.Errorf("data = %q, want %q", got, "appended\n")
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("StreamLog with keep alive: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("StreamLog did not return after context cancellation")
		}
	})
}
