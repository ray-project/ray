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

package head

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/go-logr/logr"
	"github.com/ray-project/ray/go/pkg/log"
)

func TestResolveStaticDirPrefersExplicit(t *testing.T) {
	dir := t.TempDir()
	cfg := &HeadConfig{StaticDir: dir}
	if got := resolveStaticDir(cfg); got != dir {
		t.Fatalf("resolveStaticDir() with explicit StaticDir = %q, want %q", got, dir)
	}
}

func TestResolveStaticDirPrefersEnvOverExecutable(t *testing.T) {
	envDir := t.TempDir()
	t.Setenv(envVarStaticDir, envDir)
	// Executable fallback would resolve to the real source checkout; the env
	// var must win regardless.
	if got := resolveStaticDir(&HeadConfig{}); got != envDir {
		t.Fatalf("resolveStaticDir() with RAY_DASHBOARD_GO_STATIC_DIR set = %q, want %q", got, envDir)
	}
}

func TestResolveStaticDirFallsBackToSourceCheckout(t *testing.T) {
	// Without an explicit dir or env override, resolution is relative to the
	// raygo binary (…/python/ray/go/cmd/raygo) and must not be empty.
	got := resolveStaticDir(&HeadConfig{})
	if got == "" {
		t.Fatal("resolveStaticDir() returned empty path")
	}
}

// stubModule is a HeadModule whose HTTP routes are provided by register.
type stubModule struct {
	name     string
	register func(mux *http.ServeMux) error
}

func (m stubModule) Name() string { return m.name }
func (m stubModule) Start(ctx context.Context) error {
	return nil
}
func (m stubModule) RegisterHTTP(mux *http.ServeMux) error { return m.register(mux) }
func (m stubModule) Healthy() bool                         { return true }

// captureSink is a minimal logr.LogSink that records the last Info call so
// tests can assert on emitted log lines (the access log).
type captureSink struct {
	msg           string
	keysAndValues []interface{}
}

func (c *captureSink) Init(logr.RuntimeInfo)                     {}
func (c *captureSink) Enabled(int) bool                          { return true }
func (c *captureSink) Info(_ int, msg string, kv ...interface{}) { c.msg = msg; c.keysAndValues = kv }
func (c *captureSink) Error(err error, msg string, kv ...interface{}) {
	c.msg = msg
	c.keysAndValues = kv
}
func (c *captureSink) WithValues(kv ...interface{}) logr.LogSink { return c }
func (c *captureSink) WithName(string) logr.LogSink              { return c }
func (c *captureSink) V(int) logr.LogSink                        { return c }

// logKV returns the value for key in a logr keysAndValues slice.
func logKV(kv []interface{}, key string) string {
	for i := 0; i+1 < len(kv); i += 2 {
		if s, ok := kv[i].(string); ok && s == key {
			if v, ok := kv[i+1].(string); ok {
				return v
			}
			return fmt.Sprintf("%v", kv[i+1])
		}
	}
	return ""
}

func TestRESTResponse(t *testing.T) {
	rec := httptest.NewRecorder()
	RESTResponse(rec, http.StatusOK, "ok", map[string]interface{}{"a": 1})
	body := rec.Body.String()
	if !strings.Contains(body, `"result"`) || !strings.Contains(body, `"msg": "ok"`) || !strings.Contains(body, `"data"`) {
		t.Fatalf("unexpected body: %s", body)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected code: %d", rec.Code)
	}
}

func TestWithRecoverCatchesPanic(t *testing.T) {
	h := withRecover(logr.Logger{}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected code: %d", rec.Code)
	}
}

func TestWithPathCleanRejectsTraversal(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := withPathClean(inner)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/static/../../../etc/passwd", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
}

// TestWithPathCleanNoSlashSubtreeRoots verifies trailing-slash subtree roots
// without the trailing slash return 404 (aligned with the aiohttp exact-match
// router) instead of ServeMux's automatic 301 to the slashed URL, and that the
// bare /static path returns 403 (directory listing forbidden) instead of a 301
// to /static/.
func TestWithPathCleanNoSlashSubtreeRoots(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := withPathClean(inner)

	for _, p := range []string{"/api/jobs", "/api/flow/jobs", "/api/flow/plugins", "/api/serve/applications"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", p, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", p, rec.Code)
		}
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/static", nil))
	if rec.Code != http.StatusForbidden {
		t.Errorf("GET /static = %d, want 403", rec.Code)
	}
}

// TestStaticHandlerForbidsDirectoryListing verifies directory requests under
// /static return 403 (aligned with Python routes.static) while real files are
// served, exercised through the full middleware chain and mux. The fixture
// mirrors the real frontend layout (css/js subdirectories with files).
func TestStaticHandlerForbidsDirectoryListing(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "static", "css"), 0755)
	os.MkdirAll(filepath.Join(dir, "static", "js"), 0755)
	if err := os.WriteFile(filepath.Join(dir, "static", "css", "main.css"), []byte("body{}"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "static", "js", "app.js"), []byte("js"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &HeadConfig{StaticDir: dir}
	srv, err := NewHTTPServer(cfg, nil, NewMetricsRegistry(cfg))
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		path string
		want int
	}{
		// Directory requests (bare, slashed, no-slash subdirs) -> 403.
		{"/static", 403},
		{"/static/", 403},
		{"/static/css", 403},
		{"/static/css/", 403},
		{"/static/js", 403},
		{"/static/js/", 403},
		// Real files under the tree -> 200.
		{"/static/css/main.css", 200},
		{"/static/js/app.js", 200},
		// Nonexistent paths -> 404 (FileServer).
		{"/static/nonexist", 404},
		{"/static/nonexist/", 404},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		srv.server.Handler.ServeHTTP(rec, httptest.NewRequest("GET", c.path, nil))
		if rec.Code != c.want {
			t.Errorf("GET %s = %d, want %d", c.path, rec.Code, c.want)
		}
	}
}

// TestWithExactSubtreeCheck verifies the middleware reproduces the Python
// aiohttp exact-match routing on top of Go ServeMux trailing-slash subtrees:
// unregistered deeper paths are 404, method mismatches on registered paths are
// 405, and registered paths with the correct method pass through to the mux.
func TestWithExactSubtreeCheck(t *testing.T) {
	// A real mux mirroring the job/flow/serve module registrations so pass-
	// through requests exercise actual dispatch.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/jobs/", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("POST /api/jobs/", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(201) })
	mux.HandleFunc("GET /api/jobs/{job_or_submission_id}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("POST /api/jobs/{job_or_submission_id}/stop", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /api/serve/applications/", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := withExactSubtreeCheck(mux)

	cases := []struct {
		name, method, path string
		want               int
	}{
		// Correct methods on registered paths pass through.
		{"list jobs", "GET", "/api/jobs/", 200},
		{"submit job", "POST", "/api/jobs/", 201},
		{"job detail", "GET", "/api/jobs/abc", 200},
		{"stop job", "POST", "/api/jobs/abc/stop", 200},
		{"serve applications", "GET", "/api/serve/applications/", 200},
		// Unregistered deeper paths are 404 (aiohttp exact match).
		{"deep unknown", "GET", "/api/jobs/abc/yyy", 404},
		{"deep unknown under logs", "GET", "/api/jobs/abc/logs/xyz", 404},
		{"flow deep unknown", "GET", "/api/flow/jobs/abc/unknown", 404},
		{"flow plugins deep unknown", "GET", "/api/flow/plugins/abc/unknown", 404},
		{"serve sub path", "GET", "/api/serve/applications/foo", 404},
		{"serve sub path put", "PUT", "/api/serve/applications/foo", 404},
		// Method mismatches on registered paths are 405.
		{"get stop", "GET", "/api/jobs/abc/stop", 405},
		{"delete stop", "DELETE", "/api/jobs/abc/stop", 405},
		{"post detail", "POST", "/api/jobs/abc", 405},
		{"put jobs root", "PUT", "/api/jobs/", 405},
		{"get flow stop", "GET", "/api/flow/jobs/abc/stop", 405},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(c.method, c.path, nil))
			if rec.Code != c.want {
				t.Fatalf("%s %s = %d, want %d", c.method, c.path, rec.Code, c.want)
			}
		})
	}

	// 405 responses carry an Allow header with the accepted methods.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/jobs/abc/stop", nil))
	if allow := rec.Header().Get("Allow"); !strings.Contains(allow, "POST") {
		t.Fatalf("Allow header = %q, want POST", allow)
	}
}

func TestWithBrowserPostPutBlock(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := withBrowserPostPutBlock(map[string]bool{"/api/authenticate": true}, inner)
	req := httptest.NewRequest("POST", "/api/jobs/", nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
	// whitelist path passes
	req2 := httptest.NewRequest("POST", "/api/authenticate", nil)
	req2.Header.Set("User-Agent", "Mozilla/5.0")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec2.Code)
	}
	// Non-browser POST without browser signals passes.
	req3 := httptest.NewRequest("POST", "/api/jobs/", nil)
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Fatalf("expected 200 for non-browser POST, got %d", rec3.Code)
	}
}

// TestIsBrowserRequest pins the browser heuristics against Python's
// is_browser_request (dashboard_optional_utils.py): Mozilla UA, Sec-Fetch-*,
// and CORS headers all count as browser signals.
func TestIsBrowserRequest(t *testing.T) {
	cases := []struct {
		name    string
		headers map[string]string
		want    bool
	}{
		{"no headers", nil, false},
		{"mozilla ua", map[string]string{"User-Agent": "Mozilla/5.0"}, true},
		{"mozilla prefix no slash", map[string]string{"User-Agent": "Mozilla-like-agent"}, true},
		{"sec fetch mode", map[string]string{"Sec-Fetch-Mode": "cors"}, true},
		{"sec fetch dest", map[string]string{"Sec-Fetch-Dest": "document"}, true},
		{"referer", map[string]string{"Referer": "http://localhost:18265/"}, true},
		{"origin", map[string]string{"Origin": "http://localhost:18265"}, true},
		{"cors preflight method", map[string]string{"Access-Control-Request-Method": "PUT"}, true},
		{"cors preflight headers", map[string]string{"Access-Control-Request-Headers": "content-type"}, true},
		{"curl ua", map[string]string{"User-Agent": "curl/8.0"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/api/jobs/", nil)
			for k, v := range c.headers {
				r.Header.Set(k, v)
			}
			if got := isBrowserRequest(r); got != c.want {
				t.Fatalf("isBrowserRequest = %v, want %v", got, c.want)
			}
		})
	}
}

func TestDefaultHeadConfigPort(t *testing.T) {
	cfg := DefaultHeadConfig()
	if cfg.HTTPPort != 8265 {
		t.Fatalf("HTTPPort = %d, want 8265", cfg.HTTPPort)
	}
	if cfg.MetricsExportPort != 44227 {
		t.Fatalf("MetricsExportPort = %d, want 44227", cfg.MetricsExportPort)
	}
}

// TestDefaultHeadConfigMetricsPortEnv verifies the DASHBOARD_METRIC_PORT env
// var overrides the default metrics export port (aligned with Python's
// env_integer in consts.py).
func TestDefaultHeadConfigMetricsPortEnv(t *testing.T) {
	t.Setenv("DASHBOARD_METRIC_PORT", "50000")
	cfg := DefaultHeadConfig()
	if cfg.MetricsExportPort != 50000 {
		t.Fatalf("MetricsExportPort = %d, want 50000 (DASHBOARD_METRIC_PORT override)", cfg.MetricsExportPort)
	}
}

func TestAuthenticateHandlerSetsCookie(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret-token")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/authenticate", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	authenticateHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	cookies := rec.Result().Cookies()
	var found *http.Cookie
	for _, c := range cookies {
		if c.Name == authenticationTokenCookie {
			found = c
			break
		}
	}
	if found == nil {
		t.Fatal("authentication cookie not set")
	}
	if found.Value != "secret-token" {
		t.Fatalf("cookie value = %q, want %q", found.Value, "secret-token")
	}
	if !found.HttpOnly || found.SameSite != http.SameSiteStrictMode || found.Path != "/" {
		t.Fatalf("cookie flags wrong: %+v", found)
	}
	if found.MaxAge != authenticationTokenCookieMaxAge {
		t.Fatalf("cookie max age = %d, want %d", found.MaxAge, authenticationTokenCookieMaxAge)
	}
}

func TestTimezoneHandlerReturnsValue(t *testing.T) {
	rec := httptest.NewRecorder()
	timezoneHandler(rec, httptest.NewRequest("GET", "/timezone", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body struct {
		Offset string `json:"offset"`
		Value  string `json:"value"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid timezone body %q: %v", rec.Body.String(), err)
	}
	if body.Offset == "" {
		t.Fatalf("offset missing in %s", rec.Body.String())
	}
	// The value must be one of the 33 timezone table entries (or empty when the
	// current offset is not in the table).
	if body.Value != "" {
		found := false
		for _, tz := range timezones {
			if tz.value == body.Value {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("value %q not in timezone table", body.Value)
		}
	}
}

func TestAuthenticationModeDisabledClearsCookie(t *testing.T) {
	// Ensure auth is disabled.
	if tokenAuthEnabled() {
		t.Skip("token auth enabled in environment")
	}
	rec := httptest.NewRecorder()
	authenticationModeHandler(rec, httptest.NewRequest("GET", "/api/authentication_mode", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	cookies := rec.Result().Cookies()
	var cleared *http.Cookie
	for _, c := range cookies {
		if c.Name == authenticationTokenCookie {
			cleared = c
			break
		}
	}
	if cleared == nil {
		t.Fatal("expected cookie clear on disabled auth mode")
	}
	if cleared.Value != "" {
		t.Fatalf("cookie value = %q, want empty", cleared.Value)
	}
	if cleared.MaxAge != 0 {
		t.Fatalf("cookie max age = %d, want 0", cleared.MaxAge)
	}
}

func TestIndexHandlerCacheControlNoStore(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("<html></html>"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &HeadConfig{StaticDir: dir}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	indexHandler(cfg).ServeHTTP(rec, req)
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", cc)
	}
}

func TestWithMaxBodySizeRejectsOversized(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	h := withMaxBodySize(inner)

	// A handler that never reads the body is not rejected even when the
	// declared Content-Length is over the limit: aiohttp only enforces
	// client_max_size when the handler reads the body, so there is no
	// fail-fast on Content-Length.
	big := bytes.Repeat([]byte("x"), dashboardClientMaxSize+1)
	req := httptest.NewRequest("POST", "/", bytes.NewReader(big))
	req.ContentLength = int64(len(big))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body not read, no fail-fast)", rec.Code)
	}

	// A request under the limit passes through.
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("POST", "/", bytes.NewReader([]byte("hello"))))
	if rec2.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec2.Code)
	}

	// The body is wrapped in a MaxBytesReader, so an oversized streaming body
	// errors on read even when no large Content-Length is declared.
	readErr := error(nil)
	streamHandler := withMaxBodySize(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, readErr = io.ReadAll(r.Body)
		w.WriteHeader(200)
	}))
	req3 := httptest.NewRequest("POST", "/", bytes.NewReader(big))
	req3.ContentLength = -1 // unknown length: no Content-Length to check
	rec3 := httptest.NewRecorder()
	streamHandler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (handler writes after swallow)", rec3.Code)
	}
	if readErr == nil {
		t.Fatal("expected MaxBytesReader error on oversized streaming body")
	}
}

func TestWithAccessLogRecordsRequest(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("ok"))
	})
	// Swap in a capture sink so the test can assert on the emitted access line.
	// SetLogger delegates into the package-global DelegatingLogSink (it never
	// replaces the wrapper), so the restore re-fulfills that sink with a
	// NullLogSink — the same no-op behavior it has at init.
	captured := &captureSink{}
	log.SetLogger(logr.New(captured))
	defer log.SetLogger(logr.New(&log.NullLogSink{}))
	h := withAccessLog(inner)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/jobs/?page=1", nil)
	req.Header.Set("Referer", "http://localhost:18265/")
	req.Header.Set("User-Agent", "curl/8.0")
	req.RemoteAddr = "10.0.0.1:12345"
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	if captured.msg != "access" {
		t.Fatalf("log message = %q, want access", captured.msg)
	}
	kv := captured.keysAndValues
	for _, want := range []struct{ k, v string }{
		{"method", "GET"},
		{"path", "/api/jobs/"},
		{"query", "page=1"},
		{"status", "201"},
		{"remote", "10.0.0.1"},
		{"referer", "http://localhost:18265/"},
		{"user_agent", "curl/8.0"},
	} {
		if val := logKV(kv, want.k); val != want.v {
			t.Errorf("access log %s = %q, want %q", want.k, val, want.v)
		}
	}
}

func TestHTTPServerNormalizesLocalhostHost(t *testing.T) {
	cfg := DefaultHeadConfig()
	cfg.HTTPHost = "localhost"
	cfg.StaticDir = t.TempDir()
	srv, err := NewHTTPServer(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPHost != "127.0.0.1" {
		t.Fatalf("HTTPHost = %q, want 127.0.0.1 after normalization", cfg.HTTPHost)
	}
	host, _ := srv.Address()
	if host != "127.0.0.1" {
		t.Fatalf("address host = %q, want 127.0.0.1", host)
	}
}

// TestAccessLogAllowsWebSocketHijack verifies the WebSocket upgrade works
// through the full middleware chain built by NewHTTPServer (including
// withAccessLog). The statusRecorder must forward http.Hijacker to the wrapped
// ResponseWriter, otherwise websocket.Accept fails and returns 501.
func TestAccessLogAllowsWebSocketHijack(t *testing.T) {
	cfg := DefaultHeadConfig()
	cfg.ServeFrontend = true
	cfg.StaticDir = t.TempDir()
	if err := os.WriteFile(filepath.Join(cfg.StaticDir, "index.html"), []byte("<html></html>"), 0644); err != nil {
		t.Fatal(err)
	}

	// A stub module whose WS handler accepts the upgrade and echoes one frame.
	echo := stubModule{
		name: "ws-echo",
		register: func(mux *http.ServeMux) error {
			mux.HandleFunc("GET /ws/echo", func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					t.Errorf("websocket.Accept through middleware: %v", err)
					return
				}
				defer conn.Close(websocket.StatusNormalClosure, "")
				_, data, err := conn.Read(r.Context())
				if err != nil {
					return
				}
				_ = conn.Write(r.Context(), websocket.MessageText, data)
			})
			return nil
		},
	}
	srv, err := NewHTTPServer(cfg, []HeadModule{echo}, NewMetricsRegistry(cfg))
	if err != nil {
		t.Fatal(err)
	}
	headSrv := httptest.NewServer(srv.server.Handler)
	defer headSrv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(headSrv.URL, "http")+"/ws/echo", nil)
	if err != nil {
		t.Fatalf("dial through middleware chain: %v (status %s)", err, resp.Status)
	}
	defer conn.CloseNow()
	if err := conn.Write(ctx, websocket.MessageText, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if typ != websocket.MessageText || string(data) != "ping" {
		t.Fatalf("echo = %s %q", typ, data)
	}
}

// TestBuiltinRoutesMethodRestricted verifies the built-in routes reject
// non-matching methods with 405 (aligned with aiohttp), and that a path that
// exists under GET / only (e.g. /api/authenticate with GET) is also 405.
func TestBuiltinRoutesMethodRestricted(t *testing.T) {
	cfg := DefaultHeadConfig()
	cfg.ServeFrontend = true
	cfg.StaticDir = t.TempDir()
	if err := os.WriteFile(filepath.Join(cfg.StaticDir, "index.html"), []byte("<html></html>"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.StaticDir, "favicon.ico"), []byte("ico"), 0644); err != nil {
		t.Fatal(err)
	}
	srv, err := NewHTTPServer(cfg, nil, NewMetricsRegistry(cfg))
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		method, path string
		want         int
	}{
		{"POST", "/timezone", http.StatusMethodNotAllowed},
		{"POST", "/", http.StatusMethodNotAllowed},
		{"POST", "/favicon.ico", http.StatusMethodNotAllowed},
		{"POST", "/api/authentication_mode", http.StatusMethodNotAllowed},
		{"GET", "/api/authenticate", http.StatusMethodNotAllowed},
		{"OPTIONS", "/", http.StatusMethodNotAllowed},
		{"GET", "/api/authentication_mode", http.StatusOK},
		{"GET", "/favicon.ico", http.StatusOK},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		srv.server.Handler.ServeHTTP(rec, httptest.NewRequest(c.method, c.path, nil))
		if rec.Code != c.want {
			t.Errorf("%s %s = %d, want %d", c.method, c.path, rec.Code, c.want)
		}
	}
	// A method-mismatched request to a registered path carries an Allow header.
	rec := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, httptest.NewRequest("POST", "/timezone", nil))
	if allow := rec.Header().Get("Allow"); !strings.Contains(allow, "GET") {
		t.Errorf("Allow header = %q, want GET", allow)
	}
}

func TestAuthenticateHandlerCookieEnablesWithTokenAuth(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret-token")

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := withTokenAuth(PublicExactPaths, PublicPathPrefixes, inner)

	// Request with the authentication cookie only, no Authorization header.
	req := httptest.NewRequest("POST", "/api/jobs/", nil)
	req.AddCookie(&http.Cookie{Name: authenticationTokenCookie, Value: "secret-token"})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with cookie auth, got %d", rec.Code)
	}
}

// TestErrorBodiesMatchAiohttp pins the HTTP error response text/headers that
// this patch aligns with aiohttp: 405 body and comma-separated Allow, 500 body
// without nosniff, 403 body with the status prefix, and no nosniff anywhere.
func TestErrorBodiesMatchAiohttp(t *testing.T) {
	cfg := DefaultHeadConfig()
	cfg.ServeFrontend = true
	cfg.StaticDir = t.TempDir()
	if err := os.WriteFile(filepath.Join(cfg.StaticDir, "index.html"), []byte("<html></html>"), 0644); err != nil {
		t.Fatal(err)
	}
	srv, err := NewHTTPServer(cfg, nil, NewMetricsRegistry(cfg))
	if err != nil {
		t.Fatal(err)
	}

	// 405 from a method-mismatched built-in route: body is "405: Method Not
	// Allowed" and the Allow header is comma-separated (no ", ").
	rec := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, httptest.NewRequest("POST", "/timezone", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /timezone = %d, want 405", rec.Code)
	}
	if got := rec.Body.String(); got != "405: Method Not Allowed" {
		t.Fatalf("405 body = %q, want %q", got, "405: Method Not Allowed")
	}
	if allow := rec.Header().Get("Allow"); allow != "GET,HEAD" {
		t.Fatalf("Allow = %q, want GET,HEAD (comma-separated)", allow)
	}
	if rec.Header().Get("X-Content-Type-Options") != "" {
		t.Fatalf("405 carries nosniff: %q", rec.Header().Get("X-Content-Type-Options"))
	}

	// GET /api/authenticate: 405 with Allow: POST.
	rec = httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/api/authenticate", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /api/authenticate = %d, want 405", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); allow != "POST" {
		t.Fatalf("GET /api/authenticate Allow = %q, want POST", allow)
	}
	if got := rec.Body.String(); got != "405: Method Not Allowed" {
		t.Fatalf("405 body = %q, want %q", got, "405: Method Not Allowed")
	}

	// 403 body carries the status prefix and no nosniff.
	rec = httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/static", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("GET /static = %d, want 403", rec.Code)
	}
	if got := rec.Body.String(); !strings.HasPrefix(got, "403: Forbidden") {
		t.Fatalf("403 body = %q, want prefix %q", got, "403: Forbidden")
	}
	if rec.Header().Get("X-Content-Type-Options") != "" {
		t.Fatalf("403 carries nosniff: %q", rec.Header().Get("X-Content-Type-Options"))
	}

	// 500 from a panicking handler: aiohttp-style body, no nosniff.
	boom := stubModule{
		name: "boom",
		register: func(mux *http.ServeMux) error {
			mux.HandleFunc("GET /boom", func(w http.ResponseWriter, r *http.Request) {
				panic("boom")
			})
			return nil
		},
	}
	srv2, err := NewHTTPServer(cfg, []HeadModule{boom}, NewMetricsRegistry(cfg))
	if err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	srv2.server.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/boom", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("GET /boom = %d, want 500", rec.Code)
	}
	if got := rec.Body.String(); got != "500 Internal Server Error\n\nAn unexpected error has occurred." {
		t.Fatalf("500 body = %q", got)
	}
	if rec.Header().Get("X-Content-Type-Options") != "" {
		t.Fatalf("500 carries nosniff: %q", rec.Header().Get("X-Content-Type-Options"))
	}
}

// TestStaticHandlerRejectsSymlink verifies /static rejects a symlinked asset
// with 404 when RAY_DASHBOARD_BUILD_FOLLOW_SYMLINKS is unset, and serves it
// when the env var is "1" (aligned with aiohttp follow_symlinks).
func TestStaticHandlerRejectsSymlink(t *testing.T) {
	real := t.TempDir()
	if err := os.WriteFile(filepath.Join(real, "secret.txt"), []byte("secret"), 0644); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "static"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "static", "ok.txt"), []byte("ok"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(real, "secret.txt"), filepath.Join(dir, "static", "link.txt")); err != nil {
		t.Fatal(err)
	}

	t.Setenv("RAY_DASHBOARD_BUILD_FOLLOW_SYMLINKS", "")
	cfg := &HeadConfig{StaticDir: dir}
	srv, err := NewHTTPServer(cfg, nil, NewMetricsRegistry(cfg))
	if err != nil {
		t.Fatal(err)
	}
	// Symlinked asset rejected 404.
	rec := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/static/link.txt", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /static/link.txt = %d, want 404 (symlink disabled)", rec.Code)
	}
	// Plain file still served.
	rec = httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/static/ok.txt", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /static/ok.txt = %d, want 200", rec.Code)
	}

	// With the env var set, the symlinked asset is served.
	t.Setenv("RAY_DASHBOARD_BUILD_FOLLOW_SYMLINKS", "1")
	srv2, err := NewHTTPServer(cfg, nil, NewMetricsRegistry(cfg))
	if err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	srv2.server.Handler.ServeHTTP(rec, httptest.NewRequest("GET", "/static/link.txt", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /static/link.txt = %d, want 200 (symlink enabled), body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "secret" {
		t.Fatalf("served body = %q, want secret", rec.Body.String())
	}
}
