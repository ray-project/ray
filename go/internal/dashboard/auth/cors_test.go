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

package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func alwaysCORS(*http.Request) bool { return true }
func neverCORS(*http.Request) bool  { return false }

func TestWithCORSSetsHeaders(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Custom", "value")
		_, _ = w.Write([]byte(`{}`))
	})
	h := WithCORS(alwaysCORS, next)

	req := httptest.NewRequest(http.MethodGet, "/api/v0/nodes", nil)
	req.Header.Set("Origin", "http://localhost:8265")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get(headerAllowOrigin); got != "http://localhost:8265" {
		t.Fatalf("Allow-Origin = %q, want the request origin", got)
	}
	if got := rec.Header().Get(headerAllowCredentials); got != "true" {
		t.Fatalf("Allow-Credentials = %q, want true", got)
	}
	// Only non-simple response headers are exposed; Content-Type is simple.
	if got := rec.Header().Get(headerExposeHeaders); got != "X-Custom" {
		t.Fatalf("Expose-Headers = %q, want X-Custom", got)
	}
}

func TestWithCORSNoExposeForSimpleHeadersOnly(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok"))
	})
	h := WithCORS(alwaysCORS, next)
	req := httptest.NewRequest(http.MethodGet, "/api/v0/nodes", nil)
	req.Header.Set("Origin", "http://localhost:8265")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get(headerExposeHeaders); got != "" {
		t.Fatalf("Expose-Headers = %q, want empty for simple headers", got)
	}
}

func TestWithCORSDisabled(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := WithCORS(neverCORS, next)
	req := httptest.NewRequest(http.MethodGet, "/api/v0/nodes", nil)
	req.Header.Set("Origin", "http://localhost:8265")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get(headerAllowOrigin); got != "" {
		t.Fatalf("Allow-Origin = %q, want empty when CORS is disabled", got)
	}
}

func TestWithCORSNoOrigin(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := WithCORS(alwaysCORS, next)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v0/nodes", nil))

	if got := rec.Header().Get(headerAllowOrigin); got != "" {
		t.Fatalf("Allow-Origin = %q, want empty without an Origin header", got)
	}
}

// TestWithCORSOptionsUntouched verifies that OPTIONS requests (handled by
// ServePreflight) are not decorated by the CORS writer.
func TestWithCORSOptionsUntouched(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := WithCORS(alwaysCORS, next)
	req := httptest.NewRequest(http.MethodOptions, "/api/v0/nodes", nil)
	req.Header.Set("Origin", "http://localhost:8265")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get(headerAllowOrigin); got != "" {
		t.Fatalf("Allow-Origin = %q, want empty for OPTIONS", got)
	}
}

// TestWithCORSPreservesExistingAllowOrigin verifies an explicit Allow-Origin
// set by the handler is not overwritten.
func TestWithCORSPreservesExistingAllowOrigin(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(headerAllowOrigin, "https://explicit.example")
		w.WriteHeader(http.StatusOK)
	})
	h := WithCORS(alwaysCORS, next)
	req := httptest.NewRequest(http.MethodGet, "/api/v0/nodes", nil)
	req.Header.Set("Origin", "http://localhost:8265")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get(headerAllowOrigin); got != "https://explicit.example" {
		t.Fatalf("Allow-Origin = %q, want the handler value", got)
	}
}

func TestWithCORSFlush(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("CORS writer does not implement http.Flusher")
			return
		}
		flusher.Flush()
	})
	h := WithCORS(alwaysCORS, next)
	req := httptest.NewRequest(http.MethodGet, "/api/v0/nodes", nil)
	req.Header.Set("Origin", "http://localhost:8265")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get(headerAllowOrigin); got != "http://localhost:8265" {
		t.Fatalf("Allow-Origin = %q, want the request origin after Flush", got)
	}
}

func TestServePreflight(t *testing.T) {
	cases := []struct {
		name      string
		origin    string
		method    string
		headers   string
		wantCode  int
		wantBody  string
		wantAllow string
	}{
		{
			name:     "missing origin",
			method:   http.MethodPut,
			wantCode: http.StatusForbidden,
			wantBody: "CORS preflight request failed: origin header is not specified in the request",
		},
		{
			name:     "missing method",
			origin:   "http://localhost:8265",
			wantCode: http.StatusForbidden,
			wantBody: "CORS preflight request failed: 'Access-Control-Request-Method' header is not specified",
		},
		{
			name:     "disallowed header",
			origin:   "http://localhost:8265",
			method:   http.MethodPut,
			headers:  "x-bad",
			wantCode: http.StatusForbidden,
			wantBody: "CORS preflight request failed: headers are not allowed: X-BAD",
		},
		{
			name:      "allowed",
			origin:    "http://localhost:8265",
			method:    http.MethodPut,
			headers:   "content-type, X-Header",
			wantCode:  http.StatusOK,
			wantAllow: "CONTENT-TYPE,X-HEADER",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodOptions, "/api/v0/nodes", nil)
			if c.origin != "" {
				req.Header.Set(headerOrigin, c.origin)
			}
			if c.method != "" {
				req.Header.Set(headerAccessControlRequestMethod, c.method)
			}
			if c.headers != "" {
				req.Header.Set(headerAccessControlRequestHeader, c.headers)
			}
			rec := httptest.NewRecorder()
			ServePreflight(rec, req)

			if rec.Code != c.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, c.wantCode)
			}
			if c.wantBody != "" {
				if got := rec.Body.String(); got != c.wantBody {
					t.Fatalf("body = %q, want %q", got, c.wantBody)
				}
				return
			}
			if got := rec.Header().Get(headerAllowOrigin); got != c.origin {
				t.Fatalf("Allow-Origin = %q, want %q", got, c.origin)
			}
			if got := rec.Header().Get(headerAllowCredentials); got != "true" {
				t.Fatalf("Allow-Credentials = %q, want true", got)
			}
			if got := rec.Header().Get(headerAllowMethods); got != c.method {
				t.Fatalf("Allow-Methods = %q, want %q", got, c.method)
			}
			if got := rec.Header().Get(headerAllowHeaders); got != c.wantAllow {
				t.Fatalf("Allow-Headers = %q, want %q", got, c.wantAllow)
			}
			if got := rec.Header().Get("Content-Length"); got != "0" {
				t.Fatalf("Content-Length = %q, want 0", got)
			}
		})
	}
}

func TestServePreflightWithoutRequestHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/api/v0/nodes", nil)
	req.Header.Set(headerOrigin, "http://localhost:8265")
	req.Header.Set(headerAccessControlRequestMethod, http.MethodGet)
	rec := httptest.NewRecorder()
	ServePreflight(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get(headerAllowHeaders); got != "" {
		t.Fatalf("Allow-Headers = %q, want empty when no headers are requested", got)
	}
}

// TestWithCORSAllowsWebSocketHijack verifies a WebSocket upgrade works through
// the CORS wrapper, both when CORS is enabled and when it is disabled (the
// wrapper is installed unconditionally). The wrapper must forward http.Hijacker
// and Unwrap, otherwise websocket.Accept fails and returns 501.
func TestWithCORSAllowsWebSocketHijack(t *testing.T) {
	cases := []struct {
		name    string
		enabled func(*http.Request) bool
	}{
		{"enabled", alwaysCORS},
		{"disabled", neverCORS},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			echo := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					t.Errorf("websocket.Accept through the CORS wrapper: %v", err)
					return
				}
				defer conn.Close(websocket.StatusNormalClosure, "")
				_, data, err := conn.Read(r.Context())
				if err != nil {
					return
				}
				_ = conn.Write(r.Context(), websocket.MessageText, data)
			})
			srv := httptest.NewServer(WithCORS(c.enabled, echo))
			defer srv.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
			if err != nil {
				status := ""
				if resp != nil {
					status = resp.Status
				}
				t.Fatalf("dial through the CORS wrapper: %v (status %q)", err, status)
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
				t.Fatalf("echo = %v %q, want text %q", typ, data, "ping")
			}
		})
	}
}

func TestParseRequestHeaders(t *testing.T) {
	if got := parseRequestHeaders(""); got != nil {
		t.Fatalf("parseRequestHeaders(\"\") = %v, want nil", got)
	}
	got := parseRequestHeaders(" content-type , , x-header ")
	want := []string{"CONTENT-TYPE", "X-HEADER"}
	if len(got) != len(want) {
		t.Fatalf("parseRequestHeaders = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("parseRequestHeaders = %v, want %v", got, want)
		}
	}
}
