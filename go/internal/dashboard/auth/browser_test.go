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
	"net/http"
	"net/http/httptest"
	"testing"
)

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
		{"sec fetch site", map[string]string{"Sec-Fetch-Site": "same-origin"}, true},
		{"sec fetch user", map[string]string{"Sec-Fetch-User": "?1"}, true},
		{"referer", map[string]string{"Referer": "http://localhost:18265/"}, true},
		{"origin", map[string]string{"Origin": "http://localhost:18265"}, true},
		{"cors preflight method", map[string]string{"Access-Control-Request-Method": "PUT"}, true},
		{"cors preflight headers", map[string]string{"Access-Control-Request-Headers": "content-type"}, true},
		{"curl ua", map[string]string{"User-Agent": "curl/8.0"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/api/jobs/", nil)
			for k, v := range c.headers {
				r.Header.Set(k, v)
			}
			if got := IsBrowserRequest(r); got != c.want {
				t.Fatalf("IsBrowserRequest = %v, want %v", got, c.want)
			}
		})
	}
}

func TestWithBrowserBlock(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := WithBrowserBlock(next)

	req := httptest.NewRequest(http.MethodGet, "/api/healthz", nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("browser GET = %d, want 403", rec.Code)
	}
	if got, want := rec.Body.String(), "Browser requests not allowed."; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("non-browser GET = %d, want 200", rec.Code)
	}
}

func TestWithBrowserPostPutBlock(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := WithBrowserPostPutBlock(map[string]bool{"/api/authenticate": true}, next)

	cases := []struct {
		name      string
		method    string
		path      string
		userAgent string
		want      int
		wantBody  string
	}{
		{
			name:      "browser post blocked",
			method:    http.MethodPost,
			path:      "/api/jobs/",
			userAgent: "Mozilla/5.0",
			want:      http.StatusMethodNotAllowed,
			wantBody:  "Method Not Allowed for browser traffic.",
		},
		{
			name:      "browser put blocked",
			method:    http.MethodPut,
			path:      "/api/serve/applications/",
			userAgent: "Mozilla/5.0",
			want:      http.StatusMethodNotAllowed,
			wantBody:  "Method Not Allowed for browser traffic.",
		},
		{
			name:      "browser get allowed",
			method:    http.MethodGet,
			path:      "/api/jobs/",
			userAgent: "Mozilla/5.0",
			want:      http.StatusOK,
		},
		{
			name:      "whitelisted path allowed",
			method:    http.MethodPost,
			path:      "/api/authenticate",
			userAgent: "Mozilla/5.0",
			want:      http.StatusOK,
		},
		{
			name:      "insight post allowed",
			method:    http.MethodPost,
			path:      "/insight/events",
			userAgent: "Mozilla/5.0",
			want:      http.StatusOK,
		},
		{
			name:      "non browser post allowed",
			method:    http.MethodPost,
			path:      "/api/jobs/",
			userAgent: "curl/8.0",
			want:      http.StatusOK,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(c.method, c.path, nil)
			if c.userAgent != "" {
				req.Header.Set("User-Agent", c.userAgent)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != c.want {
				t.Fatalf("status = %d, want %d", rec.Code, c.want)
			}
			if c.wantBody != "" && rec.Body.String() != c.wantBody {
				t.Fatalf("body = %q, want %q", rec.Body.String(), c.wantBody)
			}
		})
	}
}
