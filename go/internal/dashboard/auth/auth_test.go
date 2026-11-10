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
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestAuthenticationConstants(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"authorization header", AuthorizationHeaderName, "authorization"},
		{"ray authorization header", RayAuthorizationHeader, "x-ray-authorization"},
		{"token cookie", AuthenticationTokenCookie, "ray-authentication-token"},
		{"bearer prefix", BearerPrefix, "Bearer "},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Fatalf("%s = %q, want %q", c.name, c.got, c.want)
			}
		})
	}
}

func TestTokenAuthEnabledModes(t *testing.T) {
	cases := []struct {
		mode string
		want bool
	}{
		{"", false},
		{"disabled", false},
		{"token", true},
		{"k8s", true},
		{"unknown", false},
	}
	for _, c := range cases {
		t.Run(c.mode, func(t *testing.T) {
			t.Setenv("RAY_AUTH_MODE", c.mode)
			if got := TokenAuthEnabled(); got != c.want {
				t.Fatalf("TokenAuthEnabled() = %v, want %v for mode %q", got, c.want, c.mode)
			}
		})
	}
}

func TestAuthenticationModeName(t *testing.T) {
	cases := []struct {
		mode string
		want string
	}{
		{"", "disabled"},
		{"disabled", "disabled"},
		{"unknown", "disabled"},
		{"token", "token"},
		{"k8s", "k8s"},
	}
	for _, c := range cases {
		t.Run(c.mode, func(t *testing.T) {
			t.Setenv("RAY_AUTH_MODE", c.mode)
			if got := AuthenticationModeName(); got != c.want {
				t.Fatalf("AuthenticationModeName() = %q, want %q for mode %q", got, c.want, c.mode)
			}
		})
	}
}

func TestValidateRequestTokenRejectsMalformedHeader(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret")
	t.Setenv("RAY_AUTH_TOKEN_PATH", "")

	for _, header := range []string{
		"",
		"secret",
		"Token secret",
		"bearer secret",
		"Bearer ",
		"Bearer  ",
	} {
		if ValidateRequestToken(header) {
			t.Fatalf("ValidateRequestToken(%q) = true, want false", header)
		}
	}
}

func TestValidateRequestTokenFromEnv(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "env-token")
	t.Setenv("RAY_AUTH_TOKEN_PATH", "")

	if !ValidateRequestToken("Bearer env-token") {
		t.Fatal("expected the RAY_AUTH_TOKEN value to be accepted")
	}
	if ValidateRequestToken("Bearer other-token") {
		t.Fatal("expected a mismatched token to be rejected")
	}
}

// TestValidateRequestTokenFromFile pins the RAY_AUTH_TOKEN_PATH fallback and
// the per-call re-read so a rotated token file takes effect without a restart.
func TestValidateRequestTokenFromFile(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "")
	path := filepath.Join(t.TempDir(), "auth_token")
	if err := os.WriteFile(path, []byte("\n  file-token  \nsecond-line\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RAY_AUTH_TOKEN_PATH", path)

	// The first non-empty line is trimmed and used, mirroring ReadTokenFile.
	if !ValidateRequestToken("Bearer file-token") {
		t.Fatal("expected the trimmed first line of the token file to be accepted")
	}
	if ValidateRequestToken("Bearer second-line") {
		t.Fatal("expected only the first non-empty line to be accepted")
	}

	// Rotate the file: the loader re-reads it on every call.
	if err := os.WriteFile(path, []byte("rotated-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if !ValidateRequestToken("Bearer rotated-token") {
		t.Fatal("expected the rotated token to be accepted without a restart")
	}
	if ValidateRequestToken("Bearer file-token") {
		t.Fatal("expected the previous token to be rejected after rotation")
	}
}

func TestValidateRequestTokenNoExpectedToken(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "")
	t.Setenv("RAY_AUTH_TOKEN_PATH", filepath.Join(t.TempDir(), "missing"))

	if ValidateRequestToken("Bearer anything") {
		t.Fatal("expected validation to fail when no expected token is configured")
	}
}

// TestValidateRequestTokenK8sModeWithoutCluster verifies that k8s mode falls
// back to the k8s validator and rejects the token when the cluster environment
// is absent (no service host/port env), never to the static token.
func TestValidateRequestTokenK8sModeWithoutCluster(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "k8s")
	t.Setenv("RAY_AUTH_TOKEN", "env-token")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	if ValidateRequestToken("Bearer env-token") {
		t.Fatal("expected k8s mode to reject the token when the k8s client cannot initialize")
	}
}

func TestWithTokenAuthDisabledPassesThrough(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := WithTokenAuth([]string{"/public"}, []string{"/static/"}, next)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/jobs/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("disabled auth = %d, want 200", rec.Code)
	}
}

func TestWithTokenAuthPublicPaths(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := WithTokenAuth([]string{"/api/healthz"}, []string{"/static/"}, next)

	for _, path := range []string{"/api/healthz", "/static/app.js", "/static/"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("public path %q = %d, want 200", path, rec.Code)
		}
	}
	// A path that only shares a textual prefix with an exact entry is not
	// public.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/healthz/extra", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("/api/healthz/extra = %d, want 401", rec.Code)
	}
}

func TestWithTokenAuthTokenSources(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret")
	t.Setenv("RAY_AUTH_TOKEN_PATH", "")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := WithTokenAuth(nil, nil, next)

	cases := []struct {
		name   string
		mutate func(*http.Request)
	}{
		{"authorization header", func(r *http.Request) {
			r.Header.Set(AuthorizationHeaderName, BearerPrefix+"secret")
		}},
		{"ray authorization header", func(r *http.Request) {
			r.Header.Set(RayAuthorizationHeader, BearerPrefix+"secret")
		}},
		{"authentication cookie", func(r *http.Request) {
			r.AddCookie(&http.Cookie{Name: AuthenticationTokenCookie, Value: "secret"})
		}},
		{"invalid ray authorization header ignored when authorization is valid", func(r *http.Request) {
			r.Header.Set(AuthorizationHeaderName, BearerPrefix+"secret")
			r.Header.Set(RayAuthorizationHeader, BearerPrefix+"wrong")
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/jobs/", nil)
			c.mutate(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
		})
	}

	// The standard Authorization header wins over the Ray header.
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/", nil)
	req.Header.Set(AuthorizationHeaderName, BearerPrefix+"wrong")
	req.Header.Set(RayAuthorizationHeader, BearerPrefix+"secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("invalid authorization with valid ray header = %d, want 403", rec.Code)
	}
}

// TestWithTokenAuthErrorResponses pins the rejection bodies to the aiohttp
// format: plain text, exact text without a trailing newline, and an explicit
// Content-Length.
func TestWithTokenAuthErrorResponses(t *testing.T) {
	t.Setenv("RAY_AUTH_MODE", "token")
	t.Setenv("RAY_AUTH_TOKEN", "secret")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := WithTokenAuth(nil, nil, next)

	cases := []struct {
		name string
		auth string
		code int
		body string
	}{
		{"missing", "", http.StatusUnauthorized, "Unauthorized: Missing authentication token"},
		{"invalid", BearerPrefix + "wrong", http.StatusForbidden, "Forbidden: Invalid authentication token"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/jobs/", nil)
			if c.auth != "" {
				req.Header.Set(AuthorizationHeaderName, c.auth)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != c.code {
				t.Fatalf("status = %d, want %d", rec.Code, c.code)
			}
			if got := rec.Body.String(); got != c.body {
				t.Fatalf("body = %q, want %q", got, c.body)
			}
			if got := rec.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
				t.Fatalf("Content-Type = %q, want text/plain; charset=utf-8", got)
			}
			if got, want := rec.Header().Get("Content-Length"), strconv.Itoa(len(c.body)); got != want {
				t.Fatalf("Content-Length = %q, want %q", got, want)
			}
		})
	}
}
