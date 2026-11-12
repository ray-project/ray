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

// Package auth holds the authentication and browser-guard middleware shared by
// the dashboard head and the dashboard agent. It mirrors the Python
// http_token_authentication and dashboard_optional_utils behavior.
package auth

import (
	"crypto/subtle"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
)

// Header/cookie names aligned with python/ray/_private/authentication/authentication_constants.py.
const (
	AuthorizationHeaderName   = "authorization"
	RayAuthorizationHeader    = "x-ray-authorization"
	AuthenticationTokenCookie = "ray-authentication-token"
	BearerPrefix              = "Bearer "
)

// TokenAuthEnabled reports whether token authentication is enabled via the
// RAY_AUTH_MODE environment variable. The comparison is case-insensitive: the
// C++ GetAuthenticationMode lowercases the value before comparing
// (authentication_mode.cc), and the gcs dial layer already relies on that.
func TokenAuthEnabled() bool {
	mode := os.Getenv("RAY_AUTH_MODE")
	return strings.EqualFold(mode, "token") || strings.EqualFold(mode, "k8s")
}

// TokenModeEnabled reports whether the auth mode is token, the narrower gate
// the gRPC server interceptor enforces under: the C++ raylet attaches client
// credentials only in TOKEN mode (it maps "k8s" to DISABLED), so enforcing in
// k8s mode would reject the raylet's credential-free metrics calls.
func TokenModeEnabled() bool {
	return strings.EqualFold(os.Getenv("RAY_AUTH_MODE"), "token")
}

// AuthenticationModeName returns the mode name string for the
// /api/authentication_mode endpoint, aligned with
// authentication_utils.get_authentication_mode_name. The value is normalised
// to lower case, mirroring the C++ comparison; a raw "TOKEN" env value with a
// case-sensitive enablement check would otherwise report a mode the rest of
// the process does not consider enabled.
func AuthenticationModeName() string {
	if !TokenAuthEnabled() {
		return "disabled"
	}
	return strings.ToLower(strings.TrimSpace(os.Getenv("RAY_AUTH_MODE")))
}

// Environment variables read by RequestToken, matching the C++
// AuthenticationTokenLoader sources (authentication_token_loader.cc). The
// first two names are the same ones go/pkg/gcs reads; the k8s flag mirrors
// ENABLE_K8S_TOKEN_AUTH (src/ray/common/ray_config_def.h).
const (
	envAuthToken          = "RAY_AUTH_TOKEN"
	envAuthTokenPath      = "RAY_AUTH_TOKEN_PATH"
	envEnableK8sTokenAuth = "RAY_ENABLE_K8S_TOKEN_AUTH"
)

// RayServiceAccountTokenPath is the in-pod Ray service-account token file read
// as the third token source. It mirrors gcs.RayServiceAccountTokenPath and is
// a variable so tests can substitute a fixture.
var RayServiceAccountTokenPath = gcs.RayServiceAccountTokenPath

// RequestToken returns the bearer token an outgoing dashboard request must
// carry, resolving it with the C++ AuthenticationTokenLoader precedence
// (TryLoadTokenFromSources in authentication_token_loader.cc):
//  1. RAY_AUTH_TOKEN, whitespace-trimmed.
//  2. RAY_AUTH_TOKEN_PATH, first non-empty line via gcs.ReadTokenFile. A path
//     that is set but unreadable or empty is a hard stop: the C++ loader
//     returns an error instead of falling through to later sources.
//  3. The Ray service-account token when RAY_ENABLE_K8S_TOKEN_AUTH is on.
//  4. The default ~/.ray/auth_token.
//
// It returns false when token authentication is disabled (RAY_AUTH_MODE is
// neither token nor k8s) or no source yields a token, so callers attach no
// header. Nothing is cached: token files are re-read on every call so a rotated
// token takes effect without a restart, matching the gcs dial layer.
func RequestToken() (string, bool) {
	if !TokenAuthEnabled() {
		return "", false
	}
	if raw, set := os.LookupEnv(envAuthToken); set && raw != "" {
		// C++ treats a non-empty RAY_AUTH_TOKEN as authoritative and trims it; a
		// whitespace-only value trims to empty and stops the lookup instead of
		// falling through to the later sources.
		token := strings.TrimSpace(raw)
		if token == "" {
			log.Log.V(1).Info("RAY_AUTH_TOKEN is set but holds only whitespace; no token is attached")
		}
		return token, token != ""
	}
	if path := os.Getenv(envAuthTokenPath); path != "" {
		token, err := gcs.ReadTokenFile(path)
		if err != nil || token == "" {
			// Mirrors the C++ loader's abort message observability without the
			// abort: the broken path stops the lookup. V(1) because RequestToken
			// runs on every report request.
			log.Log.V(1).Info("Token authentication is enabled but RAY_AUTH_TOKEN_PATH yields no token",
				"path", path)
			return "", false
		}
		return token, true
	}
	if isEnvTrue(envEnableK8sTokenAuth) {
		if token, err := gcs.ReadTokenFile(RayServiceAccountTokenPath); err == nil && token != "" {
			return token, true
		}
	}
	if path := defaultTokenPath(); path != "" {
		if token, err := gcs.ReadTokenFile(path); err == nil && token != "" {
			return token, true
		}
	}
	log.Log.V(1).Info("Token authentication is enabled but no token was found; " +
		"requests to authenticated clusters will fail")
	return "", false
}

// isEnvTrue reports whether the environment variable is truthy, matching the
// C++ ConvertValue<bool> values ("1" and "true", case-insensitive).
func isEnvTrue(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true":
		return true
	default:
		return false
	}
}

// defaultTokenPath returns ~/.ray/auth_token, resolving HOME with a fallback to
// the user database when HOME is unset (C++ GetDefaultTokenPath).
func defaultTokenPath() string {
	home := os.Getenv("HOME")
	if home == "" {
		if u, err := user.Current(); err == nil {
			home = u.HomeDir
		}
	}
	if home == "" {
		return ""
	}
	return filepath.Join(home, gcs.DefaultAuthTokenPath)
}

// ValidateRequestToken validates an Authorization header containing a Bearer
// token. In token mode it compares against the expected token (RAY_AUTH_TOKEN
// or RAY_AUTH_TOKEN_PATH), constant-time to avoid leaking the expected token
// length. In k8s mode it asks the Kubernetes API for a TokenReview +
// SubjectAccessReview.
func ValidateRequestToken(header string) bool {
	if len(header) <= len(BearerPrefix) || header[:len(BearerPrefix)] != BearerPrefix {
		return false
	}
	token := header[len(BearerPrefix):]
	if token == "" {
		return false
	}
	// The k8s validator is the non-token branch of the enabled modes, so the
	// comparison is case-insensitive like TokenAuthEnabled.
	if !TokenModeEnabled() && TokenAuthEnabled() {
		return k8sTokenValidator.validate(token)
	}
	expected := expectedToken()
	if expected == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
}

// expectedToken returns the expected bearer token for token mode. Precedence
// 1 is the RAY_AUTH_TOKEN env var; precedence 2 is the RAY_AUTH_TOKEN_PATH
// file (first non-empty line, trimmed). This mirrors the C++
// AuthenticationTokenLoader.
//
// The env value is trimmed here as well as in RequestToken, because the C++
// loader trims it: an environment value carrying a trailing newline must
// compare equal to the one RequestToken sends, or every request the head or
// agent makes to itself under token mode would be rejected.
//
// The file is re-read on every call so a rotated token takes effect without a
// restart; there is nothing to cache, because the file itself is the state.
func expectedToken() string {
	if raw, set := os.LookupEnv(envAuthToken); set && raw != "" {
		return strings.TrimSpace(raw)
	}
	path := os.Getenv(envAuthTokenPath)
	if path == "" {
		return ""
	}
	return readTokenFileOrEmpty(path)
}

// readTokenFileOrEmpty returns the first NON-empty trimmed line of path, or ""
// when the file cannot be read or holds no token. Note: gcs.ReadTokenFile
// skips leading blank lines, unlike the reference implementation's strict
// first-line read, so a leading blank line still yields the token here
// (deliberate preservation of the OSS behavior).
func readTokenFileOrEmpty(path string) string {
	token, err := gcs.ReadTokenFile(path)
	if err != nil {
		return ""
	}
	return token
}

// WithTokenAuth is the token authentication middleware. It passes every
// request through unchanged when token auth is not enabled. When enabled, it
// skips the whitelisted public paths/prefixes and requires a valid bearer
// token for everything else, aligned with
// http_token_authentication.get_token_auth_middleware. The token may come
// from the standard Authorization header, the X-Ray-Authorization header, or
// the authentication cookie.
func WithTokenAuth(publicExact, publicPrefixes []string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !TokenAuthEnabled() {
			next.ServeHTTP(w, r)
			return
		}
		path := r.URL.Path
		for _, p := range publicExact {
			if path == p {
				next.ServeHTTP(w, r)
				return
			}
		}
		for _, p := range publicPrefixes {
			if len(path) >= len(p) && path[:len(p)] == p {
				next.ServeHTTP(w, r)
				return
			}
		}
		authHeader := r.Header.Get(AuthorizationHeaderName)
		if authHeader == "" {
			authHeader = r.Header.Get(RayAuthorizationHeader)
		}
		if authHeader == "" {
			if cookie, err := r.Cookie(AuthenticationTokenCookie); err == nil {
				authHeader = BearerPrefix + cookie.Value
			}
		}
		if authHeader == "" {
			writePlainText(w, http.StatusUnauthorized, "Unauthorized: Missing authentication token")
			return
		}
		if !ValidateRequestToken(authHeader) {
			writePlainText(w, http.StatusForbidden, "Forbidden: Invalid authentication token")
			return
		}
		next.ServeHTTP(w, r)
	})
}
