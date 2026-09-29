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
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Header/cookie names aligned with python/ray/_private/authentication/authentication_constants.py.
const (
	authorizationHeaderName         = "authorization"
	rayAuthorizationHeader          = "x-ray-authorization"
	authenticationTokenCookie       = "ray-authentication-token"
	authenticationTokenCookieMaxAge = 30 * 24 * 60 * 60 // 30 days
	bearerPrefix                    = "Bearer "
)

// K8s authentication constants aligned with
// src/ray/rpc/authentication/k8s_constants.h.
const (
	k8sServiceHostEnvVar      = "KUBERNETES_SERVICE_HOST"
	k8sServicePortEnvVar      = "KUBERNETES_SERVICE_PORT"
	k8sCaCertPath             = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	k8sSaTokenPath            = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	rayClusterNameEnvVar      = "RAY_CLUSTER_NAME"
	rayClusterNamespaceEnvVar = "RAY_CLUSTER_NAMESPACE"

	k8sTokenReviewPath   = "/apis/authentication.k8s.io/v1/tokenreviews"
	k8sSubjectAccessPath = "/apis/authorization.k8s.io/v1/subjectaccessreviews"
	k8sTokenReviewKind   = "TokenReview"
	k8sSubjectAccessKind = "SubjectAccessReview"
	k8sAuthAPIVersion    = "authentication.k8s.io/v1"
	k8sAuthzAPIVersion   = "authorization.k8s.io/v1"
	k8sResourceGroup     = "ray.io"
	k8sClusterResource   = "rayclusters"
	k8sRayUserVerb       = "ray-user"
)

// k8sTokenCacheTTL is how long a validated k8s token stays cached, aligned
// with the C++ AuthenticationTokenValidator kCacheTTL(5).
const k8sTokenCacheTTL = 5 * time.Minute

// PublicExactPaths are exact paths that do not require authentication,
// aligned with http_server_head.py public_exact_paths.
var PublicExactPaths = []string{
	"/", "/favicon.ico", "/api/authentication_mode", "/api/authenticate",
	"/api/healthz", "/api/gcs_healthz", "/api/local_raylet_healthz", "/-/healthz",
}

// PublicPathPrefixes are path prefixes that do not require authentication,
// aligned with http_server_head.py public_path_prefixes.
var PublicPathPrefixes = []string{"/static/"}

// BrowserPostPutAllowedPaths are paths allowed to accept POST/PUT from
// browsers, aligned with http_server_head.py browser_post_put_allowed_paths.
var BrowserPostPutAllowedPaths = map[string]bool{"/api/authenticate": true}

// tokenAuthEnabled reports whether token authentication is enabled via the
// RAY_AUTH_MODE environment variable, aligned with the C++ get_authentication_mode.
func tokenAuthEnabled() bool {
	mode := os.Getenv("RAY_AUTH_MODE")
	return mode == "token" || mode == "k8s"
}

// getAuthenticationModeName returns the mode name string for the
// /api/authentication_mode endpoint, aligned with
// authentication_utils.get_authentication_mode_name.
func getAuthenticationModeName() string {
	if !tokenAuthEnabled() {
		return "disabled"
	}
	return os.Getenv("RAY_AUTH_MODE")
}

// authTokenLoader caches the expected token in token mode so the file at
// RAY_AUTH_TOKEN_PATH is only re-read when it changes, and caches the k8s
// client TLS config. It mirrors the C++ AuthenticationTokenLoader precedence:
// RAY_AUTH_TOKEN env first, then the RAY_AUTH_TOKEN_PATH file.
type authTokenLoader struct {
	mu        sync.Mutex
	cached    string
	tokenPath string
}

var tokenLoader = &authTokenLoader{}

// expectedToken returns the expected bearer token for token mode. Precedence
// 1 is the RAY_AUTH_TOKEN env var; precedence 2 is the RAY_AUTH_TOKEN_PATH
// file (first non-empty line, trimmed). The file is re-read on every call so a
// rotated token takes effect without a restart (the dashboard head has no
// context to run a background reloader).
func (l *authTokenLoader) expectedToken() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if env := os.Getenv("RAY_AUTH_TOKEN"); env != "" {
		return env
	}
	path := os.Getenv("RAY_AUTH_TOKEN_PATH")
	if path == "" {
		return ""
	}
	if path != l.tokenPath {
		l.cached = readFirstLine(path)
		l.tokenPath = path
		return l.cached
	}
	// Re-read the same path so a rotated token file is picked up.
	l.cached = readFirstLine(path)
	return l.cached
}

// readFirstLine returns the first non-empty trimmed line of the file, or "" if
// the file cannot be opened or is empty (aligned with the C++
// ReadTokenFromFile + TrimWhitespace).
func readFirstLine(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// validateRequestToken validates an Authorization header containing a Bearer
// token. In token mode it compares against the expected token (RAY_AUTH_TOKEN
// or RAY_AUTH_TOKEN_PATH), constant-time to avoid leaking the expected token
// length. In k8s mode it asks the Kubernetes API for a TokenReview +
// SubjectAccessReview.
func validateRequestToken(header string) bool {
	if len(header) <= len(bearerPrefix) || header[:len(bearerPrefix)] != bearerPrefix {
		return false
	}
	token := header[len(bearerPrefix):]
	if token == "" {
		return false
	}
	if os.Getenv("RAY_AUTH_MODE") == "k8s" {
		return k8sTokenValidator.validate(token)
	}
	expected := tokenLoader.expectedToken()
	if expected == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
}

// k8sTokenValidatorImpl validates bearer tokens against the Kubernetes API
// using only the standard library (net/http + crypto/tls), avoiding a k8s.io
// dependency. Validated tokens are cached for k8sTokenCacheTTL, aligned with
// the C++ AuthenticationTokenValidator.
type k8sTokenValidatorImpl struct {
	mu     sync.Mutex
	cache  map[string]k8sCacheEntry
	client *http.Client
	sa     string
}

type k8sCacheEntry struct {
	allowed   bool
	expiresAt time.Time
}

// k8sTokenValidator is the shared k8s token validator.
var k8sTokenValidator = &k8sTokenValidatorImpl{cache: map[string]k8sCacheEntry{}}

// validate returns whether the token is authenticated and authorized. It
// returns false when the k8s client could not be initialized (missing env or
// CA), mirroring the C++ k8s_client_initialized check.
func (v *k8sTokenValidatorImpl) validate(token string) bool {
	if !v.init() {
		return false
	}
	v.mu.Lock()
	if entry, ok := v.cache[token]; ok && time.Now().Before(entry.expiresAt) {
		allowed := entry.allowed
		v.mu.Unlock()
		return allowed
	}
	v.mu.Unlock()

	allowed := v.tokenReviewAndAuthorize(token)
	if allowed {
		v.mu.Lock()
		v.cache[token] = k8sCacheEntry{allowed: true, expiresAt: time.Now().Add(k8sTokenCacheTTL)}
		v.mu.Unlock()
	}
	return allowed
}

// init builds the TLS client from the service account env and CA, or returns
// false when any required input is missing (aligned with InitK8sClientConfig).
func (v *k8sTokenValidatorImpl) init() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.client != nil {
		return true
	}
	host := os.Getenv(k8sServiceHostEnvVar)
	port := os.Getenv(k8sServicePortEnvVar)
	if host == "" || port == "" {
		return false
	}
	sa, err := os.ReadFile(k8sSaTokenPath)
	if err != nil || len(sa) == 0 {
		return false
	}
	caData, err := os.ReadFile(k8sCaCertPath)
	if err != nil {
		return false
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caData) {
		return false
	}
	v.sa = strings.TrimSpace(string(sa))
	v.client = &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    pool,
				ServerName: host,
				MinVersion: tls.VersionTLS12,
			},
		},
	}
	return true
}

// tokenReviewAndAuthorize runs the TokenReview then the SubjectAccessReview
// against the Kubernetes API, mirroring k8s_util.cc ValidateToken.
func (v *k8sTokenValidatorImpl) tokenReviewAndAuthorize(token string) bool {
	v.mu.Lock()
	client := v.client
	sa := v.sa
	host := os.Getenv(k8sServiceHostEnvVar)
	port := os.Getenv(k8sServicePortEnvVar)
	v.mu.Unlock()
	if client == nil {
		return false
	}

	userInfo, ok := v.tokenReview(client, host, port, sa, token)
	if !ok {
		return false
	}
	return v.subjectAccessReview(client, host, port, sa, userInfo)
}

// tokenReview posts the TokenReview and returns the status.user object when
// authenticated. It mirrors k8s_util.cc ValidateToken's first half.
func (v *k8sTokenValidatorImpl) tokenReview(client *http.Client, host, port, sa, token string) (map[string]interface{}, bool) {
	body, _ := json.Marshal(map[string]interface{}{
		"apiVersion": k8sAuthAPIVersion,
		"kind":       k8sTokenReviewKind,
		"spec":       map[string]interface{}{"token": token},
	})
	url := fmt.Sprintf("https://%s:%s%s", host, port, k8sTokenReviewPath)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+sa)
	resp, err := client.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, false
	}
	var review struct {
		Status struct {
			Authenticated bool                   `json:"authenticated"`
			Error         string                 `json:"error"`
			User          map[string]interface{} `json:"user"`
		} `json:"status"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&review); err != nil {
		return nil, false
	}
	if !review.Status.Authenticated {
		return nil, false
	}
	return review.Status.User, true
}

// subjectAccessReview posts the SubjectAccessReview with the user/groups/extra
// from the TokenReview and returns whether access is allowed. It mirrors
// k8s_util.cc ValidateToken's second half.
func (v *k8sTokenValidatorImpl) subjectAccessReview(client *http.Client, host, port, sa string, user map[string]interface{}) bool {
	clusterName := os.Getenv(rayClusterNameEnvVar)
	namespace := os.Getenv(rayClusterNamespaceEnvVar)
	if clusterName == "" || namespace == "" {
		return false
	}
	spec := map[string]interface{}{
		"resourceAttributes": map[string]interface{}{
			"group":     k8sResourceGroup,
			"resource":  k8sClusterResource,
			"name":      clusterName,
			"verb":      k8sRayUserVerb,
			"namespace": namespace,
		},
	}
	if username, ok := user["username"].(string); ok && username != "" {
		spec["user"] = username
	}
	if groups, ok := user["groups"].([]interface{}); ok {
		spec["groups"] = groups
	}
	if extra, ok := user["extra"]; ok {
		spec["extra"] = extra
	}
	body, _ := json.Marshal(map[string]interface{}{
		"apiVersion": k8sAuthzAPIVersion,
		"kind":       k8sSubjectAccessKind,
		"spec":       spec,
	})
	url := fmt.Sprintf("https://%s:%s%s", host, port, k8sSubjectAccessPath)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+sa)
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return false
	}
	var review struct {
		Status struct {
			Allowed bool `json:"allowed"`
		} `json:"status"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&review); err != nil {
		return false
	}
	return review.Status.Allowed
}

// withTokenAuth is the token authentication middleware. It passes every
// request through unchanged when token auth is not enabled. When enabled, it
// skips the whitelisted public paths/prefixes and requires a valid bearer
// token for everything else, aligned with
// http_token_authentication.get_token_auth_middleware. The token may come
// from the standard Authorization header, the X-Ray-Authorization header, or
// the authentication cookie.
func withTokenAuth(publicExact []string, publicPrefixes []string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !tokenAuthEnabled() {
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
		authHeader := r.Header.Get(authorizationHeaderName)
		if authHeader == "" {
			authHeader = r.Header.Get(rayAuthorizationHeader)
		}
		if authHeader == "" {
			if token, err := r.Cookie(authenticationTokenCookie); err == nil {
				authHeader = bearerPrefix + token.Value
			}
		}
		if authHeader == "" {
			http.Error(w, "Unauthorized: Missing authentication token", http.StatusUnauthorized)
			return
		}
		if !validateRequestToken(authHeader) {
			http.Error(w, "Forbidden: Invalid authentication token", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
