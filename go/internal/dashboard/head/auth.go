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
	"net/http"

	"github.com/ray-project/ray/go/internal/dashboard/auth"
)

// Aliases into the shared auth package so the head's call sites and tests keep
// their local names. The values are defined once in
// python/ray/_private/authentication/authentication_constants.py and mirrored
// by go/internal/dashboard/auth.
const (
	authorizationHeaderName         = auth.AuthorizationHeaderName
	rayAuthorizationHeader          = auth.RayAuthorizationHeader
	authenticationTokenCookie       = auth.AuthenticationTokenCookie
	authenticationTokenCookieMaxAge = 30 * 24 * 60 * 60 // 30 days
	bearerPrefix                    = auth.BearerPrefix
)

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

// tokenAuthEnabled reports whether token authentication is enabled.
func tokenAuthEnabled() bool { return auth.TokenAuthEnabled() }

// getAuthenticationModeName returns the mode name string for the
// /api/authentication_mode endpoint.
func getAuthenticationModeName() string { return auth.AuthenticationModeName() }

// validateRequestToken validates an Authorization header containing a Bearer
// token.
func validateRequestToken(header string) bool { return auth.ValidateRequestToken(header) }

// withTokenAuth is the token authentication middleware.
func withTokenAuth(publicExact, publicPrefixes []string, next http.Handler) http.Handler {
	return auth.WithTokenAuth(publicExact, publicPrefixes, next)
}

// withBrowserPostPutBlock blocks POST/PUT requests from browsers except for
// whitelisted paths and the flow insight endpoints.
func withBrowserPostPutBlock(whitelist map[string]bool, next http.Handler) http.Handler {
	return auth.WithBrowserPostPutBlock(whitelist, next)
}

// isBrowserRequest guesses whether a request came from a browser.
func isBrowserRequest(r *http.Request) bool { return auth.IsBrowserRequest(r) }
