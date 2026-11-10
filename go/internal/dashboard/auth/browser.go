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
	"strings"
)

// IsBrowserRequest guesses whether a request came from a browser, aligned with
// dashboard_optional_utils.is_browser_request: the User-Agent starts with
// "Mozilla", any Sec-Fetch-* header is present, or any CORS header
// (Referer/Origin/Access-Control-Request-*) is present.
func IsBrowserRequest(r *http.Request) bool {
	ua := r.Header.Get("User-Agent")
	if strings.HasPrefix(ua, "Mozilla") {
		return true
	}
	for _, h := range []string{
		"Referer",
		"Origin",
		"Sec-Fetch-Mode",
		"Sec-Fetch-Dest",
		"Sec-Fetch-Site",
		"Sec-Fetch-User",
		"Access-Control-Request-Method",
		"Access-Control-Request-Headers",
	} {
		if r.Header.Get(h) != "" {
			return true
		}
	}
	return false
}

// WithBrowserBlock rejects every browser request, aligned with the dashboard
// agent's browsers_no_get_middleware.
func WithBrowserBlock(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if IsBrowserRequest(r) {
			writePlainText(w, http.StatusForbidden, "Browser requests not allowed.")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// WithBrowserPostPutBlock blocks POST/PUT requests from browsers except for
// whitelisted paths and the flow insight endpoints (aligned with
// browsers_no_post_put_middleware).
func WithBrowserPostPutBlock(whitelist map[string]bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if whitelist[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/insight/") {
			next.ServeHTTP(w, r)
			return
		}
		if (r.Method == http.MethodPost || r.Method == http.MethodPut) && IsBrowserRequest(r) {
			writePlainText(w, http.StatusMethodNotAllowed, "Method Not Allowed for browser traffic.")
			return
		}
		next.ServeHTTP(w, r)
	})
}
