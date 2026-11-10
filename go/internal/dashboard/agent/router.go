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

package agent

import (
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/ray-project/ray/go/internal/dashboard/auth"
)

// aiohttpRouter adapts net/http's ServeMux to the routing semantics of the
// Python agent's aiohttp application: 404/405 responses carry aiohttp's exact
// text bodies, 405 responses list the methods the path actually serves in
// Allow, CORS preflights are answered for registered routes, and a path with
// empty or dot segments is matched literally instead of being cleaned and
// redirected the way ServeMux does by default.
type aiohttpRouter struct {
	mux *http.ServeMux
}

func (rt *aiohttpRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions && rt.isCORSEnabled(r) {
		auth.ServePreflight(w, r)
		return
	}

	if probe := rt.probe(r); probe != nil {
		rt.mux.ServeHTTP(w, probe)
		return
	}

	if allowed := rt.allowedMethods(r); len(allowed) > 0 {
		w.Header().Set("Allow", strings.Join(allowed, ","))
		writeAiohttpText(w, http.StatusMethodNotAllowed, "405: Method Not Allowed")
		return
	}
	writeAiohttpText(w, http.StatusNotFound, "404: Not Found")
}

// probe returns the request the mux would route, or nil when no handler
// matches. It probes with the escaped path so that ServeMux neither cleans the
// path nor answers with a redirect: aiohttp routes the request path literally,
// so "//api/x" and "/api/./x" are not the same resource as "/api/x".
func (rt *aiohttpRouter) probe(r *http.Request) *http.Request {
	escaped := rt.escapePath(r.URL.EscapedPath())
	probe := r
	if escaped != "" {
		raw := *r.URL
		raw.RawPath = escaped
		clone := *r
		clone.URL = &raw
		probe = &clone
	}
	h, pattern := rt.mux.Handler(probe)
	if pattern == "" || h == nil {
		return nil
	}
	// ServeMux answers a request for "/dir" when only "/dir/" is registered
	// with a 301 redirect to "/dir/". aiohttp serves the path or 404s, so
	// treat the generated redirect as no match and let 405/404 semantics
	// take over.
	if rt.isTrailingSlashRedirect(pattern, probe) {
		return nil
	}
	return probe
}

// isTrailingSlashRedirect reports whether pattern names the redirect target
// ServeMux generates for a request whose path only matches a trailing-slash
// pattern. Subtree matches ("GET /logs/" for "/logs/a.log") are not redirects
// and must keep routing normally.
func (rt *aiohttpRouter) isTrailingSlashRedirect(pattern string, r *http.Request) bool {
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		pattern = pattern[i+1:]
	}
	if !strings.HasSuffix(pattern, "/") {
		return false
	}
	path := r.URL.Path
	return !strings.HasSuffix(path, "/") && path+"/" == pattern
}

// serves reports whether any method routes the request's path.
func (rt *aiohttpRouter) serves(r *http.Request) bool {
	return rt.probe(r) != nil
}

// isCORSEnabled reports whether the request's path is served under any method,
// which is when aiohttp_cors answers a preflight for it.
func (rt *aiohttpRouter) isCORSEnabled(r *http.Request) bool {
	for _, m := range probeMethods {
		probe := *r
		probe.Method = m
		if rt.serves(&probe) {
			return true
		}
	}
	return false
}

// allowedMethods lists the methods that route the path, always including
// OPTIONS because aiohttp_cors registers a preflight route for every path.
// It returns nil when the path is not registered at all.
func (rt *aiohttpRouter) allowedMethods(r *http.Request) []string {
	allowed := map[string]bool{"OPTIONS": true}
	for _, m := range probeMethods {
		probe := *r
		probe.Method = m
		if rt.serves(&probe) {
			allowed[m] = true
		}
	}
	if len(allowed) == 1 {
		return nil
	}
	out := make([]string, 0, len(allowed))
	for m := range allowed {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

var probeMethods = []string{
	http.MethodGet, http.MethodHead, http.MethodPost,
	http.MethodPut, http.MethodPatch, http.MethodDelete,
}

// escapePath percent-escapes empty path segments and "."/".." segments so the
// escaped form of the path is not cleaned by ServeMux, and the path therefore
// matches (or fails to match) literally. It returns "" when the path needs no
// escaping.
func (rt *aiohttpRouter) escapePath(escaped string) string {
	if !strings.Contains(escaped, "//") &&
		!containsDotSegment(escaped) {
		return ""
	}
	out := strings.ReplaceAll(escaped, "//", "/%2F")
	segs := strings.Split(out, "/")
	for i, s := range segs {
		switch s {
		case ".":
			segs[i] = "%2E"
		case "..":
			segs[i] = "%2E%2E"
		}
	}
	return strings.Join(segs, "/")
}

func containsDotSegment(path string) bool {
	for _, s := range strings.Split(path, "/") {
		if s == "." || s == ".." {
			return true
		}
	}
	return false
}

// writeAiohttpText writes an aiohttp-style plain-text error response: an exact
// body with no trailing newline and an explicit Content-Length.
func writeAiohttpText(w http.ResponseWriter, code int, text string) {
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("Content-Length", strconv.Itoa(len(text)))
	w.WriteHeader(code)
	_, _ = w.Write([]byte(text))
}
