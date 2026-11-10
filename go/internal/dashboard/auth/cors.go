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
	"bufio"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
)

const (
	headerOrigin                     = "Origin"
	headerAccessControlRequestMethod = "Access-Control-Request-Method"
	headerAccessControlRequestHeader = "Access-Control-Request-Headers"
	headerAllowOrigin                = "Access-Control-Allow-Origin"
	headerAllowCredentials           = "Access-Control-Allow-Credentials"
	headerAllowMethods               = "Access-Control-Allow-Methods"
	headerAllowHeaders               = "Access-Control-Allow-Headers"
	headerExposeHeaders              = "Access-Control-Expose-Headers"
)

// simpleResponseHeaders are the CORS-safelisted response headers, which do not
// need to be listed in Access-Control-Expose-Headers.
var simpleResponseHeaders = map[string]bool{
	"Expires":          true,
	"Last-Modified":    true,
	"Pragma":           true,
	"Content-Type":     true,
	"Content-Language": true,
	"Cache-Control":    true,
}

var corsAllowedHeaders = map[string]bool{"CONTENT-TYPE": true, "X-HEADER": true}

// WithCORS decorates every response with the per-origin CORS headers when
// isCORSEnabled reports that CORS applies to the request. Preflight requests
// are handled separately by ServePreflight.
func WithCORS(isCORSEnabled func(*http.Request) bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cr := &corsResponseWriter{ResponseWriter: w, req: r, enabled: isCORSEnabled(r)}
		next.ServeHTTP(cr, r)
		cr.apply()
	})
}

type corsResponseWriter struct {
	http.ResponseWriter
	req         *http.Request
	enabled     bool
	wroteHeader bool
}

func (c *corsResponseWriter) WriteHeader(code int) {
	if c.wroteHeader {
		c.ResponseWriter.WriteHeader(code)
		return
	}
	c.wroteHeader = true
	c.apply()
	c.ResponseWriter.WriteHeader(code)
}

func (c *corsResponseWriter) Write(b []byte) (int, error) {
	if !c.wroteHeader {
		c.wroteHeader = true
		c.apply()
	}
	return c.ResponseWriter.Write(b)
}

func (c *corsResponseWriter) Flush() {
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		if !c.wroteHeader {
			c.wroteHeader = true
			c.apply()
		}
		f.Flush()
	}
}

// Unwrap returns the wrapped ResponseWriter so http.ResponseController and
// libraries that resolve the underlying writer (e.g. coder/websocket) can
// reach it.
func (c *corsResponseWriter) Unwrap() http.ResponseWriter { return c.ResponseWriter }

// Hijack forwards the WebSocket upgrade to the wrapped ResponseWriter so
// upgrades behind this middleware are not rejected with a 501.
func (c *corsResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := c.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("underlying ResponseWriter does not implement http.Hijacker")
	}
	return hj.Hijack()
}

// apply adds the CORS headers for the request origin, exposing every
// non-safelisted response header. It never overwrites an Allow-Origin already
// set by the handler.
func (c *corsResponseWriter) apply() {
	if !c.enabled {
		return
	}
	if c.req.Method == http.MethodOptions {
		return
	}
	origin := c.req.Header.Get(headerOrigin)
	if origin == "" {
		return
	}
	h := c.Header()
	if h.Get(headerAllowOrigin) != "" {
		return
	}
	var exposed []string
	for name := range h {
		if !simpleResponseHeaders[http.CanonicalHeaderKey(name)] {
			exposed = append(exposed, name)
		}
	}
	if len(exposed) > 0 {
		h.Set(headerExposeHeaders, strings.Join(exposed, ","))
	}
	h.Set(headerAllowOrigin, origin)
	h.Set(headerAllowCredentials, "true")
}

// ServePreflight answers a CORS preflight request, allowing only the headers in
// corsAllowedHeaders. It mirrors the dashboard agent's aiohttp CORS handling.
func ServePreflight(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get(headerOrigin)
	if origin == "" {
		writePlainText(w, http.StatusForbidden,
			"CORS preflight request failed: origin header is not specified in the request")
		return
	}
	method := r.Header.Get(headerAccessControlRequestMethod)
	if method == "" {
		writePlainText(w, http.StatusForbidden,
			"CORS preflight request failed: 'Access-Control-Request-Method' header is not specified")
		return
	}
	requested := parseRequestHeaders(r.Header.Get(headerAccessControlRequestHeader))
	for _, h := range requested {
		if !corsAllowedHeaders[h] {
			writePlainText(w, http.StatusForbidden,
				"CORS preflight request failed: headers are not allowed: "+h)
			return
		}
	}
	h := w.Header()
	h.Set(headerAllowOrigin, origin)
	h.Set(headerAllowCredentials, "true")
	h.Set(headerAllowMethods, method)
	if len(requested) > 0 {
		h.Set(headerAllowHeaders, strings.Join(requested, ","))
	}
	h.Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

// parseRequestHeaders splits the Access-Control-Request-Headers value into
// upper-cased names.
func parseRequestHeaders(raw string) []string {
	if raw == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.ToUpper(strings.Trim(part, " \t")); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// writePlainText writes a plain-text response with an exact body and an
// explicit Content-Length, matching aiohttp's HTTP exception responses (no
// trailing newline).
func writePlainText(w http.ResponseWriter, code int, text string) {
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("Content-Length", strconv.Itoa(len(text)))
	w.WriteHeader(code)
	_, _ = w.Write([]byte(text))
}
