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

package log

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// aiohttpCharset is the charset aiohttp appends to the content types it
// generates for text responses.
const aiohttpCharset = "utf-8"

// StaticPrefix is the URL prefix of the log file browser, mirroring the first
// argument of LogAgent's routes.static("/logs", ...) call in log_agent.py:249.
const StaticPrefix = "/logs"

// handleLogs serves files and directory indexes under /logs from the module
// log directory, refusing anything that resolves outside it. It mirrors the
// aiohttp static route configured in log_agent.py:249: GET/HEAD only,
// follow_symlinks=False, and show_index=True.
func (a *LogAgentModule) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET,HEAD,OPTIONS")
		writePlainText(w, http.StatusMethodNotAllowed, "405: Method Not Allowed")
		return
	}

	rawPath := r.URL.EscapedPath()
	if !matchesStaticPrefix(rawPath) {
		writeNotFoundText(w)
		return
	}
	name := strings.TrimPrefix(rawPath, StaticPrefix)
	name = strings.TrimPrefix(name, "/")
	if decoded, err := url.PathUnescape(name); err == nil {
		name = decoded
	}
	resolved, err := resolveLogPath(a.logDir, name)
	if err != nil {
		writeNotFoundText(w)
		return
	}
	// Containment is checked again after resolving symlinks so a link inside
	// the log directory cannot expose a file outside it. The base is resolved
	// as well: the log directory itself is frequently a symlink (a container's
	// /tmp/ray redirect, for instance), and comparing a resolved path against
	// an unresolved base would reject every file under it.
	final, err := filepath.EvalSymlinks(resolved)
	if err != nil {
		writeFileNotFound(w)
		return
	}
	base, err := filepath.EvalSymlinks(a.logDir)
	if err != nil {
		abs, absErr := filepath.Abs(a.logDir)
		if absErr != nil {
			writeNotFoundText(w)
			return
		}
		base = abs
	}
	if !inside(base, final) {
		writeNotFoundText(w)
		return
	}
	info, err := os.Stat(final)
	if err != nil {
		writeFileNotFound(w)
		return
	}
	if info.IsDir() {
		a.serveDirIndex(w, r, final)
		return
	}
	if !info.Mode().IsRegular() {
		writePlainText(w, http.StatusForbidden, "403: Forbidden")
		return
	}
	serveFile(w, r, final, info)
}

// matchesStaticPrefix reports whether the cleaned request path belongs to the
// /logs route. Cleaning makes a path with dot segments that escapes the prefix
// (for example /logs/../../etc) fail the check and produce a 404.
func matchesStaticPrefix(rawPath string) bool {
	normalized := path.Clean(rawPath)
	return normalized == StaticPrefix || strings.HasPrefix(normalized, StaticPrefix+"/")
}

// serveDirIndex renders aiohttp's show_index=True directory listing for dir:
// an "Index of /<relative path>" page with one link per entry, directories
// suffixed with "/".
func (a *LogAgentModule) serveDirIndex(w http.ResponseWriter, r *http.Request, dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		writePlainText(w, http.StatusForbidden, "403: Forbidden")
		return
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	base, err := filepath.Abs(a.logDir)
	if err != nil {
		writeFileNotFound(w)
		return
	}
	rel, err := filepath.Rel(base, dir)
	if err != nil {
		writeFileNotFound(w)
		return
	}
	rel = filepath.ToSlash(rel)
	prefix := ""
	if rel != "." {
		prefix = rel + "/"
	}
	indexOf := "Index of /" + escapeHTML(rel)

	items := make([]string, 0, len(names))
	for _, name := range names {
		display := name
		if fi, err := os.Stat(filepath.Join(dir, name)); err == nil && fi.IsDir() {
			display += "/"
		}
		items = append(items, fmt.Sprintf(`<li><a href="%s">%s</a></li>`,
			quotePath(StaticPrefix+"/"+prefix+name), escapeHTML(display)))
	}
	body := fmt.Sprintf("<html>\n<head>\n<title>%s</title>\n</head>\n<body>\n<h1>%s</h1>\n<ul>\n%s\n</ul>\n</body>\n</html>",
		indexOf, indexOf, strings.Join(items, "\n"))

	h := w.Header()
	h.Set("Content-Type", "text/html; charset="+aiohttpCharset)
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write([]byte(body))
}

// serveFile writes a regular file with the content type aiohttp's static
// handler would choose. http.ServeContent supplies the HEAD handling, range
// support, and conditional-request behavior aiohttp provides as well.
func serveFile(w http.ResponseWriter, r *http.Request, name string, info os.FileInfo) {
	f, err := os.Open(name)
	if err != nil {
		writeFileNotFound(w)
		return
	}
	defer f.Close()

	h := w.Header()
	h.Set("Content-Type", contentTypeFor(name))
	h.Set("Etag", fmt.Sprintf(`"%x-%x"`, info.ModTime().UnixNano(), info.Size()))
	http.ServeContent(w, r, name, info.ModTime(), f)
}

// quotePath percent-escapes everything outside the set aiohttp leaves
// unescaped in a directory-listing href.
func quotePath(p string) string {
	const safe = "!$&'()*+,-./:;=@_~"
	var b strings.Builder
	b.Grow(len(p))
	for i := 0; i < len(p); i++ {
		c := p[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			strings.IndexByte(safe, c) >= 0 {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

// escapeHTML escapes the five characters aiohttp's HTML escaper replaces.
func escapeHTML(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '&':
			b.WriteString("&amp;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '"':
			b.WriteString("&quot;")
		case '\'':
			b.WriteString("&#x27;")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// writePlainText writes an aiohttp-style plain text response.
func writePlainText(w http.ResponseWriter, code int, text string) {
	h := w.Header()
	h.Set("Content-Type", "text/plain; charset="+aiohttpCharset)
	h.Set("Content-Length", strconv.Itoa(len(text)))
	w.WriteHeader(code)
	_, _ = w.Write([]byte(text))
}

// writeNotFoundText writes aiohttp's router-level 404 body.
func writeNotFoundText(w http.ResponseWriter) {
	writePlainText(w, http.StatusNotFound, "404: Not Found")
}

// writeFileNotFound writes the 404 aiohttp's static handler produces when the
// resolved file does not exist: an empty chunked response.
func writeFileNotFound(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Type", fallbackContentType)
	h.Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusNotFound)
}
