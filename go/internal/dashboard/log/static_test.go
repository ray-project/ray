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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatchesStaticPrefix(t *testing.T) {
	assert.True(t, matchesStaticPrefix("/logs"))
	assert.True(t, matchesStaticPrefix("/logs/x"))
	assert.True(t, matchesStaticPrefix("/logs/sub/x.log"))
	assert.False(t, matchesStaticPrefix("/other"))
	assert.False(t, matchesStaticPrefix("/logsx"))
}

func TestQuotePath(t *testing.T) {
	assert.Equal(t, "/logs/a%20b.txt", quotePath("/logs/a b.txt"))
	assert.Equal(t, "/logs/a-b_c.txt", quotePath("/logs/a-b_c.txt"))
	assert.Equal(t, "~", quotePath("~"))
}

func TestEscapeHTML(t *testing.T) {
	assert.Equal(t, "a&amp;b&lt;c&gt;&quot;d&#x27;e", escapeHTML(`a&b<c>"d'e`))
	assert.Equal(t, "plain", escapeHTML("plain"))
}

func TestHandleLogsFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte("hello"), 0o644))
	a := &LogAgentModule{logDir: dir}

	rec := httptest.NewRecorder()
	a.handleLogs(rec, httptest.NewRequest(http.MethodGet, "/logs/a.txt", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
	assert.Equal(t, "hello", rec.Body.String())
}

func TestHandleLogsHead(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte("hello"), 0o644))
	a := &LogAgentModule{logDir: dir}

	rec := httptest.NewRecorder()
	a.handleLogs(rec, httptest.NewRequest(http.MethodHead, "/logs/a.txt", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "", rec.Body.String())
}

func TestHandleLogsMethodNotAllowed(t *testing.T) {
	dir := t.TempDir()
	a := &LogAgentModule{logDir: dir}
	rec := httptest.NewRecorder()
	a.handleLogs(rec, httptest.NewRequest(http.MethodPost, "/logs/a.log", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Equal(t, "GET,HEAD,OPTIONS", rec.Header().Get("Allow"))
}

func TestHandleLogsDirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sub", "x.log"), []byte("x"), 0o644))
	a := &LogAgentModule{logDir: dir}

	rec := httptest.NewRecorder()
	a.handleLogs(rec, httptest.NewRequest(http.MethodGet, "/logs/sub", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/html; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Body.String(), "x.log")
}

func TestHandleLogsNotFoundAndTraversal(t *testing.T) {
	dir := t.TempDir()
	a := &LogAgentModule{logDir: dir}

	// missing file
	rec := httptest.NewRecorder()
	a.handleLogs(rec, httptest.NewRequest(http.MethodGet, "/logs/missing.log", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// path traversal outside log dir
	rec = httptest.NewRecorder()
	a.handleLogs(rec, httptest.NewRequest(http.MethodGet, "/logs/../../etc/passwd", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// absolute path outside log dir
	rec = httptest.NewRecorder()
	a.handleLogs(rec, httptest.NewRequest(http.MethodGet, "/logs//etc/passwd", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandleLogsSymlinkedLogDirServes proves a log directory that is itself a
// symlink still serves its files. The containment check resolves the request
// path; resolving only one side would make every file under a symlinked log
// directory look like an escape.
func TestHandleLogsSymlinkedLogDirServes(t *testing.T) {
	realDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(realDir, "a.log"), []byte("hello"), 0o644))
	linkParent := t.TempDir()
	linkDir := filepath.Join(linkParent, "logs")
	require.NoError(t, os.Symlink(realDir, linkDir))

	a := &LogAgentModule{logDir: linkDir}

	rec := httptest.NewRecorder()
	a.handleLogs(rec, httptest.NewRequest(http.MethodGet, "/logs/a.log", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "hello", rec.Body.String())

	rec = httptest.NewRecorder()
	a.handleLogs(rec, httptest.NewRequest(http.MethodGet, "/logs", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "a.log")
}

// TestHandleLogsSymlinkEscapeRejected checks that a symlink inside the log dir
// which points outside it is refused, mirroring aiohttp's static
// follow_symlinks=False in log_agent.py:249.
func TestHandleLogsSymlinkEscapeRejected(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	require.NoError(t, os.WriteFile(outside, []byte("secret"), 0o644))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "escape.log")))
	require.NoError(t, os.WriteFile(filepath.Join(root, "a.log"), []byte("content-a.log\n"), 0o644))
	require.NoError(t, os.Symlink(filepath.Join(root, "a.log"), filepath.Join(root, "inside.log")))

	a := &LogAgentModule{logDir: root}

	rec := httptest.NewRecorder()
	a.handleLogs(rec, httptest.NewRequest(http.MethodGet, "/logs/escape.log", nil))
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// A symlink that stays inside the log dir is still served.
	rec = httptest.NewRecorder()
	a.handleLogs(rec, httptest.NewRequest(http.MethodGet, "/logs/inside.log", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "content-a.log\n", rec.Body.String())
}
