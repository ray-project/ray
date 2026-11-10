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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInside(t *testing.T) {
	assert.True(t, inside("/a/b", "/a/b/c"))
	assert.True(t, inside("/a/b", "/a/b"))
	assert.False(t, inside("/a/b", "/a/c"))
	assert.False(t, inside("/a/b", "/a/bc/d"))
	assert.False(t, inside("/a/b", "/"))
}

func TestResolveLogPath(t *testing.T) {
	root := t.TempDir()

	// relative filename stays inside root
	p, err := resolveLogPath(root, "sub/out.log")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(root, "sub", "out.log"), p)

	// absolute path inside root is accepted
	p, err = resolveLogPath(root, root)
	require.NoError(t, err)
	assert.Equal(t, root, p)

	// absolute path outside root is rejected
	_, err = resolveLogPath(root, "/etc/passwd")
	require.Error(t, err)

	// traversal escape is rejected
	_, err = resolveLogPath(root, "../../etc/passwd")
	require.Error(t, err)
}

func TestResolveFilenameExists(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "a.log"), []byte("x"), 0o644))

	// existing regular file resolves
	p, err := resolveFilename(root, "a.log")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(root, "a.log"), p)

	// missing file -> error
	_, err = resolveFilename(root, "missing.log")
	require.Error(t, err)

	// outside root -> error
	_, err = resolveFilename(root, "/etc/passwd")
	require.Error(t, err)

	// directory is not a regular file -> error
	sub := filepath.Join(root, "sub")
	require.NoError(t, os.MkdirAll(sub, 0o755))
	_, err = resolveFilename(root, "sub")
	require.Error(t, err)
}

func TestGlobToRegexConvert(t *testing.T) {
	re, err := globToRegex("*.log")
	require.NoError(t, err)
	assert.True(t, re.MatchString("a.log"))
	assert.False(t, re.MatchString("dir/a.log"))

	re, err = globToRegex("**/*.log")
	require.NoError(t, err)
	assert.True(t, re.MatchString("a.log"))
	assert.True(t, re.MatchString("dir/sub/a.log"))

	re, err = globToRegex("sub/*.txt")
	require.NoError(t, err)
	assert.True(t, re.MatchString("sub/a.txt"))
	assert.False(t, re.MatchString("a.txt"))
}

func TestFnmatchSegmentToRegex(t *testing.T) {
	assert.Equal(t, `a[^/]*`, fnmatchSegmentToRegex("a*"))
	assert.Equal(t, `[^/]`, fnmatchSegmentToRegex("?"))
	assert.Equal(t, `[ab]`, fnmatchSegmentToRegex("[ab]"))
}

func TestSafeGlobFilter(t *testing.T) {
	assert.True(t, safeGlobFilter("*.log"))
	assert.True(t, safeGlobFilter("sub/**/*.log"))
	assert.True(t, safeGlobFilter("a..b"))
	assert.False(t, safeGlobFilter("/etc/passwd"))
	assert.False(t, safeGlobFilter("sub/../x"))
	assert.False(t, safeGlobFilter(".."))
	assert.False(t, safeGlobFilter("sub/.."))
}

func TestGlobLogFiles(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "a.log"), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "b.txt"), []byte("x"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "sub", "c.log"), []byte("x"), 0o644))

	// absolute pattern unsupported
	_, err := globLogFiles(root, "/abs")
	require.Error(t, err)

	// single-level pattern matches only top level
	matches, err := globLogFiles(root, "*.log")
	require.NoError(t, err)
	assert.Equal(t, []string{filepath.Join(root, "a.log")}, matches)

	// recursive pattern matches nested too
	matches, err = globLogFiles(root, "**/*.log")
	require.NoError(t, err)
	assert.Len(t, matches, 2)
}
