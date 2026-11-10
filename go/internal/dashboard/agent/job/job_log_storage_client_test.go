// Copyright 2026 The Ray Authors.
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

package job

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetLogFilePath(t *testing.T) {
	c := NewJobLogStorageClient("/var/logs")
	assert.Equal(t, filepath.Join("/var/logs", "job-driver-sub1.log"), c.getLogFilePath("sub1"))
}

func TestGetLogs(t *testing.T) {
	dir := t.TempDir()
	c := NewJobLogStorageClient(dir)

	// A missing file yields an empty string.
	assert.Equal(t, "", c.getLogs("sub1"))

	require.NoError(t, os.WriteFile(c.getLogFilePath("sub1"), []byte("hello\nworld\n"), 0o644))
	assert.Equal(t, "hello\nworld\n", c.getLogs("sub1"))
}

func TestGetLastNLogLines(t *testing.T) {
	dir := t.TempDir()
	c := NewJobLogStorageClient(dir)
	require.NoError(t, os.WriteFile(c.getLogFilePath("sub1"), []byte("1\n2\n3\n"), 0o644))

	// numLogLines 0 falls back to numLogLinesOnError.
	assert.Equal(t, "1\n2\n3\n", c.getLastNLogLines(context.Background(), "sub1", 0))
	assert.Equal(t, "2\n3\n", c.getLastNLogLines(context.Background(), "sub1", 2))
}
