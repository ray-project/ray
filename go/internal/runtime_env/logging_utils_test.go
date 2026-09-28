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

package runtime_env

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedirectStdoutStderrIfNeeded(t *testing.T) {
	// RedirectStdoutStderrIfNeeded redirects the process-level stdout/stderr
	// file descriptors via syscall.Dup2. Save the original descriptors and
	// restore them after the test so the test process is not corrupted.
	origStdout := os.Stdout
	origStderr := os.Stderr
	stdoutFd, err := syscall.Dup(int(origStdout.Fd()))
	if err != nil {
		t.Fatalf("failed to duplicate stdout fd: %v", err)
	}
	stderrFd, err := syscall.Dup(int(origStderr.Fd()))
	if err != nil {
		t.Fatalf("failed to duplicate stderr fd: %v", err)
	}
	t.Cleanup(func() {
		syscall.Dup2(stdoutFd, int(origStdout.Fd()))
		syscall.Dup2(stderrFd, int(origStderr.Fd()))
		syscall.Close(stdoutFd)
		syscall.Close(stderrFd)
	})

	t.Run("empty paths should not redirect", func(t *testing.T) {
		err := RedirectStdoutStderrIfNeeded("", "", 1024*1024, 3)
		assert.NoError(t, err)
	})

	t.Run("stdout redirection should succeed", func(t *testing.T) {
		tmpDir := t.TempDir()
		stdoutFile := filepath.Join(tmpDir, "stdout.log")

		err := RedirectStdoutStderrIfNeeded(stdoutFile, "", 1024*1024, 3)
		assert.NoError(t, err)

		// Verify file was created
		_, err = os.Stat(stdoutFile)
		assert.NoError(t, err)
	})

	t.Run("stderr redirection should succeed", func(t *testing.T) {
		tmpDir := t.TempDir()
		stderrFile := filepath.Join(tmpDir, "stderr.log")

		err := RedirectStdoutStderrIfNeeded("", stderrFile, 1024*1024, 3)
		assert.NoError(t, err)

		// Verify file was created
		_, err = os.Stat(stderrFile)
		assert.NoError(t, err)
	})

	t.Run("both stdout and stderr redirection should succeed", func(t *testing.T) {
		tmpDir := t.TempDir()
		stdoutFile := filepath.Join(tmpDir, "stdout.log")
		stderrFile := filepath.Join(tmpDir, "stderr.log")

		err := RedirectStdoutStderrIfNeeded(stdoutFile, stderrFile, 1024*1024, 3)
		assert.NoError(t, err)

		// Verify files were created
		_, err = os.Stat(stdoutFile)
		assert.NoError(t, err)
		_, err = os.Stat(stderrFile)
		assert.NoError(t, err)
	})

	t.Run("invalid path should fail", func(t *testing.T) {
		// Try to redirect to a non-existent directory
		err := RedirectStdoutStderrIfNeeded("/nonexistent/dir/stdout.log", "", 1024*1024, 3)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to redirect stdout")
	})
}

// func TestNewRotatingWriter(t *testing.T) {
// 	t.Run("empty path should return stdout", func(t *testing.T) {
// 		writer, err := NewRotatingWriter("", 1024*1024, 3)
// 		assert.NoError(t, err)
// 		assert.Equal(t, os.Stdout, writer)
// 	})

// 	t.Run("valid path should create rotating writer", func(t *testing.T) {
// 		tmpDir := t.TempDir()
// 		logFile := filepath.Join(tmpDir, "test.log")

// 		writer, err := NewRotatingWriter(logFile, 1024*1024, 3)
// 		assert.NoError(t, err)
// 		assert.NotNil(t, writer)

// 		// Write some data
// 		data := []byte("test log message\n")
// 		n, err := writer.Write(data)
// 		assert.NoError(t, err)
// 		assert.Equal(t, len(data), n)

// 		// Verify file was created and contains data
// 		content, err := os.ReadFile(logFile)
// 		assert.NoError(t, err)
// 		assert.Contains(t, string(content), "test log message")
// 	})

// 	t.Run("rotation parameters should be respected", func(t *testing.T) {
// 		tmpDir := t.TempDir()
// 		logFile := filepath.Join(tmpDir, "rotate.log")

// 		// Create writer with small rotation size (1KB)
// 		writer, err := NewRotatingWriter(logFile, 1024, 2)
// 		assert.NoError(t, err)
// 		assert.NotNil(t, writer)

// 		// Write enough data to trigger rotation
// 		largeData := make([]byte, 2048) // 2KB
// 		for i := range largeData {
// 			largeData[i] = 'a'
// 		}

// 		n, err := writer.Write(largeData)
// 		assert.NoError(t, err)
// 		assert.Equal(t, len(largeData), n)

// 		// Verify file exists
// 		_, err = os.Stat(logFile)
// 		assert.NoError(t, err)
// 	})
// }
