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

package pathutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGetRayTempDir(t *testing.T) {
	// Save original env var
	orig := os.Getenv("RAY_TEMP_DIR")
	defer os.Setenv("RAY_TEMP_DIR", orig)

	// Test default
	os.Unsetenv("RAY_TEMP_DIR")
	if got := GetRayTempDir(); got != "/tmp/ray" {
		t.Errorf("GetRayTempDir() = %q, want %q", got, "/tmp/ray")
	}

	// Test custom
	os.Setenv("RAY_TEMP_DIR", "/custom/ray")
	if got := GetRayTempDir(); got != "/custom/ray" {
		t.Errorf("GetRayTempDir() with RAY_TEMP_DIR set = %q, want %q", got, "/custom/ray")
	}
}

func TestGetRaySessionDir(t *testing.T) {
	// Save original env var
	orig := os.Getenv("RAY_SESSION_DIR")
	defer os.Setenv("RAY_SESSION_DIR", orig)

	// Test with RAY_SESSION_DIR set
	os.Setenv("RAY_SESSION_DIR", "/custom/session")
	if got := GetRaySessionDir(); got != "/custom/session" {
		t.Errorf("GetRaySessionDir() with RAY_SESSION_DIR set = %q, want %q", got, "/custom/session")
	}

	// Test without RAY_SESSION_DIR and no session_latest symlink
	os.Unsetenv("RAY_SESSION_DIR")
	// This should return empty string since session_latest doesn't exist
	got := GetRaySessionDir()
	// We don't assert on this because session_latest might exist in some environments
	t.Logf("GetRaySessionDir() without env = %q", got)
}

func TestReadRayClusterFile(t *testing.T) {
	// Save original env vars
	origSession := os.Getenv("RAY_SESSION_DIR")
	origTemp := os.Getenv("RAY_TEMP_DIR")
	defer os.Setenv("RAY_SESSION_DIR", origSession)
	defer os.Setenv("RAY_TEMP_DIR", origTemp)

	// Create a temp directory for testing
	tmpDir := t.TempDir()
	sessionDir := filepath.Join(tmpDir, "session")
	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		t.Fatalf("Failed to create session dir: %v", err)
	}

	// Create a test file
	testFile := filepath.Join(sessionDir, "test_file")
	testContent := "test content"
	if err := os.WriteFile(testFile, []byte(testContent), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Test reading from RAY_SESSION_DIR
	os.Setenv("RAY_SESSION_DIR", sessionDir)
	content, err := ReadRayClusterFile("test_file")
	if err != nil {
		t.Fatalf("ReadRayClusterFile() error = %v", err)
	}
	if string(content) != testContent {
		t.Errorf("ReadRayClusterFile() content = %q, want %q", string(content), testContent)
	}

	// Test reading non-existent file
	os.Setenv("RAY_SESSION_DIR", sessionDir)
	_, err = ReadRayClusterFile("non_existent_file")
	if err == nil {
		t.Error("ReadRayClusterFile() expected error for non-existent file, got nil")
	}
}
