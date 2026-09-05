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

// Package pathutil provides utilities for handling Ray file paths.
package pathutil

import (
	"fmt"
	"os"
	"path/filepath"
)

// GetRayTempDir returns the Ray temp directory.
// If RAY_TEMP_DIR is set, it returns that value; otherwise returns the default "/tmp/ray".
func GetRayTempDir() string {
	if tempDir := os.Getenv("RAY_TEMP_DIR"); tempDir != "" {
		return tempDir
	}
	return "/tmp/ray"
}

// GetRaySessionDir returns the Ray session directory.
// If RAY_SESSION_DIR is set, it returns that value; otherwise attempts to return
// the session_latest directory. Returns empty string if session_latest doesn't
// exist or isn't a symlink.
func GetRaySessionDir() string {
	if sessionDir := os.Getenv("RAY_SESSION_DIR"); sessionDir != "" {
		return sessionDir
	}
	// Fallback to session_latest directory
	sessionLatest := filepath.Join(GetRayTempDir(), "session_latest")
	if info, err := os.Lstat(sessionLatest); err == nil && info.Mode()&os.ModeSymlink != 0 {
		if realPath, err := os.Readlink(sessionLatest); err == nil {
			return realPath
		}
	}
	return ""
}

// ReadRayClusterFile reads a cluster file from the Ray session directory.
// Filename examples: "ray_current_cluster" or "node_ip_address.json"
//
// Returns:
//   - []byte: file content
//   - error: error if file doesn't exist or cannot be read
func ReadRayClusterFile(filename string) ([]byte, error) {
	// Prefer reading from RAY_SESSION_DIR if set
	if sessionDir := os.Getenv("RAY_SESSION_DIR"); sessionDir != "" {
		path := filepath.Join(sessionDir, filename)
		return os.ReadFile(path)
	}

	// Fallback to default location
	rayTempDir := GetRayTempDir()
	sessionLatest := filepath.Join(rayTempDir, "session_latest")
	if info, err := os.Lstat(sessionLatest); err == nil && info.Mode()&os.ModeSymlink != 0 {
		realPath, err := os.Readlink(sessionLatest)
		if err == nil {
			path := filepath.Join(realPath, filename)
			return os.ReadFile(path)
		}
	}

	return nil, fmt.Errorf("file not found: %s", filename)
}
