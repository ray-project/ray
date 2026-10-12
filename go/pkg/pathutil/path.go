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
// Candidate locations, in priority order (mirroring Python's
// find_bootstrap_address / read_ray_address):
//  1. $RAY_SESSION_DIR/<filename>
//  2. <latest session dir>/<filename> (resolved from the session_latest symlink)
//  3. <ray temp dir>/<filename> — `ray start` writes ray_current_cluster to
//     the temp dir root (get_ray_address_file returns
//     <ray_temp_dir>/ray_current_cluster), so this fallback is what makes the
//     cluster address resolvable on a real `ray start` cluster.
//
// Returns:
//   - []byte: file content
//   - error: error if the file doesn't exist in any candidate location
func ReadRayClusterFile(filename string) ([]byte, error) {
	rayTempDir := GetRayTempDir()

	// 1. Explicit RAY_SESSION_DIR (highest priority).
	if sessionDir := os.Getenv("RAY_SESSION_DIR"); sessionDir != "" {
		if data, err := os.ReadFile(filepath.Join(sessionDir, filename)); err == nil {
			return data, nil
		}
	}

	// 2. The latest session directory (session_latest symlink). Resolved lazily:
	// the Lstat/Readlink syscalls only run when an explicit session dir did not
	// already provide the file.
	sessionLatest := filepath.Join(rayTempDir, "session_latest")
	if info, err := os.Lstat(sessionLatest); err == nil && info.Mode()&os.ModeSymlink != 0 {
		if realPath, err := os.Readlink(sessionLatest); err == nil {
			if data, err := os.ReadFile(filepath.Join(realPath, filename)); err == nil {
				return data, nil
			}
		}
	}

	// 3. The Ray temp directory itself. `ray start` writes ray_current_cluster
	// to the temp dir root (get_ray_address_file returns
	// <ray_temp_dir>/ray_current_cluster), so this fallback is what makes the
	// cluster address resolvable on a real `ray start` cluster.
	if data, err := os.ReadFile(filepath.Join(rayTempDir, filename)); err == nil {
		return data, nil
	}

	return nil, fmt.Errorf("file not found: %s", filename)
}
