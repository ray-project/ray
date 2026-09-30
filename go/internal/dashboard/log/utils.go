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
	"os"
	"path/filepath"
	"strings"
)

// resolveFilename confines a client-supplied filename to rootLogDir, mirroring
// LogAgentV1Grpc._resolve_filename in log_agent.py:300-334. The candidate path is made
// absolute and cleaned without following symlinks first, so relative paths may
// legitimately traverse symlinks that point outside rootLogDir; only after the
// containment check does the path get fully resolved (symlinks followed).
func resolveFilename(rootLogDir, filename string) (string, error) {
	candidate, err := resolveLogPath(rootLogDir, filename)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(candidate)
	if err != nil {
		return "", fmt.Errorf("A file is not found at: %s", candidate)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("A file is not found at: %s", candidate)
	}
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve file %s: %w", candidate, err)
	}
	return resolved, nil
}

// resolveLogPath makes filename absolute relative to rootLogDir and verifies
// the result stays inside rootLogDir. Unlike resolveFilename it accepts
// directories so it can back the static /logs file browser. Symlinks are not
// followed here; the caller decides whether existence checks apply.
func resolveLogPath(rootLogDir, filename string) (string, error) {
	base, err := filepath.Abs(rootLogDir)
	if err != nil {
		return "", fmt.Errorf("resolve log dir %s: %w", rootLogDir, err)
	}
	var candidate string
	if filepath.IsAbs(filename) {
		candidate = filename
	} else {
		candidate = filepath.Join(base, filename)
	}
	if cleared, err := filepath.Abs(candidate); err == nil {
		candidate = cleared
	}
	if !inside(base, candidate) {
		return "", fmt.Errorf("%s not in %s", candidate, base)
	}
	return candidate, nil
}

// inside reports whether path is lexically contained in base after cleaning.
func inside(base, path string) bool {
	rel, err := filepath.Rel(base, path)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && rel != ""
}
