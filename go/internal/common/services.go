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

package common

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// RAY_PATH is the absolute path of the Ray Go package, used to build resource
// paths such as jars.
// Corresponds to Python: os.path.abspath(os.path.dirname(os.path.dirname(__file__))).
var RAY_PATH string

func init() {
	// Prefer the RAY_PATH environment variable.
	rayPath := os.Getenv("RAY_PATH")
	if rayPath == "" {
		// Use runtime.Caller(0) to locate the current file, which is more
		// reliable than os.Args[0].
		_, currentFile, _, ok := runtime.Caller(0)
		var currentDir string
		if ok {
			currentDir = filepath.Dir(currentFile)
		} else {
			// Fall back to the current working directory if unavailable.
			currentDir, _ = os.Getwd()
		}
		rayPath = filepath.Dir(filepath.Dir(filepath.Dir(currentDir)))
	}

	RAY_PATH = rayPath
}

// GetRayJarsDir returns the directory containing all Ray-related jars and their
// dependencies.
// Corresponds to Python: get_ray_jars_dir().
// Returns: the absolute path of the jars directory.
// Errors: returns an error if the jars directory does not exist.
func GetRayJarsDir() (string, error) {
	jarsDir := filepath.Join(RAY_PATH, "jars")
	absJarsDir, err := filepath.Abs(jarsDir)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path for jars dir: %w", err)
	}

	// Check whether the jars directory exists.
	if _, err := os.Stat(absJarsDir); os.IsNotExist(err) {
		return "", fmt.Errorf("jars directory does not exist: %s", absJarsDir)
	}

	return absJarsDir, nil
}

// ExpandUser expands a leading ~ in a path to the user's home directory.
func ExpandUser(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" || path[0] != '~' {
		return path, nil
	}

	// Get the user's home directory.
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get user home directory: %w", err)
	}

	// Build the joined path.
	// Strip the leading ~.
	relativePath := path[1:]
	relativePath = strings.TrimSpace(relativePath)
	if len(relativePath) > 0 && relativePath[0] == '/' {
		relativePath = relativePath[1:]
	}

	return filepath.Join(homeDir, relativePath), nil
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func PathIsDir(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}
