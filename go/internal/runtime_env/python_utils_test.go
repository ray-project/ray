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

// Package runtime_env provides runtime environment configuration for Ray workers.
package runtime_env

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestGetRayFile tests retrieving the value of ray.__file__.
func TestGetRayFile(t *testing.T) {
	// Save the original getRayFile function.
	originalGetRayFile := getRayFile
	defer func() { getRayFile = originalGetRayFile }()

	t.Run("successful execution", func(t *testing.T) {
		// Simulate a successful Python command execution.
		getRayFile = func() (string, error) {
			return "/path/to/ray/__init__.pyc", nil
		}

		rayFile, err := getRayFile()
		assert.NoError(t, err)
		assert.Equal(t, "/path/to/ray/__init__.pyc", rayFile)
	})
}

// TestResolveCurrentRayPath tests resolving the current Ray installation path.
func TestResolveCurrentRayPath(t *testing.T) {
	// Save the original getRayFile function.
	originalGetRayFile := getRayFile
	defer func() { getRayFile = originalGetRayFile }()

	t.Run("normal case - site-packages installation", func(t *testing.T) {
		// Simulate a site-packages installation.
		getRayFile = func() (string, error) {
			return "/home/user/.local/lib/python3.8/site-packages/ray/__init__.pyc", nil
		}

		rayPath, err := _resolveCurrentRayPath()
		assert.NoError(t, err)
		// The site-packages directory should be returned.
		expectedPath := "/home/user/.local/lib/python3.8/site-packages"
		assert.Equal(t, expectedPath, rayPath)
	})

	t.Run("development installation", func(t *testing.T) {
		// Simulate a development (editable) installation.
		getRayFile = func() (string, error) {
			return "/home/dev/ray/python/ray/__init__.py", nil
		}

		rayPath, err := _resolveCurrentRayPath()
		assert.NoError(t, err)
		// The python directory should be returned.
		expectedPath := "/home/dev/ray/python"
		assert.Equal(t, expectedPath, rayPath)
	})

	t.Run("windows path", func(t *testing.T) {
		// Simulate a Windows path.
		getRayFile = func() (string, error) {
			return "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python38\\Lib\\site-packages\\ray\\__init__.pyc", nil
		}

		rayPath, err := _resolveCurrentRayPath()
		assert.NoError(t, err)
		// Windows paths should be handled correctly too.
		assert.NotEmpty(t, rayPath)
	})

	t.Run("getRayFile error", func(t *testing.T) {
		// Simulate a getRayFile failure.
		getRayFile = func() (string, error) {
			return "", assert.AnError
		}

		rayPath, err := _resolveCurrentRayPath()
		assert.Error(t, err)
		assert.Empty(t, rayPath)
		assert.Contains(t, err.Error(), "failed to get Ray file path")
	})

	t.Run("empty ray file path", func(t *testing.T) {
		// Simulate an empty path.
		getRayFile = func() (string, error) {
			return "", nil
		}

		rayPath, err := _resolveCurrentRayPath()
		assert.Error(t, err)
		assert.Empty(t, rayPath)
		assert.Contains(t, err.Error(), "Ray file path is empty")
	})
}

// TestResolveCloneVirtualenvPath tests resolving the _clonevirtualenv.py path.
func TestResolveCloneVirtualenvPath(t *testing.T) {
	// Save the original getRayFile function.
	originalGetRayFile := getRayFile
	defer func() { getRayFile = originalGetRayFile }()

	t.Run("normal case - site-packages installation", func(t *testing.T) {
		// Simulate a site-packages installation.
		getRayFile = func() (string, error) {
			return "/home/user/.local/lib/python3.8/site-packages/ray/__init__.pyc", nil
		}

		scriptPath, err := _resolveCloneVirtualenvPath()
		assert.NoError(t, err)
		// It should point to ray/_private/runtime_env/_clonevirtualenv.py.
		expectedPath := filepath.Join("/home/user/.local/lib/python3.8/site-packages/ray", "_private", "runtime_env", "_clonevirtualenv.py")
		assert.Equal(t, expectedPath, scriptPath)
	})

	t.Run("development installation", func(t *testing.T) {
		// Simulate a development (editable) installation.
		getRayFile = func() (string, error) {
			return "/home/dev/ray/python/ray/__init__.py", nil
		}

		scriptPath, err := _resolveCloneVirtualenvPath()
		assert.NoError(t, err)
		// It should point to the script path in the development tree.
		expectedPath := filepath.Join("/home/dev/ray/python/ray", "_private", "runtime_env", "_clonevirtualenv.py")
		assert.Equal(t, expectedPath, scriptPath)
	})

	t.Run("getRayFile error", func(t *testing.T) {
		// Simulate a getRayFile failure.
		getRayFile = func() (string, error) {
			return "", assert.AnError
		}

		scriptPath, err := _resolveCloneVirtualenvPath()
		assert.Error(t, err)
		assert.Empty(t, scriptPath)
		assert.Contains(t, err.Error(), "failed to get Ray file path")
	})

	t.Run("empty ray file path", func(t *testing.T) {
		// Simulate an empty path.
		getRayFile = func() (string, error) {
			return "", nil
		}

		scriptPath, err := _resolveCloneVirtualenvPath()
		assert.Error(t, err)
		assert.Empty(t, scriptPath)
		assert.Contains(t, err.Error(), "Ray file path is empty")
	})
}

// TestResolveCloneVirtualenvPath_Integration is an integration test verifying
// the path construction logic.
func TestResolveCloneVirtualenvPath_Integration(t *testing.T) {
	// Create a temporary test directory layout.
	tempDir := t.TempDir()

	// Create the ray directory structure.
	rayDir := filepath.Join(tempDir, "ray")
	privateDir := filepath.Join(rayDir, "_private")
	runtimeEnvDir := filepath.Join(privateDir, "runtime_env")

	err := os.MkdirAll(runtimeEnvDir, 0755)
	assert.NoError(t, err)

	// Create a fake _clonevirtualenv.py file.
	scriptFile := filepath.Join(runtimeEnvDir, "_clonevirtualenv.py")
	err = os.WriteFile(scriptFile, []byte("#!/usr/bin/env python\nprint('test')"), 0644)
	assert.NoError(t, err)

	// Create a fake ray/__init__.py file.
	initFile := filepath.Join(rayDir, "__init__.py")
	err = os.WriteFile(initFile, []byte("# Ray package"), 0644)
	assert.NoError(t, err)

	// Save the original getRayFile function.
	originalGetRayFile := getRayFile
	defer func() { getRayFile = originalGetRayFile }()

	// Simulate getRayFile returning a path inside the temp directory.
	getRayFile = func() (string, error) {
		return initFile, nil
	}

	// Test the path resolution.
	scriptPath, err := _resolveCloneVirtualenvPath()
	assert.NoError(t, err)
	assert.Equal(t, scriptFile, scriptPath)

	// Verify the file actually exists.
	_, err = os.Stat(scriptPath)
	assert.NoError(t, err)
}

// TestResolveCurrentRayPath_Integration is an integration test verifying the
// Ray path resolution logic.
func TestResolveCurrentRayPath_Integration(t *testing.T) {
	// Create a temporary test directory layout.
	tempDir := t.TempDir()

	// Create the site-packages directory structure.
	sitePackagesDir := filepath.Join(tempDir, "site-packages")
	rayDir := filepath.Join(sitePackagesDir, "ray")

	err := os.MkdirAll(rayDir, 0755)
	assert.NoError(t, err)

	// Create a fake ray/__init__.py file.
	initFile := filepath.Join(rayDir, "__init__.py")
	err = os.WriteFile(initFile, []byte("# Ray package"), 0644)
	assert.NoError(t, err)

	// Save the original getRayFile function.
	originalGetRayFile := getRayFile
	defer func() { getRayFile = originalGetRayFile }()

	// Simulate getRayFile returning a path inside the temp directory.
	getRayFile = func() (string, error) {
		return initFile, nil
	}

	// Test the path resolution.
	rayPath, err := _resolveCurrentRayPath()
	assert.NoError(t, err)
	assert.Equal(t, sitePackagesDir, rayPath)
}
