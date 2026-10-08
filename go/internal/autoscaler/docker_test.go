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

package autoscaler

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCheckDockerFileMounts tests the file mount checks.
func TestCheckDockerFileMounts(t *testing.T) {
	t.Run("EmptyFileMounts", func(t *testing.T) {
		// An empty file mount config must not produce any warning.
		fileMounts := make(map[string]interface{})
		CheckDockerFileMounts(fileMounts)
		// This test mainly makes sure there is no panic.
	})

	t.Run("DirectoryMounts", func(t *testing.T) {
		// Directory mounts must not produce a warning.
		tmpDir, err := os.MkdirTemp("", "test_docker_mounts")
		assert.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		fileMounts := map[string]interface{}{
			"/remote/dir": tmpDir,
		}
		CheckDockerFileMounts(fileMounts)
	})

	t.Run("FileMountsWithWarning", func(t *testing.T) {
		// File mounts must produce a warning.
		tmpFile, err := os.CreateTemp("", "test_docker_mount")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		fileMounts := map[string]interface{}{
			"/remote/file.txt": tmpFile.Name(),
		}
		CheckDockerFileMounts(fileMounts)
		// The warning goes through the log output.
	})

	t.Run("NonExistentPath", func(t *testing.T) {
		// Non-existent paths must be skipped without an error.
		fileMounts := map[string]interface{}{
			"/remote/nonexistent": "/non/existent/path",
		}
		CheckDockerFileMounts(fileMounts)
	})

	t.Run("MixedMounts", func(t *testing.T) {
		// Mixed file and directory mounts.
		tmpDir, err := os.MkdirTemp("", "test_docker_mounts")
		assert.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		tmpFile, err := os.CreateTemp("", "test_docker_mount")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		fileMounts := map[string]interface{}{
			"/remote/dir":     tmpDir,
			"/remote/file1":   tmpFile.Name(),
			"/remote/file2":   tmpFile.Name(),
			"/remote/missing": "/non/existent",
		}
		CheckDockerFileMounts(fileMounts)
		// Both file mounts must produce a warning.
	})
}

// TestValidateDockerConfig tests the Docker config validation.
func TestValidateDockerConfig(t *testing.T) {
	t.Run("NoDockerConfig", func(t *testing.T) {
		// Missing docker config must return nil.
		config := make(map[string]interface{})
		err := ValidateDockerConfig(config)
		assert.NoError(t, err)
	})

	t.Run("EmptyDockerConfig", func(t *testing.T) {
		// An empty docker config must return nil.
		config := map[string]interface{}{
			"docker": make(map[string]interface{}),
		}
		err := ValidateDockerConfig(config)
		assert.NoError(t, err)
	})

	t.Run("ValidImageOnly", func(t *testing.T) {
		// Only the image field without container_name is invalid.
		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"image": "rayproject/ray:latest",
			},
		}
		err := ValidateDockerConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must provide a container & image name")
	})

	t.Run("ValidContainerNameAndImage", func(t *testing.T) {
		// Both container_name and image are valid.
		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"container_name": "ray_container",
				"image":          "rayproject/ray:latest",
			},
		}
		err := ValidateDockerConfig(config)
		assert.NoError(t, err)
	})

	t.Run("ValidHeadWorkerImages", func(t *testing.T) {
		// head_image and worker_image without container_name are invalid.
		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"head_image":   "rayproject/ray:latest-head",
				"worker_image": "rayproject/ray:latest-worker",
			},
		}
		err := ValidateDockerConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must provide a container & image name")
	})

	t.Run("ValidAllFields", func(t *testing.T) {
		// All fields are valid.
		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"container_name": "ray_container",
				"image":          "rayproject/ray:latest",
				"head_image":     "rayproject/ray:latest-head",
				"worker_image":   "rayproject/ray:latest-worker",
			},
		}
		err := ValidateDockerConfig(config)
		assert.NoError(t, err)
	})

	t.Run("InvalidContainerNameOnly", func(t *testing.T) {
		// Only container_name without image is invalid.
		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"container_name": "ray_container",
			},
		}
		err := ValidateDockerConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must provide a container & image name")
	})
	t.Run("InvalidImageOnly", func(t *testing.T) {
		// Only image without container_name is invalid.
		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"image": "rayproject/ray:latest",
			},
		}
		err := ValidateDockerConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must provide a container & image name")
	})

	t.Run("InvalidHeadImageOnly", func(t *testing.T) {
		// Only head_image without worker_image and container_name counts as no
		// valid config and does not fail.
		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"head_image": "rayproject/ray:latest-head",
			},
		}
		err := ValidateDockerConfig(config)
		assert.NoError(t, err)
	})

	t.Run("InvalidWorkerImageOnly", func(t *testing.T) {
		// Only worker_image without head_image and container_name counts as no
		// valid config and does not fail.
		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"worker_image": "rayproject/ray:latest-worker",
			},
		}
		err := ValidateDockerConfig(config)
		assert.NoError(t, err)
	})

	t.Run("InvalidContainerNameAndHeadImage", func(t *testing.T) {
		// container_name and head_image without worker_image are invalid.
		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"container_name": "ray_container",
				"head_image":     "rayproject/ray:latest-head",
			},
		}
		err := ValidateDockerConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must provide a container & image name")
	})

	t.Run("InvalidContainerNameAndWorkerImage", func(t *testing.T) {
		// container_name and worker_image without head_image are invalid.
		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"container_name": "ray_container",
				"worker_image":   "rayproject/ray:latest-worker",
			},
		}
		err := ValidateDockerConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must provide a container & image name")
	})

	t.Run("WithFileMounts", func(t *testing.T) {
		// A config that contains file_mounts.
		tmpDir, err := os.MkdirTemp("", "test_docker_config")
		assert.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"container_name": "ray_container",
				"image":          "rayproject/ray:latest",
			},
			"file_mounts": map[string]interface{}{
				"/remote/file.txt": tmpDir, // Use a directory to avoid the warning.
			},
		}
		err = ValidateDockerConfig(config)
		assert.NoError(t, err)
	})

	t.Run("ComplexConfig", func(t *testing.T) {
		// A complex config scenario.
		tmpDir, err := os.MkdirTemp("", "test_docker_config")
		assert.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		tmpFile, err := os.CreateTemp("", "test_docker_config")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"container_name": "ray_container",
				"image":          "rayproject/ray:latest",
				"head_image":     "rayproject/ray:latest-head",
				"worker_image":   "rayproject/ray:latest-worker",
			},
			"file_mounts": map[string]interface{}{
				"/remote/dir":  tmpDir,
				"/remote/file": tmpFile.Name(),
			},
		}
		err = ValidateDockerConfig(config)
		assert.NoError(t, err)
	})
}

// TestValidateDockerConfig_EdgeCases tests the edge cases.
func TestValidateDockerConfig_EdgeCases(t *testing.T) {
	t.Run("NilConfig", func(t *testing.T) {
		// A nil config must return nil (no docker config).
		var config map[string]interface{}
		err := ValidateDockerConfig(config)
		assert.NoError(t, err)
	})

	t.Run("DockerNotMap", func(t *testing.T) {
		// A docker field that is not a map must return nil.
		config := map[string]interface{}{
			"docker": "not_a_map",
		}
		err := ValidateDockerConfig(config)
		assert.NoError(t, err)
	})

	t.Run("EmptyStringValues", func(t *testing.T) {
		// Empty string values count as unset.
		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"container_name": "",
				"image":          "",
				"head_image":     "",
				"worker_image":   "",
			},
		}
		err := ValidateDockerConfig(config)
		assert.NoError(t, err)
	})

	t.Run("WhitespaceValues", func(t *testing.T) {
		// Whitespace-only values count as set (non-empty).
		config := map[string]interface{}{
			"docker": map[string]interface{}{
				"container_name": "   ",
				"image":          "rayproject/ray:latest",
			},
		}
		err := ValidateDockerConfig(config)
		assert.NoError(t, err)
	})
}

// BenchmarkValidateDockerConfig is the benchmark.
func BenchmarkValidateDockerConfig(b *testing.B) {
	config := map[string]interface{}{
		"docker": map[string]interface{}{
			"container_name": "ray_container",
			"image":          "rayproject/ray:latest",
			"head_image":     "rayproject/ray:latest-head",
			"worker_image":   "rayproject/ray:latest-worker",
		},
		"file_mounts": map[string]interface{}{
			"/remote/dir": "/tmp",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ValidateDockerConfig(config)
	}
}
