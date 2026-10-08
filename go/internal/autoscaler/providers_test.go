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
	"path/filepath"
	"testing"

	"github.com/mohae/deepcopy"
	"github.com/stretchr/testify/assert"
)

// TestGetDefaultConfig tests the default config lookup.
func TestGetDefaultConfig(t *testing.T) {
	t.Run("ExternalProvider", func(t *testing.T) {
		// The external type returns the minimal config template.
		providerConfig := map[string]interface{}{
			"type": "external",
		}
		result, err := GetDefaultConfig(providerConfig)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// The returned config must contain the expected fields.
		assert.Contains(t, result, "available_node_types")
		assert.Contains(t, result, "head_node_type")
		assert.Contains(t, result, "head_node")
		assert.Contains(t, result, "worker_nodes")

		// available_node_types must contain the expected node types.
		nodeTypes, ok := result["available_node_types"].(map[string]interface{})
		assert.True(t, ok)
		assert.Contains(t, nodeTypes, "ray.head.default")
		assert.Contains(t, nodeTypes, "ray.worker.default")
	})

	t.Run("LocalProvider", func(t *testing.T) {
		// The local type must load its default config.
		providerConfig := map[string]interface{}{
			"type": "local",
		}
		result, err := GetDefaultConfig(providerConfig)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// The default config of the local provider does not contain the
		// provider field, which is expected.
		assert.Contains(t, result, "cluster_name")
		assert.Equal(t, "default", result["cluster_name"])
		assert.Contains(t, result, "auth")
		assert.Contains(t, result, "upscaling_speed")
		assert.Contains(t, result, "idle_timeout_minutes")
	})

	t.Run("ReadonlyProvider", func(t *testing.T) {
		// The readonly type must load its default config.
		providerConfig := map[string]interface{}{
			"type": "readonly",
		}
		result, err := GetDefaultConfig(providerConfig)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// The returned config must contain the readonly provider fields.
		assert.Contains(t, result, "provider")
		provider, ok := result["provider"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "readonly", provider["type"])
	})

	t.Run("FakeMultinodeProvider", func(t *testing.T) {
		// The fake_multinode type must load its default config.
		providerConfig := map[string]interface{}{
			"type": "fake_multinode",
		}
		result, err := GetDefaultConfig(providerConfig)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// The returned config must contain the fake_multinode provider fields.
		assert.Contains(t, result, "provider")
		provider, ok := result["provider"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "fake_multinode", provider["type"])
	})

	t.Run("FakeMultinodeDockerProvider", func(t *testing.T) {
		// The fake_multinode_docker type must load its default config.
		providerConfig := map[string]interface{}{
			"type": "fake_multinode_docker",
		}
		result, err := GetDefaultConfig(providerConfig)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// The returned config must contain the fake_multinode_docker provider
		// fields.
		assert.Contains(t, result, "provider")
		provider, ok := result["provider"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "fake_multinode_docker", provider["type"])
	})

	t.Run("UnspecifiedProviderType", func(t *testing.T) {
		// A missing provider type must return an error.
		providerConfig := map[string]interface{}{}
		result, err := GetDefaultConfig(providerConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "provider type is not specified or is not a string")
		assert.Nil(t, result)
	})

	t.Run("InvalidProviderType", func(t *testing.T) {
		// A non-string provider type must return an error.
		providerConfig := map[string]interface{}{
			"type": 123, // A number instead of a string.
		}
		result, err := GetDefaultConfig(providerConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "provider type is not specified or is not a string")
		assert.Nil(t, result)
	})

	t.Run("UnsupportedProviderType", func(t *testing.T) {
		// An unsupported provider type must return an error.
		providerConfig := map[string]interface{}{
			"type": "unsupported_provider",
		}
		result, err := GetDefaultConfig(providerConfig)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported node provider: unsupported_provider")
		assert.Nil(t, result)
	})

	t.Run("DeepCopyVerification", func(t *testing.T) {
		// The result is a deep copy, so mutating it must not affect the source
		// config.
		providerConfig := map[string]interface{}{
			"type": "local",
		}
		result1, err := GetDefaultConfig(providerConfig)
		assert.NoError(t, err)

		// Mutate the returned config.
		result1["modified"] = true

		// Fetch the config again.
		result2, err := GetDefaultConfig(providerConfig)
		assert.NoError(t, err)

		// The second result must not be affected by the first mutation.
		_, exists := result2["modified"]
		assert.False(t, exists)
	})
}

// TestLoadYamlFile tests the YAML file loading.
func TestLoadYamlFile(t *testing.T) {
	t.Run("ValidYamlFile", func(t *testing.T) {
		// Create a temporary YAML file.
		tmpFile, err := os.CreateTemp("", "test_*.yaml")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		yamlContent := `
key1: value1
key2: 42
nested:
  key3: value3
  key4: true
list:
  - item1
  - item2
`
		_, err = tmpFile.WriteString(yamlContent)
		assert.NoError(t, err)

		result, err := LoadYamlFile(tmpFile.Name())
		assert.NoError(t, err)
		assert.NotNil(t, result)

		assert.Equal(t, "value1", result["key1"])
		// YAML v3 parses integers as int instead of float64.
		assert.Equal(t, 42, result["key2"])

		nested, ok := result["nested"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "value3", nested["key3"])
		assert.Equal(t, true, nested["key4"])

		list, ok := result["list"].([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 2, len(list))
		assert.Equal(t, "item1", list[0])
		assert.Equal(t, "item2", list[1])
	})

	t.Run("NonExistentFile", func(t *testing.T) {
		// A missing file must return an error.
		result, err := LoadYamlFile("/non/existent/file.yaml")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read default config file")
		assert.Nil(t, result)
	})

	t.Run("InvalidYamlContent", func(t *testing.T) {
		// Invalid YAML content must return an error.
		tmpFile, err := os.CreateTemp("", "test_invalid_*.yaml")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		invalidYaml := `
key1: value1
  invalid_indentation: value2
`
		_, err = tmpFile.WriteString(invalidYaml)
		assert.NoError(t, err)

		result, err := LoadYamlFile(tmpFile.Name())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse YAML config file")
		assert.Nil(t, result)
	})

	t.Run("EmptyYamlFile", func(t *testing.T) {
		// An empty YAML file returns an EOF error.
		tmpFile, err := os.CreateTemp("", "test_empty_*.yaml")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		result, err := LoadYamlFile(tmpFile.Name())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "EOF")
		assert.Nil(t, result)
	})

	t.Run("YamlWithComments", func(t *testing.T) {
		// A YAML file with comments must parse correctly.
		tmpFile, err := os.CreateTemp("", "test_comments_*.yaml")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		yamlContent := `
# This is a comment
key1: value1
# Another comment
key2: value2
`
		_, err = tmpFile.WriteString(yamlContent)
		assert.NoError(t, err)

		result, err := LoadYamlFile(tmpFile.Name())
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result))
		assert.Equal(t, "value1", result["key1"])
		assert.Equal(t, "value2", result["key2"])
	})

	t.Run("ComplexYamlStructure", func(t *testing.T) {
		// A YAML file with deeply nested structures.
		tmpFile, err := os.CreateTemp("", "test_complex_*.yaml")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		yamlContent := `
cluster_name: test-cluster
max_workers: 5
available_node_types:
  ray.head.default:
    resources:
      CPU: 4
      memory: 16000
    max_workers: 0
  ray.worker.default:
    resources:
      CPU: 2
      memory: 8000
    max_workers: 10
head_node_type: ray.head.default
provider:
  type: local
  head_ip: "127.0.0.1"
  worker_ips:
    - "127.0.0.2"
    - "127.0.0.3"
auth:
  ssh_user: ubuntu
file_mounts: {}
cluster_synced_files: []
initialization_commands: []
setup_commands: []
head_setup_commands: []
worker_setup_commands: []
head_start_ray_commands: []
worker_start_ray_commands: []
`
		_, err = tmpFile.WriteString(yamlContent)
		assert.NoError(t, err)

		result, err := LoadYamlFile(tmpFile.Name())
		assert.NoError(t, err)
		assert.NotNil(t, result)

		assert.Equal(t, "test-cluster", result["cluster_name"])
		// YAML v3 parses integers as int.
		assert.Equal(t, 5, result["max_workers"])

		nodeTypes, ok := result["available_node_types"].(map[string]interface{})
		assert.True(t, ok)
		assert.Contains(t, nodeTypes, "ray.head.default")
		assert.Contains(t, nodeTypes, "ray.worker.default")

		provider, ok := result["provider"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "local", provider["type"])
		assert.Equal(t, "127.0.0.1", provider["head_ip"])

		workerIPs, ok := provider["worker_ips"].([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 2, len(workerIPs))
	})
}

// TestLoadYamlFile_BazelEnvironment tests file loading in the Bazel environment.
func TestLoadYamlFile_BazelEnvironment(t *testing.T) {
	t.Run("RunWithfilesEnv", func(t *testing.T) {
		// Simulate RUNFILES_DIR in a Bazel environment.
		originalRunfilesDir := os.Getenv("RUNFILES_DIR")
		defer os.Setenv("RUNFILES_DIR", originalRunfilesDir)

		// Create a temporary directory as the runfiles root.
		tmpDir, err := os.MkdirTemp("", "test_runfiles_*")
		assert.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		// Create the io_ray subdirectory and the test file inside the runfiles
		// directory.
		ioRayDir := filepath.Join(tmpDir, "io_ray")
		err = os.MkdirAll(ioRayDir, 0755)
		assert.NoError(t, err)

		testFile := filepath.Join(ioRayDir, "test_config.yaml")
		yamlContent := "test_key: test_value\n"
		err = os.WriteFile(testFile, []byte(yamlContent), 0644)
		assert.NoError(t, err)

		// Set the RUNFILES_DIR environment variable.
		os.Setenv("RUNFILES_DIR", tmpDir)

		// Load the relative path (resolved through the runfiles path in a
		// Bazel environment).
		result, err := LoadYamlFile("test_config.yaml")
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test_value", result["test_key"])
	})

	t.Run("TestSrcdirEnv", func(t *testing.T) {
		// This test only covers the basic logic without depending on a
		// specific Bazel path.

		// Save the original environment.
		originalTestSrcdir := os.Getenv("TEST_SRCDIR")
		defer os.Setenv("TEST_SRCDIR", originalTestSrcdir)

		// Clear the environment variable.
		os.Unsetenv("TEST_SRCDIR")

		// Without a Bazel environment, LoadYamlFile falls back to normal file
		// loading; create a temporary file and verify that path.
		tempDir := t.TempDir()
		testConfigPath := filepath.Join(tempDir, "test_config2.yaml")
		yamlContent := "test_key: another_value"
		err := os.WriteFile(testConfigPath, []byte(yamlContent), 0644)
		assert.NoError(t, err)

		// Load through the absolute path.
		result, err := LoadYamlFile(testConfigPath)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "another_value", result["test_key"])
	})

	t.Run("RunfilesDirTakesPrecedence", func(t *testing.T) {
		// When both RUNFILES_DIR and TEST_SRCDIR exist, RUNFILES_DIR wins.
		originalRunfilesDir := os.Getenv("RUNFILES_DIR")
		originalTestSrcdir := os.Getenv("TEST_SRCDIR")
		defer os.Setenv("RUNFILES_DIR", originalRunfilesDir)
		defer os.Setenv("TEST_SRCDIR", originalTestSrcdir)

		// Create two different temporary directories.
		runfilesDir, err := os.MkdirTemp("", "test_runfiles_*")
		assert.NoError(t, err)
		defer os.RemoveAll(runfilesDir)

		testSrcdir, err := os.MkdirTemp("", "test_srcdir_*")
		assert.NoError(t, err)
		defer os.RemoveAll(testSrcdir)

		// Create the correct file under RUNFILES_DIR.
		ioRayRunfiles := filepath.Join(runfilesDir, "io_ray")
		err = os.MkdirAll(ioRayRunfiles, 0755)
		assert.NoError(t, err)

		runfilesFile := filepath.Join(ioRayRunfiles, "priority_test.yaml")
		err = os.WriteFile(runfilesFile, []byte("source: runfiles\n"), 0644)
		assert.NoError(t, err)

		// Create a different file under TEST_SRCDIR.
		ioRaySrcdir := filepath.Join(testSrcdir, "io_ray")
		err = os.MkdirAll(ioRaySrcdir, 0755)
		assert.NoError(t, err)

		srcdirFile := filepath.Join(ioRaySrcdir, "priority_test.yaml")
		err = os.WriteFile(srcdirFile, []byte("source: test_srcdir\n"), 0644)
		assert.NoError(t, err)

		// Set both environment variables.
		os.Setenv("RUNFILES_DIR", runfilesDir)
		os.Setenv("TEST_SRCDIR", testSrcdir)

		// The file under RUNFILES_DIR must be used.
		result, err := LoadYamlFile("priority_test.yaml")
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "runfiles", result["source"])
	})
}

// TestMinimalExternalConfig verifies the minimal external config template.
func TestMinimalExternalConfig(t *testing.T) {
	t.Run("TemplateStructure", func(t *testing.T) {
		// Verify the MINIMAL_EXTERNAL_CONFIG structure.
		assert.Contains(t, MINIMAL_EXTERNAL_CONFIG, "available_node_types")
		assert.Contains(t, MINIMAL_EXTERNAL_CONFIG, "head_node_type")
		assert.Contains(t, MINIMAL_EXTERNAL_CONFIG, "head_node")
		assert.Contains(t, MINIMAL_EXTERNAL_CONFIG, "worker_nodes")

		// Verify the available_node_types content.
		nodeTypes, ok := MINIMAL_EXTERNAL_CONFIG["available_node_types"].(map[string]interface{})
		assert.True(t, ok)
		assert.Contains(t, nodeTypes, "ray.head.default")
		assert.Contains(t, nodeTypes, "ray.worker.default")

		// Verify the head_node_type value.
		assert.Equal(t, "ray.head.default", MINIMAL_EXTERNAL_CONFIG["head_node_type"])
	})

	t.Run("DeepCopy", func(t *testing.T) {
		// deepcopy.Copy must not modify the original template.
		copied := deepcopy.Copy(MINIMAL_EXTERNAL_CONFIG).(map[string]interface{})

		// Mutate the copied config.
		copied["modified"] = true
		copied["head_node_type"] = "modified_type"

		// The original template must be untouched.
		_, exists := MINIMAL_EXTERNAL_CONFIG["modified"]
		assert.False(t, exists)
		assert.Equal(t, "ray.head.default", MINIMAL_EXTERNAL_CONFIG["head_node_type"])
	})
}

// BenchmarkGetDefaultConfig is the benchmark.
func BenchmarkGetDefaultConfig(b *testing.B) {
	providerConfig := map[string]interface{}{
		"type": "local",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetDefaultConfig(providerConfig)
	}
}

// BenchmarkLoadYamlFile is the benchmark.
func BenchmarkLoadYamlFile(b *testing.B) {
	// Create a temporary YAML file.
	tmpFile, err := os.CreateTemp("", "benchmark_*.yaml")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	yamlContent := `
cluster_name: benchmark-cluster
max_workers: 10
available_node_types:
  ray.head.default:
    resources:
      CPU: 4
      memory: 16000
  ray.worker.default:
    resources:
      CPU: 2
      memory: 8000
head_node_type: ray.head.default
provider:
  type: local
  head_ip: "127.0.0.1"
  worker_ips:
    - "127.0.0.2"
    - "127.0.0.3"
`
	if _, err := tmpFile.WriteString(yamlContent); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = LoadYamlFile(tmpFile.Name())
	}
}
