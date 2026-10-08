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
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mohae/deepcopy"
	"github.com/ray-project/ray/go/internal/autoscaler/fake_multi_node"
	"github.com/ray-project/ray/go/internal/autoscaler/local"
	"github.com/ray-project/ray/go/internal/autoscaler/readonly"
	"github.com/ray-project/ray/go/pkg/log"
	"gopkg.in/yaml.v3"
)

// MINIMAL_EXTERNAL_CONFIG is the minimal external config template.
var MINIMAL_EXTERNAL_CONFIG = map[string]interface{}{
	"available_node_types": map[string]interface{}{
		"ray.head.default":   map[string]interface{}{},
		"ray.worker.default": map[string]interface{}{},
	},
	"head_node_type": "ray.head.default",
	"head_node":      map[string]interface{}{},
	"worker_nodes":   map[string]interface{}{},
}

// loadFakeMultinodeDefaultsConfig loads the fake_multinode default config.
func loadFakeMultinodeDefaultsConfig() ([]byte, error) {
	return fake_multi_node.LoadFakeMultinodeDefaultsConfig()
}

// loadReadonlyDefaultsConfig loads the readonly default config.
func loadReadonlyDefaultsConfig() ([]byte, error) {
	return readonly.LoadReadonlyDefaultsConfig()
}

// loadFakeMultinodeDockerDefaultsConfig loads the fake_multinode_docker default config.
func loadFakeMultinodeDockerDefaultsConfig() ([]byte, error) {
	return fake_multi_node.LoadFakeMultinodeDockerDefaultsConfig()
}

// loadLocalDefaultsConfig loads the local default config.
func loadLocalDefaultsConfig() ([]byte, error) {
	return local.LoadLocalDefaultsConfig()
}

// configLoaderFunc is the type of a config loading function.
type configLoaderFunc func() ([]byte, error)

// DEFAULT_CONFIGS maps provider types to their config loading functions.
// TODO: the aws, gcp, azure, aliyun and vsphere provider types are not
// registered here.
var DEFAULT_CONFIGS = map[string]configLoaderFunc{
	"fake_multinode":        loadFakeMultinodeDefaultsConfig,
	"fake_multinode_docker": loadFakeMultinodeDockerDefaultsConfig,
	"local":                 loadLocalDefaultsConfig,
	"readonly":              loadReadonlyDefaultsConfig,
}

// GetDefaultConfig returns the default config for the given provider type.
func GetDefaultConfig(providerConfig map[string]interface{}) (map[string]interface{}, error) {
	var err error
	err = nil
	// Read the provider type.
	providerType, ok := providerConfig["type"].(string)
	if !ok {
		err = fmt.Errorf("provider type is not specified or is not a string")
		log.Log.V(1).Error(err, "")
		return nil, err
	}

	// Handle the special "external" case.
	// The external type does not use a YAML config file; instead it returns a
	// minimal config template containing the basic structure required by a
	// multi-node-type cluster.
	if providerType == "external" {
		return deepcopy.Copy(MINIMAL_EXTERNAL_CONFIG).(map[string]interface{}), nil
	}

	// Look up the config loader for the provider type. DEFAULT_CONFIGS holds
	// the mapping between provider types and their config loading functions.
	loadConfig, exists := DEFAULT_CONFIGS[providerType]
	if !exists {
		err = fmt.Errorf("unsupported node provider: %s", providerType)
		log.Log.V(1).Error(err, "")
		return nil, err
	}

	// Load the provider's embedded default config content.
	defaultsContent, err := loadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load default config for %s: %w", providerType, err)
	}

	// Parse the YAML content into a config map.
	var defaults map[string]interface{}
	decoder := yaml.NewDecoder(bytes.NewReader(defaultsContent))
	if err := decoder.Decode(&defaults); err != nil {
		err = fmt.Errorf("failed to parse default config for %s: %w", providerType, err)
		log.Log.V(1).Error(err, "")
		return nil, err
	}

	return deepcopy.Copy(defaults).(map[string]interface{}), nil
}

// LoadYamlFile parses a YAML config file.
func LoadYamlFile(path string) (map[string]interface{}, error) {
	log.Log.V(1).Info("Loading YAML config file", "path", path)

	file, err := os.Open(path)
	if err == nil {
		defer file.Close()
		var yamlConfig map[string]interface{}
		decoder := yaml.NewDecoder(file)
		if err := decoder.Decode(&yamlConfig); err != nil {
			err = fmt.Errorf("failed to parse YAML config file %s: %w", path, err)
			log.Log.V(1).Error(err, "")
			return nil, err
		}
		return yamlConfig, nil
	}

	// In a Bazel test environment, fall back to the runfiles path.
	runfilesDir := os.Getenv("RUNFILES_DIR")
	if runfilesDir == "" {
		runfilesDir = os.Getenv("TEST_SRCDIR")
	}

	if runfilesDir != "" {
		// Build the full runfiles path.
		fullPath := filepath.Join(runfilesDir, "io_ray", path)
		log.Log.V(1).Info("Trying runfiles path", "path", fullPath)

		file, err = os.Open(fullPath)
		if err == nil {
			defer file.Close()
			var yamlConfig map[string]interface{}
			decoder := yaml.NewDecoder(file)
			if err := decoder.Decode(&yamlConfig); err != nil {
				err = fmt.Errorf("failed to parse YAML config file %s: %w", fullPath, err)
				log.Log.V(1).Error(err, "")
				return nil, err
			}
			return yamlConfig, nil
		}
	}

	err = fmt.Errorf("failed to read default config file %s: %w", path, err)
	log.Log.V(1).Error(err, "")
	return nil, err
}
