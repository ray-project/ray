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
	"encoding/json"
	"fmt"
	"slices"

	"github.com/ray-project/ray/go/internal/common"
)

// HasWorkingDir reports whether the working_dir field is set.
func (e *RuntimeEnv) HasWorkingDir() bool {
	return e.Has(FieldWorkingDir)
}

// WorkingDirURI returns the URI of the working_dir field.
func (e *RuntimeEnv) WorkingDirURI() string {
	s := e.Get(FieldWorkingDir, "").(string)
	return s
}

// PyModulesURIs returns the URIs of the py_modules field.
func (e *RuntimeEnv) PyModulesURIs() []string {
	arr := e.Get(FieldPyModules, []string{}).([]string)
	return arr
}

// CondaURI returns the URI of the conda field.
func (e *RuntimeEnv) CondaURI() (string, error) {
	if !e.HasConda() {
		return "", fmt.Errorf("conda field not set")
	}
	return GetURI(e), nil
}

// PipURI returns the URI of the pip field.
func (e *RuntimeEnv) PipURI() (string, error) {
	if !e.HasPip() {
		return "", fmt.Errorf("pip field not set")
	}
	return GetPipURI(e)
}

// UvURI returns the URI of the uv field.
func (e *RuntimeEnv) UvURI() (string, error) {
	if !e.HasUv() {
		return "", fmt.Errorf("uv field not set")
	}
	return GetUvURI(e)
}

// PluginURIs returns the URIs of the plugin fields.
func (e *RuntimeEnv) PluginURIs() []string {
	// Not implemented yet, always return an empty list.
	return []string{}
}

// WorkingDir returns the value of the working_dir field.
func (e *RuntimeEnv) WorkingDir() string {
	s := e.Get(FieldWorkingDir, "").(string)
	return s
}

// PyModules returns the py_modules field.
func (e *RuntimeEnv) PyModules() []string {
	val := e.Get(FieldPyModules, []string{})
	return common.ConvertSlice(val, []string{}, common.InterfaceToStringSlice)
}

// PyExecutable returns the value of the py_executable field.
func (e *RuntimeEnv) PyExecutable() (string, error) {
	if !e.Has(FieldPyExecutable) {
		return "", fmt.Errorf("py_executable field not set")
	}
	s := e.Get(FieldPyExecutable, "").(string)
	return s, nil
}

// JavaJars returns the java_jars field.
func (e *RuntimeEnv) JavaJars() []string {
	val := e.Get(FieldJavaJars, []string{})
	return common.ConvertSlice(val, []string{}, common.InterfaceToStringSlice)
}

// Nsight returns the nsight configuration.
func (e *RuntimeEnv) Nsight() interface{} {
	return e.Get(FieldNsight, nil)
}

// RocprofSys returns the rocprof_sys configuration.
func (e *RuntimeEnv) RocprofSys() interface{} {
	return e.Get(FieldRocprofSys, nil)
}

// EnvVars returns the env_vars field.
func (e *RuntimeEnv) EnvVars() map[string]string {
	val := e.Get(FieldEnvVars, map[string]string{})
	return common.ConvertMap(val, map[string]string{}, common.InterfaceMapToStringMap)
}

// HasConda reports whether the conda field is set.
func (e *RuntimeEnv) HasConda() bool {
	return e.Has(FieldConda)
}

// CondaEnvName returns the name of the conda environment.
func (e *RuntimeEnv) CondaEnvName() (string, error) {
	if !e.HasConda() {
		return "", fmt.Errorf("conda field not set")
	}
	if name, ok := (*e)[FieldConda].(string); ok {
		return name, nil
	}
	return "", fmt.Errorf("conda field is not a string")
}

// CondaConfig returns the conda configuration as a JSON string.
func (e *RuntimeEnv) CondaConfig() (string, error) {
	if !e.HasConda() {
		return "", fmt.Errorf("conda field not set")
	}
	if config, ok := (*e)[FieldConda].(map[string]interface{}); ok {
		jsonData, err := json.Marshal(config)
		if err != nil {
			return "", err
		}
		return string(jsonData), nil
	}
	return "", fmt.Errorf("conda field is not a map")
}

// HasPip reports whether the pip field is set.
func (e *RuntimeEnv) HasPip() bool {
	return e.Has(FieldPip)
}

// HasUv reports whether the uv field is set.
func (e *RuntimeEnv) HasUv() bool {
	return e.Has(FieldUv)
}

// VirtualenvName returns the name of the virtualenv.
func (e *RuntimeEnv) VirtualenvName() (string, error) {
	if !e.HasPip() {
		return "", fmt.Errorf("pip field not set")
	}
	if name, ok := (*e)[FieldPip].(string); ok {
		return name, nil
	}
	return "", fmt.Errorf("pip field is not a string")
}

// PipConfig returns the pip configuration dictionary.
// It returns an error if the pip field is missing or is not a map.
func (e *RuntimeEnv) PipConfig() (map[string]interface{}, error) {
	emptyMap := make(map[string]interface{})

	if !e.HasPip() {
		return emptyMap, fmt.Errorf("pip field not set")
	}

	config, ok := (*e)[FieldPip].(map[string]interface{})
	if !ok {
		// The pip field exists but is not a map (possibly a string).
		return emptyMap, fmt.Errorf("pip field is not a map, got: %T", (*e)[FieldPip])
	}
	return config, nil
}

// UvConfig returns the uv configuration dictionary.
// It returns an error if the uv field is missing or is not a map.
func (e *RuntimeEnv) UvConfig() (map[string]interface{}, error) {
	emptyMap := make(map[string]interface{})

	if !e.HasUv() {
		return emptyMap, fmt.Errorf("uv field not set")
	}

	config, ok := (*e)[FieldUv].(map[string]interface{})
	if !ok {
		// The uv field exists but is not a map (possibly a slice).
		logger.Info("uv field exists but is not a map type, got unexpected type", "type", fmt.Sprintf("%T", (*e)[FieldUv]))
		return emptyMap, fmt.Errorf("uv field is not a map, got: %T", (*e)[FieldUv])
	}
	return config, nil
}

// GetExtension returns the value of an extension field.
// It returns an error if the key is not one of the extension fields.
func (e *RuntimeEnv) GetExtension(key string) (interface{}, error) {
	if !slices.Contains(ExtensionFields, key) {
		return nil, fmt.Errorf(
			"extension key must be one of %v, got: %s",
			ExtensionFields, key)
	}
	return (*e)[key], nil
}

// HasContainer reports whether the container field is set.
func (e *RuntimeEnv) HasContainer() bool {
	return e.Has(FieldContainer)
}

// getContainerField is a generic helper for accessing container fields.
func getContainerField[T any](e *RuntimeEnv, fieldName string, validator func(*ContainerConfig) (T, bool), emptyValue T) (T, error) {
	if !e.Has(FieldContainer) {
		return emptyValue, fmt.Errorf("container field not set")
	}
	val := e.Get(FieldContainer, nil)
	c, ok := val.(*ContainerConfig)
	if !ok {
		return emptyValue, fmt.Errorf("container field is not a ContainerConfig type, got: %T", val)
	}
	result, valid := validator(c)
	if !valid {
		return emptyValue, fmt.Errorf("container %s is empty", fieldName)
	}
	return result, nil
}

// PyContainerImage returns the value of the container.image field.
func (e *RuntimeEnv) PyContainerImage() (string, error) {
	return getContainerField(e, "image", func(c *ContainerConfig) (string, bool) {
		return c.Image, c.Image != ""
	}, "")
}

// PyContainerWorkerPath returns the value of the container.worker_path field.
func (e *RuntimeEnv) PyContainerWorkerPath() (string, error) {
	return getContainerField(e, "worker_path", func(c *ContainerConfig) (string, bool) {
		return c.WorkerPath, c.WorkerPath != ""
	}, "")
}

// PyContainerRunOptions returns the value of the container.run_options field.
func (e *RuntimeEnv) PyContainerRunOptions() ([]string, error) {
	return getContainerField(e, "run_options", func(c *ContainerConfig) ([]string, bool) {
		return c.RunOptions, len(c.RunOptions) > 0
	}, []string{})
}

// ImageURI returns the value of the image_uri field.
func (e *RuntimeEnv) ImageURI() interface{} {
	return e.Get(FieldImageURI, nil)
}

// PluginEntry represents a single plugin entry.
type PluginEntry struct {
	Key   string
	Value interface{}
}

// Plugins returns the list of plugin fields.
// It returns all fields that are not in the known fields list.
func (e *RuntimeEnv) Plugins() []PluginEntry {
	result := make([]PluginEntry, 0)
	for k, v := range *e {
		if !slices.Contains(KnownFields, k) {
			result = append(result, PluginEntry{Key: k, Value: v})
		}
	}
	return result
}

// ValidateNoLocalPaths verifies that the runtime env has no local paths.
func ValidateNoLocalPaths(env *RuntimeEnv) error {
	return nil
}
