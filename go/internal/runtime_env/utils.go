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
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/mohae/deepcopy"
	"github.com/ray-project/ray/go/internal/common"
)

// IsRuntimeEnvParamNilOrEmpty reports whether the parameter is nil or empty.
func IsRuntimeEnvParamNilOrEmpty(param interface{}) bool {
	if param == nil {
		return true
	}

	// Check for a *ContainerConfig value.
	if config, ok := param.(*ContainerConfig); ok {
		if config == nil {
			return true
		}
		// Treat an all-empty config as empty.
		return config.Image == "" && config.WorkerPath == "" && len(config.RunOptions) == 0
	}

	// Check for empty maps of the supported types.
	if m, ok := param.(map[string]interface{}); ok {
		return len(m) == 0
	}
	if m, ok := param.(map[string]string); ok {
		return len(m) == 0
	}

	// Check for empty slices of the supported types.
	if s, ok := param.([]interface{}); ok {
		return len(s) == 0
	}
	if s, ok := param.([]string); ok {
		return len(s) == 0
	}

	// Check for an empty string.
	if str, ok := param.(string); ok {
		return str == ""
	}

	// Treat any other type as non-empty.
	return false
}

// Deserialize builds a RuntimeEnv from a JSON string.
func Deserialize(data string) (*RuntimeEnv, error) {
	var dataMap map[string]interface{}
	err := json.Unmarshal([]byte(data), &dataMap)
	if err != nil {
		return nil, err
	}

	return FromMap(dataMap)
}

// Serialize serializes a RuntimeEnv to a JSON string.
func Serialize(env *RuntimeEnv) (string, error) {
	if env == nil {
		return "", nil
	}

	data, err := json.Marshal(*env)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// extractStringField extracts a string field from the map.
func extractStringField(data map[string]interface{}, key string) string {
	if v, ok := data[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// extractStringSliceField extracts a string slice field from the map.
func extractStringSliceField(data map[string]interface{}, key string) []string {
	if v, ok := data[key]; ok {
		if arr, ok := v.([]interface{}); ok {
			return common.InterfaceToStringSlice(arr)
		} else if arr, ok := v.([]string); ok {
			return arr
		}
	}
	return nil
}

// extractStringMapField extracts a string map field from the map.
func extractStringMapField(data map[string]interface{}, key string) map[string]string {
	if v, ok := data[key]; ok {
		if m, ok := v.(map[string]interface{}); ok {
			return common.InterfaceMapToStringMap(m)
		} else if m, ok := v.(map[string]string); ok {
			return m
		}
	}
	return nil
}

// extractDirectField extracts a field from the map without type conversion.
func extractDirectField(data map[string]interface{}, key string) interface{} {
	if v, ok := data[key]; ok {
		return v
	}
	return nil
}

// extractBoolField extracts a boolean field from the map.
func extractBoolField(data map[string]interface{}, key string, defaultValue bool) bool {
	if v, ok := data[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return defaultValue
}

// extractContainerField extracts the container config field from the map.
func extractContainerField(data map[string]interface{}) *ContainerConfig {
	if v, ok := data[FieldContainer]; ok {
		if m, ok := v.(map[string]interface{}); ok {
			return mapToContainerConfig(m)
		} else if c, ok := v.(*ContainerConfig); ok {
			return c
		}
	}
	return nil
}

// extractConfigField extracts the config field from the map.
func extractConfigField(data map[string]interface{}) RuntimeEnvConfig {
	if v, ok := data[FieldConfig]; ok {
		if m, ok := v.(map[string]interface{}); ok {
			if config, err := ParseAndValidateRuntimeEnvConfig(m); err == nil {
				return config
			}
		} else if c, ok := v.(RuntimeEnvConfig); ok {
			return c
		}
	}
	return nil
}

// FromMap builds a RuntimeEnv from a map.
func FromMap(data map[string]interface{}) (*RuntimeEnv, error) {
	if data == nil {
		return nil, nil
	}

	// Build the options directly and call NewRuntimeEnv for validation and conversion.
	opts := &RuntimeEnvOptions{
		ExtraFields: make(map[string]interface{}),
		Validate:    true,

		// Options extracted from data.
		PyModules:              extractStringSliceField(data, FieldPyModules),
		PyExecutable:           extractStringField(data, FieldPyExecutable),
		JavaJars:               extractStringSliceField(data, FieldJavaJars),
		WorkingDir:             extractStringField(data, FieldWorkingDir),
		Conda:                  extractDirectField(data, FieldConda),
		Pip:                    extractDirectField(data, FieldPip),
		Uv:                     extractDirectField(data, FieldUv),
		Container:              extractContainerField(data),
		EnvVars:                extractStringMapField(data, FieldEnvVars),
		Config:                 extractConfigField(data),
		ImageURI:               extractStringField(data, FieldImageURI),
		Nsight:                 extractDirectField(data, FieldNsight),
		RocprofSys:             extractDirectField(data, FieldRocprofSys),
		WorkerProcessSetupHook: extractDirectField(data, FieldWorkerProcessSetupHook),
		RayRelease:             extractStringField(data, FieldRayRelease),
		RayCommit:              extractStringField(data, FieldRayCommit),
		InjectCurrentRay:       extractBoolField(data, FieldInjectCurrentRay, false),
		Excludes:               extractStringSliceField(data, FieldExcludes),
	}

	// Collect the extra fields.
	for k, v := range data {
		if !slices.Contains(KnownFields, k) {
			opts.ExtraFields[k] = v
		}
	}

	return NewRuntimeEnv(func(o *RuntimeEnvOptions) error {
		*o = *opts
		return nil
	})
}

// ToDict converts the RuntimeEnv to a map (deep copy).
func (e *RuntimeEnv) ToDict() map[string]interface{} {
	return ToDictRuntimeEnv(e)
}

// ToDictRuntimeEnv converts a RuntimeEnv to a map (deep copy).
func ToDictRuntimeEnv(env *RuntimeEnv) map[string]interface{} {
	if env == nil || len(*env) == 0 {
		return nil
	}

	result := make(map[string]interface{}, len(*env))
	for k, v := range *env {
		result[k] = deepcopy.Copy(v)
	}
	return result
}

// mapToContainerConfig converts a map to a ContainerConfig.
// It returns a single value (nil on error) for compatibility with existing tests.
func mapToContainerConfig(data map[string]interface{}) *ContainerConfig {
	config, _ := mapToContainerConfigWithError(data)
	return config
}

// mapToContainerConfigWithError converts a map to a ContainerConfig and returns an error.
func mapToContainerConfigWithError(data map[string]interface{}) (*ContainerConfig, error) {
	config := &ContainerConfig{}

	if image, ok := data["image"]; ok {
		if imageStr, ok := image.(string); ok {
			config.Image = imageStr
		} else {
			return nil, fmt.Errorf("container.image must be a string, got: %T", image)
		}
	}

	if workerPath, ok := data["worker_path"]; ok {
		if workerPathStr, ok := workerPath.(string); ok {
			config.WorkerPath = workerPathStr
		} else {
			return nil, fmt.Errorf("container.worker_path must be a string, got: %T", workerPath)
		}
	}

	if runOptions, ok := data["run_options"]; ok {
		if options, ok := runOptions.([]interface{}); ok {
			config.RunOptions = make([]string, len(options))
			for i, opt := range options {
				if optStr, ok := opt.(string); ok {
					config.RunOptions[i] = optStr
				} else {
					return nil, fmt.Errorf("container.run_options must be a slice of strings")
				}
			}
		} else if options, ok := runOptions.([]string); ok {
			config.RunOptions = options
		} else {
			return nil, fmt.Errorf("container.run_options must be a slice of strings, got: %T", runOptions)
		}
	}

	return config, nil
}

// EnvVarFilter is the type of environment variable filter functions.
// It receives a variable name and value and reports whether to keep the variable.
type EnvVarFilter func(key, value string) bool

// FilterEnvVarsWithPrefix filters environment variables by name prefix.
// It returns the matching variables as a map[string]string.
// It mirrors part of the Python _get_runtime_env_vars() behavior.
func FilterEnvVarsWithPrefix(prefix string) map[string]string {
	return FilterEnvVars(func(key, _ string) bool {
		return strings.HasPrefix(key, prefix)
	})
}

// FilterEnvVars filters environment variables with a custom filter function.
// It returns a map[string]string consistent with RuntimeEnvContext.EnvVars.
// It is a generalized version of collectRayEnvVars() for better reusability.
func FilterEnvVars(filter EnvVarFilter) map[string]string {
	allEnvs := os.Environ()
	result := make(map[string]string, len(allEnvs)/4) // Estimate that roughly a quarter of the variables match.

	for _, envVar := range allEnvs {
		// Each environment entry has the form "KEY=VALUE".
		parts := strings.SplitN(envVar, "=", 2)
		if len(parts) != 2 {
			// Skip malformed entries.
			continue
		}
		key, value := parts[0], parts[1]
		if filter(key, value) {
			result[key] = value
		}
	}
	return result
}

// EnvVarsToStringSlice converts environment variables from map[string]string to []string,
// with each element in "KEY=VALUE" form for command-line usage.
func EnvVarsToStringSlice(envVars map[string]string) []string {
	result := make([]string, 0, len(envVars))
	for k, v := range envVars {
		result = append(result, fmt.Sprintf("%s=%s", k, v))
	}
	return result
}

// withOptions returns an options-applier function.
func withOptions(opts *RuntimeEnvOptions) func(*RuntimeEnvOptions) error {
	return func(o *RuntimeEnvOptions) error {
		*o = *opts
		return nil
	}
}

// _marshalJSONWithSortedKeys serializes a Go map to a JSON string that matches Python json.dumps(sort_keys=True).
// Python: json.dumps({"b": 1, "a": 2}, sort_keys=True) => '{"a": 2, "b": 1}'
// Keys are sorted alphabetically and the separators are ": " and ", " (with spaces).
func _marshalJSONWithSortedKeys(v interface{}) string {
	switch val := v.(type) {
	case map[string]interface{}:
		if val == nil {
			return "null"
		}
		if len(val) == 0 {
			return "{}"
		}
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var buf strings.Builder
		buf.WriteString("{")
		for i, k := range keys {
			if i > 0 {
				buf.WriteString(", ")
			}
			kb, _ := json.Marshal(k)
			buf.Write(kb)
			buf.WriteString(": ")
			buf.WriteString(_marshalJSONWithSortedKeys(val[k]))
		}
		buf.WriteString("}")
		return buf.String()

	case []interface{}:
		if len(val) == 0 {
			return "[]"
		}
		var buf strings.Builder
		buf.WriteString("[")
		for i, item := range val {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(_marshalJSONWithSortedKeys(item))
		}
		buf.WriteString("]")
		return buf.String()

	case nil:
		return "null"
	case bool:
		if val {
			return "true"
		}
		return "false"
	case float64:
		if val == float64(int64(val)) && val >= -1<<53 && val <= 1<<53 {
			return fmt.Sprintf("%.0f", val)
		}
		return fmt.Sprintf("%g", val)
	case string:
		b, _ := json.Marshal(val)
		return string(b)

	default:
		b, _ := json.Marshal(val)
		return string(b)
	}
}

// validatePackagesConfig validates the type of the packages field in a pip/uv config.
// When present it must be a list; a missing field is not an error.
// It reuses toStringSlice for the type check to avoid duplicating it within the package.
func validatePackagesConfig(config map[string]interface{}, field string) error {
	packagesRaw, ok := config["packages"]
	if !ok {
		return nil
	}
	if _, err := toStringSlice(packagesRaw); err != nil {
		return fmt.Errorf("%s config 'packages' must be a list of strings, got: %T", field, packagesRaw)
	}
	return nil
}
