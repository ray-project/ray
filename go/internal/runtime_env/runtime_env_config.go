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
	"fmt"
	"slices"

	"github.com/mohae/deepcopy"
	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/proto"
)

// RuntimeEnvConfig is the runtime environment configuration.
// It specifies the configuration options for the runtime environment and uses
// a map to store the options, allowing dynamic extension.
type RuntimeEnvConfig map[string]interface{}

// Field name constants for the RuntimeEnvConfig.
// They avoid string typos and provide type-safe field access.
const (
	ConfigFieldSetupTimeoutSeconds = "setup_timeout_seconds"
	ConfigFieldEagerInstall        = "eager_install"
	ConfigFieldLogFiles            = "log_files"
)

// configKnownFields lists the known configuration fields.
var configKnownFields = []string{
	ConfigFieldSetupTimeoutSeconds,
	ConfigFieldEagerInstall,
	ConfigFieldLogFiles,
}

// DefaultConfig returns the default runtime environment configuration.
func DefaultConfig() RuntimeEnvConfig {
	return RuntimeEnvConfig{
		ConfigFieldSetupTimeoutSeconds: common.DefaultRuntimeEnvTimeoutSeconds,
		ConfigFieldEagerInstall:        true,
		ConfigFieldLogFiles:            []string{},
	}
}

// NewRuntimeEnvConfig creates a new runtime environment configuration.
// setupTimeoutSeconds is the timeout in seconds, where -1 disables the timeout.
func NewRuntimeEnvConfig(setupTimeoutSeconds int, eagerInstall bool, logFiles []string) (RuntimeEnvConfig, error) {
	if setupTimeoutSeconds <= 0 && setupTimeoutSeconds != -1 {
		return nil, fmt.Errorf("setup_timeout_seconds must be greater than zero or equals to -1, got: %d", setupTimeoutSeconds)
	}

	if logFiles == nil {
		logFiles = []string{}
	}

	return RuntimeEnvConfig{
		ConfigFieldSetupTimeoutSeconds: setupTimeoutSeconds,
		ConfigFieldEagerInstall:        eagerInstall,
		ConfigFieldLogFiles:            logFiles,
	}, nil
}

// ParseAndValidateRuntimeEnvConfig parses and validates a runtime environment
// configuration from a map input.
func ParseAndValidateRuntimeEnvConfig(config map[string]interface{}) (RuntimeEnvConfig, error) {
	// Extract the parameters from the config map.
	setupTimeoutSeconds := common.DefaultRuntimeEnvTimeoutSeconds
	eagerInstall := true
	logFiles := []string{}
	unknownFields := []string{}

	for key, value := range config {
		if !slices.Contains(configKnownFields, key) {
			// Unknown fields are ignored.
			unknownFields = append(unknownFields, key)
			continue
		}
		switch key {
		case ConfigFieldSetupTimeoutSeconds:
			// JSON decoding parses numbers as float64.
			if v, ok := value.(int); ok {
				setupTimeoutSeconds = v
			} else if v, ok := value.(float64); ok {
				setupTimeoutSeconds = int(v)
			}
		case ConfigFieldEagerInstall:
			if v, ok := value.(bool); ok {
				eagerInstall = v
			}
		case ConfigFieldLogFiles:
			if v, ok := value.([]interface{}); ok {
				logFiles = common.InterfaceToStringSlice(v)
			}
		}
	}

	if len(unknownFields) > 0 {
		logger.Info("The following unknown entries in the runtime_env_config "+
			"dictionary will be ignored", "unknown_fields", unknownFields)
	}

	// Create the configuration via NewRuntimeEnvConfig.
	return NewRuntimeEnvConfig(setupTimeoutSeconds, eagerInstall, logFiles)
}

// BuildProtoRuntimeEnvConfig builds a proto RuntimeEnvConfig.
func BuildProtoRuntimeEnvConfig(c RuntimeEnvConfig) (*proto.RuntimeEnvConfig, error) {
	runtimeEnvConfig := &proto.RuntimeEnvConfig{}

	// Handle the setup_timeout_seconds field.
	if v, exists := c[ConfigFieldSetupTimeoutSeconds]; exists {
		if val, ok := v.(int); ok {
			runtimeEnvConfig.SetupTimeoutSeconds = int32(val)
		} else if val, ok := v.(float64); ok {
			runtimeEnvConfig.SetupTimeoutSeconds = int32(val)
		} else {
			return nil, fmt.Errorf("%s must be of type int or float64, got: %T", ConfigFieldSetupTimeoutSeconds, v)
		}
	}

	// Handle the eager_install field.
	if v, exists := c[ConfigFieldEagerInstall]; exists {
		if val, ok := v.(bool); ok {
			runtimeEnvConfig.EagerInstall = val
		} else {
			return nil, fmt.Errorf("%s must be of type bool, got: %T", ConfigFieldEagerInstall, v)
		}
	}

	// Handle the log_files field.
	if v, exists := c[ConfigFieldLogFiles]; exists {
		if val, ok := v.([]string); ok {
			runtimeEnvConfig.LogFiles = val
		} else {
			return nil, fmt.Errorf("%s must be of type []string or []interface{}, got: %T", ConfigFieldLogFiles, v)
		}
	}
	return runtimeEnvConfig, nil
}

// FromProtoRuntimeEnvConfig creates a configuration from a proto
// RuntimeEnvConfig.
func FromProtoRuntimeEnvConfig(protoConfig *proto.RuntimeEnvConfig) (RuntimeEnvConfig, error) {
	setupTimeoutSeconds := int(protoConfig.GetSetupTimeoutSeconds())

	if setupTimeoutSeconds == 0 {
		setupTimeoutSeconds = common.DefaultRuntimeEnvTimeoutSeconds
	}

	// Ensure logFiles is always a []string to avoid later type assertion
	// problems.
	var logFiles []string
	if protoLogFiles := protoConfig.GetLogFiles(); protoLogFiles != nil {
		// Explicitly copy so that the type is []string.
		logFiles = make([]string, len(protoLogFiles))
		copy(logFiles, protoLogFiles)
	} else {
		logFiles = []string{}
	}

	config, err := NewRuntimeEnvConfig(setupTimeoutSeconds, protoConfig.GetEagerInstall(), logFiles)
	if err != nil {
		return nil, err
	}

	// The returned config has been validated by NewRuntimeEnvConfig, so all
	// field types are canonical.
	return config, nil
}

// ToDict converts the configuration to a map, using a deep copy so that
// external modification does not affect the original configuration.
func ToDict(c RuntimeEnvConfig) map[string]interface{} {
	if c == nil {
		return nil
	}

	result := make(map[string]interface{}, len(c))
	for k, v := range c {
		result[k] = deepcopy.Copy(v)
	}
	return result
}

// parseAndValidateRuntimeEnvConfigOption parses and validates a config option.
func parseAndValidateRuntimeEnvConfigOption(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	// Return the config directly if it is already a RuntimeEnvConfig.
	if config, ok := value.(RuntimeEnvConfig); ok {
		return config, nil
	}

	// Parse maps via ParseAndValidateRuntimeEnvConfig.
	if configMap, ok := value.(map[string]interface{}); ok {
		config, err := ParseAndValidateRuntimeEnvConfig(configMap)
		if err != nil {
			return nil, err
		}
		return config, nil
	}

	// Any other type is an error.
	return nil, fmt.Errorf(
		"runtime_env['config'] must be of type map or RuntimeEnvConfig, got: %T",
		value)
}
