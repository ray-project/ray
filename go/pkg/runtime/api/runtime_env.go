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

package api

import (
	"encoding/json"
	"fmt"

	"github.com/ray-project/ray/go/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

// runtimeEnvConfigKey is the field key under which the runtime env
// configuration is stored inside RuntimeEnv.fields. It matches
// internal/runtime_env's FieldConfig ("config") so that Serialize() output is
// directly consumable by extractConfigField on the worker side.
const runtimeEnvConfigKey = "config"

// defaultSetupTimeoutSeconds is the setup timeout applied when a JSON config
// map omits setup_timeout_seconds (0 in proto/JSON semantics means "unset").
// Kept in sync with internal/common.DefaultRuntimeEnvTimeoutSeconds (600); api
// cannot import go/internal (dependency inversion), so the value is mirrored
// here.
const defaultSetupTimeoutSeconds int32 = 600

// RuntimeEnvConfig is the runtime env configuration.
// Consistent with Java's io.ray.api.runtimeenv.RuntimeEnvConfig.
type RuntimeEnvConfig struct {
	// SetupTimeoutSeconds is the timeout (seconds) for setting up the env; -1 disables it.
	SetupTimeoutSeconds int32 `json:"setup_timeout_seconds"`
	// EagerInstall installs the env eagerly.
	EagerInstall bool `json:"eager_install"`
	// LogFiles is the list of log files to redirect.
	LogFiles []string `json:"log_files"`
}

// RuntimeEnv is the user-facing structured runtime env value object.
// Consistent with Java's io.ray.api.runtimeenv.RuntimeEnv.
//
// It holds a single fields map (JSON object); the configuration is stored
// under the "config" key, mirroring Java RuntimeEnvImpl and the worker-side
// extractConfigField.
type RuntimeEnv struct {
	fields map[string]interface{}
}

// NewRuntimeEnv creates an empty RuntimeEnv via the internal() thin layer.
// Returns errors.ErrRuntimeNotInitialized when the runtime is not initialized.
func NewRuntimeEnv() (*RuntimeEnv, error) {
	rt, err := internal()
	if err != nil {
		return nil, err
	}
	env := rt.CreateRuntimeEnv()
	// Normalize a nil map to an empty map so the returned RuntimeEnv is never
	// in a nil-fields state (which would panic on the first Set/SetConfig
	// write). Mirrors the normalization in NewRuntimeEnvFromJSON.
	if env == nil {
		env = map[string]interface{}{}
	}
	return &RuntimeEnv{fields: env}, nil
}

// NewRuntimeEnvFromJSON deserializes a serialized runtime env JSON string.
// Returns errors.ErrRuntimeNotInitialized when the runtime is not initialized.
func NewRuntimeEnvFromJSON(serializedRuntimeEnv string) (*RuntimeEnv, error) {
	rt, err := internal()
	if err != nil {
		return nil, err
	}
	env, err := rt.DeserializeRuntimeEnv(serializedRuntimeEnv)
	if err != nil {
		return nil, err
	}
	// json.Unmarshal of "null" yields a nil map; normalize it to an empty map so
	// the returned RuntimeEnv is never in a nil-fields state (which would panic
	// on the first Set/SetConfig write).
	if env == nil {
		env = map[string]interface{}{}
	}
	return &RuntimeEnv{fields: env}, nil
}

// Set sets a field. For the "config" key, value must be a *RuntimeEnvConfig or
// a JSON-compatible map[string]interface{} (aligned with Java
// RuntimeEnvImpl.set type checking).
func (e *RuntimeEnv) Set(name string, value interface{}) error {
	// Lazy-init so the zero value (var env RuntimeEnv) is usable: RuntimeEnv is
	// an exported struct, so a user may legally hold one without going through
	// NewRuntimeEnv, and assigning into a nil map would panic.
	if e.fields == nil {
		e.fields = make(map[string]interface{})
	}
	if name == runtimeEnvConfigKey {
		switch v := value.(type) {
		case *RuntimeEnvConfig:
			// Reuse SetConfig so the setup_timeout_seconds validation and the
			// nil-clear semantics stay consistent across both "config" writers.
			return e.SetConfig(v)
		case map[string]interface{}:
			// Route JSON-compatible config maps through SetConfig so the
			// setup_timeout_seconds validation applies to this branch too
			// (aligned with Java RuntimeEnvImpl.set, which validates the
			// config regardless of how it is supplied). A nil map clears the
			// config, mirroring SetConfig(nil).
			if v == nil {
				return e.SetConfig(nil)
			}
			data, err := json.Marshal(v)
			if err != nil {
				return err
			}
			var c RuntimeEnvConfig
			if err := json.Unmarshal(data, &c); err != nil {
				return err
			}
			// A missing/zero setup_timeout_seconds in a JSON map means "unset"
			// and is normalized to the default 600 (aligned with worker-side
			// ParseAndValidateRuntimeEnvConfig and FromProtoRuntimeEnvConfig,
			// which default 0 to 600). SetConfig still rejects an explicit 0
			// for the typed *RuntimeEnvConfig branch, where 0 is an explicit
			// value rather than "unset".
			if c.SetupTimeoutSeconds == 0 {
				c.SetupTimeoutSeconds = defaultSetupTimeoutSeconds
			}
			return e.SetConfig(&c)
		default:
			return fmt.Errorf("runtime_env[config] must be of type *RuntimeEnvConfig or map, got: %T", value)
		}
	}
	e.fields[name] = value
	return nil
}

// SetJSON sets a field from a JSON string value.
func (e *RuntimeEnv) SetJSON(name, jsonStr string) error {
	var v interface{}
	if err := json.Unmarshal([]byte(jsonStr), &v); err != nil {
		return err
	}
	return e.Set(name, v)
}

// Get returns the value for a field and whether it exists.
func (e *RuntimeEnv) Get(name string) (interface{}, bool) {
	v, ok := e.fields[name]
	return v, ok
}

// GetJSON returns the JSON representation of a field.
// Returns an error when the field is missing (Go idiom; Java returns "null").
func (e *RuntimeEnv) GetJSON(name string) (string, error) {
	v, ok := e.fields[name]
	if !ok {
		return "", fmt.Errorf("field %q not found", name)
	}
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Contains reports whether a field exists.
func (e *RuntimeEnv) Contains(name string) bool {
	_, ok := e.fields[name]
	return ok
}

// Remove deletes a field; reports whether it existed.
func (e *RuntimeEnv) Remove(name string) bool {
	if _, ok := e.fields[name]; !ok {
		return false
	}
	delete(e.fields, name)
	return true
}

// IsEmpty reports whether the runtime env has no fields.
func (e *RuntimeEnv) IsEmpty() bool {
	return len(e.fields) == 0
}

// Serialize serializes the full fields map (including the "config" key) into a
// serialized_runtime_env JSON object string, directly consumable by the
// existing WithRuntimeEnv(json) submission path.
func (e *RuntimeEnv) Serialize() (string, error) {
	data, err := json.Marshal(e.fields)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// SetConfig writes the config under the "config" key.
// A nil config clears the key (GetConfig then returns nil, IsEmpty reflects
// the removal); aligned with Java's setConfig(null) rejection, expressed as a
// Go-idiomatic clear instead of an error.
// SetupTimeoutSeconds must be > 0 or -1, validated in SetConfig.
func (e *RuntimeEnv) SetConfig(config *RuntimeEnvConfig) error {
	if config == nil {
		delete(e.fields, runtimeEnvConfigKey)
		return nil
	}
	if config.SetupTimeoutSeconds <= 0 && config.SetupTimeoutSeconds != -1 {
		return fmt.Errorf("setup_timeout_seconds must be greater than zero or equal to -1, got: %d", config.SetupTimeoutSeconds)
	}
	m, err := runtimeEnvConfigToMap(config)
	if err != nil {
		return err
	}
	// Same zero-value lazy-init as Set: SetConfig is also a public entry point.
	if e.fields == nil {
		e.fields = make(map[string]interface{})
	}
	e.fields[runtimeEnvConfigKey] = m
	return nil
}

// GetConfig reads the config from the "config" key; returns nil when unset or
// when the stored value is nil.
func (e *RuntimeEnv) GetConfig() *RuntimeEnvConfig {
	raw, ok := e.fields[runtimeEnvConfigKey]
	if !ok || raw == nil {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var c RuntimeEnvConfig
	if err := json.Unmarshal(data, &c); err != nil {
		return nil
	}
	return &c
}

// SerializeToRuntimeEnvInfo serializes into a proto.RuntimeEnvInfo JSON string
// (serialized_runtime_env + independent runtime_env_config field), using proto3
// JSON field names aligned with Java's JsonFormat.printer().print(protoRuntimeEnvInfo)
// (both emit lowerCamelCase per the proto3 JSON spec). The config is extracted from
// fields["config"] and mapped to proto.RuntimeEnvConfig locally (aligned with
// internal/runtime_env.BuildProtoRuntimeEnvConfig, without importing go/internal).
func (e *RuntimeEnv) SerializeToRuntimeEnvInfo() (string, error) {
	serialized, err := e.Serialize()
	if err != nil {
		return "", err
	}
	info := &proto.RuntimeEnvInfo{
		SerializedRuntimeEnv: serialized,
	}
	if cfg := e.GetConfig(); cfg != nil {
		info.RuntimeEnvConfig = &proto.RuntimeEnvConfig{
			SetupTimeoutSeconds: cfg.SetupTimeoutSeconds,
			EagerInstall:        cfg.EagerInstall,
			LogFiles:            cfg.LogFiles,
		}
	}
	out, err := protojson.Marshal(info)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// runtimeEnvConfigToMap converts a RuntimeEnvConfig to a JSON-compatible map
// using its json tags.
func runtimeEnvConfigToMap(c *RuntimeEnvConfig) (map[string]interface{}, error) {
	if c == nil {
		return nil, nil
	}
	data, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}
