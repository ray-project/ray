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
	"os"
	"slices"

	"github.com/ray-project/ray/go/pkg/log"
)

var logger = log.WithName("runtime-env-agent")

// Get returns the value of a runtime environment field.
func (e *RuntimeEnv) Get(name string, defaultVal interface{}) interface{} {
	if val, ok := (*e)[name]; ok {
		return val
	}
	return defaultVal
}

// Has reports whether a field exists and is non-empty.
func (e *RuntimeEnv) Has(name string) bool {
	val, ok := (*e)[name]
	if !ok {
		return false
	}
	return !IsRuntimeEnvParamNilOrEmpty(val)
}

// setItem sets a single field, running the validation logic.
func (e *RuntimeEnv) setItem(key string, value interface{}) error {
	// Run validation/transformation for known fields that have a validator.
	validateValue, err := validateAndTransform(key, value)
	if err != nil {
		return err
	}

	(*e)[key] = validateValue
	return nil
}

// validateAndTransform validates and transforms a field value.
func validateAndTransform(key string, value interface{}) (any, error) {
	// Check for a registered validation function.
	if validateFn, exists := OptionToValidationFn[key]; exists {
		validateValue, err := validateFn(value)
		return validateValue, err
	}
	// Without a validation function, return the value unchanged.
	return value, nil
}

// getNonEmptyFieldKeys returns the keys of all non-empty fields in the
// RuntimeEnv.
func (env *RuntimeEnv) getNonEmptyFieldKeys() []string {
	keys := make([]string, 0, len(*env))
	for k, v := range *env {
		if !IsRuntimeEnvParamNilOrEmpty(v) {
			keys = append(keys, k)
		}
	}
	return keys
}

// validateContainerExclusiveWithKeys verifies the exclusivity of the container
// field. The container field cannot be used together with other fields except
// config and env_vars.
func validateContainerExclusiveWithKeys(nonEmptyKeys []string) error {
	// Collect all non-empty keys and filter out the allowed ones.
	invalidKeys := make([]string, 0)
	for _, key := range nonEmptyKeys {
		if key != FieldContainer && key != FieldConfig && key != FieldEnvVars {
			invalidKeys = append(invalidKeys, key)
		}
	}

	if len(invalidKeys) > 0 {
		return fmt.Errorf(
			"the 'container' field cannot be used together with other fields "+
				"of runtime_env (except 'config' and 'env_vars'). "+
				"Conflicting fields: %v. "+
				"To use container, please remove the above fields or use 'image_uri' instead",
			invalidKeys)
	}

	return nil
}

// fieldConfig describes a known runtime environment field.
type fieldConfig struct {
	name     string
	hasValue func(*RuntimeEnvOptions) bool
	getValue func(*RuntimeEnvOptions) interface{}
}

// fieldConfigs is the package-level field configuration table, built once at
// package initialization.
var fieldConfigs = []fieldConfig{
	{
		name:     FieldPyModules,
		hasValue: func(o *RuntimeEnvOptions) bool { return len(o.PyModules) > 0 },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.PyModules },
	},
	{
		name:     FieldPyExecutable,
		hasValue: func(o *RuntimeEnvOptions) bool { return o.PyExecutable != "" },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.PyExecutable },
	},
	{
		name:     FieldJavaJars,
		hasValue: func(o *RuntimeEnvOptions) bool { return len(o.JavaJars) > 0 },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.JavaJars },
	},
	{
		name:     FieldWorkingDir,
		hasValue: func(o *RuntimeEnvOptions) bool { return o.WorkingDir != "" },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.WorkingDir },
	},
	{
		name:     FieldConda,
		hasValue: func(o *RuntimeEnvOptions) bool { return o.Conda != nil },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.Conda },
	},
	{
		name:     FieldPip,
		hasValue: func(o *RuntimeEnvOptions) bool { return o.Pip != nil },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.Pip },
	},
	{
		name:     FieldUv,
		hasValue: func(o *RuntimeEnvOptions) bool { return o.Uv != nil },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.Uv },
	},
	{
		name:     FieldContainer,
		hasValue: func(o *RuntimeEnvOptions) bool { return o.Container != nil },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.Container },
	},
	{
		name:     FieldEnvVars,
		hasValue: func(o *RuntimeEnvOptions) bool { return len(o.EnvVars) > 0 },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.EnvVars },
	},
	{
		name:     FieldConfig,
		hasValue: func(o *RuntimeEnvOptions) bool { return len(o.Config) > 0 },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.Config },
	},
	{
		name:     FieldImageURI,
		hasValue: func(o *RuntimeEnvOptions) bool { return o.ImageURI != "" },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.ImageURI },
	},
	{
		name:     FieldNsight,
		hasValue: func(o *RuntimeEnvOptions) bool { return o.Nsight != nil },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.Nsight },
	},
	{
		name:     FieldRocprofSys,
		hasValue: func(o *RuntimeEnvOptions) bool { return o.RocprofSys != nil },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.RocprofSys },
	},
	{
		name:     FieldWorkerProcessSetupHook,
		hasValue: func(o *RuntimeEnvOptions) bool { return o.WorkerProcessSetupHook != nil },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.WorkerProcessSetupHook },
	},
	{
		name:     FieldRayRelease,
		hasValue: func(o *RuntimeEnvOptions) bool { return o.RayRelease != "" },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.RayRelease },
	},
	{
		name:     FieldRayCommit,
		hasValue: func(o *RuntimeEnvOptions) bool { return o.RayCommit != "" },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.RayCommit },
	},
	{
		name:     FieldInjectCurrentRay,
		hasValue: func(o *RuntimeEnvOptions) bool { return o.InjectCurrentRay },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.InjectCurrentRay },
	},
	{
		name:     FieldExcludes,
		hasValue: func(o *RuntimeEnvOptions) bool { return len(o.Excludes) > 0 },
		getValue: func(o *RuntimeEnvOptions) interface{} { return o.Excludes },
	},
}

// setKnownFields sets the known fields in batch.
func (e *RuntimeEnv) setKnownFields(opts *RuntimeEnvOptions) error {
	for _, fc := range fieldConfigs {
		if fc.hasValue(opts) {
			if err := e.setItem(fc.name, fc.getValue(opts)); err != nil {
				return err
			}
		}
	}
	return nil
}

// setExtraFields sets the extra fields.
func (e *RuntimeEnv) setExtraFields(extraFields map[string]interface{}) error {
	for k, v := range extraFields {
		if err := e.setItem(k, v); err != nil {
			return err
		}
	}
	return nil
}

// validateMutualExclusion verifies that conda, pip and uv are mutually
// exclusive; these three fields cannot be specified at the same time.
func (e *RuntimeEnv) validateMutualExclusion() error {
	nonEmptyCount := 0
	if e.HasConda() {
		nonEmptyCount++
	}
	if e.HasPip() {
		nonEmptyCount++
	}
	if e.HasUv() {
		nonEmptyCount++
	}
	if nonEmptyCount > 1 {
		var condaStr, pipStr, uvStr string
		if e.HasConda() {
			condaStr = fmt.Sprintf("%v", (*e).Get(FieldConda, nil))
		}
		if e.HasPip() {
			pipStr = fmt.Sprintf("%v", (*e).Get(FieldPip, nil))
		}
		if e.HasUv() {
			uvStr = fmt.Sprintf("%v", (*e).Get(FieldUv, nil))
		}
		return fmt.Errorf(
			"The 'pip' field, 'uv' field, and 'conda' field of "+
				"runtime_env cannot be specified at the same time.\n"+
				"specified pip field: %s\n"+
				"specified conda field: %s\n"+
				"specified uv field: %s\n"+
				"To use pip with conda, please only set the 'conda'"+
				"field, and specify your pip dependencies within the conda YAML "+
				"config dict: see https://conda.io/projects/conda/en/latest/"+
				"user-guide/tasks/manage-environments.html"+
				"#create-env-file-manually",
			pipStr, condaStr, uvStr)
	}
	return nil
}

// validateImageURICompatibility verifies the compatibility of the image_uri
// field. The image_uri field cannot be used together with incompatible fields.
func (e *RuntimeEnv) validateImageURICompatibility(allNonEmptyKeys []string) error {
	// Find the incompatible fields.
	incompatibleKeys := []string{}
	for _, key := range allNonEmptyKeys {
		if !slices.Contains(ImageURICompatibleKeys, key) {
			incompatibleKeys = append(incompatibleKeys, key)
		}
	}

	if len(incompatibleKeys) > 0 {
		return fmt.Errorf(
			"The 'image_uri' field cannot be used together with "+
				"the following incompatible fields of runtime_env: %v",
			incompatibleKeys)
	}
	return nil
}

// autoSetDefaultFields sets the default fields automatically.
// 1. If pip or conda is specified and _ray_commit is not set, _ray_commit is
// set automatically.
// 2. If the RAY_RUNTIME_ENV_LOCAL_DEV_MODE environment variable is set,
// _inject_current_ray is set automatically.
func (e *RuntimeEnv) autoSetDefaultFields() error {
	// Auto-set _ray_commit: if pip or conda is specified and _ray_commit is
	// not set.
	if (e.HasPip() || e.HasConda()) && e.Get(FieldRayCommit, "") == "" {
		if err := e.setItem(FieldRayCommit, RayCommit); err != nil {
			return err
		}
	}

	// Auto-set _inject_current_ray: if the
	// RAY_RUNTIME_ENV_LOCAL_DEV_MODE environment variable is set.
	if os.Getenv("RAY_RUNTIME_ENV_LOCAL_DEV_MODE") != "" {
		if err := e.setItem(FieldInjectCurrentRay, true); err != nil {
			return err
		}
	}
	return nil
}

// NewRuntimeEnv creates a new runtime environment.
//
// Validation logic:
// 1. The conda, pip and uv fields cannot be specified at the same time.
// 2. The container field cannot be used together with other fields (except
// config and env_vars).
// 3. The image_uri field cannot be used together with incompatible fields.
func NewRuntimeEnv(options ...func(*RuntimeEnvOptions) error) (*RuntimeEnv, error) {
	// Initialize the options; validation defaults to true.
	opts := &RuntimeEnvOptions{
		EnvVars:     make(map[string]string),
		ExtraFields: make(map[string]interface{}),
		Validate:    true,
	}

	// Apply all option functions.
	for _, opt := range options {
		if err := opt(opts); err != nil {
			return nil, err
		}
	}

	// Create the RuntimeEnv instance (backed by a map).
	env := &RuntimeEnv{}
	*env = make(RuntimeEnv)

	// Set the known fields in batch.
	if err := env.setKnownFields(opts); err != nil {
		return nil, err
	}

	// Set the extra fields.
	if err := env.setExtraFields(opts.ExtraFields); err != nil {
		return nil, err
	}

	// If validation is not required, the set fields are returned as-is.
	if !opts.Validate {
		return env, nil
	}

	// Validate: conda, pip and uv cannot be specified at the same time.
	if err := env.validateMutualExclusion(); err != nil {
		return nil, err
	}

	// Cache the result of getNonEmptyFieldKeys to avoid recomputation.
	// It is only computed when container or image_uri validation is needed.
	hasContainer := env.HasContainer()
	hasImageURI := env.ImageURI() != nil

	var allNonEmptyKeys []string
	if hasContainer || hasImageURI {
		allNonEmptyKeys = env.getNonEmptyFieldKeys()
	}

	// Validate: the container field cannot be used together with other fields
	// (except config and env_vars).
	if hasContainer {
		if err := validateContainerExclusiveWithKeys(allNonEmptyKeys); err != nil {
			return nil, err
		}
		// Emit a deprecation warning when the container field is used.
		logger.V(1).Info("The `container` runtime environment field is DEPRECATED and will be removed. Use `image_uri` instead. See https://docs.ray.io/en/latest/serve/advanced-guides/multi-app-container.html.")
	}

	// Validate: the image_uri field cannot be used together with incompatible
	// fields.
	if hasImageURI {
		if err := env.validateImageURICompatibility(allNonEmptyKeys); err != nil {
			return nil, err
		}
	}

	// Auto-set the default fields.
	if err := env.autoSetDefaultFields(); err != nil {
		return nil, err
	}

	// Empty cleanup: return nil if all fields are empty.
	if len(*env) == 0 {
		return nil, nil
	}

	return env, nil
}
