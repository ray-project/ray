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
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
	"gopkg.in/yaml.v3"
)

var validationLogger = log.WithName("runtime-env-agent")

// pipConfigKeys lists the valid key names of a pip config.
const (
	PipKeyPackages          = "packages"
	PipKeyPipCheck          = "pip_check"
	PipKeyPipVersion        = "pip_version"
	PipKeyPipInstallOptions = "pip_install_options"
)

// validatePath checks that a path is well formed and exists.
func validatePath(path string) error {
	return parsePath(path)
}

// validateURI parses and validates a URI.
func validateURI(uri string) error {
	protocol, pkgName, err := ParseURI(uri)
	if err != nil {
		return fmt.Errorf("is not a valid URI")
	}

	// Remote protocols require a .zip or .whl suffix.
	if IsRemoteProtocol(protocol) && !strings.HasSuffix(pkgName, ".zip") && !strings.HasSuffix(pkgName, ".whl") {
		return fmt.Errorf("only .zip or .whl files supported for remote URIs")
	}

	return nil
}

// handleLocalDepsRequirementFile reads a local requirements file and returns all required dependencies.
func handleLocalDepsRequirementFile(requirementsFile string) ([]string, error) {
	requirementsPath := filepath.Clean(requirementsFile)
	info, err := os.Stat(requirementsPath)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid file: %w", requirementsPath, err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s is not a valid file", requirementsPath)
	}

	content, err := os.ReadFile(requirementsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read requirements file %s: %w", requirementsPath, err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	return lines, nil
}

// validatePyModulesURIs parses and validates the py_modules option (URIs only).
func validatePyModulesURIs(pyModulesURIs any) error {
	if pyModulesURIs == nil {
		return fmt.Errorf("`py_modules` must be a list of strings, got nil")
	}

	list, err := toStringSlice(pyModulesURIs)
	if err != nil {
		return fmt.Errorf("`py_modules` must be of type []string")
	}

	for i, module := range list {
		if module == "" {
			return fmt.Errorf("`py_module` at index %d must be a non-empty string", i)
		}
		if err := validateURI(module); err != nil {
			return err
		}
	}

	return nil
}

// parseAndValidatePyModules parses and validates the py_modules option (local paths or URIs).
func parseAndValidatePyModules(pyModules any) (any, error) {
	if pyModules == nil {
		return nil, fmt.Errorf("`py_modules` must be a list of strings, got nil")
	}
	list, err := toStringSlice(pyModules)
	if err != nil {
		return nil, fmt.Errorf("`py_modules` must be of type []string, got %T", pyModules)
	}

	for i, str := range list {
		if str == "" {
			return nil, fmt.Errorf("`py_module` at index %d must be a non-empty string", i)
		}

		if common.IsPath(str) {
			if err := validatePath(str); err != nil {
				return nil, err
			}
		} else {
			if err := validateURI(str); err != nil {
				return nil, err
			}
		}
	}
	return list, nil
}

// validateWorkingDirURI parses and validates the working_dir option (URIs only).
func validateWorkingDirURI(workingDirURI any) error {
	str, ok := workingDirURI.(string)
	if !ok {
		return fmt.Errorf("`working_dir` must be of type string, got %T", workingDirURI)
	}

	if err := validateURI(str); err != nil {
		return err
	}

	return nil
}

// parseAndValidateWorkingDir parses and validates the working_dir option (path or URI).
func parseAndValidateWorkingDir(workingDir any) (any, error) {
	str, ok := workingDir.(string)
	if !ok {
		return nil, fmt.Errorf("`working_dir` must be of type string, got %T", workingDir)
	}

	if common.IsPath(str) {
		if err := validatePath(str); err != nil {
			return nil, err
		}
	} else {
		if err := validateURI(str); err != nil {
			return nil, err
		}
	}

	return str, nil
}

// parseAndValidateConda parses and validates the user-provided conda option.
func parseAndValidateConda(conda any) (any, error) {
	if conda == nil {
		return nil, fmt.Errorf("conda cannot be nil")
	}

	// Warn on Windows.
	if runtime.GOOS == "windows" {
		validationLogger.V(1).Info(
			"runtime environment support is experimental on Windows. " +
				"If you run into issues please file a report at " +
				"https://github.com/ray-project/ray/issues.")
	}

	switch v := conda.(type) {
	case string:
		filePath := filepath.Clean(v)
		ext := strings.ToLower(filepath.Ext(filePath))

		// Handle YAML files.
		if ext == ".yaml" || ext == ".yml" {
			info, err := os.Stat(filePath)
			if err != nil {
				return nil, fmt.Errorf("can't find conda YAML file %s: %w", filePath, err)
			}
			if info.IsDir() {
				return nil, fmt.Errorf("can't find conda YAML file %s: not a file", filePath)
			}

			// Read and parse the YAML file.
			content, err := os.ReadFile(filePath)
			if err != nil {
				return nil, fmt.Errorf("failed to read conda file %s: %w", filePath, err)
			}

			var result any
			if err := yaml.Unmarshal(content, &result); err != nil {
				return nil, fmt.Errorf("failed to parse conda YAML file %s: %w", filePath, err)
			}

			return result, nil
		}

		// Handle absolute directory paths.
		if filepath.IsAbs(v) {
			info, err := os.Stat(v)
			if err != nil {
				return nil, fmt.Errorf("can't find conda env directory %s: %w", v, err)
			}
			if !info.IsDir() {
				return nil, fmt.Errorf("can't find conda env directory %s: not a directory", v)
			}
			return v, nil
		}

		// A plain string (environment name) is returned as-is.
		return v, nil

	case map[string]string:
		return v, nil

	default:
		return nil, fmt.Errorf("runtime_env['conda'] must be of type string or map, got %T", conda)
	}
}

// parseAndValidateUv parses and validates the user-provided uv option.
func parseAndValidateUv(uv any) (any, error) {
	if uv == nil {
		return nil, fmt.Errorf("uv cannot be nil")
	}

	if runtime.GOOS == "windows" {
		validationLogger.V(1).Info(
			"runtime environment support is experimental on Windows. " +
				"If you run into issues please file a report at " +
				"https://github.com/ray-project/ray/issues.")
	}

	var result map[string]any

	switch v := uv.(type) {
	case string:
		// Local requirements.txt file path.
		uvList, err := handleLocalDepsRequirementFile(v)
		if err != nil {
			return nil, err
		}
		result = map[string]any{
			"packages": uvList,
			"uv_check": false,
		}

	case []string:
		// Already a []string.
		result = map[string]any{
			"packages": v,
			"uv_check": false,
		}

	case []interface{}:
		// A list produced by JSON deserialization; convert it to []string.
		uvList, err := toStringSlice(v)
		if err != nil {
			return nil, fmt.Errorf("runtime_env['uv'] must be a list of strings: %w", err)
		}
		result = map[string]any{
			"packages": uvList,
			"uv_check": false,
		}

	case map[string]any:
		// Validate the keys.
		validKeys := map[string]bool{
			"packages":               true,
			"uv_check":               true,
			"uv_version":             true,
			"uv_pip_install_options": true,
		}
		for key := range v {
			if !validKeys[key] {
				return nil, fmt.Errorf("runtime_env['uv'] can only have these fields: packages, uv_check, uv_version, uv_pip_install_options, but got: %v", key)
			}
		}

		// The packages field must be present.
		packagesVal, ok := v["packages"]
		if !ok {
			return nil, fmt.Errorf("runtime_env['uv'] must include field 'packages', but got %v", v)
		}

		// Validate the uv_check type.
		if uvCheckVal, exists := v["uv_check"]; exists {
			if _, ok := uvCheckVal.(bool); !ok {
				return nil, fmt.Errorf("runtime_env['uv']['uv_check'] must be of type bool, got %T", uvCheckVal)
			}
		}

		// Validate the uv_version type.
		if uvVersionVal, exists := v["uv_version"]; exists {
			if _, ok := uvVersionVal.(string); !ok {
				return nil, fmt.Errorf("runtime_env['uv']['uv_version'] must be of type string, got %T", uvVersionVal)
			}
		}

		// Validate the uv_pip_install_options type.
		if uvPipOptsVal, exists := v["uv_pip_install_options"]; exists {
			if _, err := toStringSlice(uvPipOptsVal); err != nil {
				return nil, fmt.Errorf("runtime_env['uv']['uv_pip_install_options'] must be of type []string, got %T", uvPipOptsVal)
			}
		}

		// Copy the input map.
		result = make(map[string]any)
		for k, val := range v {
			result[k] = val
		}

		// Set defaults.
		if _, exists := result["uv_check"]; !exists {
			result["uv_check"] = false
		}
		if _, exists := result["uv_pip_install_options"]; !exists {
			result["uv_pip_install_options"] = []string{"--no-cache"}
		} else if opts, err := toStringSlice(result["uv_pip_install_options"]); err == nil {
			// Normalize to []string so downstream code can assert on it directly.
			result["uv_pip_install_options"] = opts
		}

		// Validate the packages type.
		packagesList, err := toStringSlice(packagesVal)
		if err != nil {
			return nil, fmt.Errorf("runtime_env['uv']['packages'] must be of type list, got: %T", packagesVal)
		}

		result["packages"] = packagesList

	default:
		return nil, fmt.Errorf("runtime_env['uv'] must be of type []string, string, or map, got %T", uv)
	}

	// Deduplicate packages.
	if packagesVal, ok := result["packages"].([]string); ok {
		result["packages"] = common.DeduplicateStrings(packagesVal)
	}

	// Return nil when packages is empty.
	if packages, ok := result["packages"].([]string); ok && len(packages) == 0 {
		return nil, nil
	}

	return result, nil
}

// parseAndValidatePip parses and validates the user-provided pip option.
func parseAndValidatePip(pip any) (any, error) {
	if pip == nil {
		return nil, fmt.Errorf("pip cannot be nil")
	}

	if runtime.GOOS == "windows" {
		validationLogger.V(1).Info(
			"runtime environment support is experimental on Windows. " +
				"If you run into issues please file a report at " +
				"https://github.com/ray-project/ray/issues.")
	}

	var result map[string]any

	switch v := pip.(type) {
	case string:
		pipList, err := handleLocalDepsRequirementFile(v)
		if err != nil {
			return nil, fmt.Errorf("failed to read requirements file %q: %w", v, err)
		}
		result = map[string]any{
			"packages":  pipList,
			"pip_check": false,
		}

	case []string:
		result = map[string]any{
			"packages":  v,
			"pip_check": false,
		}

	case []interface{}:
		// A list produced by JSON deserialization; convert it to []string.
		pipList, err := toStringSlice(v)
		if err != nil {
			return nil, fmt.Errorf("runtime_env['pip'] must be a list of strings: %w", err)
		}
		result = map[string]any{
			"packages":  pipList,
			"pip_check": false,
		}

	case map[string]any:
		validKeys := map[string]bool{
			PipKeyPackages:          true,
			PipKeyPipCheck:          true,
			PipKeyPipVersion:        true,
			PipKeyPipInstallOptions: true,
		}
		for key := range v {
			if !validKeys[key] {
				return nil, fmt.Errorf("runtime_env['pip'] can only have these fields: packages, pip_check, pip_install_options, pip_version, but got: %v", key)
			}
		}

		if val, ok := v[PipKeyPipCheck]; ok {
			if _, ok := val.(bool); !ok {
				return nil, fmt.Errorf("runtime_env['pip']['pip_check'] must be of type bool, got %T", val)
			}
		}
		if val, ok := v[PipKeyPipVersion]; ok {
			if _, ok := val.(string); !ok {
				return nil, fmt.Errorf("runtime_env['pip']['pip_version'] must be of type string, got %T", val)
			}
		}
		if val, ok := v[PipKeyPipInstallOptions]; ok {
			if _, err := toStringSlice(val); err != nil {
				return nil, fmt.Errorf("runtime_env['pip']['pip_install_options'] must be of type []string, got %T", val)
			}
		}

		result = make(map[string]any, len(v)+1)
		for k, val := range v {
			result[k] = val
		}
		// Normalize pip_install_options to []string so downstream code can assert on it directly.
		if optsVal, ok := v[PipKeyPipInstallOptions]; ok {
			if opts, err := toStringSlice(optsVal); err == nil {
				result[PipKeyPipInstallOptions] = opts
			}
		}
		if pipCheckVal, ok := v[PipKeyPipCheck]; ok {
			if b, ok := pipCheckVal.(bool); ok {
				result[PipKeyPipCheck] = b
			} else {
				result[PipKeyPipCheck] = false
			}
		} else {
			result[PipKeyPipCheck] = false
		}

		packagesVal, ok := v[PipKeyPackages]
		if !ok {
			return nil, fmt.Errorf("runtime_env['pip'] must include field 'packages', but got %v", v)
		}

		switch pkgs := packagesVal.(type) {
		case string:
			pipList, err := handleLocalDepsRequirementFile(pkgs)
			if err != nil {
				return nil, fmt.Errorf("failed to read requirements file %q: %w", pkgs, err)
			}
			result[PipKeyPackages] = pipList
		case []string:
			// Use the package list as-is without extra validation (matches the Python logic).
			result[PipKeyPackages] = pkgs
		case []interface{}:
			// A list produced by JSON deserialization; convert it to []string.
			pipList, err := toStringSlice(pkgs)
			if err != nil {
				return nil, fmt.Errorf("runtime_env['pip']['packages'] must be of type string or list, got: %T", packagesVal)
			}
			result[PipKeyPackages] = pipList
		default:
			return nil, fmt.Errorf("runtime_env['pip']['packages'] must be of type string or list, got: %T", packagesVal)
		}

	default:
		return nil, fmt.Errorf("runtime_env['pip'] must be of type string, []string, or map, got %T", pip)
	}

	result[PipKeyPackages] = common.DeduplicateStrings(result[PipKeyPackages].([]string))

	if len(result[PipKeyPackages].([]string)) == 0 {
		return nil, nil
	}

	return result, nil
}

// parseAndValidateContainer parses and validates the user-provided container option.
func parseAndValidateContainer(container any) (any, error) {
	if container == nil {
		return nil, fmt.Errorf("container cannot be nil")
	}
	return container, nil
}

// parseAndValidateExcludes parses and validates the user-provided excludes option.
func parseAndValidateExcludes(excludes any) (any, error) {
	if excludes == nil {
		return nil, fmt.Errorf("excludes cannot be nil")
	}

	excludesList, err := toStringSlice(excludes)
	if err != nil {
		return nil, fmt.Errorf("runtime_env['excludes'] must be of type List[str], got %T", excludes)
	}

	// An empty list yields nil.
	if len(excludesList) == 0 {
		return nil, nil
	}

	return excludesList, nil
}

// parseAndValidateEnvVars parses and validates the user-provided env_vars option.
func parseAndValidateEnvVars(envVars any) (any, error) {
	if envVars == nil {
		return nil, fmt.Errorf("env_vars cannot be nil")
	}

	var envVarsMap map[string]string
	var ok bool

	// Try a direct map[string]string conversion.
	if envVarsMap, ok = envVars.(map[string]string); !ok {
		// Otherwise convert from map[string]interface{}.
		if interfaceMap, isInterfaceMap := envVars.(map[string]interface{}); isInterfaceMap {
			envVarsMap = make(map[string]string, len(interfaceMap))
			for k, v := range interfaceMap {
				if strVal, isString := v.(string); isString {
					envVarsMap[k] = strVal
				} else {
					return nil, fmt.Errorf("runtime_env['env_vars']['%s'] must be of type string, got %T", k, v)
				}
			}
		} else {
			return nil, fmt.Errorf("runtime_env['env_vars'] must be of type map[string]string or map[string]interface{}, got %T", envVars)
		}
	}

	// An empty map yields nil.
	if len(envVarsMap) == 0 {
		return nil, nil
	}

	return envVarsMap, nil
}

// OptionValidationFn is the type of option validation functions.
type OptionValidationFn func(any) (any, error)

// OptionNoPathValidationFn is the type of validation functions that skip path checks.
type OptionNoPathValidationFn func(any) error

// OptionToValidationFn maps runtime env options to their validation functions.
var OptionToValidationFn = map[string]OptionValidationFn{
	FieldPyModules:  parseAndValidatePyModules,
	FieldWorkingDir: parseAndValidateWorkingDir,
	FieldExcludes:   parseAndValidateExcludes,
	FieldConda:      parseAndValidateConda,
	FieldPip:        parseAndValidatePip,
	FieldUv:         parseAndValidateUv,
	FieldEnvVars:    parseAndValidateEnvVars,
	FieldContainer:  parseAndValidateContainer,
	FieldConfig:     parseAndValidateRuntimeEnvConfigOption,
}

// OptionToNoPathValidationFn maps runtime env options to their path-free validation functions.
var OptionToNoPathValidationFn = map[string]OptionNoPathValidationFn{
	FieldWorkingDir: validateWorkingDirURI,
	FieldPyModules:  validatePyModulesURIs,
}
