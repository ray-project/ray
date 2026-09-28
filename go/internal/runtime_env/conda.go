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
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
	"gopkg.in/yaml.v3"
)

// _getRaySetupSpec locates the setup_spec of the currently running Ray.
// It also works when Ray was built from source and installed via pip install -e.
func _getRaySetupSpec() (map[string]interface{}, error) {
	rayPath, err := _resolveCurrentRayPath()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve current Ray path: %w", err)
	}

	// Build the full path to setup.py.
	setupPyPath := filepath.Join(rayPath, "setup.py")

	// Create a Python script that runs setup.py and prints the JSON representation of setup_spec.
	pythonScript := fmt.Sprintf(`
import runpy
import json
import sys

try:
    setup_py_path = r"%s"
    result = runpy.run_path(setup_py_path)
    setup_spec = result.get("setup_spec", {})
    print(json.dumps(setup_spec))
except Exception as e:
    print(json.dumps({"error": str(e)}), file=sys.stderr)
    sys.exit(1)
`, setupPyPath)

	// Run the Python script via the shared helper.
	output, err := runPythonScript(pythonScript)
	if err != nil {
		return nil, err
	}

	// Parse the JSON output.
	var setupSpec map[string]interface{}
	if err := json.Unmarshal([]byte(output), &setupSpec); err != nil {
		return nil, fmt.Errorf("failed to parse setup_spec JSON: %w", err)
	}

	// Check for an error message.
	if errMsg, ok := setupSpec["error"].(string); ok {
		return nil, fmt.Errorf("python script error: %s", errMsg)
	}

	return setupSpec, nil
}

// _resolveInstallFromSourceRayDependencies resolves the Ray dependencies used when Ray is installed from source.
func _resolveInstallFromSourceRayDependencies() ([]string, error) {
	setupSpec, err := _getRaySetupSpec()
	if err != nil {
		return nil, err
	}

	// Get install_requires and extras["default"].
	installRequires, ok := setupSpec["install_requires"].([]string)
	if !ok {
		installRequires = []string{}
	}

	extras, ok := setupSpec["extras"].(map[string]interface{})
	if !ok {
		extras = make(map[string]interface{})
	}

	defaultExtras, ok := extras["default"].([]string)
	if !ok {
		defaultExtras = []string{}
	}

	// Merge the dependencies and deduplicate them via the shared helper.
	deps := append(installRequires, defaultExtras...)
	return common.DeduplicateStrings(deps), nil
}

// _injectRayToCondaSite injects the current Ray site-packages directory into the new site.
func _injectRayToCondaSite(condaPath string) error {
	var pythonBinary string
	if runtime.GOOS == "windows" {
		// On Windows the Python executable usually lives under the Scripts directory.
		pythonBinary = filepath.Join(condaPath, "python")
	} else {
		pythonBinary = filepath.Join(condaPath, "bin", "python")
	}

	// Get the site-packages path.
	script := "import sysconfig; print(sysconfig.get_paths()['purelib'])"
	sitePackagesPath, err := runPythonScriptWithBinary(pythonBinary, script)
	if err != nil {
		return fmt.Errorf("failed to get site-packages path: %w", err)
	}

	rayPath, err := _resolveCurrentRayPath()
	if err != nil {
		return fmt.Errorf("failed to resolve current Ray path: %w", err)
	}

	log.Log.Info(fmt.Sprintf("Injecting %s to environment site-packages %s because _inject_current_ray flag is on.",
		rayPath, sitePackagesPath))

	maybeRayDir := filepath.Join(sitePackagesPath, "ray")
	if _, err := os.Stat(maybeRayDir); err == nil {
		log.Log.Info(fmt.Sprintf("Replacing existing ray installation with %s", rayPath))
		if err := os.RemoveAll(maybeRayDir); err != nil {
			return fmt.Errorf("failed to remove existing ray directory: %w", err)
		}
	}

	// Create the .pth file.
	pthFilePath := filepath.Join(sitePackagesPath, "ray_shared.pth")
	if err := os.WriteFile(pthFilePath, []byte(rayPath), 0644); err != nil {
		return fmt.Errorf("failed to write .pth file: %w", err)
	}

	return nil
}

// _currentPyVersion returns the current Python version.
func _currentPyVersion() (string, error) {
	script := `import sys; print(".".join(map(str, sys.version_info[:3])))`
	return runPythonScript(script)
}

// currentRayPipSpecifier returns the pip requirement specifier of the currently running Ray version.
// It returns an empty string if Ray was built locally from source.
func currentRayPipSpecifier() (string, error) {
	// Check whether we are running in Buildkite CI.
	if os.Getenv("RAY_CI_POST_WHEEL_TESTS") != "" {
		// In CI the wheel lives under the ray/.whl directory.
		rayFile, err := getRayFile()
		if err != nil {
			return "", fmt.Errorf("failed to get Ray file in CI: %w", err)
		}

		// Get the parent of the parent directory.
		rayDir := filepath.Dir(filepath.Dir(rayFile))
		wheelFilename := getWheelFilename()
		return filepath.Join(rayDir, ".whl", wheelFilename), nil
	}

	// Check whether this is a locally built version from source.
	rayCommit := getRayCommit()
	if rayCommit == "{{RAY_COMMIT_SHA}}" {
		if os.Getenv("RAY_RUNTIME_ENV_LOCAL_DEV_MODE") != "1" {
			log.Log.Info("Current Ray version could not be detected, most likely because you have manually built Ray from source. " +
				"To use runtime_env in this case, set the environment variable RAY_RUNTIME_ENV_LOCAL_DEV_MODE=1.")
		}
		return "", nil
	}

	// Check whether this is a nightly version.
	rayVersion := getRayVersion()
	if strings.Contains(rayVersion, "dev") {
		return getMasterWheelURL(), nil
	}

	// Stable release.
	return getReleaseWheelURL(), nil
}

// getRayCommit returns the Ray commit hash.
var getRayCommit = func() string {
	commit := os.Getenv("RAY_COMMIT")
	if commit != "" {
		return commit
	}
	return RayCommit // Use the global variable defined in types.go.
}

// TODO: to be replaced by querying the Python interpreter for the data.
// getRayVersion returns the Ray version.
var getRayVersion = func() string {
	return os.Getenv("RAY_VERSION")
}

// getWheelFilename returns the wheel file name.
var getWheelFilename = func() string {
	return os.Getenv("WHEEL_FILENAME")
}

// getMasterWheelURL returns the wheel URL for the master build.
var getMasterWheelURL = func() string {
	return os.Getenv("MASTER_WHEEL_URL")
}

// getReleaseWheelURL returns the wheel URL for the release build.
var getReleaseWheelURL = func() string {
	return os.Getenv("RELEASE_WHEEL_URL")
}

// injectDependencies adds Ray, Python, and (optionally) extra pip dependencies to the conda dict.
// Args:
//
//	condaDict: the JSON-serialized dict representing the conda env YAML file; it is modified and returned.
//	pyVersion: the Python version string to inject into the conda dependencies, e.g. "3.7.7".
//	pipDependencies: the pip dependencies to prepend to the pip list in the conda dict.
//	  If the conda dict has no "pip" field yet, one is created.
//
// Returns:
//
//	The modified dict (note: the input conda_dict is modified in place and returned).
func injectDependencies(condaDict map[string]interface{}, pyVersion string, pipDependencies []string) map[string]interface{} {
	if pipDependencies == nil {
		pipDependencies = []string{}
	}

	// Make sure the dependencies field exists.
	if depsInterface, ok := condaDict["dependencies"]; !ok || depsInterface == nil {
		condaDict["dependencies"] = []interface{}{}
	}

	deps, ok := condaDict["dependencies"].([]interface{})
	if !ok {
		// Re-initialize if the type is wrong.
		deps = []interface{}{}
		condaDict["dependencies"] = deps
	}

	// Inject the Python dependency.
	// If the user already included a Python version dependency, conda raises a readable error when the two conflict.
	deps = append(deps, fmt.Sprintf("python=%s", pyVersion))

	// Check whether a pip dependency already exists.
	hasPip := false
	for _, dep := range deps {
		if dep == "pip" {
			hasPip = true
			break
		}
	}
	if !hasPip {
		deps = append(deps, "pip")
	}

	// Insert the pip dependencies.
	foundPipDict := false
	for i, dep := range deps {
		if depMap, ok := dep.(map[string]interface{}); ok {
			if pipList, exists := depMap["pip"]; exists {
				if pipSlice, ok := pipList.([]interface{}); ok {
					// Prepend the new pip dependencies to the existing list.
					newPipList := make([]interface{}, 0, len(pipDependencies)+len(pipSlice))
					for _, pipDep := range pipDependencies {
						newPipList = append(newPipList, pipDep)
					}
					newPipList = append(newPipList, pipSlice...)
					depMap["pip"] = newPipList
					foundPipDict = true
					deps[i] = depMap
					break
				}
			}
		}
	}

	if !foundPipDict {
		// Create a new pip dict if no existing one was found.
		pipMap := map[string]interface{}{
			"pip": interfaceSlice(pipDependencies),
		}
		deps = append(deps, pipMap)
	}

	condaDict["dependencies"] = deps
	return condaDict
}

// interfaceSlice converts a string slice to an interface{} slice.
func interfaceSlice(strs []string) []interface{} {
	result := make([]interface{}, len(strs))
	for i, s := range strs {
		result[i] = s
	}
	return result
}

// _getCondaEnvHash computes the hash of a conda dict.
// The serialization format matches Python json.dumps(sort_keys=True) exactly to guarantee cross-language cache hits.
func _getCondaEnvHash(condaDict map[string]interface{}) (string, error) {
	serialized := _marshalJSONWithSortedKeys(condaDict)
	hasher := sha1.New()
	hasher.Write([]byte(serialized))
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// GetURI returns "conda://<hashed_dependencies>", or an empty string if there is nothing to garbage-collect.
func GetURI(runtimeEnv *RuntimeEnv) string {
	conda := (*runtimeEnv)[FieldConda]
	if conda == nil {
		return ""
	}

	switch v := conda.(type) {
	case string:
		// A user-preinstalled conda env is never garbage-collected, so no URI is tracked.
		return ""
	case map[string]interface{}:
		hash, err := _getCondaEnvHash(v)
		if err != nil {
			log.Log.V(1).Info("Failed to compute conda environment hash", "error", err)
			return ""
		}
		return fmt.Sprintf("conda://%s", hash)
	default:
		// Wrong type; return an empty string.
		log.Log.V(1).Info(fmt.Sprintf("conda field received by RuntimeEnvAgent must be str or dict, not %T", conda))
		return ""
	}
}

// _getCondaDictWithRayInserted returns the conda spec with Ray and python dependencies inserted.
func _getCondaDictWithRayInserted(runtimeEnv *RuntimeEnv) (map[string]interface{}, error) {
	// Get the conda config.
	condaConfigStr, err := runtimeEnv.CondaConfig()
	if err != nil {
		return nil, err
	}

	// Parse the JSON.
	var condaDict map[string]interface{}
	if err := json.Unmarshal([]byte(condaConfigStr), &condaDict); err != nil {
		return nil, fmt.Errorf("failed to parse conda config JSON: %w", err)
	}

	if condaDict == nil {
		return nil, fmt.Errorf("conda_dict is nil")
	}

	// Get the pip specifier of the current Ray.
	rayPip, err := currentRayPipSpecifier()
	if err != nil {
		log.Log.V(1).Info("Failed to get current Ray pip specifier", "error", err)
		rayPip = ""
	}

	var extraPipDependencies []string
	if rayPip != "" {
		extraPipDependencies = []string{rayPip, "ray[default]"}
	} else if injectCurrentRay, _ := runtimeEnv.GetExtension(FieldInjectCurrentRay); injectCurrentRay == true {
		// Dependencies used when installing from source.
		deps, err := _resolveInstallFromSourceRayDependencies()
		if err != nil {
			log.Log.V(1).Info("Failed to resolve install from source Ray dependencies", "error", err)
			extraPipDependencies = []string{}
		} else {
			extraPipDependencies = deps
		}
	} else {
		extraPipDependencies = []string{}
	}

	// Get the current Python version.
	pyVersion, err := _currentPyVersion()
	if err != nil {
		log.Log.V(1).Info("Failed to get current Python version", "error", err)
		return nil, err
	}

	// Inject the dependencies.
	condaDict = injectDependencies(condaDict, pyVersion, extraPipDependencies)
	return condaDict, nil
}

// CondaPlugin is the conda plugin implementing the RuntimeEnvPlugin interface.
type CondaPlugin struct {
	resourcesDir                 string
	installsAndDeletionsFileLock string
	validatedNamedCondaEnv       map[string]bool
	validatedNamedCondaEnvMutex  sync.RWMutex
}

// NewCondaPlugin creates a new CondaPlugin instance.
func NewCondaPlugin(resourcesDir string) (*CondaPlugin, error) {
	// Create the conda resources directory using the shared helper.
	condaDir, err := createResourcesSubdir(resourcesDir, "conda")
	if err != nil {
		return nil, fmt.Errorf("failed to create conda resources directory: %w", err)
	}

	plugin := &CondaPlugin{
		resourcesDir:           condaDir,
		validatedNamedCondaEnv: make(map[string]bool),
	}

	// Set up the lock file path.
	plugin.installsAndDeletionsFileLock = filepath.Join(plugin.resourcesDir, "ray-conda-installs-and-deletions.lock")

	return plugin, nil
}

// Name returns the plugin name.
func (p *CondaPlugin) Name() string {
	return "conda"
}

// Priority returns the plugin priority.
func (p *CondaPlugin) Priority() int {
	return RayRuntimeEnvPluginDefaultPriority
}

// Validate validates the user-provided runtime environment configuration.
func (p *CondaPlugin) Validate(runtimeEnv *RuntimeEnv) error {
	// The conda plugin needs no extra validation; validation already happened in parseAndValidateConda.
	return nil
}

// GetURIs returns the conda URI from the RuntimeEnv if present, or an empty list otherwise.
func (p *CondaPlugin) GetURIs(runtimeEnv *RuntimeEnv) []string {
	condaURI, err := runtimeEnv.CondaURI()
	if err != nil || condaURI == "" {
		return []string{}
	}
	return []string{condaURI}
}

// DeleteURI deletes the URI and returns the number of bytes deleted.
func (p *CondaPlugin) DeleteURI(uri string) int64 {
	log.Log.Info(fmt.Sprintf("Got request to delete URI %s", uri))

	protocol, hash, _ := ParseURI(uri)

	if protocol != ProtocolConda {
		log.Log.Error(fmt.Errorf("CondaPlugin can only delete URIs with protocol conda. Received protocol %s, URI %s", protocol, uri), "")
		return 0
	}

	condaEnvPath := filepath.Join(p.resourcesDir, fmt.Sprintf("ray-%s", hash))
	localDirSize, err := common.DirSizeBytes(condaEnvPath)
	if err != nil {
		log.Log.Error(err, fmt.Sprintf("Failed to get directory size for %s", condaEnvPath))
		return 0
	}

	// Use a file lock for concurrency safety.
	var fileLock sync.Mutex
	fileLock.Lock()
	defer fileLock.Unlock()

	successful, err := deleteCondaEnv(context.Background(), condaEnvPath, log.Log)
	if err != nil || !successful {
		log.Log.Error(nil, fmt.Sprintf("Error when deleting conda env %s", condaEnvPath))
		return 0
	}

	return localDirSize
}

// Create creates the conda env.
// It returns the disk space (bytes) occupied by the plugin installation and a possible error.
func (p *CondaPlugin) Create(ctx context.Context, uri string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) (int64, error) {
	if !runtimeEnv.HasConda() {
		return 0, nil
	}

	// Run the creation in a separate goroutine to avoid blocking the event loop.
	resultChan := make(chan createResult, 1)
	go func() {
		result := p.createInternal(ctx, uri, runtimeEnv, context)
		resultChan <- result
	}()

	result := <-resultChan
	return result.size, result.err
}

type createResult struct {
	size int64
	err  error
}

func (p *CondaPlugin) createInternal(ctx context.Context, uri string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) createResult {
	// Parse and validate the conda config.
	condaValue := (*runtimeEnv)[FieldConda]

	// A string value means the given conda env name.
	if condaName, ok := condaValue.(string); ok {
		// Check whether the env has already been validated.
		p.validatedNamedCondaEnvMutex.RLock()
		if p.validatedNamedCondaEnv[condaName] {
			p.validatedNamedCondaEnvMutex.RUnlock()
			return createResult{size: 0, err: nil}
		}
		p.validatedNamedCondaEnvMutex.RUnlock()

		// Get the conda info.
		condaInfo, err := getCondaInfoJSON(ctx)
		if err != nil {
			return createResult{size: 0, err: fmt.Errorf("failed to get conda info: %w", err)}
		}

		envs := getCondaEnvs(condaInfo)

		// Accept condaName as either an env name or a full path.
		found := false
		for _, env := range envs {
			if condaName == env[0] || condaName == env[1] {
				found = true
				break
			}
		}

		if !found {
			return createResult{size: 0, err: fmt.Errorf("The given conda environment '%s' from the runtime env doesn't exist from the output of `conda info --json`. "+
				"You can only specify an env that already exists. Please make sure to create an env %s", condaName, condaName)}
		}

		// Mark the env as validated.
		p.validatedNamedCondaEnvMutex.Lock()
		p.validatedNamedCondaEnv[condaName] = true
		p.validatedNamedCondaEnvMutex.Unlock()

		return createResult{size: 0, err: nil}
	}

	// A map value means a new conda env must be created.
	log.Log.Info("Setting up conda for runtime_env")

	protocol, hash, _ := ParseURI(uri)

	if protocol != ProtocolConda {
		return createResult{size: 0, err: fmt.Errorf("expected conda protocol, got %s", protocol)}
	}

	condaEnvName := filepath.Join(p.resourcesDir, fmt.Sprintf("ray-%s", hash))

	// Get the conda dict with Ray inserted.
	condaDict, err := _getCondaDictWithRayInserted(runtimeEnv)
	if err != nil {
		return createResult{size: 0, err: fmt.Errorf("failed to get conda dict with Ray inserted: %w", err)}
	}

	log.Log.Info(fmt.Sprintf("Setting up conda environment with %v", runtimeEnv))

	// Use a file lock for concurrency safety.
	var fileLock sync.Mutex
	fileLock.Lock()
	defer fileLock.Unlock()

	// Create a temporary conda YAML file.
	condaYamlFile := filepath.Join(p.resourcesDir, "environment.yml")

	// Write the conda dict to the YAML file.
	yamlData, err := yaml.Marshal(condaDict)
	if err != nil {
		return createResult{size: 0, err: fmt.Errorf("failed to marshal conda dict to YAML: %w", err)}
	}

	if err := os.WriteFile(condaYamlFile, yamlData, 0644); err != nil {
		return createResult{size: 0, err: fmt.Errorf("failed to write conda YAML file: %w", err)}
	}

	// Make sure the temporary file is removed when the function returns.
	defer os.Remove(condaYamlFile)

	// Create the conda env.
	if err := createCondaEnvIfNeeded(ctx, condaYamlFile, condaEnvName, log.Log); err != nil {
		return createResult{size: 0, err: fmt.Errorf("failed to create conda environment: %w", err)}
	}

	// Inject the current Ray if requested.
	if injectCurrentRay, _ := runtimeEnv.GetExtension(FieldInjectCurrentRay); injectCurrentRay == true {
		if err := _injectRayToCondaSite(condaEnvName); err != nil {
			return createResult{size: 0, err: fmt.Errorf("failed to inject Ray to conda site: %w", err)}
		}
	}

	log.Log.Info(fmt.Sprintf("Finished creating conda environment at %s", condaEnvName))

	// Return the size of the env directory.
	size, err := common.DirSizeBytes(condaEnvName)
	if err != nil {
		log.Log.Error(err, fmt.Sprintf("Failed to get directory size for %s", condaEnvName))
		return createResult{size: 0, err: err}
	}
	return createResult{size: size, err: nil}
}

// ModifyContext modifies the context to change worker startup behavior.
func (p *CondaPlugin) ModifyContext(uris []string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) error {
	if !runtimeEnv.HasConda() {
		return nil
	}

	var condaEnvName string

	// Prefer the conda env name when available.
	if name, err := runtimeEnv.CondaEnvName(); err == nil && name != "" {
		condaEnvName = name
	} else {
		// Otherwise fall back to the URI.
		condaURIStr, err := runtimeEnv.CondaURI()
		if err != nil {
			log.Log.V(1).Info("No conda URI found, skipping context modification")
			return nil
		}
		protocol, hash, _ := ParseURI(condaURIStr)
		if protocol != ProtocolConda {
			return fmt.Errorf("expected conda protocol, got %s", protocol)
		}
		condaEnvName = filepath.Join(p.resourcesDir, fmt.Sprintf("ray-%s", hash))
	}

	// Set the Python executable.
	context.PyExecutable = "python"

	// Append the conda activation commands to the command prefix.
	context.CommandPrefix = append(context.CommandPrefix, getCondaActivateCommands(condaEnvName)...)

	return nil
}
