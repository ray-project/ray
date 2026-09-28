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
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
)

// _getUvHash computes a deterministic hash for uv-related runtime envs.
func _getUvHash(uvDict map[string]interface{}) (string, error) {
	// Serialize to JSON to guarantee consistent, reproducible output.
	serialized, err := json.Marshal(uvDict)
	if err != nil {
		return "", fmt.Errorf("failed to marshal uv dict: %w", err)
	}
	hasher := sha1.New()
	hasher.Write(serialized)
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// GetUvURI returns the uv URI from the RuntimeEnv.
// It returns "uv://<hashed_dependencies>", or an empty string when there is
// nothing for the garbage collector to track.
func GetUvURI(runtimeEnv *RuntimeEnv) (string, error) {
	uvVal, ok := (*runtimeEnv)[FieldUv]
	if !ok || uvVal == nil {
		return "", nil
	}

	var hashVal string
	var err error
	switch v := uvVal.(type) {
	case map[string]interface{}:
		hashVal, err = _getUvHash(v)
		if err != nil {
			return "", err
		}
	case []interface{}:
		uvDict := map[string]interface{}{
			"packages": v,
		}
		hashVal, err = _getUvHash(uvDict)
		if err != nil {
			return "", err
		}
	default:
		log.Log.V(1).Info("uv field received by RuntimeEnvAgent must be list or dict", "type", fmt.Sprintf("%T", uvVal))
		return "", nil
	}

	if hashVal == "" {
		return "", nil
	}
	return "uv://" + hashVal, nil
}

// UvProcessor is the uv environment manager.
type UvProcessor struct {
	targetDir  string
	runtimeEnv *RuntimeEnv
	uvConfig   map[string]interface{}
	uvEnv      map[string]string
	execCwd    string
}

// NewUvProcessor creates a new UvProcessor instance.
func NewUvProcessor(targetDir string, runtimeEnv *RuntimeEnv) (*UvProcessor, error) {
	// Check that virtualenv is available (by attempting to run it).
	pythonPath, pythonErr := getPythonExecutable()
	if pythonErr != nil {
		return nil, fmt.Errorf("please install virtualenv `%s -m pip install virtualenv` to enable uv runtime env: %w", pythonPath, pythonErr)
	}
	cmd := exec.Command(pythonPath, "-m", "virtualenv", "--version")
	if cmdErr := cmd.Run(); cmdErr != nil {
		return nil, fmt.Errorf("please install virtualenv `%s -m pip install virtualenv` to enable uv runtime env: %w", pythonPath, cmdErr)
	}

	uvConfig, err := runtimeEnv.UvConfig()
	if err != nil {
		return nil, err
	}

	if err := validatePackagesConfig(uvConfig, "uv"); err != nil {
		return nil, err
	}

	// Copy the environment variables and add the runtime_env ones.
	uvEnv := make(map[string]string)
	for _, e := range os.Environ() {
		if key, value, ok := strings.Cut(e, "="); ok {
			uvEnv[key] = value
		}
	}
	for k, v := range runtimeEnv.EnvVars() {
		uvEnv[k] = v
	}

	execCwd := filepath.Join(targetDir, "exec_cwd")

	return &UvProcessor{
		targetDir:  targetDir,
		runtimeEnv: runtimeEnv,
		uvConfig:   uvConfig,
		uvEnv:      uvEnv,
		execCwd:    execCwd,
	}, nil
}

// installUv makes sure the requested uv version (if any) is installed before
// installing packages.
func (p *UvProcessor) installUv(ctx context.Context, path string, cwd string, pipEnv map[string]string) error {
	virtualenvPath := getVirtualenvPath(path)
	python := getVirtualenvPython(path)

	// Determine which uv executable (with version) to install.
	getUvExecToInstall := func() string {
		uvVersionRaw, ok := p.uvConfig["uv_version"]
		if ok && uvVersionRaw != nil {
			if uvVersionStr, ok := uvVersionRaw.(string); ok && uvVersionStr != "" {
				return "uv" + uvVersionStr
			}
		}
		// Fall back to the default version.
		return "uv"
	}

	uvInstallCmd := []string{
		python,
		"-m",
		"pip",
		"install",
		"--disable-pip-version-check",
		"--no-cache-dir",
		getUvExecToInstall(),
	}

	log.Log.Info("Installing package uv to virtualenv_path", "virtualenv_path", virtualenvPath)

	cmdIndexGen := newCmdIndexGen()
	_, err := checkOutputCmd(ctx, uvInstallCmd, cwd, pipEnv, cmdIndexGen)
	return err
}

// checkUvExistence checks whether uv is present in the virtualenv.
func (p *UvProcessor) checkUvExistence(ctx context.Context, path string, cwd string, env map[string]string) (bool, error) {
	python := getVirtualenvPython(path)

	checkExistenceCmd := []string{
		python,
		"-m",
		"uv",
		"version",
	}

	cmdIndexGen := newCmdIndexGen()
	_, err := checkOutputCmd(ctx, checkExistenceCmd, cwd, env, cmdIndexGen)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// uvCheck checks virtualenv dependency compatibility.
// It returns an error if any incompatibility is detected.
func (p *UvProcessor) uvCheck(ctx context.Context, python string, cwd string) error {
	cmd := []string{python, "-m", "uv", "pip", "check"}

	cmdIndexGen := newCmdIndexGen()
	_, err := checkOutputCmd(ctx, cmd, cwd, nil, cmdIndexGen)
	return err
}

// installUvPackages installs the required Python packages via uv.
func (p *UvProcessor) installUvPackages(ctx context.Context, path string, uvPackages []string, cwd string, pipEnv map[string]string) error {
	virtualenvPath := getVirtualenvPath(path)
	python := getVirtualenvPython(path)

	// Build the requirements file path.
	requirementsFile, err := getRequirementsFile(path, uvPackages)
	if err != nil {
		return err
	}

	// Generate the requirements file asynchronously to avoid blocking this
	// goroutine (mirroring the Python implementation).
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("panic in genRequirementsTxt: %v", r)
			}
		}()
		err := genRequirementsTxt(requirementsFile, uvPackages)
		done <- err
	}()

	// Wait for the asynchronous operation to finish.
	if err := <-done; err != nil {
		return err
	}
	// Check for uv to see whether installing it can be skipped.
	uvExists, err := p.checkUvExistence(ctx, path, cwd, pipEnv)
	if err != nil {
		return err
	}

	// Install uv as the default package manager.
	_, hasUvVersion := p.uvConfig["uv_version"]
	if (!uvExists) || hasUvVersion {
		if err := p.installUv(ctx, path, cwd, pipEnv); err != nil {
			return err
		}
	}

	// Install all dependencies.
	// Difference from pip:
	// 1. `--disable-pip-version-check` has no effect on uv.
	uvInstallCmd := []string{
		python,
		"-m",
		"uv",
		"pip",
		"install",
		"-r",
		requirementsFile,
	}

	// Collect the uv pip install options.
	uvOptListRaw, ok := p.uvConfig["uv_pip_install_options"]
	var uvOptList []string
	if ok && uvOptListRaw != nil {
		if opts, ok := uvOptListRaw.([]interface{}); ok {
			for _, opt := range opts {
				if optStr, ok := opt.(string); ok {
					uvOptList = append(uvOptList, optStr)
				}
			}
		}
	} else {
		// Default options.
		uvOptList = []string{"--no-cache"}
	}

	if len(uvOptList) > 0 {
		uvInstallCmd = append(uvInstallCmd, uvOptList...)
	}

	log.Log.Info("Installing python requirements to %s", virtualenvPath)

	cmdIndexGen := newCmdIndexGen()
	_, err = checkOutputCmd(ctx, uvInstallCmd, cwd, pipEnv, cmdIndexGen)
	if err != nil {
		return err
	}

	// Check the Python environment for conflicts.
	uvCheckRaw, ok := p.uvConfig["uv_check"]
	uvCheck := false
	if uc, ok := uvCheckRaw.(bool); ok {
		uvCheck = uc
	}
	if uvCheck {
		if err := p.uvCheck(ctx, python, cwd); err != nil {
			return err
		}
	}

	return nil
}

// Run executes the uv installation flow.
func (p *UvProcessor) Run(ctx context.Context) error {
	path := p.targetDir

	uvPackagesRaw, ok := p.uvConfig["packages"]
	if !ok {
		return fmt.Errorf("uv config must contain 'packages' key")
	}

	var uvPackages []string
	switch v := uvPackagesRaw.(type) {
	case []string:
		uvPackages = v
	case []interface{}:
		for _, pkg := range v {
			if pkgStr, ok := pkg.(string); ok {
				uvPackages = append(uvPackages, pkgStr)
			} else {
				return fmt.Errorf("packages must be a list of strings")
			}
		}
	default:
		return fmt.Errorf("packages must be a list of strings")
	}

	// Create an empty directory to run commands in; this makes the commands
	// more stable. For example, if the cwd contains ray, a check for ray
	// would resolve it from the cwd instead of site-packages.
	if err := os.MkdirAll(p.execCwd, 0755); err != nil {
		return fmt.Errorf("failed to create exec_cwd: %w", err)
	}

	// Wrap all of the logic so cleanup happens uniformly on failure.
	err := p.runWithCleanup(ctx, path, uvPackages, p.execCwd)
	if err != nil {
		log.Log.Info("Delete incomplete virtualenv", "path", path)
		os.RemoveAll(path)
		log.Log.Error(err, "Failed to install uv packages")
	}
	return err
}

// runWithCleanup implements the core logic of the uv installation flow.
// The caller is responsible for cleanup on failure.
func (p *UvProcessor) runWithCleanup(ctx context.Context, path string, uvPackages []string, execCwd string) error {
	// Create or get the virtualenv.
	if err := createOrGetVirtualenv(ctx, path, execCwd); err != nil {
		return err
	}

	python := getVirtualenvPython(path)

	// Check whether ray gets overridden.
	if err := checkRay(ctx, python, execCwd); err != nil {
		return err
	}

	// Install the uv packages.
	if err := p.installUvPackages(ctx, path, uvPackages, execCwd, p.uvEnv); err != nil {
		return err
	}

	return nil
}

// UvPlugin is the uv plugin.
// It implements the RuntimeEnvPlugin interface.
type UvPlugin struct {
	resourcesDir       string
	creatingTask       map[string]context.CancelFunc
	createLocks        map[string]*sync.Mutex
	createdHashBytes   map[string]int64
	createLocksMu      sync.RWMutex
	creatingTaskMu     sync.RWMutex
	createdHashBytesMu sync.RWMutex
}

// Name returns the plugin name.
func (p *UvPlugin) Name() string {
	return "uv"
}

// Priority returns the plugin priority.
func (p *UvPlugin) Priority() int {
	return RayRuntimeEnvPluginDefaultPriority
}

// Validate validates the runtime env config.
func (p *UvPlugin) Validate(runtimeEnv *RuntimeEnv) error {
	// The uv plugin requires no extra validation.
	return nil
}

// NewUvPlugin creates a new UvPlugin instance.
func NewUvPlugin(resourcesDir string) (*UvPlugin, error) {
	uvResourcesDir, err := createResourcesSubdir(resourcesDir, "uv")
	if err != nil {
		return nil, fmt.Errorf("failed to create uv resources directory: %w", err)
	}

	return &UvPlugin{
		resourcesDir:     uvResourcesDir,
		creatingTask:     make(map[string]context.CancelFunc),
		createLocks:      make(map[string]*sync.Mutex),
		createdHashBytes: make(map[string]int64),
	}, nil
}

// getPathFromHash derives the path from the uv spec hash.
func (p *UvPlugin) getPathFromHash(hashVal string) string {
	return filepath.Join(p.resourcesDir, hashVal)
}

// GetURIs returns the uv URI from the RuntimeEnv if present, or an empty list.
func (p *UvPlugin) GetURIs(runtimeEnv *RuntimeEnv) []string {
	uvURI, err := GetUvURI(runtimeEnv)
	if err != nil {
		log.Log.Error(err, "failed to get uv URI")
		return []string{}
	}
	if uvURI == "" {
		return []string{}
	}
	return []string{uvURI}
}

// DeleteURI deletes the URI and returns the number of bytes deleted.
func (p *UvPlugin) DeleteURI(uri string) int64 {
	log.Log.Info("Got request to delete uv URI", "uri", uri)

	protocol, hashVal, err := ParseURI(uri)
	if err != nil {
		log.Log.Error(err, "Failed to parse URI", "uri", uri)
		return 0
	}
	if protocol != ProtocolUv {
		log.Log.Error(fmt.Errorf("invalid protocol"), "UvPlugin can only delete URIs with protocol uv",
			"protocol", protocol, "uri", uri)
		return 0
	}

	// Cancel any in-flight creation task.
	p.creatingTaskMu.Lock()
	task, exists := p.creatingTask[hashVal]
	if exists {
		task()
		delete(p.creatingTask, hashVal)
	}
	p.creatingTaskMu.Unlock()

	p.createdHashBytesMu.Lock()
	delete(p.createdHashBytes, hashVal)
	p.createdHashBytesMu.Unlock()

	uvEnvPath := p.getPathFromHash(hashVal)
	localDirSize, _ := common.DirSizeBytes(uvEnvPath)

	p.createLocksMu.Lock()
	delete(p.createLocks, uri)
	p.createLocksMu.Unlock()

	if err := os.RemoveAll(uvEnvPath); err != nil {
		log.Log.Error(err, "Error when deleting uv env", "uv_env_path", uvEnvPath)
		return 0
	}

	return localDirSize
}

// Create creates the uv environment.
func (p *UvPlugin) Create(ctx context.Context, uri string, runtimeEnv *RuntimeEnv, runtimeEnvContext *RuntimeEnvContext) (int64, error) {
	if !runtimeEnv.HasUv() {
		return 0, nil
	}

	protocol, hashVal, err := ParseURI(uri)
	if err != nil {
		return 0, err
	}
	if protocol != ProtocolUv {
		return 0, fmt.Errorf("expected uv protocol, got %s", protocol)
	}

	targetDir := p.getPathFromHash(hashVal)

	createForHash := func() (int64, error) {
		processor, err := NewUvProcessor(targetDir, runtimeEnv)
		if err != nil {
			return 0, err
		}

		if err := processor.Run(ctx); err != nil {
			return 0, err
		}

		size, err := common.DirSizeBytes(targetDir)
		if err != nil {
			return 0, err
		}
		return size, nil
	}

	// Get or create the per-URI lock.
	p.createLocksMu.Lock()
	lock, exists := p.createLocks[uri]
	if !exists {
		lock = &sync.Mutex{}
		p.createLocks[uri] = lock
	}
	p.createLocksMu.Unlock()

	lock.Lock()
	defer lock.Unlock()

	// Check whether it has already been created.
	p.createdHashBytesMu.RLock()
	sizeBytes, exists := p.createdHashBytes[hashVal]
	p.createdHashBytesMu.RUnlock()
	if exists {
		return sizeBytes, nil
	}

	// Create a context used for cancellation.
	createCtx, cancelFunc := context.WithCancel(context.Background())
	p.creatingTaskMu.Lock()
	p.creatingTask[hashVal] = cancelFunc
	p.creatingTaskMu.Unlock()

	// Use createCtx in place of ctx (note: the parameter ctx cannot be
	// reassigned here because Go passes arguments by value).
	// The closure must reference createCtx instead.
	_ = createCtx // avoid an unused-variable error

	defer func() {
		p.creatingTaskMu.Lock()
		delete(p.creatingTask, hashVal)
		p.creatingTaskMu.Unlock()
	}()

	sizeBytes, err = createForHash()
	if err != nil {
		return 0, err
	}

	p.createdHashBytesMu.Lock()
	p.createdHashBytes[hashVal] = sizeBytes
	p.createdHashBytesMu.Unlock()

	return sizeBytes, nil
}

// ModifyContext updates the context to set py_executable and the command prefix.
func (p *UvPlugin) ModifyContext(uris []string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) error {
	if !runtimeEnv.HasUv() {
		return nil
	}

	// UvPlugin uses a single URI only.
	if len(uris) == 0 {
		return fmt.Errorf("no URIs provided for uv plugin")
	}
	uri := uris[0]

	// Update py_executable.
	_, hashVal, err := ParseURI(uri)
	if err != nil {
		return err
	}

	targetDir := p.getPathFromHash(hashVal)
	virtualenvPython := getVirtualenvPython(targetDir)

	if _, err := os.Stat(virtualenvPython); os.IsNotExist(err) {
		return fmt.Errorf("local directory %s for URI %s does not exist on the cluster. Something may have gone wrong while installing the runtime_env `uv` packages", targetDir, uri)
	}

	context.PyExecutable = virtualenvPython
	context.CommandPrefix = append(context.CommandPrefix, getVirtualenvActivateCommand(targetDir)...)

	return nil
}
