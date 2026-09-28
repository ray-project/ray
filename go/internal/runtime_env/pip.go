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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
)

// _getPipHash computes the hash of a pip config dict.
// The serialization exactly matches Python's json.dumps(sort_keys=True) to
// guarantee cross-language cache hits.
func _getPipHash(pipDict map[string]interface{}) (string, error) {
	serialized := _marshalJSONWithSortedKeys(pipDict)
	hasher := sha1.New()
	hasher.Write([]byte(serialized))
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// GetPipURI returns the pip URI from the RuntimeEnv.
// It returns "pip://<hashed_dependencies>" and any error encountered.
func GetPipURI(runtimeEnv *RuntimeEnv) (string, error) {
	pipVal, ok := (*runtimeEnv)[FieldPip]
	if !ok || pipVal == nil {
		return "", fmt.Errorf("pip field not set")
	}

	var hashVal string
	var err error
	switch v := pipVal.(type) {
	case map[string]interface{}:
		hashVal, err = _getPipHash(v)
		if err != nil {
			return "", err
		}
	case []interface{}:
		pipDict := map[string]interface{}{
			"packages": v,
		}
		hashVal, err = _getPipHash(pipDict)
		if err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("pip field received by RuntimeEnvAgent must be list or dict, not %T", pipVal)
	}

	return "pip://" + hashVal, nil
}

// PipProcessor is the pip environment manager.
type PipProcessor struct {
	targetDir  string
	runtimeEnv *RuntimeEnv
	pipConfig  map[string]interface{}
	pipEnv     map[string]string
}

// NewPipProcessor creates a new PipProcessor instance.
func NewPipProcessor(targetDir string, runtimeEnv *RuntimeEnv) (*PipProcessor, error) {
	// Check that virtualenv is available (by attempting to run it).
	pythonPath, pythonErr := getPythonExecutable()
	if pythonErr != nil {
		return nil, fmt.Errorf("please install virtualenv `python -m pip install virtualenv` to enable pip runtime env: %w", pythonErr)
	}
	cmd := exec.Command(pythonPath, "-m", "virtualenv", "--version")
	if cmdErr := cmd.Run(); cmdErr != nil {
		return nil, fmt.Errorf("please install virtualenv `%s -m pip install virtualenv` to enable pip runtime env: %w", pythonPath, cmdErr)
	}

	pipConfig, err := runtimeEnv.PipConfig()
	if err != nil {
		return nil, err
	}

	if err := validatePackagesConfig(pipConfig, "pip"); err != nil {
		return nil, err
	}

	// Copy the environment variables and add the runtime_env ones.
	pipEnv := make(map[string]string)
	for _, e := range os.Environ() {
		if key, value, ok := strings.Cut(e, "="); ok {
			pipEnv[key] = value
		}
	}
	for k, v := range runtimeEnv.EnvVars() {
		pipEnv[k] = v
	}

	return &PipProcessor{
		targetDir:  targetDir,
		runtimeEnv: runtimeEnv,
		pipConfig:  pipConfig,
		pipEnv:     pipEnv,
	}, nil
}

// ensurePipVersion runs a pip command to reinstall pip at the requested version.
func (p *PipProcessor) ensurePipVersion(ctx context.Context, path string, pipVersion *string, cwd string, pipEnv map[string]string) error {
	if pipVersion == nil || *pipVersion == "" {
		return nil
	}

	python := getVirtualenvPython(path)
	// Ensure the pip version.
	pipReinstallCmd := []string{
		python,
		"-m",
		"pip",
		"install",
		"--disable-pip-version-check",
		"pip" + *pipVersion,
	}
	log.Log.V(1).Info("Installing pip with version", "version", *pipVersion)

	cmdIndexGen := newCmdIndexGen()
	_, err := checkOutputCmd(ctx, pipReinstallCmd, cwd, pipEnv, cmdIndexGen)
	return err
}

// pipCheck runs the pip check command to detect Python dependency conflicts.
func (p *PipProcessor) pipCheck(ctx context.Context, path string, pipCheck bool, cwd string, pipEnv map[string]string) error {
	if !pipCheck {
		log.Log.V(1).Info("Skip pip check.")
		return nil
	}

	python := getVirtualenvPython(path)
	cmd := []string{python, "-m", "pip", "check", "--disable-pip-version-check"}

	cmdIndexGen := newCmdIndexGen()
	_, err := checkOutputCmd(ctx, cmd, cwd, pipEnv, cmdIndexGen)
	if err != nil {
		return err
	}

	log.Log.V(1).Info("Pip check on path successfully.", "path", path)
	return nil
}

// installPipPackages installs the pip packages.
func (p *PipProcessor) installPipPackages(ctx context.Context, path string, pipPackages []string, cwd string, pipEnv map[string]string) error {
	virtualenvPath := getVirtualenvPath(path)
	python := getVirtualenvPython(path)

	// Build the requirements file path.
	pipRequirementsFile, err := getRequirementsFile(path, pipPackages)
	if err != nil {
		return err
	}

	// Write the requirements file.
	if err := genRequirementsTxt(pipRequirementsFile, pipPackages); err != nil {
		return err
	}

	// Install all dependencies.
	// Default options:
	// --disable-pip-version-check
	//   Do not periodically check PyPI to determine whether a newer version of
	//   pip is available for download.
	//
	// --no-cache-dir
	//   Disable the cache. A pip runtime env is a one-shot installation, so we
	//   do not need to deal with corrupted pip caches.
	//
	// Users may supply their own options via `pip_install_options` to install packages.
	pipInstallCmd := []string{
		python,
		"-m",
		"pip",
		"install",
		"-r",
		pipRequirementsFile,
	}

	// Collect the pip install options.
	pipOptList, ok := p.pipConfig["pip_install_options"].([]string)
	if !ok {
		pipOptList = []string{"--disable-pip-version-check", "--no-cache-dir"}
	}
	for _, opt := range pipOptList {
		pipInstallCmd = append(pipInstallCmd, opt)
	}

	log.Log.V(1).Info("Installing python requirements to virtualenv_path", "virtualenv_path", virtualenvPath)

	cmdIndexGen := newCmdIndexGen()
	_, err = checkOutputCmd(ctx, pipInstallCmd, cwd, pipEnv, cmdIndexGen)
	return err
}

// Run executes the pip installation flow.
func (p *PipProcessor) Run(ctx context.Context) error {
	path := p.targetDir
	pipPackagesRaw, ok := p.pipConfig["packages"]
	if !ok {
		return fmt.Errorf("pip config must contain 'packages' key")
	}

	var pipPackages []string
	switch v := pipPackagesRaw.(type) {
	case []string:
		pipPackages = v
	default:
		return fmt.Errorf("packages must be a list of strings")
	}

	// Create an empty directory to run commands in; this makes the commands
	// more stable. For example, if the cwd contains ray, a pip check for ray
	// would resolve it from the cwd instead of site-packages.
	execCwd := filepath.Join(path, "exec_cwd")
	if err := os.MkdirAll(execCwd, 0755); err != nil {
		return fmt.Errorf("failed to create exec_cwd: %w", err)
	}

	// Wrap all of the logic so cleanup happens uniformly on failure.
	err := p.runWithCleanup(ctx, path, pipPackages, execCwd)
	if err != nil {
		log.Log.V(1).Info("Delete incomplete virtualenv", "path", path)
		os.RemoveAll(path)
		log.Log.Error(err, "Failed to install pip packages")
	}
	return err
}

// runWithCleanup implements the core logic of the pip installation flow.
// The caller is responsible for cleanup on failure.
func (p *PipProcessor) runWithCleanup(ctx context.Context, path string, pipPackages []string, execCwd string) error {
	// Create or get the virtualenv.
	if err := createOrGetVirtualenv(ctx, path, execCwd); err != nil {
		return err
	}

	python := getVirtualenvPython(path)

	// Check whether ray gets overridden.
	if err := checkRay(ctx, python, execCwd); err != nil {
		return err
	}

	// Ensure the pip version.
	pipVersionRaw, _ := p.pipConfig["pip_version"]
	var pipVersion *string
	if pipVersionStr, ok := pipVersionRaw.(string); ok && pipVersionStr != "" {
		pipVersion = &pipVersionStr
	}
	if err := p.ensurePipVersion(ctx, path, pipVersion, execCwd, p.pipEnv); err != nil {
		return err
	}

	// Install the pip packages.
	if err := p.installPipPackages(ctx, path, pipPackages, execCwd, p.pipEnv); err != nil {
		return err
	}

	// Check the Python environment for conflicts.
	pipCheckRaw, _ := p.pipConfig["pip_check"]
	pipCheck := false
	if pc, ok := pipCheckRaw.(bool); ok {
		pipCheck = pc
	}
	if err := p.pipCheck(ctx, path, pipCheck, execCwd, p.pipEnv); err != nil {
		return err
	}

	return nil
}

// PipPlugin is the pip plugin.
// It implements the RuntimeEnvPlugin interface.
type PipPlugin struct {
	resourcesDir       string
	creatingTask       map[string]context.CancelFunc
	createLocks        map[string]*sync.Mutex
	createdHashBytes   map[string]int64
	createLocksMu      sync.RWMutex
	creatingTaskMu     sync.RWMutex
	createdHashBytesMu sync.RWMutex
}

// Name returns the plugin name.
func (p *PipPlugin) Name() string {
	return "pip"
}

// Priority returns the plugin priority.
func (p *PipPlugin) Priority() int {
	return 10 // default priority
}

// Validate validates the runtime env config.
func (p *PipPlugin) Validate(runtimeEnv *RuntimeEnv) error {
	// The pip plugin requires no extra validation.
	return nil
}

// NewPipPlugin creates a new PipPlugin instance.
func NewPipPlugin(resourcesDir string) (*PipPlugin, error) {
	pipResourcesDir, err := createResourcesSubdir(resourcesDir, "pip")
	if err != nil {
		return nil, fmt.Errorf("failed to create pip resources directory: %w", err)
	}

	return &PipPlugin{
		resourcesDir:     pipResourcesDir,
		creatingTask:     make(map[string]context.CancelFunc),
		createLocks:      make(map[string]*sync.Mutex),
		createdHashBytes: make(map[string]int64),
	}, nil
}

// getPathFromHash derives the path from the pip spec hash.
func (p *PipPlugin) getPathFromHash(hashVal string) string {
	return filepath.Join(p.resourcesDir, hashVal)
}

// GetURIs returns the pip URI from the RuntimeEnv if present, or an empty list.
func (p *PipPlugin) GetURIs(runtimeEnv *RuntimeEnv) []string {
	pipURI, err := runtimeEnv.PipURI()
	if err != nil || pipURI == "" {
		return []string{}
	}
	return []string{pipURI}
}

// DeleteURI deletes the URI and returns the number of bytes deleted.
func (p *PipPlugin) DeleteURI(uri string) int64 {
	log.Log.V(1).Info("Got request to delete pip URI", "uri", uri)
	protocol, hashVal, err := ParseURI(uri)
	if err != nil {
		log.Log.Error(err, "Failed to parse URI", "uri", uri)
		return 0
	}
	if protocol != ProtocolPip {
		log.Log.Error(fmt.Errorf("invalid protocol"), "PipPlugin can only delete URIs with protocol pip",
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

	p.createLocksMu.Lock()
	delete(p.createLocks, uri)
	p.createLocksMu.Unlock()

	pipEnvPath := p.getPathFromHash(hashVal)
	localDirSize, _ := common.DirSizeBytes(pipEnvPath)

	if err := os.RemoveAll(pipEnvPath); err != nil {
		log.Log.Error(err, "Error when deleting pip env", "pip_env_path", pipEnvPath)
		return 0
	}

	return localDirSize
}

// Create creates the pip environment.
func (p *PipPlugin) Create(ctx context.Context, uri string, runtimeEnv *RuntimeEnv, runtimeEnvContext *RuntimeEnvContext) (int64, error) {
	if !runtimeEnv.HasPip() {
		return 0, nil
	}

	protocol, hashVal, err := ParseURI(uri)
	if err != nil {
		return 0, err
	}
	if protocol != ProtocolPip {
		return 0, fmt.Errorf("expected pip protocol, got %s", protocol)
	}

	targetDir := p.getPathFromHash(hashVal)

	createForHash := func() (int64, error) {
		processor, err := NewPipProcessor(targetDir, runtimeEnv)
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
func (p *PipPlugin) ModifyContext(uris []string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) error {
	if !runtimeEnv.HasPip() {
		return nil
	}

	// PipPlugin uses a single URI only.
	if len(uris) == 0 {
		return fmt.Errorf("no URIs provided for pip plugin")
	}
	uri := uris[0]

	// Update py_executable.
	protocol, hashVal, err := ParseURI(uri)
	if err != nil {
		return err
	}
	if protocol != ProtocolPip {
		return fmt.Errorf("expected pip protocol, got %s", protocol)
	}

	targetDir := p.getPathFromHash(hashVal)
	virtualenvPython := getVirtualenvPython(targetDir)

	if _, err := os.Stat(virtualenvPython); os.IsNotExist(err) {
		return fmt.Errorf("local directory %s for URI %s does not exist on the cluster. Something may have gone wrong while installing the runtime_env `pip` packages", targetDir, uri)
	}

	context.PyExecutable = virtualenvPython
	context.CommandPrefix = append(context.CommandPrefix, getVirtualenvActivateCommand(targetDir)...)

	return nil
}
