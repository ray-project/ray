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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/ray-project/ray/go/pkg/log"
)

type tempDirManager struct {
	mu       sync.Mutex
	tempDirs map[string]bool
}

func newTempDirManager() *tempDirManager {
	return &tempDirManager{
		tempDirs: make(map[string]bool),
	}
}

func (m *tempDirManager) createTempDir(pattern string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tmpDir, err := os.MkdirTemp("", pattern)
	if err != nil {
		return "", err
	}

	m.tempDirs[tmpDir] = true
	return tmpDir, nil
}

func (m *tempDirManager) removeTempDir(tmpDir string) error {
	m.mu.Lock()
	delete(m.tempDirs, tmpDir)
	m.mu.Unlock()

	return os.RemoveAll(tmpDir)
}

func (m *tempDirManager) cleanupAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for dir := range m.tempDirs {
		os.RemoveAll(dir)
	}
	m.tempDirs = make(map[string]bool)
}

var globalTempDirManager = newTempDirManager()

const (
	PodmanCommand        = "podman"
	PodmanRunFlag        = "run"
	PodmanRmFlag         = "--rm"
	PodmanVolumeFlag     = "-v"
	PodmanCgroupFlag     = "--cgroup-manager=cgroupfs"
	PodmanNetworkFlag    = "--network=host"
	PodmanPidFlag        = "--pid=host"
	PodmanIpcFlag        = "--ipc=host"
	PodmanUsernsFlag     = "--userns=keep-id"
	PodmanEntrypointFlag = "--entrypoint"
	PodmanEnvFlag        = "--env"
)

func pullImageAndGetWorkerPathShared(ctx context.Context, imageURI string) (string, error) {
	tmpDir, err := globalTempDirManager.createTempDir(fmt.Sprintf("ray_image_uri_%s_", uuid.New().String()[:8]))
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() {
		if err := globalTempDirManager.removeTempDir(tmpDir); err != nil {
			log.Log.Error(err, "Failed to cleanup temp dir", "dir", tmpDir)
		}
	}()

	// Use safe permissions (owner read/write/execute).
	// The :Z suffix already lets Podman handle the SELinux context, so no world-readable 0777 is needed.
	if err := os.Chmod(tmpDir, 0750); err != nil {
		return "", fmt.Errorf("failed to chmod temp dir: %w", err)
	}

	resultFile := filepath.Join(tmpDir, "worker_path.txt")

	getWorkerPathScript := `
import ray._private.workers.default_worker as dw
with open('/shared/worker_path.txt', 'w') as f:
    f.write(dw.__file__)
`

	cmd := exec.CommandContext(ctx,
		PodmanCommand,
		PodmanRunFlag,
		PodmanRmFlag,
		PodmanVolumeFlag, fmt.Sprintf("%s:/shared:Z", tmpDir),
		imageURI,
		"python",
		"-c", getWorkerPathScript,
	)

	log.Log.Info("Running podman command to get worker path", "command", cmd.String())

	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("podman command failed: %w, output: %s", err, string(output))
	}

	workerPathBytes, err := os.ReadFile(resultFile)
	if err != nil {
		return "", fmt.Errorf("failed to read worker path file: %w", err)
	}

	workerPath := strings.TrimSpace(string(workerPathBytes))

	if !strings.HasSuffix(workerPath, ".py") {
		return "", fmt.Errorf("invalid worker path inferred in image %s: %s", imageURI, workerPath)
	}

	return workerPath, nil
}

// modifyContextShared modifies the context to set up the container command (package-level shared helper).
// It mirrors the Python _modify_context_impl().
func modifyContextShared(config ModifyContextConfig) error {
	// Set the worker entrypoint.
	config.Context.OverrideWorkerEntrypoint = config.WorkerPath

	// Collect the RAY_-prefixed environment variables with the shared helper (returns map[string]string).
	rayEnvMap := FilterEnvVarsWithPrefix("RAY_")

	// Pre-size containerCommand to reduce reallocations:
	// about 10 base command elements + env vars (2 elements each) + runOptions + entrypoint and image.
	estimatedCapacity := 10 + len(rayEnvMap)*2 + len(config.Context.EnvVars)*2 + len(config.RunOptions) + 4
	containerCommand := make([]string, 0, estimatedCapacity)

	containerCommand = append(containerCommand,
		PodmanCommand,
		PodmanRunFlag,
		PodmanVolumeFlag, fmt.Sprintf("%s:%s", config.RayTmpDir, config.RayTmpDir),
		PodmanCgroupFlag,
		PodmanNetworkFlag,
		PodmanPidFlag,
		PodmanIpcFlag,
		PodmanUsernsFlag,
	)

	// Add the RAY_-prefixed environment variables.
	for k, v := range rayEnvMap {
		containerCommand = append(containerCommand, PodmanEnvFlag, fmt.Sprintf("%s=%s", k, v))
	}

	// Add the user-supplied environment variables.
	for k, v := range config.Context.EnvVars {
		containerCommand = append(containerCommand, PodmanEnvFlag, fmt.Sprintf("%s='%s'", k, v))
	}

	containerCommand = append(containerCommand, PodmanEnvFlag, "RAY_JOB_ID=$RAY_JOB_ID")

	if len(config.RunOptions) > 0 {
		containerCommand = append(containerCommand, config.RunOptions...)
	}

	containerCommand = append(containerCommand, PodmanEntrypointFlag, "python", config.ImageURI)

	// Build the command string with strings.Builder.
	var commandBuilder strings.Builder
	for i, part := range containerCommand {
		if i > 0 {
			commandBuilder.WriteByte(' ')
		}
		commandBuilder.WriteString(part)
	}
	containerCommandStr := commandBuilder.String()
	log.Log.Info("Starting worker in container with prefix", "command", containerCommandStr)

	// Set py_executable to the container command.
	config.Context.PyExecutable = containerCommandStr

	return nil
}

// collectRayEnvVars collects every RAY_-prefixed environment variable.
// It returns the formatted variable strings in KEY=VALUE form.
// Deprecated: use utils.FilterEnvVarsWithPrefix("RAY_") instead.
func collectRayEnvVars() []string {
	rayEnvMap := FilterEnvVarsWithPrefix("RAY_")
	return EnvVarsToStringSlice(rayEnvMap)
}

// baseContainerPlugin is the base of the container plugins.
// It provides shared image pulling and context modification logic.
type baseContainerPlugin struct {
	// rayTmpDir is the Ray temporary directory.
	rayTmpDir string
	// workerPath is the worker path inside the container (obtained after pulling the image).
	workerPath string
	// name is the plugin name.
	name string
	// priority is the plugin priority.
	priority int
}

// Name returns the plugin name.
func (p *baseContainerPlugin) Name() string {
	return p.name
}

// Priority returns the plugin priority.
func (p *baseContainerPlugin) Priority() int {
	return p.priority
}

// GetURIs returns the container image URIs.
func (p *baseContainerPlugin) GetURIs(runtimeEnv *RuntimeEnv, fieldName string) []string {
	containerVal, ok := (*runtimeEnv)[fieldName]
	if !ok || containerVal == nil {
		return []string{}
	}

	switch config := containerVal.(type) {
	case map[string]interface{}:
		if imageVal, ok := config["image"]; ok {
			if imageURI, ok := imageVal.(string); ok && imageURI != "" {
				return []string{imageURI}
			}
		}
	case string:
		if config != "" {
			return []string{config}
		}
	}
	return []string{}
}

// Create pulls the container image.
func (p *baseContainerPlugin) Create(
	ctx context.Context,
	uri string,
	runtimeEnv *RuntimeEnv,
	context *RuntimeEnvContext,
) (int64, error) {

	log.Log.Info("Pulling container image", "uri", uri)

	// Pull the image and infer the worker path with the shared helper.
	workerPath, err := pullImageAndGetWorkerPathShared(ctx, uri)
	if err != nil {
		return 0, fmt.Errorf("failed to pull container image %s: %w", uri, err)
	}

	p.workerPath = workerPath
	log.Log.Info("Inferred worker path in container", "uri", uri, "worker_path", workerPath)

	// The image size cannot be determined cheaply, so return 0.
	return 0, nil
}

// ModifyContextWithConfig modifies the context to set up the container command with a custom config.
func (p *baseContainerPlugin) ModifyContextWithConfig(
	uris []string,
	runtimeEnv *RuntimeEnv,
	context *RuntimeEnvContext,
	config ContainerConfig,
	fieldName string,
) error {

	if len(uris) == 0 {
		return nil
	}

	imageURI := uris[0]

	workerPath := config.WorkerPath
	if workerPath == "" {
		workerPath = p.workerPath
	}

	modifyConfig := ModifyContextConfig{
		RayTmpDir:  p.rayTmpDir,
		ImageURI:   imageURI,
		WorkerPath: workerPath,
		RunOptions: config.RunOptions,
		Context:    context,
	}

	return modifyContextShared(modifyConfig)
}

func (p *baseContainerPlugin) DeleteURI(uri string) int64 {

	log.Log.Info("Cleaning up container plugin resources", "uri", uri)

	globalTempDirManager.cleanupAll()

	return 0
}

// ImageURIPlugin is the image_uri plugin.
// It starts the worker process inside a custom container image.
type ImageURIPlugin struct {
	baseContainerPlugin
}

// Validate validates the runtime env config.
func (p *ImageURIPlugin) Validate(runtimeEnv *RuntimeEnv) error {
	imageURIVal, ok := (*runtimeEnv)[FieldImageURI]
	if !ok || imageURIVal == nil {
		return nil // No image_uri field, nothing to validate.
	}

	imageURI, ok := imageURIVal.(string)
	if !ok {
		return fmt.Errorf("image_uri must be a string, got %T", imageURIVal)
	}
	if imageURI == "" {
		return fmt.Errorf("image_uri cannot be empty")
	}

	return nil
}

// GetURIs returns the image URIs.
func (p *ImageURIPlugin) GetURIs(runtimeEnv *RuntimeEnv) []string {
	return p.baseContainerPlugin.GetURIs(runtimeEnv, FieldImageURI)
}

// ModifyContext modifies the context to set up the container command.
func (p *ImageURIPlugin) ModifyContext(
	uris []string,
	runtimeEnv *RuntimeEnv,
	context *RuntimeEnvContext,
) error {
	return p.baseContainerPlugin.ModifyContextWithConfig(uris, runtimeEnv, context, ContainerConfig{}, FieldImageURI)
}

// NewImageURIPlugin creates a new ImageURIPlugin.
func NewImageURIPlugin(resourcesDir string) (*ImageURIPlugin, error) {
	// Create the dedicated image cache directory through the shared resources-dir mechanism.
	imageURIResourcesDir, err := createResourcesSubdir(resourcesDir, "image_uri_files")
	if err != nil {
		return nil, fmt.Errorf("failed to create image URI resources directory: %w", err)
	}

	return &ImageURIPlugin{
		baseContainerPlugin: baseContainerPlugin{
			rayTmpDir:  imageURIResourcesDir,
			name:       FieldImageURI,
			priority:   10,
			workerPath: "",
		},
	}, nil
}

// ContainerPlugin is the container plugin.
// It supports container startup with a custom worker_path and run_options.
type ContainerPlugin struct {
	baseContainerPlugin
}

// Validate validates the runtime env config.
func (p *ContainerPlugin) Validate(runtimeEnv *RuntimeEnv) error {
	containerVal, ok := (*runtimeEnv)[FieldContainer]
	if !ok || containerVal == nil {
		return nil // No container field, nothing to validate.
	}

	// container must be an object/map.
	containerConfig, ok := containerVal.(map[string]interface{})
	if !ok {
		return fmt.Errorf("container must be an object, got %T", containerVal)
	}

	// image is required and must be a string.
	imageVal, ok := containerConfig["image"]
	if !ok || imageVal == nil {
		return fmt.Errorf("container.image is required")
	}
	if _, ok := imageVal.(string); !ok {
		return fmt.Errorf("container.image must be a string, got %T", imageVal)
	}

	// worker_path, when present, must be a string.
	if workerPathVal, ok := containerConfig["worker_path"]; ok && workerPathVal != nil {
		if _, ok := workerPathVal.(string); !ok {
			return fmt.Errorf("container.worker_path must be a string, got %T", workerPathVal)
		}
	}

	// run_options, when present, must be an array.
	if runOptionsVal, ok := containerConfig["run_options"]; ok && runOptionsVal != nil {
		switch options := runOptionsVal.(type) {
		case []interface{}:
			for i, opt := range options {
				if _, ok := opt.(string); !ok {
					return fmt.Errorf("container.run_options[%d] must be a string, got %T", i, opt)
				}
			}
		case []string:
			// Valid.
		default:
			return fmt.Errorf("container.run_options must be an array of strings, got %T", runOptionsVal)
		}
	}

	return nil
}

// GetURIs returns the container image URIs.
func (p *ContainerPlugin) GetURIs(runtimeEnv *RuntimeEnv) []string {
	return p.baseContainerPlugin.GetURIs(runtimeEnv, FieldContainer)
}

// Create pulls the container image.
func (p *ContainerPlugin) Create(
	ctx context.Context,
	uri string,
	runtimeEnv *RuntimeEnv,
	context *RuntimeEnvContext,
) (int64, error) {
	return p.baseContainerPlugin.Create(ctx, uri, runtimeEnv, context)
}

// ModifyContext modifies the context to set up the container command.
func (p *ContainerPlugin) ModifyContext(
	uris []string,
	runtimeEnv *RuntimeEnv,
	context *RuntimeEnvContext,
) error {
	if len(uris) == 0 {
		return nil
	}

	containerVal, ok := (*runtimeEnv)[FieldContainer]
	if !ok || containerVal == nil {
		return nil
	}

	containerConfig, ok := containerVal.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid container config type")
	}

	config := ContainerConfig{}

	if workerPathVal, ok := containerConfig["worker_path"]; ok && workerPathVal != nil {
		if wp, ok := workerPathVal.(string); ok {
			log.Log.Info("Using custom worker_path from container config", "worker_path", wp)
			config.WorkerPath = wp
		}
	}

	if runOptionsVal, ok := containerConfig["run_options"]; ok && runOptionsVal != nil {
		switch opts := runOptionsVal.(type) {
		case []interface{}:
			for _, opt := range opts {
				if str, ok := opt.(string); ok {
					config.RunOptions = append(config.RunOptions, str)
				}
			}
		case []string:
			config.RunOptions = opts
		}
	}

	return p.baseContainerPlugin.ModifyContextWithConfig(uris, runtimeEnv, context, config, FieldContainer)
}

// DeleteURI deletes the image for the given URI (container images are usually not deleted).
func (p *ContainerPlugin) DeleteURI(uri string) int64 {
	return p.baseContainerPlugin.DeleteURI(uri)
}

// NewContainerPlugin creates a new ContainerPlugin.
func NewContainerPlugin(rayTmpDir string) (*ContainerPlugin, error) {
	return &ContainerPlugin{
		baseContainerPlugin: baseContainerPlugin{
			rayTmpDir:  rayTmpDir,
			name:       FieldContainer,
			priority:   RayRuntimeEnvPluginDefaultPriority,
			workerPath: "",
		},
	}, nil
}
