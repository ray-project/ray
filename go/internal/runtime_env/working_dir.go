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
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
)

// Note: directory size calculation has been extracted to common.DirSizeBytes().

const RAY_RUNTIME_ENV_CREATE_WORKING_DIR_ENV_VAR = "RAY_RUNTIME_ENV_CREATE_WORKING_DIR"

// WorkingDirEnvScope is a scope handle for the working directory environment
// variable. It manages setting and restoring the environment variable.
type WorkingDirEnvScope struct {
	key        string
	prevValue  string
	prevExists bool
	restored   bool
}

// Restore restores the environment variable to its previous state (idempotent).
// Call it via defer, for example:
//
//	scope, err := plugin.SetupWorkingDirEnv(uri)
//	if err != nil { return err }
//	defer scope.Restore()
func (s *WorkingDirEnvScope) Restore() {
	if s.restored {
		return
	}
	s.restored = true
	if !s.prevExists {
		os.Unsetenv(s.key)
	} else {
		os.Setenv(s.key, s.prevValue)
	}
}

// UploadWorkingDirIfNeeded uploads the working directory if needed.
// If working_dir is already a URI, it is returned as is.
func UploadWorkingDirIfNeeded(
	runtimeEnv map[string]interface{},
	includeGitignore bool,
	scratchDir string,
	uploadFn func(string, []string) error,
) (map[string]interface{}, error) {

	// Get the working_dir field.
	workingDirVal, ok := runtimeEnv["working_dir"]
	if !ok || workingDirVal == nil {
		return runtimeEnv, nil
	}

	// Validate that working_dir is a string.
	workingDir, ok := workingDirVal.(string)
	if !ok {
		return nil, fmt.Errorf("working_dir must be a string (either a local path or remote URI), got %T", workingDirVal)
	}

	// Try to parse the URI protocol.
	protocol, path, err := ParseURI(workingDir)
	if err != nil {
		// Not a valid URI; treat it as a local path.
		protocol = ""
		path = ""
	}

	// If it is already a remote URI, return as is.
	if protocol != "" {
		remoteProtocols := GetRemoteProtocols()
		isRemote := false
		for _, p := range remoteProtocols {
			if p == protocol {
				isRemote = true
				break
			}
		}
		if isRemote && !strings.HasSuffix(path, ".zip") {
			return nil, fmt.Errorf("only .zip files supported for remote URIs")
		}
		return runtimeEnv, nil
	}

	// Get the excludes configuration.
	var excludes []string
	if excludesVal, ok := runtimeEnv["excludes"]; ok {
		// Check the exact []string type first.
		if arr, ok := excludesVal.([]string); ok {
			excludes = arr
		} else if arr, ok := excludesVal.([]interface{}); ok {
			// Type produced by JSON deserialization; it needs conversion.
			excludes = common.InterfaceToStringSlice(arr)
		}
	}

	// Try to generate a URI for the directory.
	workingDirURI, err := GetURIForDirectory(workingDir, includeGitignore, excludes)
	if err != nil {
		// working_dir is not a directory; check whether it is a zip file.
		pkgPath := filepath.Clean(workingDir)
		info, statErr := os.Stat(pkgPath)
		if statErr != nil || info.IsDir() || !strings.HasSuffix(pkgPath, ".zip") {
			return nil, fmt.Errorf("directory %s must be an existing directory or a zip package", pkgPath)
		}

		// Get the package URI and upload it to GCS.
		pkgURI, uriErr := GetURIForPackage(pkgPath)
		if uriErr != nil {
			return nil, uriErr
		}

		// Read the file contents.
		pkgBytes, readErr := os.ReadFile(pkgPath)
		if readErr != nil {
			return nil, readErr
		}

		// Upload to GCS.
		uploadErr := UploadPackageToGCS(pkgURI, pkgBytes)
		if uploadErr != nil {
			return nil, fmt.Errorf("failed to upload package %s to the Ray cluster: %w", pkgPath, uploadErr)
		}

		runtimeEnv["working_dir"] = pkgURI
		return runtimeEnv, nil
	}

	// Upload the directory package.
	if uploadFn == nil {
		_, uploadErr := UploadPackageIfNeededWithConfig(
			context.Background(),
			&PackageUploadConfig{
				PkgURI:           workingDirURI,
				BaseDirectory:    scratchDir,
				ModulePath:       workingDir,
				IncludeGitignore: includeGitignore,
				IncludeParentDir: false,
				Excludes:         excludes,
			},
		)
		if uploadErr != nil {
			return nil, fmt.Errorf("failed to upload working_dir %s to the Ray cluster: %w", workingDir, uploadErr)
		}
	} else {
		uploadErr := uploadFn(workingDir, excludes)
		if uploadErr != nil {
			return nil, fmt.Errorf("failed to upload working_dir %s to the Ray cluster: %w", workingDir, uploadErr)
		}
	}

	runtimeEnv["working_dir"] = workingDirURI
	return runtimeEnv, nil
}

// SetPythonpathInContext prepends the path to the PYTHONPATH environment variable.
// Import precedence:
// pythonPath argument > PYTHONPATH in context.EnvVars > existing cluster PYTHONPATH.
func SetPythonpathInContext(pythonPath string, context *RuntimeEnvContext) {
	if context.EnvVars == nil {
		context.EnvVars = make(map[string]string)
	}

	// If the context already has a PYTHONPATH, append it after pythonPath.
	if existingPythonPath, ok := context.EnvVars["PYTHONPATH"]; ok && existingPythonPath != "" {
		pythonPath += string(os.PathListSeparator) + existingPythonPath
	}

	// If the system environment already has a PYTHONPATH, append it as well.
	if sysPythonPath := os.Getenv("PYTHONPATH"); sysPythonPath != "" {
		pythonPath += string(os.PathListSeparator) + sysPythonPath
	}

	context.EnvVars["PYTHONPATH"] = pythonPath
}

// WorkingDirPlugin is the working directory plugin.
// It implements the RuntimeEnvPlugin interface.
type WorkingDirPlugin struct {
	// resourcesDir is the resources directory.
	resourcesDir string
	// gcsClient is the GCS client.
	gcsClient gcs.Client
}

// Name returns the plugin name.
func (p *WorkingDirPlugin) Name() string {
	return "working_dir"
}

// Priority returns the plugin priority.
// The working directory plugin runs before the other plugins.
func (p *WorkingDirPlugin) Priority() int {
	return 5
}

// Validate validates the runtime environment configuration.
func (p *WorkingDirPlugin) Validate(runtimeEnv *RuntimeEnv) error {
	return nil
}

// GetURIs returns the list of working directory URIs.
func (p *WorkingDirPlugin) GetURIs(runtimeEnv *RuntimeEnv) []string {
	workingDirURI := ""
	if val, ok := (*runtimeEnv)["working_dir"]; ok {
		if str, ok := val.(string); ok {
			workingDirURI = str
		}
	}
	if workingDirURI != "" {
		return []string{workingDirURI}
	}
	return []string{}
}

// Create creates and installs the working directory.
// It downloads and unpacks the working directory package.
func (p *WorkingDirPlugin) Create(
	ctx context.Context,
	uri string,
	runtimeEnv *RuntimeEnv,
	context *RuntimeEnvContext,
) (int64, error) {
	localDir, err := DownloadAndUnpackPackage(
		ctx,
		uri,
		p.resourcesDir,
		p.gcsClient,
		true,
	)
	if err != nil {
		return 0, err
	}

	// Compute the directory size.
	size, err := common.DirSizeBytes(localDir)
	if err != nil {
		return 0, err
	}

	return int64(size), nil
}

// ModifyContext updates the context to set up the working directory.
// It switches to the working directory and sets PYTHONPATH before the worker starts.
func (p *WorkingDirPlugin) ModifyContext(
	uris []string,
	runtimeEnv *RuntimeEnv,
	context *RuntimeEnvContext,
) error {

	if len(uris) == 0 {
		return nil
	}

	// WorkingDirPlugin uses a single URI.
	uri := uris[0]
	localDir := GetLocalDirFromURI(uri, p.resourcesDir)

	// Check that the directory exists.
	info, err := os.Stat(localDir)
	if err != nil {
		return fmt.Errorf("local directory %s for URI %s does not exist on the cluster: %w", localDir, uri, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("local path %s for URI %s is not a directory", localDir, uri)
	}

	// Add the cd command to command_prefix.
	if runtime.GOOS == "windows" {
		// Windows: cd /d <dir> && (/d switches the drive)
		context.CommandPrefix = append(context.CommandPrefix, "cd", "/d", localDir, "&&")
	} else {
		// Unix/Linux/macOS: cd <dir> &&
		context.CommandPrefix = append(context.CommandPrefix, "cd", localDir, "&&")
	}

	// Set PYTHONPATH.
	SetPythonpathInContext(localDir, context)

	return nil
}

// DeleteURI deletes the working directory for the given URI.
// It returns the amount of space freed (in bytes).
func (p *WorkingDirPlugin) DeleteURI(uri string) int64 {

	log.Log.Info("Got request to delete working dir URI", "uri", uri)
	localDir := GetLocalDirFromURI(uri, p.resourcesDir)

	// Get the directory size.
	size := int64(0)
	if info, err := os.Stat(localDir); err == nil && info.IsDir() {
		dirSize, _ := common.DirSizeBytes(localDir)
		size = dirSize
	}

	// Delete the package.
	deleted, err := DeletePackage(context.Background(), uri, p.resourcesDir)
	if err != nil {
		log.Log.Error(err, "Failed to delete working dir URI", "uri", uri)
		return 0
	}
	if !deleted {
		log.Log.V(1).Info("Tried to delete nonexistent URI", "uri", uri)
		return 0
	}

	return size
}

// SetupWorkingDirEnv sets the working directory environment variable.
// It returns a scope handle; the caller must call scope.Restore() afterwards to
// restore the environment variable.
//
// Typical usage:
//
//	scope, err := plugin.SetupWorkingDirEnv(uri)
//	if err != nil {
//	    return err
//	}
//	defer scope.Restore()
//	// Use the RAY_RUNTIME_ENV_CREATE_WORKING_DIR environment variable here,
//	// e.g. pip install -r ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/requirements.txt
//
// Parameters:
//   - uri: working directory URI
//
// Returns:
//   - *WorkingDirEnvScope: the scope handle
//   - error: returned when setup fails
func (p *WorkingDirPlugin) SetupWorkingDirEnv(uri string) (*WorkingDirEnvScope, error) {
	if uri == "" {
		// For an empty URI, return an empty no-op scope.
		return &WorkingDirEnvScope{}, nil
	}

	localDir := GetLocalDirFromURI(uri, p.resourcesDir)

	// Check that the directory exists.
	info, err := os.Stat(localDir)
	if err != nil {
		return nil, fmt.Errorf("local directory %s for URI %s does not exist: %w", localDir, uri, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("local path %s for URI %s is not a directory", localDir, uri)
	}

	// Save the previous value.
	prevValue, prevExists := os.LookupEnv(RAY_RUNTIME_ENV_CREATE_WORKING_DIR_ENV_VAR)

	// Convert Windows backslash paths to forward slashes (for compatibility with tools like pip).
	envValue := filepath.ToSlash(localDir)
	if err := os.Setenv(RAY_RUNTIME_ENV_CREATE_WORKING_DIR_ENV_VAR, envValue); err != nil {
		return nil, fmt.Errorf("failed to set working dir env: %w", err)
	}

	return &WorkingDirEnvScope{
		key:        RAY_RUNTIME_ENV_CREATE_WORKING_DIR_ENV_VAR,
		prevValue:  prevValue,
		prevExists: prevExists,
		restored:   false,
	}, nil
}

// NewWorkingDirPlugin creates a new working directory plugin instance.
func NewWorkingDirPlugin(resourcesDir string, gcsClient gcs.Client) (*WorkingDirPlugin, error) {
	workingDirResourcesDir, err := createResourcesSubdir(resourcesDir, "working_dir_files")
	if err != nil {
		return nil, fmt.Errorf("failed to create working directory resources directory: %w", err)
	}

	return &WorkingDirPlugin{
		resourcesDir: workingDirResourcesDir,
		gcsClient:    gcsClient,
	}, nil
}
