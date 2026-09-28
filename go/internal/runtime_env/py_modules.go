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
	"strings"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/log"
)

// PyModulesPlugin is the plugin for Python modules (py_modules).
// It implements the RuntimeEnvPlugin interface.
type PyModulesPlugin struct {
	// resourcesDir is the directory holding the downloaded module resources.
	resourcesDir string
	// gcsClient is the client used to access GCS.
	gcsClient gcs.Client
}

// Name returns the plugin name.
func (p *PyModulesPlugin) Name() string {
	return "py_modules"
}

// Priority returns the plugin priority.
// The py_modules plugin uses priority 10 (the default).
func (p *PyModulesPlugin) Priority() int {
	return 10
}

// Validate checks the runtime environment configuration.
// The py_modules plugin requires no special validation.
func (p *PyModulesPlugin) Validate(runtimeEnv *RuntimeEnv) error {
	return nil
}

// GetURIs returns the list of URIs associated with py_modules.
func (p *PyModulesPlugin) GetURIs(runtimeEnv *RuntimeEnv) []string {
	pyModulesVal, ok := (*runtimeEnv)["py_modules"]
	if !ok || pyModulesVal == nil {
		return []string{}
	}

	var uris []string
	switch v := pyModulesVal.(type) {
	case []string:
		uris = v
	case []interface{}:
		uris = make([]string, 0, len(v))
		for _, item := range v {
			if str, ok := item.(string); ok {
				uris = append(uris, str)
			}
		}
	}

	return uris
}

// Create downloads and installs the py_modules package.
// It fetches and unpacks/installs the Python module package.
func (p *PyModulesPlugin) Create(
	ctx context.Context,
	uri string,
	runtimeEnv *RuntimeEnv,
	runtimeEnvContext *RuntimeEnvContext,
) (int64, error) {

	// Download and unpack the package.
	moduleDir, err := DownloadAndUnpackPackage(
		ctx,
		uri,
		p.resourcesDir,
		p.gcsClient,
		false, // overwrite=false
	)
	if err != nil {
		return 0, err
	}

	// For a wheel URI, the wheel package must be installed.
	if IsWhlURI(uri) {
		wheelURI := moduleDir
		moduleDir = GetLocalDirFromURI(uri, p.resourcesDir)

		// Install the wheel package.
		err = InstallWheelPackage(ctx, wheelURI, moduleDir)
		if err != nil {
			return 0, err
		}
	}

	// Compute the directory size.
	size, err := common.DirSizeBytes(moduleDir)
	if err != nil {
		return 0, err
	}

	return size, nil
}

// ModifyContext updates the context to set PYTHONPATH.
// It adds the py_modules directories to the PYTHONPATH environment variable.
func (p *PyModulesPlugin) ModifyContext(
	uris []string,
	runtimeEnv *RuntimeEnv,
	runtimeEnvContext *RuntimeEnvContext,
) error {

	moduleDirs := make([]string, 0, len(uris))

	for _, uri := range uris {
		moduleDir := GetLocalDirFromURI(uri, p.resourcesDir)

		// Check that the directory exists.
		info, err := os.Stat(moduleDir)
		if err != nil {
			return fmt.Errorf("local directory %s for URI %s does not exist on the cluster: %w", moduleDir, uri, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("local path %s for URI %s is not a directory", moduleDir, uri)
		}

		moduleDirs = append(moduleDirs, moduleDir)
	}

	// Add all module directories to PYTHONPATH.
	if len(moduleDirs) > 0 {
		pythonPath := strings.Join(moduleDirs, string(os.PathListSeparator))
		SetPythonpathInContext(pythonPath, runtimeEnvContext)
	}

	return nil
}

// DeleteURI deletes the py_modules for the given URI.
// It returns the amount of space freed, in bytes.
func (p *PyModulesPlugin) DeleteURI(uri string) int64 {
	log.Log.Info("Got request to delete py_modules URI", "uri", uri)

	localDir := GetLocalDirFromURI(uri, p.resourcesDir)

	// Get the directory size.
	var size int64
	if info, err := os.Stat(localDir); err == nil && info.IsDir() {
		dirSize, _ := common.DirSizeBytes(localDir)
		size = dirSize
	}

	// Delete the package.
	deleted, err := DeletePackage(context.Background(), uri, p.resourcesDir)
	if err != nil {
		log.Log.Error(err, "Failed to delete py_modules URI", "uri", uri)
		return 0
	}
	if !deleted {
		log.Log.V(1).Info("Tried to delete nonexistent URI", "uri", uri)
		return 0
	}

	return size
}

// NewPyModulesPlugin creates a new py_modules plugin instance.
func NewPyModulesPlugin(resourcesDir string, gcsClient gcs.Client) (*PyModulesPlugin, error) {
	pyModulesResourcesDir, err := createResourcesSubdir(resourcesDir, "py_modules_files")
	if err != nil {
		return nil, fmt.Errorf("failed to create py_modules resources directory: %w", err)
	}

	return &PyModulesPlugin{
		resourcesDir: pyModulesResourcesDir,
		gcsClient:    gcsClient,
	}, nil
}
