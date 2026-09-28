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

	"github.com/ray-project/ray/go/pkg/log"
)

// PyExecutablePlugin implements the RuntimeEnvPlugin interface to support a
// custom Python executable.
//
// This plugin allows Ray workers to run with a custom Python executable. It can
// be used via ray.init(runtime_env={"py_executable": "<command> <args>"}). If a
// working_dir is specified in the runtime environment, the executable will have
// access to the working directory — for example to reach requirements.txt (for
// package managers), a debugger script, or the executable itself may be a shell
// script in the working directory. This plugin can also be used to run worker
// processes under a custom profiler, or with a custom Python interpreter or a
// python invocation with custom arguments.
type PyExecutablePlugin struct {
}

// NewPyExecutablePlugin creates a new PyExecutablePlugin instance.
func NewPyExecutablePlugin() *PyExecutablePlugin {
	return &PyExecutablePlugin{}
}

// Name returns the plugin name.
func (p *PyExecutablePlugin) Name() string {
	return "py_executable"
}

// Priority returns the plugin priority.
func (p *PyExecutablePlugin) Priority() int {
	return RayRuntimeEnvPluginDefaultPriority
}

// Validate validates the user-provided runtime environment configuration.
func (p *PyExecutablePlugin) Validate(runtimeEnv *RuntimeEnv) error {
	// The py_executable plugin does not require any special validation.
	return nil
}

// GetURIs returns the list of URIs associated with the plugin.
// The py_executable plugin does not use URIs, so it returns an empty list.
func (p *PyExecutablePlugin) GetURIs(runtimeEnv *RuntimeEnv) []string {
	return []string{}
}

// Create creates and installs the runtime environment.
// It is invoked when the runtime env agent installs the environment. The uri
// argument can be used as a caching mechanism. It returns the disk space (in
// bytes) consumed by the plugin's installation, plus any error.
func (p *PyExecutablePlugin) Create(ctx context.Context, uri string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) (int64, error) {
	// The py_executable plugin does not create any resources, so return 0.
	return 0, nil
}

// ModifyContext modifies the context to change worker startup behavior.
// For the py_executable plugin, this sets context.py_executable to the value
// specified in the runtime environment.
func (p *PyExecutablePlugin) ModifyContext(uris []string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) error {
	log.Log.Info("Running py_executable plugin")

	// Get the py_executable value from the runtime environment.
	pyExecutable, err := runtimeEnv.PyExecutable()
	if err != nil {
		// If the py_executable field is missing or not a string, log a warning
		// but do not interrupt the flow.
		log.Log.V(1).Info("Failed to get py_executable from runtime environment", "error", err)
		return nil
	}

	if pyExecutable != "" {
		context.PyExecutable = pyExecutable
	}

	return nil
}

// DeleteURI deletes the runtime environment for the given URI.
// The py_executable plugin does not use URIs, so it returns 0.
func (p *PyExecutablePlugin) DeleteURI(uri string) int64 {
	return 0
}
