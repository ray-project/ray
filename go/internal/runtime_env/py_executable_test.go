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
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPyExecutablePlugin_Name tests the plugin name.
func TestPyExecutablePlugin_Name(t *testing.T) {
	plugin := NewPyExecutablePlugin()
	assert.Equal(t, "py_executable", plugin.Name())
}

// TestPyExecutablePlugin_Priority tests the plugin priority.
func TestPyExecutablePlugin_Priority(t *testing.T) {
	plugin := NewPyExecutablePlugin()
	assert.Equal(t, RayRuntimeEnvPluginDefaultPriority, plugin.Priority())
}

// TestPyExecutablePlugin_Validate tests the Validate method.
func TestPyExecutablePlugin_Validate(t *testing.T) {
	plugin := NewPyExecutablePlugin()
	runtimeEnv := &RuntimeEnv{}

	// An empty environment should pass validation.
	err := plugin.Validate(runtimeEnv)
	assert.NoError(t, err)

	// An environment with a py_executable field should also pass validation.
	runtimeEnvWithExe := &RuntimeEnv{FieldPyExecutable: "/usr/bin/python3"}
	err = plugin.Validate(runtimeEnvWithExe)
	assert.NoError(t, err)
}

// TestPyExecutablePlugin_GetURIs tests GetURIs.
func TestPyExecutablePlugin_GetURIs(t *testing.T) {
	plugin := NewPyExecutablePlugin()
	runtimeEnv := &RuntimeEnv{FieldPyExecutable: "/usr/bin/python3"}

	uris := plugin.GetURIs(runtimeEnv)
	assert.Empty(t, uris)
}

// TestPyExecutablePlugin_Create tests the Create method.
func TestPyExecutablePlugin_Create(t *testing.T) {
	plugin := NewPyExecutablePlugin()
	ctx := context.Background()
	runtimeEnv := &RuntimeEnv{FieldPyExecutable: "/usr/bin/python3"}
	envContext := &RuntimeEnvContext{}

	size, err := plugin.Create(ctx, "", runtimeEnv, envContext)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), size)
}

// TestPyExecutablePlugin_ModifyContext tests the ModifyContext method.
func TestPyExecutablePlugin_ModifyContext(t *testing.T) {
	tests := []struct {
		name        string
		runtimeEnv  *RuntimeEnv
		expectedExe string
	}{
		{
			name:        "with py_executable field",
			runtimeEnv:  &RuntimeEnv{FieldPyExecutable: "/custom/python3"},
			expectedExe: "/custom/python3",
		},
		{
			name:        "without py_executable field",
			runtimeEnv:  &RuntimeEnv{},
			expectedExe: "",
		},
		{
			name:        "with empty py_executable",
			runtimeEnv:  &RuntimeEnv{FieldPyExecutable: ""},
			expectedExe: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := NewPyExecutablePlugin()
			envContext := &RuntimeEnvContext{}

			err := plugin.ModifyContext([]string{}, tt.runtimeEnv, envContext)
			assert.NoError(t, err)

			assert.Equal(t, tt.expectedExe, envContext.PyExecutable)
		})
	}
}

// TestPyExecutablePlugin_DeleteURI tests the DeleteURI method.
func TestPyExecutablePlugin_DeleteURI(t *testing.T) {
	plugin := NewPyExecutablePlugin()

	space := plugin.DeleteURI("some-uri")
	assert.Equal(t, int64(0), space)
}

// TestPyExecutablePlugin_Integration tests the full end-to-end flow.
func TestPyExecutablePlugin_Integration(t *testing.T) {
	// Create the runtime environment and context.
	runtimeEnv := &RuntimeEnv{
		FieldPyExecutable: "/opt/venv/bin/python",
	}
	envContext := &RuntimeEnvContext{}

	// Create the plugin instance.
	plugin := NewPyExecutablePlugin()

	// Validate the configuration.
	err := plugin.Validate(runtimeEnv)
	assert.NoError(t, err)

	// Get the URIs (should be empty).
	uris := plugin.GetURIs(runtimeEnv)
	assert.Empty(t, uris)

	// Create the environment.
	ctx := context.Background()
	size, err := plugin.Create(ctx, "", runtimeEnv, envContext)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), size)

	// Modify the context.
	err = plugin.ModifyContext(uris, runtimeEnv, envContext)
	assert.NoError(t, err)

	// Verify the context was modified correctly.
	assert.Equal(t, "/opt/venv/bin/python", envContext.PyExecutable)
}

// TestPyExecutablePlugin_ManagerIntegration tests integration with the plugin
// manager.
func TestPyExecutablePlugin_ManagerIntegration(t *testing.T) {
	manager := NewRuntimeEnvPluginManager()
	plugin := NewPyExecutablePlugin()

	// Add the plugin to the manager.
	err := manager.AddPlugin(plugin)
	assert.NoError(t, err)

	// Verify the plugin was registered correctly.
	registeredPlugin, exists := manager.GetPlugin("py_executable")
	assert.True(t, exists)
	assert.NotNil(t, registeredPlugin)
	assert.Equal(t, "py_executable", registeredPlugin.Name)
	assert.Equal(t, RayRuntimeEnvPluginDefaultPriority, registeredPlugin.Priority)
}

// TestPyExecutablePlugin_CreateForPluginIfNeeded tests the
// CreateForPluginIfNeeded function.
func TestPyExecutablePlugin_CreateForPluginIfNeeded(t *testing.T) {
	plugin := NewPyExecutablePlugin()
	runtimeEnv := &RuntimeEnv{FieldPyExecutable: "/test/python"}
	envContext := &RuntimeEnvContext{}
	uriCache := NewURICache()

	createCtx := &PluginCreateContext{
		Ctx:        context.Background(),
		RuntimeEnv: runtimeEnv,
		Plugin:     plugin,
		URICache:   uriCache,
		Context:    envContext,
	}

	err := CreateForPluginIfNeeded(createCtx)
	assert.NoError(t, err)

	// Verify the context was modified correctly.
	assert.Equal(t, "/test/python", envContext.PyExecutable)
}

// TestPyExecutablePlugin_EmptyRuntimeEnv tests the empty runtime environment
// case.
func TestPyExecutablePlugin_EmptyRuntimeEnv(t *testing.T) {
	plugin := NewPyExecutablePlugin()
	runtimeEnv := &RuntimeEnv{}
	envContext := &RuntimeEnvContext{PyExecutable: "/default/python"}

	// When runtimeEnv has no py_executable field, CreateForPluginIfNeeded
	// should return without touching the context.
	createCtx := &PluginCreateContext{
		Ctx:        context.Background(),
		RuntimeEnv: runtimeEnv,
		Plugin:     plugin,
		URICache:   NewURICache(),
		Context:    envContext,
	}

	err := CreateForPluginIfNeeded(createCtx)
	assert.NoError(t, err)

	// Verify the context was left unmodified.
	assert.Equal(t, "/default/python", envContext.PyExecutable)
}
