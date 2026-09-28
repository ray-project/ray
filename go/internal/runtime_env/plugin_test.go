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
	"os"
	"testing"
)

// mockPlugin is a mock plugin used for testing.
type mockPlugin struct {
	name          string
	priority      int
	validateErr   error
	getURIsFunc   func(*RuntimeEnv) []string
	createFunc    func(context.Context, string, *RuntimeEnv, *RuntimeEnvContext) (int64, error)
	modifyCtxFunc func([]string, *RuntimeEnv, *RuntimeEnvContext) error
	deleteURIFunc func(string) int64
}

// MockPluginOption is the option function type for configuring a mockPlugin.
type MockPluginOption func(*mockPlugin)

// newMockPlugin creates a new mockPlugin instance with optional configuration.
// name is the plugin name, priority is the plugin priority, and opts are
// optional configuration options.
func newMockPlugin(name string, priority int, opts ...MockPluginOption) *mockPlugin {
	plugin := &mockPlugin{
		name:     name,
		priority: priority,
	}
	for _, opt := range opts {
		opt(plugin)
	}
	return plugin
}

// WithValidateErr sets the validation error.
func WithValidateErr(err error) MockPluginOption {
	return func(p *mockPlugin) {
		p.validateErr = err
	}
}

// WithGetURIsFunc sets the GetURIs callback function.
func WithGetURIsFunc(fn func(*RuntimeEnv) []string) MockPluginOption {
	return func(p *mockPlugin) {
		p.getURIsFunc = fn
	}
}

// WithCreateFunc sets the Create callback function.
func WithCreateFunc(fn func(context.Context, string, *RuntimeEnv, *RuntimeEnvContext) (int64, error)) MockPluginOption {
	return func(p *mockPlugin) {
		p.createFunc = fn
	}
}

// WithModifyCtxFunc sets the ModifyContext callback function.
func WithModifyCtxFunc(fn func([]string, *RuntimeEnv, *RuntimeEnvContext) error) MockPluginOption {
	return func(p *mockPlugin) {
		p.modifyCtxFunc = fn
	}
}

// WithDeleteURIFunc sets the DeleteURI callback function.
func WithDeleteURIFunc(fn func(string) int64) MockPluginOption {
	return func(p *mockPlugin) {
		p.deleteURIFunc = fn
	}
}

func (m *mockPlugin) Name() string {
	return m.name
}

func (m *mockPlugin) Priority() int {
	return m.priority
}

func (m *mockPlugin) Validate(runtimeEnv *RuntimeEnv) error {
	return m.validateErr
}

func (m *mockPlugin) GetURIs(runtimeEnv *RuntimeEnv) []string {
	if m.getURIsFunc != nil {
		return m.getURIsFunc(runtimeEnv)
	}
	return []string{}
}

func (m *mockPlugin) Create(ctx context.Context, uri string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) (int64, error) {
	if m.createFunc != nil {
		return m.createFunc(ctx, uri, runtimeEnv, context)
	}
	return 0, nil
}

func (m *mockPlugin) ModifyContext(uris []string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) error {
	if m.modifyCtxFunc != nil {
		return m.modifyCtxFunc(uris, runtimeEnv, context)
	}
	return nil
}

func (m *mockPlugin) DeleteURI(uri string) int64 {
	if m.deleteURIFunc != nil {
		return m.deleteURIFunc(uri)
	}
	return 0
}

// TestNewRuntimeEnvPluginManager tests creating a new plugin manager.
func TestNewRuntimeEnvPluginManager(t *testing.T) {
	// Clear the environment variable.
	os.Unsetenv(RayRuntimeEnvPluginsEnvVar)

	manager := NewRuntimeEnvPluginManager()
	if manager == nil {
		t.Fatal("Expected non-nil manager")
	}
	if manager.plugins == nil {
		t.Fatal("Expected non-nil plugins map")
	}
	if len(manager.plugins) != 0 {
		t.Errorf("Expected empty plugins map, got %d entries", len(manager.plugins))
	}
}

// TestValidatePluginClass tests plugin class validation.
func TestValidatePluginClass(t *testing.T) {
	manager := NewRuntimeEnvPluginManager()

	// Test a nil plugin.
	err := manager.ValidatePluginClass(nil)
	if err == nil {
		t.Error("Expected error for nil plugin")
	}

	// Test a plugin with an empty name.
	emptyNamePlugin := newMockPlugin("", 10)
	err = manager.ValidatePluginClass(emptyNamePlugin)
	if err == nil {
		t.Error("Expected error for plugin with empty name")
	}

	// Test a valid plugin.
	validPlugin := newMockPlugin("TestPlugin", 10)
	err = manager.ValidatePluginClass(validPlugin)
	if err != nil {
		t.Errorf("Unexpected error for valid plugin: %v", err)
	}

	// Add the valid plugin to the manager first.
	err = manager.AddPlugin(validPlugin)
	if err != nil {
		t.Fatalf("Failed to add valid plugin: %v", err)
	}

	// Test a name conflict.
	conflictingPlugin := newMockPlugin("TestPlugin", 20)
	err = manager.ValidatePluginClass(conflictingPlugin)
	if err == nil {
		t.Error("Expected error for conflicting plugin name")
	}
}

// TestValidatePriority tests priority validation.
func TestValidatePriority(t *testing.T) {
	manager := NewRuntimeEnvPluginManager()

	// Test valid priorities.
	tests := []struct {
		priority int
		wantErr  bool
	}{
		{0, false},                              // Minimum value.
		{50, false},                             // Middle value.
		{100, false},                            // Maximum value.
		{-1, true},                              // Below the minimum.
		{101, true},                             // Above the maximum.
		{RayRuntimeEnvPluginMinPriority, false}, // Boundary value.
		{RayRuntimeEnvPluginMaxPriority, false}, // Boundary value.
	}

	for _, tt := range tests {
		err := manager.ValidatePriority(tt.priority)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidatePriority(%d) error = %v, wantErr %v", tt.priority, err, tt.wantErr)
		}
	}
}

// TestAddPlugin tests adding plugins.
func TestAddPlugin(t *testing.T) {
	manager := NewRuntimeEnvPluginManager()

	// Test adding a valid plugin.
	plugin := newMockPlugin("TestPlugin", 10)
	err := manager.AddPlugin(plugin)
	if err != nil {
		t.Errorf("Unexpected error adding plugin: %v", err)
	}

	// Verify the plugin was added.
	if _, exists := manager.plugins["TestPlugin"]; !exists {
		t.Error("Expected plugin to be added")
	}

	// Test adding a plugin with an invalid priority.
	invalidPriorityPlugin := newMockPlugin("InvalidPlugin", 150) // Out of range.
	err = manager.AddPlugin(invalidPriorityPlugin)
	if err == nil {
		t.Error("Expected error for invalid priority")
	}

	// Test adding a plugin with a duplicate name.
	duplicatePlugin := newMockPlugin("TestPlugin", 20)
	err = manager.AddPlugin(duplicatePlugin)
	if err == nil {
		t.Error("Expected error for duplicate plugin name")
	}
}

// TestCreateURICacheForPlugin tests creating a URI cache for a plugin.
func TestCreateURICacheForPlugin(t *testing.T) {
	manager := NewRuntimeEnvPluginManager()

	// Test the default cache size.
	plugin := newMockPlugin("CacheTestPlugin", 10)
	cache := manager.CreateURICacheForPlugin(plugin)
	if cache == nil {
		t.Fatal("Expected non-nil cache")
	}

	// Verify the default size (10GB).
	if cache.maxTotalSizeBytes != DefaultMaxURICacheSizeBytes {
		t.Errorf("Expected default cache size %d, got %d", DefaultMaxURICacheSizeBytes, cache.maxTotalSizeBytes)
	}

	// Test setting the cache size via the environment variable.
	os.Setenv("RAY_RUNTIME_ENV_CacheTestPlugin_CACHE_SIZE_GB", "5")
	defer os.Unsetenv("RAY_RUNTIME_ENV_CacheTestPlugin_CACHE_SIZE_GB")

	cache2 := manager.CreateURICacheForPlugin(plugin)
	expectedSize := int64(5 * 1024 * 1024 * 1024)
	if cache2.maxTotalSizeBytes != expectedSize {
		t.Errorf("Expected cache size %d from env var, got %d", expectedSize, cache2.maxTotalSizeBytes)
	}

	// Test an invalid environment variable value (should fall back to the
	// default).
	os.Setenv("RAY_RUNTIME_ENV_CacheTestPlugin_CACHE_SIZE_GB", "invalid")
	cache3 := manager.CreateURICacheForPlugin(plugin)
	if cache3.maxTotalSizeBytes != DefaultMaxURICacheSizeBytes {
		t.Errorf("Expected default cache size for invalid env var, got %d", cache3.maxTotalSizeBytes)
	}
}

// TestSortedPluginSetupContexts tests sorting plugins by priority.
func TestSortedPluginSetupContexts(t *testing.T) {
	manager := NewRuntimeEnvPluginManager()

	// Add multiple plugins with different priorities.
	plugins := []*mockPlugin{
		newMockPlugin("HighPriority", 5),
		newMockPlugin("LowPriority", 50),
		newMockPlugin("MediumPriority", 25),
		newMockPlugin("HighestPriority", 1),
	}

	for _, p := range plugins {
		err := manager.AddPlugin(p)
		if err != nil {
			t.Fatalf("Failed to add plugin %s: %v", p.name, err)
		}
	}

	contexts := manager.SortedPluginSetupContexts()

	// Verify the sort order (ascending).
	expectedOrder := []string{"HighestPriority", "HighPriority", "MediumPriority", "LowPriority"}
	if len(contexts) != len(expectedOrder) {
		t.Fatalf("Expected %d contexts, got %d", len(expectedOrder), len(contexts))
	}

	for i, expectedName := range expectedOrder {
		if contexts[i].Name != expectedName {
			t.Errorf("Position %d: expected %s, got %s", i, expectedName, contexts[i].Name)
		}
		if contexts[i].Priority != getExpectedPriority(expectedName) {
			t.Errorf("Position %d: expected priority %d, got %d", i, getExpectedPriority(expectedName), contexts[i].Priority)
		}
	}
}

func getExpectedPriority(name string) int {
	switch name {
	case "HighestPriority":
		return 1
	case "HighPriority":
		return 5
	case "MediumPriority":
		return 25
	case "LowPriority":
		return 50
	default:
		return 0
	}
}

// TestCreateForPluginIfNeeded tests creating the plugin environment when
// needed.
func TestCreateForPluginIfNeeded(t *testing.T) {
	ctx := context.Background()

	t.Run("PluginNotInRuntimeEnv", func(t *testing.T) {
		// Test the case where the plugin is not in the runtimeEnv.
		runtimeEnv := &RuntimeEnv{}
		plugin := newMockPlugin("TestPlugin", 10)
		uriCache := NewURICache()
		context := &RuntimeEnvContext{}

		err := CreateForPluginIfNeeded(&PluginCreateContext{
			Ctx:        ctx,
			RuntimeEnv: runtimeEnv,
			Plugin:     plugin,
			URICache:   uriCache,
			Context:    context,
		})
		if err != nil {
			t.Errorf("Unexpected error when plugin not in runtimeEnv: %v", err)
		}
	})

	t.Run("PluginValidationFailed", func(t *testing.T) {
		// Test the case where plugin validation fails.
		runtimeEnv := &RuntimeEnv{"TestPlugin": "some_value"}
		plugin := newMockPlugin("TestPlugin", 10, WithValidateErr(os.ErrNotExist))
		uriCache := NewURICache()
		context := &RuntimeEnvContext{}

		err := CreateForPluginIfNeeded(&PluginCreateContext{
			Ctx:        ctx,
			RuntimeEnv: runtimeEnv,
			Plugin:     plugin,
			URICache:   uriCache,
			Context:    context,
		})
		if err == nil {
			t.Error("Expected error for validation failure")
		}
	})

	t.Run("NoURIs", func(t *testing.T) {
		// Test the case with no URIs.
		runtimeEnv := &RuntimeEnv{"TestPlugin": "some_value"}
		createCalled := false
		plugin := newMockPlugin("TestPlugin", 10, WithCreateFunc(func(ctx context.Context, uri string, re *RuntimeEnv, ctx2 *RuntimeEnvContext) (int64, error) {
			createCalled = true
			if uri != "" {
				t.Errorf("Expected empty URI, got %s", uri)
			}
			return 100, nil
		}))
		uriCache := NewURICache()
		context := &RuntimeEnvContext{}

		err := CreateForPluginIfNeeded(&PluginCreateContext{
			Ctx:        ctx,
			RuntimeEnv: runtimeEnv,
			Plugin:     plugin,
			URICache:   uriCache,
			Context:    context,
		})
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if !createCalled {
			t.Error("Expected Create to be called")
		}
	})

	t.Run("CacheMiss", func(t *testing.T) {
		// Test the cache miss case.
		runtimeEnv := &RuntimeEnv{"TestPlugin": "some_value"}
		createCalled := false
		plugin := newMockPlugin("TestPlugin", 10,
			WithGetURIsFunc(func(re *RuntimeEnv) []string {
				return []string{"test://uri1"}
			}),
			WithCreateFunc(func(ctx context.Context, uri string, re *RuntimeEnv, ctx2 *RuntimeEnvContext) (int64, error) {
				createCalled = true
				if uri != "test://uri1" {
					t.Errorf("Expected URI test://uri1, got %s", uri)
				}
				return 200, nil
			}),
		)
		uriCache := NewURICache()
		context := &RuntimeEnvContext{}

		err := CreateForPluginIfNeeded(&PluginCreateContext{
			Ctx:        ctx,
			RuntimeEnv: runtimeEnv,
			Plugin:     plugin,
			URICache:   uriCache,
			Context:    context,
		})
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if !createCalled {
			t.Error("Expected Create to be called on cache miss")
		}
		if !uriCache.Contains("test://uri1") {
			t.Error("Expected URI to be added to cache")
		}
	})

	t.Run("CacheHit", func(t *testing.T) {
		// Test the cache hit case.
		runtimeEnv := &RuntimeEnv{"TestPlugin": "some_value"}
		createCalled := false
		plugin := newMockPlugin("TestPlugin", 10,
			WithGetURIsFunc(func(re *RuntimeEnv) []string {
				return []string{"test://uri2"}
			}),
			WithCreateFunc(func(ctx context.Context, uri string, re *RuntimeEnv, ctx2 *RuntimeEnvContext) (int64, error) {
				createCalled = true
				return 300, nil
			}),
		)
		// Pre-add to the cache.
		uriCache := NewURICache()
		uriCache.Add("test://uri2", 300)
		context := &RuntimeEnvContext{}

		err := CreateForPluginIfNeeded(&PluginCreateContext{
			Ctx:        ctx,
			RuntimeEnv: runtimeEnv,
			Plugin:     plugin,
			URICache:   uriCache,
			Context:    context,
		})
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if createCalled {
			t.Error("Create should not be called on cache hit")
		}
	})

	t.Run("CreateFailed", func(t *testing.T) {
		// Test the case where Create fails.
		runtimeEnv := &RuntimeEnv{"TestPlugin": "some_value"}
		plugin := newMockPlugin("TestPlugin", 10,
			WithGetURIsFunc(func(re *RuntimeEnv) []string {
				return []string{"test://uri3"}
			}),
			WithCreateFunc(func(ctx context.Context, uri string, re *RuntimeEnv, ctx2 *RuntimeEnvContext) (int64, error) {
				return 0, os.ErrPermission
			}),
		)
		uriCache := NewURICache()
		context := &RuntimeEnvContext{}

		err := CreateForPluginIfNeeded(&PluginCreateContext{
			Ctx:        ctx,
			RuntimeEnv: runtimeEnv,
			Plugin:     plugin,
			URICache:   uriCache,
			Context:    context,
		})
		if err == nil {
			t.Error("Expected error for Create failure")
		}
	})

	t.Run("ModifyContextFailed", func(t *testing.T) {
		// Test the case where ModifyContext fails.
		runtimeEnv := &RuntimeEnv{"TestPlugin": "some_value"}
		plugin := newMockPlugin("TestPlugin", 10, WithModifyCtxFunc(func(uris []string, re *RuntimeEnv, ctx *RuntimeEnvContext) error {
			return os.ErrPermission
		}))
		uriCache := NewURICache()
		context := &RuntimeEnvContext{}

		err := CreateForPluginIfNeeded(&PluginCreateContext{
			Ctx:        ctx,
			RuntimeEnv: runtimeEnv,
			Plugin:     plugin,
			URICache:   uriCache,
			Context:    context,
		})
		if err == nil {
			t.Error("Expected error for ModifyContext failure")
		}
	})
}

// TestPluginInterfaceImplementation tests the plugin interface implementation.
func TestPluginInterfaceImplementation(t *testing.T) {
	// Ensure mockPlugin implements the RuntimeEnvPlugin interface.
	var _ RuntimeEnvPlugin = (*mockPlugin)(nil)
}

// TestPluginSetupContext tests the PluginSetupContext struct.
func TestPluginSetupContext(t *testing.T) {
	plugin := newMockPlugin("TestPlugin", 20)
	uriCache := NewURICache()

	ctx := &PluginSetupContext{
		Name:          plugin.Name(),
		ClassInstance: plugin,
		Priority:      plugin.Priority(),
		URICache:      uriCache,
	}

	if ctx.Name != "TestPlugin" {
		t.Errorf("Expected name TestPlugin, got %s", ctx.Name)
	}
	if ctx.Priority != 20 {
		t.Errorf("Expected priority 20, got %d", ctx.Priority)
	}
	if ctx.URICache != uriCache {
		t.Error("Expected same URI cache instance")
	}
	if ctx.ClassInstance == nil {
		t.Error("Expected non-nil ClassInstance")
	}
}

// TestManagerWithMultiplePlugins tests the manager with multiple plugins.
func TestManagerWithMultiplePlugins(t *testing.T) {
	manager := NewRuntimeEnvPluginManager()

	// Add multiple plugins.
	plugins := []RuntimeEnvPlugin{
		newMockPlugin("Plugin1", 10),
		newMockPlugin("Plugin2", 20),
		newMockPlugin("Plugin3", 30),
	}

	for _, p := range plugins {
		err := manager.AddPlugin(p)
		if err != nil {
			t.Fatalf("Failed to add plugin %s: %v", p.Name(), err)
		}
	}

	// Verify that all plugins were added.
	if len(manager.plugins) != 3 {
		t.Errorf("Expected 3 plugins, got %d", len(manager.plugins))
	}

	// Verify the configuration of each plugin.
	for _, p := range plugins {
		ctx, exists := manager.plugins[p.Name()]
		if !exists {
			t.Errorf("Plugin %s not found in manager", p.Name())
			continue
		}
		if ctx.Priority != p.Priority() {
			t.Errorf("Plugin %s: expected priority %d, got %d", p.Name(), p.Priority(), ctx.Priority)
		}
		if ctx.URICache == nil {
			t.Errorf("Plugin %s: expected non-nil URICache", p.Name())
		}
	}
}

// TestCreateForPluginIfNeededWithNilLogger tests the nil logger case.
func TestCreateForPluginIfNeededWithNilLogger(t *testing.T) {
	ctx := context.Background()
	runtimeEnv := &RuntimeEnv{"TestPlugin": "value"}
	plugin := newMockPlugin("TestPlugin", 10)
	uriCache := NewURICache()
	context := &RuntimeEnvContext{}

	err := CreateForPluginIfNeeded(&PluginCreateContext{
		Ctx:        ctx,
		RuntimeEnv: runtimeEnv,
		Plugin:     plugin,
		URICache:   uriCache,
		Context:    context,
	})
	if err != nil {
		t.Errorf("Unexpected error with nil logger: %v", err)
	}
}

// TestValidatePluginClassWithRealPlugin tests validation with a real plugin.
func TestValidatePluginClassWithRealPlugin(t *testing.T) {
	manager := NewRuntimeEnvPluginManager()

	// Create a complete mock plugin.
	plugin := newMockPlugin("CompletePlugin", 50,
		WithGetURIsFunc(func(re *RuntimeEnv) []string {
			return []string{"uri1", "uri2"}
		}),
		WithCreateFunc(func(ctx context.Context, uri string, re *RuntimeEnv, ctx2 *RuntimeEnvContext) (int64, error) {
			return 1000, nil
		}),
		WithModifyCtxFunc(func(uris []string, re *RuntimeEnv, ctx *RuntimeEnvContext) error {
			return nil
		}),
		WithDeleteURIFunc(func(uri string) int64 {
			return 500
		}),
	)

	err := manager.ValidatePluginClass(plugin)
	if err != nil {
		t.Errorf("Unexpected error validating complete plugin: %v", err)
	}

	err = manager.ValidatePriority(plugin.Priority())
	if err != nil {
		t.Errorf("Unexpected error validating priority: %v", err)
	}
}
