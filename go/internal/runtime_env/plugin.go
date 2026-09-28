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
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"

	"github.com/ray-project/ray/go/internal/common"
	"github.com/ray-project/ray/go/pkg/log"
)

var pluginLogger = log.WithName("runtime-env-plugin")

// RuntimeEnvPlugin is the abstract interface for a runtime environment plugin.
//
// Every plugin must implement this interface.
type RuntimeEnvPlugin interface {
	// Name returns the plugin name.
	Name() string

	// Priority returns the plugin priority.
	Priority() int

	// Validate validates the user-provided runtime environment configuration.
	// It is called when the runtime environment is being set up and should
	// return an error if validation fails.
	Validate(runtimeEnv *RuntimeEnv) error

	// GetURIs returns the list of URIs associated with the plugin.
	GetURIs(runtimeEnv *RuntimeEnv) []string

	// Create creates and installs the runtime environment.
	// It is called by the runtime environment agent during setup. The uri
	// argument can be used as a cache mechanism. It returns the disk space (in
	// bytes) occupied by the plugin installation.
	Create(ctx context.Context, uri string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) (int64, error)

	// ModifyContext modifies the context to change the worker startup behavior.
	// For example, it can prepend a "cd <dir>" command to the worker startup
	// command or add new environment variables.
	ModifyContext(uris []string, runtimeEnv *RuntimeEnv, context *RuntimeEnvContext) error

	// DeleteURI deletes the runtime environment for the given URI and returns
	// the freed space in bytes.
	DeleteURI(uri string) int64
}

// PluginSetupContext holds the plugin's name, instance, priority and URI cache.
type PluginSetupContext struct {
	Name          string
	ClassInstance RuntimeEnvPlugin
	Priority      int
	URICache      *URICache
}

// RuntimeEnvPluginManager loads and manages plugins in the runtime environment
// agent.
type RuntimeEnvPluginManager struct {
	plugins      map[string]*PluginSetupContext
	pluginsMutex sync.RWMutex
}

// NewRuntimeEnvPluginManager creates a new plugin manager.
func NewRuntimeEnvPluginManager() *RuntimeEnvPluginManager {
	manager := &RuntimeEnvPluginManager{
		plugins: make(map[string]*PluginSetupContext),
	}

	// Load the plugin configuration from the environment variable.
	if pluginConfigStr := os.Getenv(RayRuntimeEnvPluginsEnvVar); pluginConfigStr != "" {
		// Parsing JSON and loading third-party custom plugins is not
		// implemented yet.
		log.Log.Info("Plugin config found in environment variable", "config", pluginConfigStr)
	}

	return manager
}

// ValidatePluginClass verifies that a plugin class is valid.
// The type check is automatic: if the argument type does not implement all the
// methods of the RuntimeEnvPlugin interface, the code will not compile.
//
// Therefore, this validation only checks conditions that are determined at
// runtime:
// 1. The plugin instance must not be nil.
// 2. The plugin name must not be empty.
// 3. The plugin name must not conflict with an already registered plugin.
func (m *RuntimeEnvPluginManager) ValidatePluginClass(plugin RuntimeEnvPlugin) error {
	if plugin == nil {
		return fmt.Errorf("invalid runtime env plugin: nil")
	}

	name := plugin.Name()
	if name == "" {
		return fmt.Errorf("no valid name in runtime env plugin: %T", plugin)
	}

	m.pluginsMutex.RLock()
	defer m.pluginsMutex.RUnlock()
	if _, exists := m.plugins[name]; exists {
		return fmt.Errorf("the name of runtime env plugin %T conflicts with existing plugin %s",
			plugin, m.plugins[name].Name)
	}

	return nil
}

// ValidatePriority verifies that a priority is valid.
// The priority should be an integer between 0 and 100.
func (m *RuntimeEnvPluginManager) ValidatePriority(priority int) error {
	if priority < RayRuntimeEnvPluginMinPriority || priority > RayRuntimeEnvPluginMaxPriority {
		return fmt.Errorf("invalid runtime env priority %d, it should be an integer between %d and %d",
			priority, RayRuntimeEnvPluginMinPriority, RayRuntimeEnvPluginMaxPriority)
	}
	return nil
}

// AddPlugin adds a single plugin to the manager and creates its URI cache.
func (m *RuntimeEnvPluginManager) AddPlugin(plugin RuntimeEnvPlugin) error {
	if err := m.ValidatePluginClass(plugin); err != nil {
		return err
	}

	priority := plugin.Priority()
	if err := m.ValidatePriority(priority); err != nil {
		return err
	}

	uriCache := m.CreateURICacheForPlugin(plugin)

	m.pluginsMutex.Lock()
	defer m.pluginsMutex.Unlock()
	m.plugins[plugin.Name()] = &PluginSetupContext{
		Name:          plugin.Name(),
		ClassInstance: plugin,
		Priority:      priority,
		URICache:      uriCache,
	}

	return nil
}

// CreateURICacheForPlugin creates a URI cache for a plugin.
func (m *RuntimeEnvPluginManager) CreateURICacheForPlugin(plugin RuntimeEnvPlugin) *URICache {
	// The default cache size is 10 GB.
	cacheSizeEnvVar := fmt.Sprintf("RAY_RUNTIME_ENV_%s_CACHE_SIZE_GB", plugin.Name())
	size := common.EnvFloat(cacheSizeEnvVar, float64(DefaultMaxURICacheSizeBytes)/(1024*1024*1024))
	cacheSizeBytes := int64((1024 * 1024 * 1024) * size)

	return NewURICache(
		WithDeleteFn(plugin.DeleteURI),
		WithMaxTotalSizeBytes(cacheSizeBytes),
	)
}

// SortedPluginSetupContexts returns the plugin setup contexts sorted by
// priority in ascending order.
func (m *RuntimeEnvPluginManager) SortedPluginSetupContexts() []*PluginSetupContext {
	m.pluginsMutex.RLock()
	defer m.pluginsMutex.RUnlock()
	contexts := slices.SortedFunc(maps.Values(m.plugins), func(a, b *PluginSetupContext) int {
		return a.Priority - b.Priority
	})
	return contexts
}

// GetPlugin returns the plugin with the given name.
func (m *RuntimeEnvPluginManager) GetPlugin(name string) (*PluginSetupContext, bool) {
	m.pluginsMutex.RLock()
	defer m.pluginsMutex.RUnlock()
	plugin, exists := m.plugins[name]
	return plugin, exists
}

// GetPlugins returns all plugins.
func (m *RuntimeEnvPluginManager) GetPlugins() map[string]*PluginSetupContext {
	m.pluginsMutex.RLock()
	defer m.pluginsMutex.RUnlock()
	pluginsCopy := make(map[string]*PluginSetupContext, len(m.plugins))
	for k, v := range m.plugins {
		pluginsCopy[k] = v
	}
	return pluginsCopy
}

// PluginCreateContext wraps the arguments of CreateForPluginIfNeeded to avoid
// argument sprawl.
type PluginCreateContext struct {
	Ctx        context.Context
	RuntimeEnv *RuntimeEnv
	Plugin     RuntimeEnvPlugin
	URICache   *URICache
	Context    *RuntimeEnvContext
}

// CreateForPluginIfNeeded sets up the plugin environment if needed (if it has
// not been set up and is not cached).
func CreateForPluginIfNeeded(createCtx *PluginCreateContext) error {
	ctx := createCtx.Ctx
	runtimeEnv := createCtx.RuntimeEnv
	plugin := createCtx.Plugin
	uriCache := createCtx.URICache
	context := createCtx.Context

	// Check whether the plugin is present in the runtimeEnv.
	name := plugin.Name()

	if runtimeEnv == nil || runtimeEnv.Get(name, nil) == nil {
		return nil
	}

	// Validate the plugin configuration.
	if err := plugin.Validate(runtimeEnv); err != nil {
		return fmt.Errorf("plugin %s validation failed: %w", name, err)
	}

	// Get the URIs.
	uris := plugin.GetURIs(runtimeEnv)

	if len(uris) == 0 {
		log.Log.V(1).Info("No URIs for runtime env plugin; create always without checking the cache",
			"plugin", name)
		if _, err := plugin.Create(ctx, "", runtimeEnv, context); err != nil {
			return fmt.Errorf("plugin %s create failed: %w", name, err)
		}
	} else {
		for _, uri := range uris {
			if !uriCache.Contains(uri) {
				log.Log.V(1).Info("Cache miss for URI", "uri", uri)
				sizeBytes, err := plugin.Create(ctx, uri, runtimeEnv, context)
				if err != nil {
					return fmt.Errorf("plugin %s create failed for URI %s: %w", name, uri, err)
				}
				uriCache.Add(uri, sizeBytes)
			} else {
				log.Log.Info("Runtime env is already installed and will be reused",
					"plugin", name, "uri", uri)
				uriCache.MarkUsed(uri)
			}
		}
	}

	// Modify the context.
	if err := plugin.ModifyContext(uris, runtimeEnv, context); err != nil {
		return fmt.Errorf("plugin %s modifyContext failed: %w", name, err)
	}

	return nil
}
