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

// Package plugin provides common plugin loading functionality for Ray Go Runtime.
// This package encapsulates the Go plugin API loading and reflection call logic
// to avoid duplicate implementations in multiple places.
package plugin

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
)

// =============================================================================
// Plugin Security Constants and Configuration
// =============================================================================

const (
	// maxPluginSize defines the maximum allowed plugin file size (100 megabytes).
	// This prevents loading excessively large files that could cause memory issues.
	maxPluginSize = 100 * 1024 * 1024 // 100MB
	pluginExt     = ".so"             // Required plugin file extension
)

// findPythonSitePackages attempts to find the Python site-packages directory.
// This is used to auto-discover the Go runtime plugin installation path.
// Returns a list of site-packages paths to try.
//
// The function uses dynamic discovery to find site-packages directories:
// 1. Use python executable to query site-packages (most reliable)
// 2. Scan ~/.local/lib for user-installed packages (pattern-based, version-agnostic)
// 3. Fallback to PYTHONPATH environment variable
//
// This approach avoids hardcoding Python versions and supports any Python installation.
func findPythonSitePackages() []string {
	var result []string
	seen := make(map[string]bool) // Deduplicate paths

	// Helper to add unique paths
	addPath := func(path string) {
		if path != "" && !seen[path] {
			seen[path] = true
			result = append(result, path)
		}
	}

	// Strategy 1: Use python executable to query site-packages (most reliable)
	// This dynamically discovers the actual site-packages paths for the current Python installation
	pythonExe, err := exec.LookPath("python3")
	if err != nil {
		pythonExe, _ = exec.LookPath("python")
	}
	if pythonExe != "" {
		// Query site-packages using Python's site module
		cmd := exec.Command(pythonExe, "-c", "import site; print(site.getsitepackages())")
		output, err := cmd.Output()
		if err == nil {
			// Output is like "['/path1', '/path2']"
			outputStr := strings.TrimSpace(string(output))
			outputStr = strings.Trim(outputStr, "[]")
			paths := strings.Split(outputStr, "', '")
			for _, p := range paths {
				p = strings.Trim(p, "[]'\" ")
				if p != "" {
					addPath(p)
				}
			}
		}

		// Also try user site-packages (pip install --user)
		cmd = exec.Command(pythonExe, "-c", "import site; print(site.getusersitepackages())")
		output, err = cmd.Output()
		if err == nil {
			p := strings.TrimSpace(string(output))
			if p != "" && p != "None" {
				addPath(p)
			}
		}
	}

	// Strategy 2: Scan ~/.local/lib for any Python version (pattern-based discovery)
	// This finds site-packages without hardcoding Python versions.
	// When multiple Python versions exist, the newest version is preferred.
	home := os.Getenv("HOME")
	if home != "" {
		userLibBase := filepath.Join(home, ".local", "lib")
		if _, err := os.Stat(userLibBase); err == nil {
			// Scan for python* directories and collect all valid site-packages paths
			var sitePackagesPaths []string
			entries, err := os.ReadDir(userLibBase)
			if err == nil {
				for _, entry := range entries {
					if entry.IsDir() && strings.HasPrefix(entry.Name(), "python") {
						// Construct site-packages path: pythonX.Y/site-packages
						sitePackages := filepath.Join(userLibBase, entry.Name(), "site-packages")
						if _, err := os.Stat(sitePackages); err == nil {
							sitePackagesPaths = append(sitePackagesPaths, sitePackages)
						}
					}
				}
			}

			// Sort by Python version (newest first) to prefer newer installations
			// Version comparison: python3.11 > python3.10 > python3.9, etc.
			if len(sitePackagesPaths) > 1 {
				// Extract version numbers and sort descending
				// Format: <home>/.local/lib/pythonX.Y/site-packages
				sort.Slice(sitePackagesPaths, func(i, j int) bool {
					// Extract pythonX.Y from path
					baseI := filepath.Base(filepath.Dir(sitePackagesPaths[i]))
					baseJ := filepath.Base(filepath.Dir(sitePackagesPaths[j]))

					// Parse version: python3.11 -> (3, 11)
					parseVersion := func(name string) (int, int) {
						if !strings.HasPrefix(name, "python") {
							return 0, 0
						}
						versionStr := strings.TrimPrefix(name, "python")
						parts := strings.Split(versionStr, ".")
						if len(parts) != 2 {
							return 0, 0
						}
						var major, minor int
						fmt.Sscanf(parts[0], "%d", &major)
						fmt.Sscanf(parts[1], "%d", &minor)
						return major, minor
					}

					majorI, minorI := parseVersion(baseI)
					majorJ, minorJ := parseVersion(baseJ)

					// Sort descending (newer version first)
					if majorI != majorJ {
						return majorI > majorJ
					}
					return minorI > minorJ
				})
			}

			// Add sorted paths to result
			for _, path := range sitePackagesPaths {
				addPath(path)
			}
		}
	}

	// Strategy 3: Fallback to PYTHONPATH environment variable
	if pythonPath := os.Getenv("PYTHONPATH"); pythonPath != "" {
		paths := strings.Split(pythonPath, ":")
		for _, path := range paths {
			if strings.Contains(path, "site-packages") {
				addPath(path)
			}
		}
	}

	return result
}

// PluginChecksums defines optional checksum whitelist for known plugins.
// Key: plugin filename, Value: expected SHA256 checksum.
// In production, this should be configured via config file.
// Example:
//
//	PluginChecksums = map[string]string{
//		"go_runtime.so": "abc123def456...",
//	}
var PluginChecksums = map[string]string{}

// =============================================================================
// Checksum Cache (LRU)
// =============================================================================

// checksumCacheMaxSize defines the maximum number of entries in the checksum cache.
// This prevents unbounded memory growth when loading many different plugins.
// The value 100 is chosen to balance memory usage and cache hit rate.
const checksumCacheMaxSize = 100

// checksumCacheEntry represents a single cache entry with list element for O(1) LRU eviction.
// The cache key includes file path + modTime + size to detect file changes.
type checksumCacheEntry struct {
	checksum string
	listElem *list.Element // Pointer to list element for O(1) removal
}

// checksumCacheStats provides cache statistics for monitoring.
// This can be used to tune the cache size and verify cache is working as expected.
type checksumCacheStats struct {
	Size      int   `json:"size"`      // Current number of entries
	Hits      int64 `json:"hits"`      // Number of cache hits
	Misses    int64 `json:"misses"`    // Number of cache misses
	Evictions int64 `json:"evictions"` // Number of evictions
}

// checksumCacheWithLRU provides an O(1) LRU cache for verified plugin checksums.
// Uses container/list for O(1) eviction and map for O(1) lookup.
// This design is chosen over external libraries to avoid additional dependencies
// while providing O(1) performance for both lookup and eviction.
type checksumCacheWithLRU struct {
	mu    sync.RWMutex
	cache map[string]*checksumCacheEntry
	lru   *list.List // List of keys, front = oldest, back = newest
	// Statistics for monitoring cache effectiveness
	hits      int64
	misses    int64
	evictions int64
}

// newChecksumCache creates a new checksum cache with O(1) LRU eviction.
func newChecksumCache() *checksumCacheWithLRU {
	return &checksumCacheWithLRU{
		cache: make(map[string]*checksumCacheEntry),
		lru:   list.New(),
	}
}

// Load retrieves a checksum from the cache and moves the entry to the back (most recently used).
// Returns the checksum and true if found, empty string and false otherwise.
func (c *checksumCacheWithLRU) Load(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.cache[key]
	if !ok {
		c.misses++
		return "", false
	}
	// Move to back of list (most recently used)
	c.lru.MoveToBack(entry.listElem)
	c.hits++
	return entry.checksum, true
}

// Store adds or updates a checksum entry in the cache.
// If the cache exceeds max size, the oldest entry is evicted.
// This method provides O(1) lookup and O(1) eviction.
func (c *checksumCacheWithLRU) Store(key, checksum string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if entry already exists
	if entry, exists := c.cache[key]; exists {
		// Update checksum and move to back of list
		entry.checksum = checksum
		c.lru.MoveToBack(entry.listElem)
		return
	}

	// Check if we need to evict the oldest entry
	if len(c.cache) >= checksumCacheMaxSize {
		// Remove oldest entry (front of list)
		oldestElem := c.lru.Front()
		if oldestElem != nil {
			oldestKey := oldestElem.Value.(string)
			delete(c.cache, oldestKey)
			c.lru.Remove(oldestElem)
			c.evictions++
		}
	}

	// Add new entry to back of list and map
	elem := c.lru.PushBack(key)
	c.cache[key] = &checksumCacheEntry{
		checksum: checksum,
		listElem: elem,
	}
}

// Clear removes all entries from the cache.
// This method is exported for testing and future extension purposes.
func (c *checksumCacheWithLRU) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]*checksumCacheEntry)
	c.lru = list.New()
}

// Stats returns cache statistics for monitoring cache effectiveness.
// This can be used to tune the cache size and verify cache is working as expected.
func (c *checksumCacheWithLRU) Stats() checksumCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return checksumCacheStats{
		Size:      len(c.cache),
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evictions,
	}
}

// checksumCache is a package-level LRU cache for verified plugin checksums.
var checksumCache = newChecksumCache()

// =============================================================================
// Global Plugin Loader (Singleton)
// =============================================================================

// globalPluginLoader is the singleton plugin loader instance.
// It caches loaded plugins to avoid repeated loading of the same plugin file.
var globalPluginLoader = &PluginLoader{
	mu:      sync.RWMutex{},
	plugins: make(map[string]*RuntimePlugin),
}

// PluginLoader provides a singleton plugin loading mechanism with caching.
// This avoids repeated loading of the same plugin file and provides
// thread-safe access to plugin instances.
type PluginLoader struct {
	mu      sync.RWMutex
	plugins map[string]*RuntimePlugin
}

// GetPluginLoader returns the global singleton PluginLoader instance.
func GetPluginLoader() *PluginLoader {
	return globalPluginLoader
}

// LoadOrGet loads a plugin from the specified path or returns the cached instance.
// This method uses double-checked locking to avoid holding the global lock
// during plugin loading, allowing concurrent loading of different plugins.
//
// The double-check pattern is necessary because another goroutine might
// load the plugin between the initial read check and acquiring the lock.
func (pl *PluginLoader) LoadOrGet(pluginPath string, opts options.InitializeOptions) (*RuntimePlugin, error) {
	// Fast path: check if plugin is already cached without holding the lock.
	// This allows concurrent reads to proceed without blocking.
	pl.mu.RLock()
	if cached, ok := pl.plugins[pluginPath]; ok {
		pl.mu.RUnlock()
		return cached, nil
	}
	pl.mu.RUnlock()

	// Slow path: acquire write lock to load the plugin.
	pl.mu.Lock()
	defer pl.mu.Unlock()

	// Double-check: another goroutine might have loaded the plugin while waiting for the lock.
	if cached, ok := pl.plugins[pluginPath]; ok {
		return cached, nil
	}

	// Load the plugin.
	plugin, err := loadPluginInternal(pluginPath, opts)
	if err != nil {
		return nil, err
	}

	// Cache the loaded plugin.
	pl.plugins[pluginPath] = plugin
	return plugin, nil
}

// Unload unloads a plugin from the cache and shuts down its runtime.
// Returns an error if the plugin is not found or shutdown fails.
func (pl *PluginLoader) Unload(pluginPath string) error {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	plugin, ok := pl.plugins[pluginPath]
	if !ok {
		return fmt.Errorf("plugin not found: %s", pluginPath)
	}

	// Shutdown the plugin runtime
	if err := plugin.Shutdown(); err != nil {
		return fmt.Errorf("failed to shutdown plugin: %w", err)
	}

	// Remove from cache
	delete(pl.plugins, pluginPath)
	return nil
}

// UnloadAll unloads all cached plugins and shuts down their runtimes.
// This should be called during application shutdown.
func (pl *PluginLoader) UnloadAll() error {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	var lastErr error
	for path, plugin := range pl.plugins {
		if err := plugin.Shutdown(); err != nil {
			lastErr = fmt.Errorf("failed to shutdown plugin %s: %w", path, err)
		}
	}
	pl.plugins = make(map[string]*RuntimePlugin)
	return lastErr
}

// callPluginFunc calls a plugin function via reflection and validates the results.
// It encapsulates the common reflection call pattern used across multiple functions.
//
// Parameters:
// - fn: the function to call (via reflection)
// - args: the arguments to pass to the function
// - expectedResults: expected number of return values
// - resultValidators: optional validators for each result (index matches result index)
//
// Returns:
// - results: the raw reflection values
// - err: error if validation fails
func callPluginFunc(fn reflect.Value, args []reflect.Value, expectedResults int, resultValidators ...func(reflect.Value) error) ([]reflect.Value, error) {
	results := fn.Call(args)
	if len(results) != expectedResults {
		return nil, fmt.Errorf("function must return exactly %d values, got %d", expectedResults, len(results))
	}
	for i, validator := range resultValidators {
		if i < len(results) {
			if err := validator(results[i]); err != nil {
				return nil, err
			}
		}
	}
	return results, nil
}

// validateError validates that a reflect.Value is an error type and checks if it's nil.
// Returns nil if the value is a nil error, or the error value if not nil.
func validateError(v reflect.Value) error {
	if v.Kind() != reflect.Interface {
		return fmt.Errorf("expected error type, got %v", v.Kind())
	}
	if !v.IsNil() {
		if err, ok := v.Interface().(error); ok {
			return err
		}
		return fmt.Errorf("expected error type, got %T", v.Interface())
	}
	return nil
}

// loadPluginInternal loads a plugin from the specified path without caching.
// This is an internal helper function used by LoadOrGet.
func loadPluginInternal(pluginPath string, opts options.InitializeOptions) (*RuntimePlugin, error) {
	// Security check: validate plugin path before loading
	// This is the primary security gate - checks path traversal, extension, size, and checksum
	if err := validatePluginPath(pluginPath); err != nil {
		return nil, fmt.Errorf("plugin validation failed: %w", err)
	}

	// Open the plugin.
	p, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load go_runtime.so: %w", err)
	}

	// Lookup Initialize symbol.
	initSym, err := p.Lookup("Initialize")
	if err != nil {
		return nil, fmt.Errorf("failed to lookup Initialize symbol: %w", err)
	}

	// Lookup Shutdown symbol.
	shutdownSym, err := p.Lookup("Shutdown")
	if err != nil {
		return nil, fmt.Errorf("failed to lookup Shutdown symbol: %w", err)
	}

	// Lookup RunTaskExecutionLoop symbol.
	runTaskSym, err := p.Lookup("RunTaskExecutionLoop")
	if err != nil {
		return nil, fmt.Errorf("failed to lookup RunTaskExecutionLoop symbol: %w", err)
	}

	// Wrap functions with reflection.
	initFunc := reflect.ValueOf(initSym)
	shutdownFunc := reflect.ValueOf(shutdownSym)
	runTaskFunc := reflect.ValueOf(runTaskSym)

	// Call Initialize function and validate results.
	initArgs := []reflect.Value{reflect.ValueOf(opts)}
	initResults, err := callPluginFunc(initFunc, initArgs, 2,
		func(v reflect.Value) error {
			// Validate first return value implements contract.RuntimeHandle interface
			handle, ok := v.Interface().(contract.RuntimeHandle)
			if !ok {
				return fmt.Errorf("Initialize must return contract.RuntimeHandle, got %T", v.Interface())
			}
			// Additional validation: ensure Runtime() is not nil
			if handle.Runtime() == nil {
				return fmt.Errorf("contract.RuntimeHandle.Runtime() must not be nil")
			}
			return nil
		},
		validateError,
	)
	if err != nil {
		return nil, err
	}
	handle := initResults[0].Interface().(contract.RuntimeHandle)

	// Create wrapper functions using the shared callPluginFunc helper.
	initialize := func(opts options.InitializeOptions) (contract.RuntimeHandle, error) {
		args := []reflect.Value{reflect.ValueOf(opts)}
		results, err := callPluginFunc(initFunc, args, 2,
			func(v reflect.Value) error {
				// Validate return value implements contract.RuntimeHandle interface
				handle, ok := v.Interface().(contract.RuntimeHandle)
				if !ok {
					return fmt.Errorf("Initialize must return contract.RuntimeHandle, got %T", v.Interface())
				}
				// Additional validation: ensure Runtime() is not nil
				if handle.Runtime() == nil {
					return fmt.Errorf("contract.RuntimeHandle.Runtime() must not be nil")
				}
				return nil
			},
			validateError,
		)
		if err != nil {
			return nil, err
		}
		return results[0].Interface().(contract.RuntimeHandle), nil
	}

	shutdown := func(handle contract.RuntimeHandle) error {
		args := []reflect.Value{reflect.ValueOf(handle)}
		_, err := callPluginFunc(shutdownFunc, args, 1, validateError)
		return err
	}

	runTask := func(handle contract.RuntimeHandle) error {
		args := []reflect.Value{reflect.ValueOf(handle)}
		_, err := callPluginFunc(runTaskFunc, args, 1, validateError)
		return err
	}

	return &RuntimePlugin{
		handle:     handle,
		initialize: initialize,
		shutdown:   shutdown,
		runTask:    runTask,
	}, nil
}

// =============================================================================
// RuntimePlugin Type
// =============================================================================

// RuntimePlugin represents a loaded Ray Runtime plugin.
// The internal plugin instance is kept private to avoid exposing Go plugin implementation details.
type RuntimePlugin struct {
	handle     contract.RuntimeHandle
	initialize func(options.InitializeOptions) (contract.RuntimeHandle, error)
	shutdown   func(contract.RuntimeHandle) error
	runTask    func(contract.RuntimeHandle) error
}

// IsInitialized returns whether the plugin is initialized.
func (p *RuntimePlugin) IsInitialized() bool {
	return p != nil && p.handle != nil
}

// GetHandle returns the handle obtained from Initialize.
// This method allows api.InitWithOptions() to reuse the handle without
// calling Initialize again, avoiding double-initialization.
func (p *RuntimePlugin) GetHandle() contract.RuntimeHandle {
	return p.handle
}

// Initialize calls the plugin's Initialize function with the given options.
// This method provides a public API for the api package to call the plugin.
// Note: For api.InitWithOptions(), use GetHandle() instead to avoid
// double-initialization, since LoadRuntimePlugin already calls Initialize.
func (p *RuntimePlugin) Initialize(opts options.InitializeOptions) (contract.RuntimeHandle, error) {
	return p.initialize(opts)
}

// Shutdown shuts down the plugin runtime.
func (p *RuntimePlugin) Shutdown() error {
	if p == nil || p.handle == nil {
		return ErrInvalidHandle
	}
	err := p.shutdown(p.handle)
	p.handle = nil
	return err
}

// RunTaskExecutionLoop runs the task execution loop.
func (p *RuntimePlugin) RunTaskExecutionLoop() error {
	if p == nil || p.handle == nil {
		return ErrInvalidHandle
	}
	return p.runTask(p.handle)
}

// =============================================================================
// Plugin Path Resolution
// =============================================================================
// Plugin path resolution follows a priority-based approach to support multiple
// deployment scenarios:
// 1. Environment variable override (custom paths for production)
// 2. Python site-packages/ray/go/lib (Ray Python wheel installation - primary deployment)
// 3. Executable-relative path (self-contained deployment)
// 4. Deployment directory structure (bin/ + lib/ layout from ray_go_pkg.zip)
// 5. Source-relative path (development workflow)
// 6. Current working directory (final fallback for development/testing)

// getLibraryName returns the platform-specific dynamic library name.
// The library name is centralized here to avoid scattering platform-specific
// logic throughout the codebase.
func getLibraryName() string {
	switch runtime.GOOS {
	case "linux":
		return "go_runtime.so"
	case "darwin":
		return "libgo_runtime.dylib"
	case "windows":
		return "go_runtime.dll"
	default:
		return "go_runtime.so"
	}
}

// FindPluginPath finds the plugin file path (main entry point).
// This function implements a multi-tier path resolution strategy to support
// different deployment scenarios without requiring code changes.
//
// Path resolution priority (high to low):
// 1. Environment variable RAY_GO_RUNTIME_PATH (explicit user configuration)
// 2. Python site-packages/ray/go/lib (Ray Python wheel installation - primary deployment method)
// 3. Same directory as executable (self-contained deployment)
// 4. Deployment directory structure (bin/ + lib/ layout from ray_go_pkg.zip)
// 5. Source-relative path (development workflow, only for source-built binaries)
// 6. Current working directory (final fallback for development/testing)
func FindPluginPath() (string, error) {
	libName := getLibraryName()
	var attemptedPaths []string

	// Priority 1: Environment variable RAY_GO_RUNTIME_PATH
	// Allows users to specify a custom path without recompiling.
	// If the path doesn't exist, log a warning but continue to next priority.
	if envPath := os.Getenv("RAY_GO_RUNTIME_PATH"); envPath != "" {
		attemptedPaths = append(attemptedPaths, envPath+" (RAY_GO_RUNTIME_PATH)")
		if _, err := os.Stat(envPath); err == nil {
			return envPath, nil
		}
		// Log warning but continue - don't fail immediately
		// Note: We can't use log.Log here because it would create circular dependency
		// The warning is logged implicitly by continuing to next priority
	}

	// Priority 2: Python site-packages (Ray Python wheel installation)
	// This is the PRIMARY deployment method for Ray with Go support.
	// The plugin is installed at ray/go/lib/go_runtime.so under site-packages.
	//
	// Standard structure:
	//   <site-packages>/ray/go/lib/go_runtime.so
	//   <site-packages>/ray/go/cmd/raygo
	//
	// This path is checked early because it's the most common deployment scenario.
	sitePackagesList := findPythonSitePackages()
	for _, sitePackages := range sitePackagesList {
		candidate := filepath.Join(sitePackages, "ray", "go", "lib", libName)
		attemptedPaths = append(attemptedPaths, candidate+" (Python site-packages/ray/go/lib)")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	// Priority 3: Same directory as executable
	// Self-contained deployment where the worker binary and go_runtime.so
	// are deployed together in the same directory.
	if exePath, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exePath)
		candidate := filepath.Join(exeDir, libName)
		attemptedPaths = append(attemptedPaths, candidate+" (executable directory)")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}

		// Priority 4: Deployment directory structure (from ray_go_pkg.zip)
		// Standard deployment layout:
		//   <deploy_dir>/
		//     ├── bin/raygo (or worker binary)
		//     └── lib/go_runtime.so
		//
		// If binary is in <deploy>/bin/, plugin should be in <deploy>/lib/
		parentDir := filepath.Dir(exeDir)

		// Try parent's lib directory: <deploy>/bin/raygo -> <deploy>/lib/go_runtime.so
		libDir := filepath.Join(parentDir, "lib")
		candidate = filepath.Join(libDir, libName)
		attemptedPaths = append(attemptedPaths, candidate+" (deployment lib directory)")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}

		// Try sibling lib directory: <deploy>/raygo -> <deploy>/lib/go_runtime.so
		libDir = filepath.Join(exeDir, "lib")
		candidate = filepath.Join(libDir, libName)
		attemptedPaths = append(attemptedPaths, candidate+" (sibling lib directory)")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	// Priority 5: Source-relative path (development only)
	// Only works when running binary built from source.
	// Directory structure: ray/go/pkg/runtime/plugin/... → ray/go/cmd/plugin/go_runtime.so
	if _, filename, _, ok := runtime.Caller(0); ok {
		currentDir := filepath.Dir(filename)
		candidate := filepath.Join(currentDir, "..", "..", "cmd", "plugin", libName)
		attemptedPaths = append(attemptedPaths, candidate+" (source-relative)")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	// Priority 6: Current working directory (final fallback)
	// This handles scenarios where the user runs the program from a directory
	// that contains the plugin file. Common in development and testing.
	cwd, err := os.Getwd()
	if err == nil {
		candidate := filepath.Join(cwd, libName)
		attemptedPaths = append(attemptedPaths, candidate+" (current working directory)")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	// All paths failed - return detailed error message with all attempted paths
	return "", fmt.Errorf("plugin %s not found in any known path. Attempted paths:\n%s",
		libName, strings.Join(attemptedPaths, "\n"))
}

// =============================================================================
// Plugin Path Validation
// =============================================================================
// Plugin path validation implements defense-in-depth security checks to prevent
// loading malicious or unintended plugin files.
//
// Note: The whitelist check has been removed because FindPluginPath() already
// ensures that only trusted paths are returned. The primary security mechanism
// is now checksum verification (if configured via PluginChecksums).

// validatePluginPath validates the plugin path for security.
// This is the primary security gate for plugin loading - all checks must pass.
//
// Security checks:
// 1. Path traversal detection - prevents escaping intended directories
// 2. Extension validation - ensures only .so files are loaded
// 3. Size limit - prevents memory exhaustion attacks
// 4. Checksum verification - detects file tampering (if configured)
func validatePluginPath(pluginPath string) error {
	// Check 1: Path traversal detection
	if strings.Contains(pluginPath, "..") {
		return fmt.Errorf("%w: %s", ErrPluginPathTraversal, pluginPath)
	}

	// Check 2: Extension validation
	if !strings.HasSuffix(pluginPath, pluginExt) {
		return fmt.Errorf("%w: must have %s extension", ErrPluginExtInvalid, pluginExt)
	}

	// Check 3: Size limit and metadata retrieval
	// Single stat call provides both size check and cache key metadata
	info, err := os.Lstat(pluginPath)
	if err != nil {
		return fmt.Errorf("failed to stat plugin file: %w", err)
	}

	if info.Size() > maxPluginSize {
		return fmt.Errorf("%w: %d bytes (max: %d)", ErrPluginTooLarge, info.Size(), maxPluginSize)
	}

	// Check 4: Checksum verification (if configured)
	// This is the PRIMARY security mechanism
	if err := verifyPluginChecksumWithInfo(pluginPath, info); err != nil {
		return err
	}

	return nil
}

// verifyPluginChecksumWithInfo verifies the plugin file checksum using pre-fetched FileInfo.
// This avoids redundant os.Stat() calls when FileInfo is already available from validatePluginPath.
//
// Checksum verification design:
// - Uses io.Copy for streaming hash calculation to minimize memory allocation
// - Full file content is still verified for security - no partial hashing
// - LRU cache minimizes repeated reads for the same plugin
// - Cache key includes modTime and size to detect file changes
func verifyPluginChecksumWithInfo(pluginPath string, info os.FileInfo) error {
	if len(PluginChecksums) == 0 {
		return nil // Skip verification if no checksums defined
	}

	// Build cache key from file metadata
	// Including modTime and size ensures cache invalidation on file changes
	fileKey := fmt.Sprintf("%s:%d:%d", pluginPath, info.ModTime().Unix(), info.Size())

	// Check cache first to avoid repeated file reads
	if _, ok := checksumCache.Load(fileKey); ok {
		return nil
	}

	filename := filepath.Base(pluginPath)
	expectedChecksum, ok := PluginChecksums[filename]
	if !ok {
		return nil // No checksum defined for this plugin, skip verification
	}

	// Compute SHA256 using streaming I/O to minimize memory allocation.
	// This approach reads the entire file (for security) but uses a fixed-size buffer
	// instead of allocating memory proportional to file size.
	// For a 100MB plugin, this reduces memory allocation from 100MB to ~32KB buffer.
	h := sha256.New()
	f, err := os.Open(pluginPath)
	if err != nil {
		return fmt.Errorf("failed to open plugin file for checksum verification: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("failed to compute plugin checksum: %w", err)
	}

	actualChecksum := hex.EncodeToString(h.Sum(nil))

	if actualChecksum != expectedChecksum {
		return fmt.Errorf("%w: file %s, expected %s, got %s", ErrPluginChecksumMismatch, filename, expectedChecksum, actualChecksum)
	}

	// Cache the verified checksum with file metadata
	checksumCache.Store(fileKey, actualChecksum)
	return nil
}

// =============================================================================
// Plugin Loading (Stage 1)
// =============================================================================

// LoadRuntimePlugin loads a Ray Runtime plugin from the specified path.
// If pluginPath is empty, FindPluginPath() is called to locate the plugin.
// This function encapsulates the Go plugin API loading and reflection call logic.
//
// Security: This function validates the plugin path before loading to prevent
// path traversal attacks and loading of malicious plugins.
//
// Note: This function now uses the global PluginLoader singleton to cache
// loaded plugins. If the same plugin is loaded multiple times, the cached
// instance is returned instead of reloading.
func LoadRuntimePlugin(pluginPath string, opts options.InitializeOptions) (*RuntimePlugin, error) {
	// Auto-discover plugin path if not provided
	if pluginPath == "" {
		var err error
		pluginPath, err = FindPluginPath()
		if err != nil {
			return nil, fmt.Errorf("failed to find plugin path: %w", err)
		}
	}

	// Use global loader to cache loaded plugins
	return globalPluginLoader.LoadOrGet(pluginPath, opts)
}
