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

package function

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"

	"github.com/ray-project/ray/go/pkg/log"
)

// Registry is the global function registry.
// This is a singleton that manages function registration and lookup.
// Both driver and worker processes maintain their own independent instances.
var Registry = &FunctionRegistry{
	functions: make(map[string]FunctionEntry),
}

// FunctionRegistry manages the mapping from function descriptors to function implementations.
//
// Design notes:
//   1. Thread-safe using sync.RWMutex for concurrent access.
//   2. Maps string descriptor ("main.goAdd") to FunctionEntry (containing descriptor and function).
//   3. Each process (driver/worker) maintains its own independent registry.
//   4. Functions must be registered before they can be looked up.
//   5. Storing FunctionEntry instead of just interface{} avoids the need for string parsing
//      in ListEntries(), which would otherwise need to reconstruct descriptors from keys.
type FunctionRegistry struct {
	mu        sync.RWMutex
	functions map[string]FunctionEntry
	readonly  bool // Prevent further registration after worker startup
}

// FunctionEntry represents a registered function.
type FunctionEntry struct {
	descriptor *GoFunctionDescriptor
	fn         interface{}
}

// Descriptor returns the function descriptor.
func (e FunctionEntry) Descriptor() *GoFunctionDescriptor {
	return e.descriptor
}

// Function returns the registered function.
func (e FunctionEntry) Function() interface{} {
	return e.fn
}

// Register registers a function with its descriptor.
//
// Parameters:
//   - fn: The function to register (must be a Go function)
//
// Returns:
//   - error: An error if the function is invalid or already registered.
//
// Example:
//
//	func goAdd(x, y int) int { return x + y }
//	err := Registry.Register(goAdd)
//	if err != nil {
//	    log.Fatalf("Failed to register goAdd: %v", err)
//	}
func (r *FunctionRegistry) Register(fn interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.readonly {
		return fmt.Errorf("function registry is read-only, cannot register new functions")
	}

	// Validate that fn is a function
	funcValue := reflect.ValueOf(fn)
	if funcValue.Kind() != reflect.Func {
		return fmt.Errorf("expected a function, got %v", funcValue.Kind())
	}

	// Extract descriptor from the function
	desc := ExtractFunctionDescriptor(fn)

	key := desc.String()
	log.Log.V(2).Info("Register: function registered",
		"key", key, "desc", desc)
	// Store as FunctionEntry to avoid string parsing in ListEntries()
	r.functions[key] = FunctionEntry{
		descriptor: desc,
		fn:         fn,
	}

	return nil
}

// Get retrieves a function by its descriptor.
//
// Parameters:
//   - desc: The function descriptor to look up
//
// Returns:
//   - interface{}: The registered function
//   - error: An error if the function is not registered
func (r *FunctionRegistry) Get(desc *GoFunctionDescriptor) (interface{}, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := desc.String()
	entry, ok := r.functions[key]
	if !ok {
		return nil, fmt.Errorf("function not registered: %s", key)
	}

	return entry.fn, nil
}

// GetByName retrieves a function by its string descriptor.
// This is a convenience method for worker-side lookup.
//
// Parameters:
//   - name: The string descriptor (e.g., "main.goAdd")
//
// Returns:
//   - interface{}: The registered function
//   - error: An error if the function is not registered
func (r *FunctionRegistry) GetByName(name string) (interface{}, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.functions[name]
	if !ok {
		return nil, fmt.Errorf("function not registered: %s", name)
	}

	return entry.fn, nil
}

// List returns a list of all registered function descriptors.
func (r *FunctionRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]string, 0, len(r.functions))
	for key := range r.functions {
		result = append(result, key)
	}
	return result
}

// ListEntries returns all registered functions as FunctionEntry slices.
// Since FunctionEntry is stored directly in the registry (not just the function),
// this method can return entries without needing to parse keys back into descriptors.
func (r *FunctionRegistry) ListEntries() ([]FunctionEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	log.Log.V(2).Info("ListEntries: scanning registry",
		"count", len(r.functions))
	entries := make([]FunctionEntry, 0, len(r.functions))
	for key, entry := range r.functions {
		log.Log.V(2).Info("ListEntries: found entry",
			"key", key, "descriptor", entry.descriptor)
		entries = append(entries, entry)
	}
	return entries, len(entries) > 0
}

// MarkReadonly marks the registry as read-only to prevent further registration.
// This is called after worker startup to prevent accidental registration.
func (r *FunctionRegistry) MarkReadonly() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readonly = true
}

// IsReadonly returns whether the registry is marked as read-only.
func (r *FunctionRegistry) IsReadonly() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.readonly
}

// ExtractFunctionDescriptor extracts the GoFunctionDescriptor from a function.
// This uses reflection and runtime.FuncForPC to determine the function's module and name.
//
// Parameters:
//   - fn: The function to extract descriptor from
//
// Returns:
//   - *GoFunctionDescriptor: The extracted descriptor
//
// Note: This function never returns an error. For invalid inputs or extraction failures,
// it returns a fallback descriptor with "main" as module name and a string representation
// of the function as the function name. This ensures that Remote() and RemoteVoid() can
// always proceed without failing due to descriptor extraction issues.
//
// For functions in the main package, ModuleName is set to "main".
// For functions in other packages, ModuleName is the full module path if extractable.
func ExtractFunctionDescriptor(fn interface{}) *GoFunctionDescriptor {
	// Handle nil input
	if fn == nil {
		return &GoFunctionDescriptor{
			ModuleName:   "main",
			PackagePath:  "",
			FunctionName: "nil",
			methodName:   "",
		}
	}

	funcValue := reflect.ValueOf(fn)
	if funcValue.Kind() != reflect.Func {
		// Return fallback instead of error for non-function types
		// This can happen for plugin-loaded functions or other edge cases
		return &GoFunctionDescriptor{
			ModuleName:   "main",
			PackagePath:  "",
			FunctionName: fmt.Sprintf("%v", fn),
			methodName:   "",
		}
	}

	// Get function name using runtime.FuncForPC
	// This is more reliable than funcValue.Pointer() in CGO environments
	funcPC := funcValue.Pointer()
	funcObj := runtime.FuncForPC(funcPC)
	if funcObj == nil {
		// Fallback: return descriptor with unknown function name
		// This can happen for plugin-loaded functions or dynamically generated functions
		return &GoFunctionDescriptor{
			ModuleName:   "main",
			PackagePath:  "",
			FunctionName: "unknown",
			methodName:   "",
		}
	}

	funcName := funcObj.Name()
	log.Log.V(2).Info("ExtractFunctionDescriptor: extracted funcName",
		"funcName", funcName)

	// Parse function name to extract module, package, and function name
	// Format: module/path.Package.Function or module/path.Function
	moduleName, pkgPath, funcNameOnly, err := parseFunctionName(funcName)
	log.Log.V(2).Info("ExtractFunctionDescriptor: parseFunctionName result",
		"funcName", funcName, "moduleName", moduleName, "pkgPath", pkgPath,
		"funcNameOnly", funcNameOnly, "err", err)
	if err != nil {
		// Fallback: use the full function name as the function name
		// This preserves whatever information we could extract
		return &GoFunctionDescriptor{
			ModuleName:   "main",
			PackagePath:  "",
			FunctionName: funcName,
			methodName:   "",
		}
	}

	desc := &GoFunctionDescriptor{
		ModuleName:   moduleName,
		PackagePath:  pkgPath,
		FunctionName: funcNameOnly,
		methodName:   "",
	}
	log.Log.V(2).Info("ExtractFunctionDescriptor: returning descriptor",
		"desc", desc, "key", desc.String())
	return desc
}

// parseFunctionName parses a Go function name into module, package, and function components.
// Deprecated: Use ParseFunctionName instead (same package, direct call).
func parseFunctionName(fullName string) (moduleName, pkgPath, funcNameOnly string, err error) {
	return ParseFunctionName(fullName)
}

// splitModuleAndPackage splits a string like "github.com/example/app/pkg/tasks" into module and package.
// Deprecated: Use SplitModuleAndPackage instead (same package, direct call).
func splitModuleAndPackage(s string) (moduleName, pkgPath string) {
	return SplitModuleAndPackage(s)
}

