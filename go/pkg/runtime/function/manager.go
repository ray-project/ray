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
	"sync"

	"github.com/ray-project/ray/go/pkg/log"
)

var managerLogger = log.WithName("function_manager")

// FunctionManager manages function registration and lookup for Go Ray tasks.
//
// Design notes:
//  1. Go does not support lambda serialization like Java, so we use explicit registration.
//  2. Functions are registered with a GoFunctionDescriptor as the key.
//  3. The manager is thread-safe for concurrent access during task execution.
//  4. This is a simpler design compared to Java's FunctionManager which supports
//     dynamic class loading and lambda deserialization.
type FunctionManager struct {
	// mu protects the functions map
	mu sync.RWMutex
	// functions maps function descriptors to their implementations
	functions map[string]Function
	// codeSearchPath is unused in Go (kept for API compatibility with Java)
	codeSearchPath []string
}

// NewFunctionManager creates a new FunctionManager instance.
//
// Parameters:
//   - codeSearchPath: Optional list of paths to search for code (unused in Go)
//
// Returns:
//   - A pointer to the newly created FunctionManager
func NewFunctionManager(codeSearchPath []string) *FunctionManager {
	return &FunctionManager{
		functions:      make(map[string]Function),
		codeSearchPath: codeSearchPath,
	}
}

// RegisterFunction registers a function with the given descriptor.
//
// This method allows users to explicitly register their Ray functions.
// Go does not support automatic lambda registration like Java.
//
// Parameters:
//   - descriptor: The function descriptor (unique identifier)
//   - fn: The function implementation to register
//
// Returns:
//   - error: An error if the descriptor is invalid or registration fails.
//
// Example:
//
//	func myTask(x int) int { return x * 2 }
//
//	desc, err := NewGoFunctionDescriptor("github.com/example/myapp", "pkg/tasks", "myTask")
//	if err != nil {
//	    log.Fatalf("Failed to create descriptor: %v", err)
//	}
//	err = manager.RegisterFunction(desc, myTask)
//	if err != nil {
//	    log.Fatalf("Failed to register function: %v", err)
//	}
func (m *FunctionManager) RegisterFunction(descriptor *GoFunctionDescriptor, fn Function) error {
	// Validate descriptor
	if descriptor == nil {
		return fmt.Errorf("descriptor cannot be nil")
	}
	if descriptor.ModuleName == "" {
		return fmt.Errorf("descriptor.ModuleName is required")
	}
	if descriptor.FunctionName == "" {
		return fmt.Errorf("descriptor.FunctionName is required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := descriptor.String()
	m.functions[key] = fn
	managerLogger.Info("Function registered", "descriptor", key)
	return nil
}

// GetFunction retrieves a registered function by its descriptor.
//
// Parameters:
//   - descriptor: The function descriptor to look up
//
// Returns:
//   - The registered function, or nil if not found
//   - An error if the function is not registered
func (m *FunctionManager) GetFunction(descriptor *GoFunctionDescriptor) (Function, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := descriptor.String()
	fn, ok := m.functions[key]
	if !ok {
		return nil, fmt.Errorf("function not registered: %s", key)
	}

	return fn, nil
}

// GetFunctionByBaseDescriptor retrieves a function using FunctionDescriptor.
//
// This method is used when receiving task requests from the C++ core worker,
// which uses the cross-language FunctionDescriptor format.
//
// Parameters:
//   - descriptor: The function descriptor to look up
//
// Returns:
//   - The registered function, or nil if not found
//   - An error if the function is not registered or descriptor is invalid
func (m *FunctionManager) GetFunctionByBaseDescriptor(descriptor FunctionDescriptor) (Function, error) {
	goDesc, err := FromBaseFunctionDescriptor(descriptor)
	if err != nil {
		return nil, fmt.Errorf("invalid function descriptor: %w", err)
	}

	return m.GetFunction(goDesc)
}

// IsRegistered checks if a function with the given descriptor is registered.
//
// Parameters:
//   - descriptor: The function descriptor to check
//
// Returns:
//   - true if the function is registered, false otherwise
func (m *FunctionManager) IsRegistered(descriptor *GoFunctionDescriptor) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := descriptor.String()
	_, ok := m.functions[key]
	return ok
}

// ListRegisteredFunctions returns a list of all registered function descriptors.
//
// Returns:
//   - A slice of function descriptor strings for all registered functions
func (m *FunctionManager) ListRegisteredFunctions() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]string, 0, len(m.functions))
	for key := range m.functions {
		result = append(result, key)
	}
	return result
}

// UnregisterFunction removes a function from the registry.
//
// This is typically used for cleanup or testing purposes.
//
// Parameters:
//   - descriptor: The function descriptor to unregister
func (m *FunctionManager) UnregisterFunction(descriptor *GoFunctionDescriptor) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := descriptor.String()
	delete(m.functions, key)
	managerLogger.Info("Function unregistered", "descriptor", key)
}

// Clear removes all registered functions.
//
// This is typically used for testing or when shutting down the runtime.
func (m *FunctionManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.functions = make(map[string]Function)
	managerLogger.Info("All functions cleared")
}

// FromBaseFunctionDescriptor creates a GoFunctionDescriptor from FunctionDescriptor.
func FromBaseFunctionDescriptor(desc FunctionDescriptor) (*GoFunctionDescriptor, error) {
	if goDesc, ok := desc.(*GoFunctionDescriptor); ok {
		return goDesc, nil
	}
	// Convert from other implementations
	list := desc.ToList()
	goDesc, err := FunctionDescriptorFromList(list)
	if err != nil {
		return nil, fmt.Errorf("failed to convert function descriptor to Go descriptor: %w", err)
	}
	return goDesc.(*GoFunctionDescriptor), nil
}
