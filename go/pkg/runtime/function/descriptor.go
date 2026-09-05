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

// Package function provides types and utilities for managing Ray functions.
package function

import (
	"fmt"
	"strings"

	"github.com/ray-project/ray/go/pkg/log"
)

// FunctionDescriptor uniquely identifies a Ray task function.
//
// Design notes:
// 1. Interface is kept minimal, containing only essential methods.
// 2. Corresponds to Java's io.ray.runtime.functionmanager.FunctionDescriptor.
// 3. Implementations are provided for each supported language (Go, Python, Java, C++).
type FunctionDescriptor interface {
	// ToList returns a list of strings that uniquely identifies the function.
	// Format varies by language:
	// - Go: [moduleName, packagePath, functionName, methodName]
	// - Python: [moduleName, className, functionName]
	// - Java: [className, functionName, signature]
	// - C++: [functionName, language, className]
	ToList() []string

	// GetLanguage returns the language of the function.
	GetLanguage() Language

	// Hash returns a hash code for the function descriptor.
	// This is used for task scheduling and caching.
	Hash() int
}

// Language represents the programming language of a function.
// Consistent with ray::rpc::Language enum.
type Language int32

const (
	// LanguagePython represents Python language.
	LanguagePython Language = 0
	// LanguageJava represents Java language.
	LanguageJava Language = 1
	// LanguageCpp represents C++ language.
	LanguageCpp Language = 2
	// LanguageGo represents Go language.
	LanguageGo Language = 3
)

// GoFunctionDescriptor implements FunctionDescriptor for Go functions.
//
// This struct contains all information needed to identify and load a Go function
// in worker processes. The descriptor is serialized and sent with task specs.
type GoFunctionDescriptor struct {
	// ModuleName is the Go module path (e.g., "github.com/example/myapp").
	ModuleName string
	// PackagePath is the package path within the module (e.g., "pkg/tasks").
	PackagePath string
	// FunctionName is the name of the function (e.g., "MyTask").
	FunctionName string
	// methodName is optional method name for actor methods (e.g., "DoWork").
	methodName string
}

// NewGoFunctionDescriptor creates a new GoFunctionDescriptor for a normal function.
//
// Parameters:
//   - moduleName: The Go module path (e.g., "github.com/example/myapp"). Required, must be valid.
//   - packagePath: The package path within the module (e.g., "pkg/tasks"). Can be empty.
//   - functionName: The name of the function (e.g., "MyTask"). Required, must be valid.
//
// Returns:
//   - *GoFunctionDescriptor: The created function descriptor.
//   - error: An error if validation fails.
//
// Example usage:
//
//	desc, err := NewGoFunctionDescriptor("github.com/example/app", "pkg/tasks", "MyTask")
//	if err != nil {
//	    log.Fatalf("Failed to create function descriptor: %v", err)
//	}
func NewGoFunctionDescriptor(moduleName, packagePath, functionName string) (*GoFunctionDescriptor, error) {
	// Validate module name
	if moduleName == "" {
		log.Log.V(2).Info("NewGoFunctionDescriptor: empty moduleName")
		return nil, fmt.Errorf("module name is required")
	}
	if !isValidModulePath(moduleName) {
		return nil, fmt.Errorf("invalid module path '%s': must contain at least one '/' and no spaces", moduleName)
	}

	// Validate function name
	if functionName == "" {
		return nil, fmt.Errorf("function name is required")
	}
	if !isValidFunctionName(functionName) {
		return nil, fmt.Errorf("invalid function name '%s': must start with letter/underscore and contain only letters, digits, and underscores", functionName)
	}

	desc := &GoFunctionDescriptor{
		ModuleName:   moduleName,
		PackagePath:  packagePath,
		FunctionName: functionName,
		methodName:   "",
	}

	return desc, nil
}

// NewGoFunctionDescriptorOrUnknown creates a new GoFunctionDescriptor for a normal function.
// Unlike NewGoFunctionDescriptor, this function never fails - it will use "unknown" for invalid fields.
// This is useful for fallback scenarios where validation errors should not stop execution.
//
// Parameters:
//   - moduleName: The Go module path. Will be set to "unknown" if invalid.
//   - packagePath: The package path within the module. Will be set to "unknown" if invalid.
//   - functionName: The name of the function. Will be set to "unknown" if invalid.
//
// Returns:
//   - *GoFunctionDescriptor: The created function descriptor (never nil).
func NewGoFunctionDescriptorOrUnknown(moduleName, packagePath, functionName string) *GoFunctionDescriptor {
	// Validate each field individually and use "unknown" for invalid fields
	// This preserves valid fields instead of discarding everything
	validModuleName := moduleName
	if !isValidModulePath(moduleName) {
		validModuleName = "unknown"
	}

	validPackagePath := packagePath
	if packagePath != "" && !isValidPackagePath(packagePath) {
		validPackagePath = "unknown"
	}

	validFunctionName := functionName
	if !isValidFunctionName(functionName) {
		validFunctionName = "unknown"
	}

	return &GoFunctionDescriptor{
		ModuleName:   validModuleName,
		PackagePath:  validPackagePath,
		FunctionName: validFunctionName,
		methodName:   "",
	}
}

// isValidPackagePath validates a Go package path.
// A valid package path should:
// - Not contain spaces or invalid characters
// - Be a valid Go import path component
func isValidPackagePath(path string) bool {
	if path == "" {
		return true // Empty package path is valid (e.g., for main package)
	}
	// Check for spaces (invalid in package paths)
	if strings.Contains(path, " ") {
		return false
	}
	// Basic validation: must be a valid Go import path
	// Package paths can contain letters, digits, underscores, dots, and hyphens
	for i := 0; i < len(path); i++ {
		c := path[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-' || c == '/') {
			return false
		}
	}
	return true
}

// NewGoActorMethodDescriptor creates a new GoFunctionDescriptor for an actor method.
//
// Parameters:
//   - moduleName: The Go module path (e.g., "github.com/example/myapp"). Required, must be valid.
//   - packagePath: The package path within the module (e.g., "pkg/actors"). Can be empty.
//   - actorType: The name of the actor type (e.g., "MyActor"). Required, must be valid.
//   - methodName: The name of the method (e.g., "DoWork"). Required, must be valid.
//     Special value "<init>" is reserved for actor constructors.
//
// Returns:
//   - *GoFunctionDescriptor: The created function descriptor.
//   - error: An error if validation fails.
//
// Example usage:
//
//	desc, err := NewGoActorMethodDescriptor("github.com/example/app", "pkg/actors", "MyActor", "DoWork")
//	if err != nil {
//	    log.Fatalf("Failed to create actor method descriptor: %v", err)
//	}
func NewGoActorMethodDescriptor(moduleName, packagePath, actorType, methodName string) (*GoFunctionDescriptor, error) {
	// Validate module name and actor type (which serves as functionName)
	desc, err := NewGoFunctionDescriptor(moduleName, packagePath, actorType)
	if err != nil {
		return nil, fmt.Errorf("invalid actor type: %w", err)
	}

	// Validate method name
	if methodName == "" {
		return nil, fmt.Errorf("method name is required for actor methods")
	}
	if !isValidMethodName(methodName) {
		return nil, fmt.Errorf("invalid method name '%s': must be a valid Go identifier or '<init>' for constructors", methodName)
	}

	desc.methodName = methodName
	return desc, nil
}

// NewGoActorMethodDescriptorOrUnknown creates a new GoFunctionDescriptor for an actor method.
// Unlike NewGoActorMethodDescriptor, this function never fails - it will use "unknown" for invalid fields.
// This is useful for fallback scenarios where validation errors should not stop execution.
//
// Parameters:
//   - moduleName: The Go module path. Will be set to "unknown" if invalid.
//   - packagePath: The package path within the module.
//   - actorType: The name of the actor type. Will be set to "unknown" if invalid.
//   - methodName: The name of the method. Will be set to "" if invalid.
//
// Returns:
//   - *GoFunctionDescriptor: The created function descriptor (never nil).
func NewGoActorMethodDescriptorOrUnknown(moduleName, packagePath, actorType, methodName string) *GoFunctionDescriptor {
	desc, err := NewGoActorMethodDescriptor(moduleName, packagePath, actorType, methodName)
	if err != nil {
		// Fallback to unknown values
		return &GoFunctionDescriptor{
			ModuleName:   "unknown",
			PackagePath:  packagePath,
			FunctionName: "unknown",
			methodName:   "",
		}
	}
	return desc
}

// ToList returns a list of strings that uniquely identifies the function.
// Format: [moduleName, packagePath, functionName, methodName]
// methodName is empty for normal functions.
func (f *GoFunctionDescriptor) ToList() []string {
	return []string{f.ModuleName, f.PackagePath, f.FunctionName, f.methodName}
}

// GetLanguage returns the language of the function (LanguageGo).
func (f *GoFunctionDescriptor) GetLanguage() Language {
	return LanguageGo
}

// Hash returns a hash code for the function descriptor.
// This is used for task scheduling and caching.
func (f *GoFunctionDescriptor) Hash() int {
	h := 17
	h = h*31 + hashString(f.ModuleName)
	h = h*31 + hashString(f.PackagePath)
	h = h*31 + hashString(f.FunctionName)
	h = h*31 + hashString(f.methodName)
	return h
}

// HasMethodName returns true if this descriptor is for an actor method.
func (f *GoFunctionDescriptor) HasMethodName() bool {
	return f.methodName != ""
}

// MethodName returns the method name (empty for normal functions).
func (f *GoFunctionDescriptor) MethodName() string {
	return f.methodName
}

// String returns a string representation of the descriptor.
// This is used as the key in the function registry.
// Format: "module/package.Function" or "module.Function" (if package is empty)
// For actor methods: "module/package.Function.Method"
func (f *GoFunctionDescriptor) String() string {
	if f.methodName != "" {
		// Actor method: "module/package.Function.Method"
		if f.PackagePath != "" {
			return fmt.Sprintf("%s/%s.%s.%s", f.ModuleName, f.PackagePath, f.FunctionName, f.methodName)
		}
		return fmt.Sprintf("%s.%s.%s", f.ModuleName, f.FunctionName, f.methodName)
	}
	// Normal function: "module/package.Function" or "module.Function"
	if f.PackagePath != "" {
		return fmt.Sprintf("%s/%s.%s", f.ModuleName, f.PackagePath, f.FunctionName)
	}
	return fmt.Sprintf("%s.%s", f.ModuleName, f.FunctionName)
}

// hashString computes a simple hash for a string.
func hashString(s string) int {
	h := 0
	for i := 0; i < len(s); i++ {
		h = h*31 + int(s[i])
	}
	return h
}

// isValidModulePath validates a Go module path.
// A valid module path should:
// - Not be empty
// - Contain at least one '/' (e.g., "github.com/user/repo"), OR be "main" for the main package
// - Not contain spaces or invalid characters
func isValidModulePath(path string) bool {
	if path == "" {
		return false
	}
	// Allow "main" as a special case for the main package
	if path == "main" {
		return true
	}
	// Basic validation: must contain at least one '/'
	if !strings.Contains(path, "/") {
		return false
	}
	// Check for spaces (invalid in module paths)
	if strings.Contains(path, " ") {
		return false
	}
	// Could add more validation here (e.g., regex for valid module path format)
	return true
}

// isValidFunctionName validates a Go function name.
// A valid function name should:
// - Not be empty
// - Start with a letter or underscore
// - Contain only letters, digits, and underscores
func isValidFunctionName(name string) bool {
	if name == "" {
		return false
	}
	// Check first character
	first := name[0]
	if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_') {
		return false
	}
	// Check remaining characters
	for i := 1; i < len(name); i++ {
		c := name[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}
	return true
}

// isValidMethodName validates a Go method name.
// Same rules as function name, but allows special names like "<init>" for constructors.
func isValidMethodName(name string) bool {
	if name == "" {
		return false
	}
	// Allow special constructor name
	if name == "<init>" {
		return true
	}
	return isValidFunctionName(name)
}

// FunctionDescriptorFromList creates a FunctionDescriptor from a string list.
// This is used when receiving function descriptors from C++ side.
//
// Parameters:
//   - list: A list of strings representing the function descriptor.
//     For Go functions: [moduleName, packagePath, functionName, methodName]
//     methodName is empty for normal functions.
//
// Returns:
//   - FunctionDescriptor: The created function descriptor.
//   - error: An error if the list is malformed.
//
// Example usage:
//
//	desc, err := FunctionDescriptorFromList([]string{"mod", "pkg", "func", ""})
//	if err != nil {
//	    log.Printf("Invalid descriptor: %v", err)
//	}
func FunctionDescriptorFromList(list []string) (FunctionDescriptor, error) {
	if len(list) != 4 {
		return nil, fmt.Errorf("Go function descriptor must have exactly 4 elements (moduleName, packagePath, functionName, methodName), got %d: %v", len(list), list)
	}

	moduleName := list[0]
	packagePath := list[1]
	functionName := list[2]
	methodName := list[3]

	// Validate that moduleName is not empty (required field)
	if moduleName == "" {
		return nil, fmt.Errorf("module name is required in function descriptor, got: %v", list)
	}

	// Validate that functionName is not empty (required field)
	if functionName == "" {
		return nil, fmt.Errorf("function name is required in function descriptor, got: %v", list)
	}

	var desc FunctionDescriptor
	var err error
	if methodName != "" {
		desc, err = NewGoActorMethodDescriptor(moduleName, packagePath, functionName, methodName)
	} else {
		desc, err = NewGoFunctionDescriptor(moduleName, packagePath, functionName)
	}

	return desc, err
}
