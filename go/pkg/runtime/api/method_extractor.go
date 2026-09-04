// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"github.com/ray-project/ray/go/pkg/runtime/function"
)

// MethodExtractor extracts method information from actor handles.
// Consistent with Java's method extraction mechanism.
type MethodExtractor struct{}

// NewMethodExtractor creates a new MethodExtractor.
func NewMethodExtractor() *MethodExtractor {
	return &MethodExtractor{}
}

// ExtractActorMethodDescriptor extracts a GoActorMethodDescriptor from a method.
// This is used when calling actor methods via handle.Task(method).
//
// The method parameter should be a method value or method expression, such as:
//   - (*MyActor).MethodName (method expression)
//   - actorInstance.MethodName (method value)
//
// Parameters:
//   - method: the actor method to extract descriptor from
//
// Returns:
//   - *function.GoFunctionDescriptor: the extracted method descriptor
//   - error: any error encountered during extraction
func (e *MethodExtractor) ExtractActorMethodDescriptor(method interface{}) (*function.GoFunctionDescriptor, error) {
	funcValue := reflect.ValueOf(method)
	if funcValue.Kind() != reflect.Func {
		return nil, fmt.Errorf("expected a function or method, got %v", funcValue.Kind())
	}

	funcType := funcValue.Type()
	if funcType.NumIn() < 1 {
		return nil, fmt.Errorf("expected a method with at least one receiver parameter")
	}

	receiverType := funcType.In(0)
	if receiverType.Kind() == reflect.Ptr {
		receiverType = receiverType.Elem()
	}

	funcPointer := funcValue.Pointer()
	funcObj := runtime.FuncForPC(funcPointer)
	if funcObj == nil {
		return nil, fmt.Errorf("failed to get function info")
	}

	fullName := funcObj.Name()
	methodName := extractMethodName(fullName)

	desc, err := function.NewGoActorMethodDescriptor(
		"unknown",
		receiverType.PkgPath(),
		receiverType.Name(),
		methodName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create method descriptor: %w", err)
	}

	return desc, nil
}

// extractFunctionDescriptorFromName extracts a GoFunctionDescriptor from a regular function using runtime.FuncForPC.
// This is a helper function that parses the function name string to extract module and package information.
//
// Parameters:
//   - fn: the function to extract descriptor from
//
// Returns:
//   - *function.GoFunctionDescriptor: the extracted function descriptor
//   - error: any error encountered during extraction
//
// Note: This function is deprecated. Use function.ExtractFunctionDescriptor instead, which provides
// more robust handling of plugin-loaded functions and edge cases.
func extractFunctionDescriptorFromName(fn interface{}) (*function.GoFunctionDescriptor, error) {
	funcValue := reflect.ValueOf(fn)
	if funcValue.Kind() != reflect.Func {
		return nil, fmt.Errorf("expected a function, got %v", funcValue.Kind())
	}

	funcPointer := funcValue.Pointer()
	funcObj := runtime.FuncForPC(funcPointer)
	if funcObj == nil {
		return nil, fmt.Errorf("failed to get function info")
	}

	fullName := funcObj.Name()

	// Use the shared ParseFunctionName utility from function package
	// This ensures consistent parsing logic across registry and method extractor
	moduleName, pkgPath, funcNameOnly, err := function.ParseFunctionName(fullName)
	if err != nil {
		// Fallback: use simple parsing for edge cases
		dotIndex := strings.LastIndex(fullName, ".")
		if dotIndex == -1 {
			funcNameOnly = "unknown"
			moduleName, pkgPath = fullName, ""
		} else {
			funcNameOnly = fullName[dotIndex+1:]
			packagePart := fullName[:dotIndex]
			parts := strings.Split(packagePart, "/")
			if len(parts) == 1 {
				moduleName = parts[0]
				pkgPath = ""
			} else {
				moduleName = parts[0]
				pkgPath = strings.Join(parts[1:], "/")
			}
		}
	}

	return function.NewGoFunctionDescriptorOrUnknown(moduleName, pkgPath, funcNameOnly), nil
}

// extractMethodName extracts the method name from a full function path.
func extractMethodName(fullName string) string {
	// Function name format: package/path.Type.Method
	if dotIndex := strings.LastIndex(fullName, "."); dotIndex != -1 {
		return fullName[dotIndex+1:]
	}
	return fullName
}

// ExtractMethodFromInterface extracts a method descriptor from an interface method.
// This is useful for defining actor interfaces similar to Java.
//
// Parameters:
//   - actorInterface: an interface type (not an instance)
//   - methodName: the name of the method to extract
//
// Returns:
//   - *function.GoFunctionDescriptor: the extracted method descriptor
//   - error: any error encountered during extraction
func ExtractMethodFromInterface(actorInterface interface{}, methodName string) (*function.GoFunctionDescriptor, error) {
	interfaceType := reflect.TypeOf(actorInterface)
	if interfaceType == nil || interfaceType.Kind() != reflect.Interface {
		return nil, fmt.Errorf("expected an interface, got %v", interfaceType)
	}

	method, found := interfaceType.MethodByName(methodName)
	if !found {
		return nil, fmt.Errorf("method %s not found in interface %v", methodName, interfaceType)
	}

	desc, err := function.NewGoActorMethodDescriptor(
		"unknown",
		interfaceType.PkgPath(),
		interfaceType.Name(),
		method.Name,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create method descriptor: %w", err)
	}

	return desc, nil
}
