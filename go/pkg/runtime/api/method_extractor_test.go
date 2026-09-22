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
	"strings"
	"testing"

	"github.com/ray-project/ray/go/pkg/runtime/function"
)

// TestExtractActorMethodDescriptorValidModulePath verifies that the method
// descriptor is built with a valid module path (containing '/') derived from the
// receiver type's import path, rather than the hardcoded "unknown" placeholder.
func TestExtractActorMethodDescriptorValidModulePath(t *testing.T) {
	extractor := NewMethodExtractor()

	desc, err := extractor.ExtractActorMethodDescriptor((*registerTestActor).Increment)
	if err != nil {
		t.Fatalf("ExtractActorMethodDescriptor failed: %v", err)
	}

	if desc == nil {
		t.Fatal("expected non-nil descriptor")
	}
	if desc.ModuleName == "unknown" || desc.ModuleName == "" {
		t.Errorf("expected a real module name, got %q", desc.ModuleName)
	}
	if !strings.Contains(desc.ModuleName, "/") {
		t.Errorf("expected module name to contain '/', got %q", desc.ModuleName)
	}
	if desc.FunctionName != "registerTestActor" {
		t.Errorf("expected FunctionName registerTestActor, got %q", desc.FunctionName)
	}
	if desc.MethodName() != "Increment" {
		t.Errorf("expected method name Increment, got %q", desc.MethodName())
	}
}

// TestExtractActorMethodDescriptorMatchesRegistration verifies that the module
// and package paths extracted for an actor method match those registered under
// the "<init>" descriptor, so the worker can resolve the actor class.
func TestExtractActorMethodDescriptorMatchesRegistration(t *testing.T) {
	_ = RegisterActorClass((*registerTestActor)(nil), newRegisterTestActor)

	extractor := NewMethodExtractor()
	desc, err := extractor.ExtractActorMethodDescriptor((*registerTestActor).Increment)
	if err != nil {
		t.Fatalf("ExtractActorMethodDescriptor failed: %v", err)
	}

	entries, ok := GetRegisteredFunctions()
	if !ok {
		t.Fatal("expected registered functions, got none")
	}

	var initDesc *function.GoFunctionDescriptor
	for _, e := range entries {
		if e.Descriptor().MethodName() == function.ConstructorName &&
			e.Descriptor().FunctionName == "registerTestActor" {
			initDesc = e.Descriptor()
			break
		}
	}
	if initDesc == nil {
		t.Fatal("expected a '<init>' descriptor for registerTestActor, not found")
	}

	if desc.ModuleName != initDesc.ModuleName {
		t.Errorf("module name mismatch: method=%q init=%q", desc.ModuleName, initDesc.ModuleName)
	}
	if desc.PackagePath != initDesc.PackagePath {
		t.Errorf("package path mismatch: method=%q init=%q", desc.PackagePath, initDesc.PackagePath)
	}
	if desc.FunctionName != initDesc.FunctionName {
		t.Errorf("function name mismatch: method=%q init=%q", desc.FunctionName, initDesc.FunctionName)
	}
}
