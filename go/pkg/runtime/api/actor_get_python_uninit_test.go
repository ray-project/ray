// Copyright 2026 The Ray Authors.
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

//go:build apigetpython0

package api

import (
	"errors"
	"testing"

	rayerrors "github.com/ray-project/ray/go/pkg/errors"
)

// TestGetPythonActorWithNamespaceNotInitialized verifies that
// GetPythonActorWithNamespace returns ErrRuntimeNotInitialized when the runtime
// has not been initialized, mirroring the error path of GetActorWithNamespace.
//
// It runs in its own tagged target (api_get_python_uninit_test, gotags
// apigetpython0), separate from the untagged api_test where other tests install
// a handle, and forces the uninitialized precondition with clearHandle() so it
// holds regardless of test order.
func TestGetPythonActorWithNamespaceNotInitialized(t *testing.T) {
	clearHandle()
	handle, err := GetPythonActorWithNamespace("my_actor", "my_namespace", "my_module", "MyClass")
	if err == nil {
		t.Fatal("GetPythonActorWithNamespace should return an error when runtime is not initialized")
	}
	if !errors.Is(err, rayerrors.ErrRuntimeNotInitialized) {
		t.Fatalf("should return ErrRuntimeNotInitialized, got %v", err)
	}
	if handle != nil {
		t.Fatalf("should return a nil handle, got %v", handle)
	}
}
