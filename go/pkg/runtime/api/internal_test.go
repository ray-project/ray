//go:build apiinternal

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

// This file verifies the error semantics of the runtime accessors when the
// runtime is not initialized: the package-level Internal() escape hatch panics
// (see ray.go) and getTaskSubmitter returns nil, which callers map to the
// submitter_not_available runtime error. It is gated behind the "apiinternal"
// build tag because these tests assume the package-global handle state is
// unset; run them in isolation with:
//
//	go test -tags apiinternal ./pkg/runtime/api/ -run 'TestInternal|TestGetTaskSubmitter'
package api

import (
	"testing"
)

// TestGetTaskSubmitterNotInitialized verifies that getTaskSubmitter returns
// nil when the runtime is not initialized, preserving the
// submitter_not_available error semantics for uninitialized callers.
func TestGetTaskSubmitterNotInitialized(t *testing.T) {
	if IsInitialized() {
		t.Skip("runtime already initialized; run this test in isolation with -tags apiinternal")
	}
	if s := getTaskSubmitter(); s != nil {
		t.Fatalf("getTaskSubmitter() should return nil when not initialized, got %v", s)
	}
}

// TestPackageLevelInternalNotInitialized verifies that the package-level
// Internal() escape hatch panics when the runtime is not initialized.
func TestPackageLevelInternalNotInitialized(t *testing.T) {
	if IsInitialized() {
		t.Skip("runtime already initialized; run this test in isolation with -tags apiinternal")
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Internal() should panic when runtime is not initialized")
		}
	}()
	Internal()
}
