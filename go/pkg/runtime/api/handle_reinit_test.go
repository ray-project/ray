//go:build apireinit

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

// Verifies that a re-init after Shutdown resets shutdownComplete to false. It is
// gated behind the "apireinit" build tag because setHandle mutates
// package-global handle state that the default api tests assume is unset; run it
// in isolation with: go test -tags apireinit ./pkg/runtime/api/ -run Reinit
package api

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/runtime/contract"
)

// reinitTestHandle is a minimal contract.RuntimeHandle for driving
// storeHandleLocked in the re-init test without touching any real runtime.
type reinitTestHandle struct{}

func (reinitTestHandle) IsRuntimeHandle()          {}
func (reinitTestHandle) Runtime() contract.Runtime { return nil }

// TestShutdownCompleteResetOnReinit ensures a re-init (storeHandleLocked) after a
// completed Shutdown resets shutdownComplete to false. Without this reset, every
// ObjectRef finalizer created after the re-init would observe shutdownComplete
// true and skip RemoveLocalReference, leaking local references in the C++ object
// store (plasma). Shutdown clears the `initialized` flag so that Init can run
// again, so re-init must reset shutdownComplete alongside it to keep the two
// flags consistent.
func TestShutdownCompleteResetOnReinit(t *testing.T) {
	// Simulate a completed shutdown (clearHandle's terminal state).
	shutdownComplete.Store(true)

	// Re-init: setHandle -> storeHandleLocked must reset shutdownComplete.
	setHandle(reinitTestHandle{})
	if shutdownComplete.Load() {
		t.Fatal("re-init after shutdown must reset shutdownComplete to false")
	}

	// Restore a clean shut-down state for any subsequent test in this target.
	clearHandle()
}
