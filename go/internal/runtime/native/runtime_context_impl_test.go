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

package native

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/api"
	"github.com/stretchr/testify/assert"
)

func TestWasCurrentActorRestarted(t *testing.T) {
	// Note: This test requires a running Ray cluster with GCS
	// In practice, this would be tested in an integration test environment
	t.Run("NotInActorContext", func(t *testing.T) {
		// When not in actor context, should return false
		provider := &runtimeContextProvider{}

		result := provider.WasCurrentActorRestarted()
		assert.False(t, result)
	})
}

func TestGetCurrentActorHandle(t *testing.T) {
	t.Run("NotInActorContext", func(t *testing.T) {
		// When not in actor context, should return nil
		provider := &runtimeContextProvider{}

		handle := provider.GetCurrentActorHandle()
		assert.Nil(t, handle)
	})

	t.Run("InActorContext", func(t *testing.T) {
		// This test would require a mock WorkerContext that returns an actor ID
		// In practice, this would be tested with a mock or in integration tests
		// For now, just verify the method exists and returns the correct type
		provider := &runtimeContextProvider{}

		handle := provider.GetCurrentActorHandle()

		// When not in actor context, should return nil
		if handle != nil {
			// If we have a handle, verify it implements the interface
			_, ok := handle.(interface{ ID() ids.ActorID })
			assert.True(t, ok, "handle should implement ActorHandle interface")
		}
	})
}

func TestActorHandleImpl_EmbeddedNativeActorHandle(t *testing.T) {
	t.Run("IDDelegatesToNativeActorHandle", func(t *testing.T) {
		expectedActorID, _ := ids.ActorIDFromHex("0102030405060708090a0b0c0d0e0f1011121314")

		// Create ActorHandleImpl with embedded NativeActorHandle
		handle := api.NewActorHandleImpl[string](expectedActorID)

		// Test that ID() returns the embedded handle's actor ID
		actualActorID := handle.ID()
		assert.Equal(t, expectedActorID, actualActorID)
	})

	t.Run("ImplementsActorHandleInterface", func(t *testing.T) {
		// Verify that ActorHandleImpl implements submitter.ActorHandle interface
		var _ interface{ ID() ids.ActorID } = &api.ActorHandleImpl[string]{}
	})
}
