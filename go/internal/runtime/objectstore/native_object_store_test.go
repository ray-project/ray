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

package objectstore

import (
	"context"
	"sync"
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestObject constructs a test object. Shared by the native and local-mode
// object store tests (the latter now live in the pure-Go localstore package).
func newTestObject(data, metadata string, contained [][]byte) *object.NativeRayObject {
	return &object.NativeRayObject{
		Data:               []byte(data),
		Metadata:           []byte(metadata),
		ContainedObjectIds: contained,
	}
}

// newObjectID generates a random ObjectID pointer.
func newObjectID() *ids.ObjectID {
	id := ids.NewObjectID()
	return &id
}

func TestNewNativeObjectStore(t *testing.T) {
	lock := &sync.RWMutex{}
	store := NewNativeObjectStore(lock)
	require.NotNil(t, store)
	assert.Same(t, lock, store.shutdownLock)
}

func TestGetRawWithContext_AlreadyCancelled(t *testing.T) {
	store := NewNativeObjectStore(&sync.RWMutex{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.GetRawWithContext(ctx, []*ids.ObjectID{newObjectID()}, -1, "RAW")
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// nativeGetView/nativeWait/nativeDelete return early on empty input without
// touching the C++ runtime, so they can be tested safely.
func TestNativeGetView_EmptyInput(t *testing.T) {
	store := NewNativeObjectStore(&sync.RWMutex{})

	result, err := store.nativeGetView(nil, 1000)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestNativeWait_EmptyInput(t *testing.T) {
	store := NewNativeObjectStore(&sync.RWMutex{})

	result, err := store.nativeWait(nil, 0, 1000, false)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestNativeDelete_EmptyInput(t *testing.T) {
	store := NewNativeObjectStore(&sync.RWMutex{})

	err := store.nativeDelete(nil, false)
	require.NoError(t, err)
}

func TestNativeObjectStoreInterfaceCompliance(t *testing.T) {
	// Compile-time check that NativeObjectStore implements object.ObjectStore.
	var _ object.ObjectStore = (*NativeObjectStore)(nil)
}
