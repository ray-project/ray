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
	"errors"
	"sync"
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Note: newTestObject and newObjectID are shared with the local-mode object
// store tests in this package (local_mode_object_store_test.go).

func TestNewNativeObjectStore(t *testing.T) {
	lock := &sync.RWMutex{}
	resolver := func(context.Context, ids.ActorID) (*proto.Address, error) {
		return &proto.Address{}, nil
	}

	store := NewNativeObjectStore(lock, resolver)
	require.NotNil(t, store)
	assert.Same(t, lock, store.shutdownLock)
	assert.NotNil(t, store.resolveActorAddress)
}

func TestNewNativeObjectStore_NilResolver(t *testing.T) {
	store := NewNativeObjectStore(&sync.RWMutex{}, nil)
	require.NotNil(t, store)
	assert.Nil(t, store.resolveActorAddress)
}

func TestPutRawWithOwner_NoResolver(t *testing.T) {
	store := NewNativeObjectStore(&sync.RWMutex{}, nil)
	ownerActorID := ids.NilActorID()

	_, err := store.PutRawWithOwner(newTestObject("data", "RAW", nil), &ownerActorID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "actor address resolver not configured")
}

func TestPutRawWithOwner_ResolverError(t *testing.T) {
	expectedErr := errors.New("resolve failed")
	store := NewNativeObjectStore(&sync.RWMutex{}, func(context.Context, ids.ActorID) (*proto.Address, error) {
		return nil, expectedErr
	})
	ownerActorID := ids.NilActorID()

	_, err := store.PutRawWithOwner(newTestObject("data", "RAW", nil), &ownerActorID)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, expectedErr), "expected error to wrap %v, got %v", expectedErr, err)
	assert.Contains(t, err.Error(), "failed to resolve actor address")
}

func TestGetRawWithContext_AlreadyCancelled(t *testing.T) {
	store := NewNativeObjectStore(&sync.RWMutex{}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.GetRawWithContext(ctx, []*ids.ObjectID{newObjectID()}, -1, "RAW")
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// nativeGetView/nativeWait/nativeDelete return early on empty input without
// touching the C++ runtime, so they can be tested safely.
func TestNativeGetView_EmptyInput(t *testing.T) {
	store := NewNativeObjectStore(&sync.RWMutex{}, nil)

	result, err := store.nativeGetView(nil, 1000)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestNativeWait_EmptyInput(t *testing.T) {
	store := NewNativeObjectStore(&sync.RWMutex{}, nil)

	result, err := store.nativeWait(nil, 0, 1000, false)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestNativeDelete_EmptyInput(t *testing.T) {
	store := NewNativeObjectStore(&sync.RWMutex{}, nil)

	err := store.nativeDelete(nil, false)
	require.NoError(t, err)
}

func TestNativeObjectStoreInterfaceCompliance(t *testing.T) {
	// Compile-time check that NativeObjectStore implements object.ObjectStore.
	var _ object.ObjectStore = (*NativeObjectStore)(nil)
}
