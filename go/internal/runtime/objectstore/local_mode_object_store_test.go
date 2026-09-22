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
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestObject builds a test object.
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

func TestNewLocalModeObjectStore(t *testing.T) {
	store := NewLocalModeObjectStore()
	require.NotNil(t, store)
	assert.Equal(t, int64(1), store.checkIntervalMs)
	assert.Empty(t, store.objectPutCallbacks)
	assert.Empty(t, store.store)
}

func TestNewLocalModeObjectStore_WithCheckIntervalMs(t *testing.T) {
	store := NewLocalModeObjectStore(WithCheckIntervalMs(500))
	assert.Equal(t, int64(500), store.checkIntervalMs)

	// A non-positive interval keeps the default value.
	store = NewLocalModeObjectStore(WithCheckIntervalMs(-10))
	assert.Equal(t, int64(1), store.checkIntervalMs)
}

func TestPutRaw(t *testing.T) {
	store := NewLocalModeObjectStore()

	obj := newTestObject("hello", "RAW", nil)
	id, err := store.PutRaw(obj)
	require.NoError(t, err)
	require.NotNil(t, id)
	assert.False(t, id.IsNil())

	assert.True(t, store.IsObjectReady(*id))
}

func TestPutRawWithID(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	err := store.PutRawWithID(newTestObject("data", "RAW", nil), id)
	require.NoError(t, err)
	assert.True(t, store.IsObjectReady(*id))
}

func TestPutRawWithID_NilObject(t *testing.T) {
	store := NewLocalModeObjectStore()
	err := store.PutRawWithID(nil, newObjectID())
	assert.Error(t, err)
}

func TestPutRawWithID_NilObjectID(t *testing.T) {
	store := NewLocalModeObjectStore()
	err := store.PutRawWithID(newTestObject("data", "RAW", nil), nil)
	assert.Error(t, err)
}

func TestPutRawWithID_PutIfAbsent(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	err := store.PutRawWithID(newTestObject("first", "RAW", nil), id)
	require.NoError(t, err)

	// Writing the same ID again must not overwrite the original data.
	err = store.PutRawWithID(newTestObject("second", "RAW", nil), id)
	require.NoError(t, err)

	objs, err := store.GetRaw([]*ids.ObjectID{id}, -1, "RAW")
	require.NoError(t, err)
	require.Len(t, objs, 1)
	assert.Equal(t, "first", string(objs[0].Data))
}

func TestPutRawWithOwner_NotImplemented(t *testing.T) {
	store := NewLocalModeObjectStore()
	ownerActorID := ids.NilActorID()
	_, err := store.PutRawWithOwner(newTestObject("data", "RAW", nil), &ownerActorID)
	assert.Error(t, err)
}

func TestGetRaw_ReferenceSemantics(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	// Mutations to the original object are reflected in the stored data
	// (reference semantics, aligned with Java).
	obj := newTestObject("original", "RAW", nil)
	require.NoError(t, store.PutRawWithID(obj, id))

	obj.Data[0] = 'X'
	obj.Metadata[0] = 'X'

	objs, err := store.GetRaw([]*ids.ObjectID{id}, -1, "RAW")
	require.NoError(t, err)
	require.Len(t, objs, 1)
	assert.Equal(t, "Xriginal", string(objs[0].Data))
	assert.Equal(t, "XAW", string(objs[0].Metadata))
}

func TestPutRawWithID_ReferenceSemantics(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	// After Put, Get returns the same reference stored in the store (zero copy),
	// not an independent copy.
	obj := newTestObject("hello", "RAW", nil)
	require.NoError(t, store.PutRawWithID(obj, id))

	objs, err := store.GetRaw([]*ids.ObjectID{id}, -1, "RAW")
	require.NoError(t, err)
	require.Len(t, objs, 1)
	// The same underlying array is shared (reference semantics, zero copy, not an
	// independent bytes.Clone copy). getRaw returns a shallow-copied standalone
	// struct (same pattern as GetRawReadOnly) whose Data/Metadata underlying
	// arrays are identical to those of the caller and the stored object; Close()
	// on the returned object only clears the shallow copy itself and does not
	// affect the stored object.
	assert.Same(t, &obj.Data[0], &objs[0].Data[0])
	assert.Same(t, &obj.Metadata[0], &objs[0].Metadata[0])
	// Underlying arrays are shared: mutating the Get result affects the stored
	// object (a subsequent Get sees the change).
	objs[0].Data[0] = 'J'
	objs2, err := store.GetRaw([]*ids.ObjectID{id}, -1, "RAW")
	require.NoError(t, err)
	require.Len(t, objs2, 1)
	assert.Equal(t, "Jello", string(objs2[0].Data))
}

func TestGetRaw_DoesNotCorruptStoreOnClose(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	require.NoError(t, store.PutRawWithID(newTestObject("persistent", "RAW", nil), id))

	// First Get followed by Close() (simulating the defer nativeObjects[0].Close()
	// in the api.Get path).
	objs1, err := store.GetRaw([]*ids.ObjectID{id}, -1, "RAW")
	require.NoError(t, err)
	require.Len(t, objs1, 1)
	require.NoError(t, objs1[0].Close())

	// Second Get of the same ObjectID: the stored object's Data must not have
	// been set to nil.
	objs2, err := store.GetRaw([]*ids.ObjectID{id}, -1, "RAW")
	require.NoError(t, err)
	require.Len(t, objs2, 1)
	assert.Equal(t, "persistent", string(objs2[0].Data))
}

func TestGetRaw_TimeoutNotReady(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	_, err := store.GetRaw([]*ids.ObjectID{id}, 50, "RAW")
	assert.Error(t, err)
}

func TestGetRaw_InfiniteWait(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = store.PutRawWithID(newTestObject("late", "RAW", nil), id)
	}()

	objs, err := store.GetRaw([]*ids.ObjectID{id}, -1, "RAW")
	require.NoError(t, err)
	require.Len(t, objs, 1)
	assert.Equal(t, "late", string(objs[0].Data))
}

func TestGetRawWithContext_Cancelled(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.GetRawWithContext(ctx, []*ids.ObjectID{id}, -1, "RAW")
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestGetRawReadOnly_ShallowCopy(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	require.NoError(t, store.PutRawWithID(newTestObject("data", "RAW", nil), id))

	objs, err := store.GetRawReadOnly([]*ids.ObjectID{id}, -1, "RAW")
	require.NoError(t, err)
	require.Len(t, objs, 1)
	// Read-only mode returns a reference sharing the underlying array.
	assert.Equal(t, "data", string(objs[0].Data))
}

func TestGetRawReadOnly_TimeoutNotReady(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	_, err := store.GetRawReadOnly([]*ids.ObjectID{id}, 50, "RAW")
	assert.Error(t, err)
}

func TestIsObjectReady(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	assert.False(t, store.IsObjectReady(*id))
	require.NoError(t, store.PutRawWithID(newTestObject("data", "RAW", nil), id))
	assert.True(t, store.IsObjectReady(*id))
}

func TestWait_Ready(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()
	require.NoError(t, store.PutRawWithID(newTestObject("data", "RAW", nil), id))

	ready, err := store.Wait([]*ids.ObjectID{id}, 1, 100, false)
	require.NoError(t, err)
	require.Len(t, ready, 1)
	assert.True(t, ready[0])
}

func TestWait_NotReady(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	ready, err := store.Wait([]*ids.ObjectID{id}, 1, 50, false)
	require.NoError(t, err)
	require.Len(t, ready, 1)
	assert.False(t, ready[0])
}

func TestWaitWithOptions(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()
	require.NoError(t, store.PutRawWithID(newTestObject("data", "RAW", nil), id))

	ready, err := store.WaitWithOptions(object.WaitOptions{
		ObjectIDs:  []*ids.ObjectID{id},
		NumObjects: 1,
		TimeoutMs:  100,
	})
	require.NoError(t, err)
	assert.True(t, ready[0])
}

func TestAddObjectPutCallback(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	var called []ids.ObjectID
	store.AddObjectPutCallback(func(oid ids.ObjectID) {
		called = append(called, oid)
	})

	require.NoError(t, store.PutRawWithID(newTestObject("data", "RAW", nil), id))
	require.Len(t, called, 1)
	assert.Equal(t, *id, called[0])
}

func TestAddObjectPutCallback_Nil(t *testing.T) {
	store := NewLocalModeObjectStore()
	assert.Panics(t, func() {
		store.AddObjectPutCallback(nil)
	})
}

func TestDelete(t *testing.T) {
	store := NewLocalModeObjectStore()
	id := newObjectID()

	require.NoError(t, store.PutRawWithID(newTestObject("data", "RAW", nil), id))
	assert.True(t, store.IsObjectReady(*id))

	err := store.Delete([]*ids.ObjectID{id}, true)
	require.NoError(t, err)
	assert.False(t, store.IsObjectReady(*id))
}

func TestAddLocalReference_Noop(t *testing.T) {
	store := NewLocalModeObjectStore()
	assert.NoError(t, store.AddLocalReference(newObjectID()))
}

func TestRemoveLocalReference_Noop(t *testing.T) {
	store := NewLocalModeObjectStore()
	assert.NoError(t, store.RemoveLocalReference(newObjectID()))
}

func TestGetOwnershipInfo(t *testing.T) {
	store := NewLocalModeObjectStore()
	info, err := store.GetOwnershipInfo(newObjectID())
	require.NoError(t, err)
	assert.Empty(t, info)
}

func TestRegisterOwnershipInfoAndResolveFuture_Noop(t *testing.T) {
	store := NewLocalModeObjectStore()
	err := store.RegisterOwnershipInfoAndResolveFuture(newObjectID(), newObjectID(), nil)
	assert.NoError(t, err)
}

func TestGetOwnerAddress(t *testing.T) {
	store := NewLocalModeObjectStore()
	addr, err := store.GetOwnerAddress(newObjectID())
	require.NoError(t, err)
	assert.Empty(t, addr)
}

func TestGetAllReferenceCounts(t *testing.T) {
	store := NewLocalModeObjectStore()
	counts, err := store.GetAllReferenceCounts()
	require.NoError(t, err)
	assert.Empty(t, counts)
}

func TestObjectStoreInterfaceCompliance(t *testing.T) {
	// Compile-time check that LocalModeObjectStore implements object.ObjectStore.
	var _ object.ObjectStore = NewLocalModeObjectStore()
}
