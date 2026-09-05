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

/*
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

// CGO bridge struct definitions, matching the C++ structs in native_object_store.cc.
typedef struct {
	char* data;
	int size;
	char* metadata;
	int metadata_size;
	char** contained_ids;
	int contained_ids_count;
} CObjectReference;

typedef struct {
	CObjectReference* objects;
	int count;
} CObjectArray;

typedef struct {
	bool* ready;
	int count;
} CWaitResult;

// C++ side function declarations.
CObjectReference CObjectStore_Put(const char* data, int data_size,
                                   const char* metadata, int metadata_size,
                                   const char* owner_address, int owner_address_size);
int CObjectStore_PutWithID(const char* object_id_data, int object_id_size,
                            const char* data, int data_size,
                            const char* metadata, int metadata_size);
CObjectArray* CObjectStore_Get(const char** object_ids, int* object_id_sizes,
                               int count, long long timeout_ms);
CWaitResult CObjectStore_Wait(const char** object_ids, int* object_id_sizes,
                               int count, int num_objects,
                               long long timeout_ms, bool fetch_local);
int CObjectStore_Delete(const char** object_ids, int* object_id_sizes,
                          int count, bool local_only);
int CObjectStore_AddLocalReference(const char* object_id_data, int object_id_size);
int CObjectStore_RemoveLocalReference(const char* object_id_data, int object_id_size);
char* CObjectStore_GetAllReferenceCounts();
CObjectReference CObjectStore_GetOwnerAddress(const char* object_id_data, int object_id_size);
CObjectReference CObjectStore_GetOwnershipInfo(const char* object_id_data, int object_id_size);
int CObjectStore_RegisterOwnershipInfoAndResolveFuture(
    const char* object_id_data, int object_id_size,
    const char* outer_object_id_data, int outer_object_id_size,
    const char* owner_address, int owner_address_size);
void CObjectStore_FreeObjectReference(CObjectReference ref);
void CObjectStore_FreeObjectArray(CObjectArray* array);
void CObjectStore_FreeWaitResult(CWaitResult result);
void CObjectStore_FreeString(char* str);
*/
import "C"
import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"

	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// RayObjectIDSize is the fixed size of Ray ObjectID in bytes.
const RayObjectIDSize = 28

// cgoByteSlice converts a Go byte slice to C memory using runtime.Pinner.
// It returns the pointer, size, and the Pinner which must be kept alive during the CGO call.
// The caller is responsible for calling pinner.Unpin() after the CGO call completes.
//
// Usage pattern:
//
//	ptr, size, pinner := cgoByteSlice(data)
//	defer pinner.Unpin()
//	// use ptr in CGO call
func cgoByteSlice(data []byte) (*C.char, C.int, runtime.Pinner) {
	if data == nil {
		return nil, 0, runtime.Pinner{}
	}

	p := runtime.Pinner{}
	p.Pin(&data[0])
	return (*C.char)(unsafe.Pointer(&data[0])), C.int(len(data)), p
}

// cgoBytes allocates C memory and copies Go byte slice data.
// Returns the pointer, size, and a cleanup function.
// The caller MUST call cleanup() to free the allocated memory.
//
// Usage pattern:
//
//	ptr, size, cleanup := cgoBytes(data)
//	defer cleanup()
//	// use ptr in CGO call
func cgoBytes(data []byte) (*C.char, C.int, func()) {
	if data == nil {
		return nil, 0, func() {}
	}

	ptr := (*C.char)(C.CBytes(data))
	return ptr, C.int(len(data)), func() {
		if ptr != nil {
			C.free(unsafe.Pointer(ptr))
		}
	}
}

// cgoObjectIDArray allocates C arrays for object IDs and their sizes.
// Returns cObjectIDs, cObjectIDSizes, and a cleanup function.
// The caller MUST call cleanup() to free the allocated memory.
//
// Usage pattern:
//
//	cObjectIDs, cObjectIDSizes, cleanup := cgoObjectIDArray(objectIDs)
//	defer cleanup()
//	// use cObjectIDs and cObjectIDSizes in CGO call
func cgoObjectIDArray(objectIDs []*ids.ObjectID) ([]*C.char, []C.int, func()) {
	cObjectIDs := make([]*C.char, len(objectIDs))
	cObjectIDSizes := make([]C.int, len(objectIDs))
	for i, oid := range objectIDs {
		binary := oid.Binary()
		cObjectIDs[i] = (*C.char)(C.CBytes(binary))
		cObjectIDSizes[i] = C.int(len(binary))
	}
	return cObjectIDs, cObjectIDSizes, func() {
		for i := 0; i < len(cObjectIDs); i++ {
			if cObjectIDs[i] != nil {
				C.free(unsafe.Pointer(cObjectIDs[i]))
			}
		}
	}
}

type NativeObjectStore struct {
	shutdownLock        *sync.RWMutex
	resolveActorAddress func(context.Context, ids.ActorID) (*proto.Address, error)
	// cgoMu serializes the CGO write/refcount operations that mutate the C++
	// CoreWorker object store (Put, PutWithID, Add/RemoveLocalReference,
	// Delete). Concurrent CGO writes are unsafe in the C++ reference counter,
	// and the ObjectRef release worker can issue a RemoveLocalReference on its
	// own goroutine while the caller performs a Put. Read-only queries
	// (Get/Wait/GetOwnerAddress/GetOwnershipInfo) are not protected; they do not
	// mutate reference counts.
	cgoMu sync.Mutex
}

func NewNativeObjectStore(shutdownLock *sync.RWMutex, resolveActorAddress func(context.Context, ids.ActorID) (*proto.Address, error)) *NativeObjectStore {
	return &NativeObjectStore{
		shutdownLock:        shutdownLock,
		resolveActorAddress: resolveActorAddress,
	}
}

func (n *NativeObjectStore) PutRaw(obj *object.NativeRayObject) (*ids.ObjectID, error) {
	return n.PutRawWithOwner(obj, nil)
}

func (n *NativeObjectStore) PutRawWithOwner(obj *object.NativeRayObject, ownerActorID *ids.ActorID) (*ids.ObjectID, error) {
	if ownerActorID == nil {
		return n.nativePut(obj, nil)
	}
	if n.resolveActorAddress == nil {
		return nil, fmt.Errorf("actor address resolver not configured")
	}
	addr, err := n.resolveActorAddress(context.Background(), *ownerActorID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve actor address: %w", err)
	}
	ownerAddressBytes, err := protolib.Marshal(addr)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal owner address: %w", err)
	}
	return n.nativePut(obj, ownerAddressBytes)
}

func (n *NativeObjectStore) PutRawWithID(obj *object.NativeRayObject, objectID *ids.ObjectID) error {
	return n.nativePutWithID(objectID, obj)
}

func (n *NativeObjectStore) GetRaw(objectIDs []*ids.ObjectID, timeoutMs int64, objectType string) ([]*object.NativeRayObject, error) {
	return n.nativeGet(objectIDs, timeoutMs)
}

func (n *NativeObjectStore) GetRawWithContext(ctx context.Context, objectIDs []*ids.ObjectID, timeoutMs int64, objectType string) ([]*object.NativeRayObject, error) {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Use a channel to run the native get operation in a goroutine
	done := make(chan struct{})
	var result []*object.NativeRayObject
	var err error

	go func() {
		result, err = n.nativeGet(objectIDs, timeoutMs)
		close(done)
	}()

	// Wait for either completion or context cancellation
	select {
	case <-done:
		return result, err
	case <-ctx.Done():
		// Context cancelled - note: the native operation may still complete
		// but we return the context error to the caller
		return nil, ctx.Err()
	}
}

func (n *NativeObjectStore) Wait(objectIDs []*ids.ObjectID, numObjects int, timeoutMs int64, fetchLocal bool) ([]bool, error) {
	return n.nativeWait(objectIDs, numObjects, timeoutMs, fetchLocal)
}

func (n *NativeObjectStore) WaitWithOptions(opts object.WaitOptions) ([]bool, error) {
	return n.Wait(opts.ObjectIDs, opts.NumObjects, opts.TimeoutMs, opts.FetchLocal)
}

func (n *NativeObjectStore) Delete(objectIDs []*ids.ObjectID, localOnly bool) error {
	return n.nativeDelete(objectIDs, localOnly)
}

func (n *NativeObjectStore) AddLocalReference(objectID *ids.ObjectID) error {
	return n.nativeAddLocalReference(objectID)
}

func (n *NativeObjectStore) RemoveLocalReference(objectID *ids.ObjectID) error {
	n.shutdownLock.RLock()
	defer n.shutdownLock.RUnlock()
	return n.nativeRemoveLocalReference(objectID)
}

func (n *NativeObjectStore) GetOwnershipInfo(objectID *ids.ObjectID) ([]byte, error) {
	return n.nativeGetOwnershipInfo(objectID)
}

func (n *NativeObjectStore) RegisterOwnershipInfoAndResolveFuture(
	objectID *ids.ObjectID,
	outerObjectID *ids.ObjectID,
	ownerAddress []byte,
) error {
	return n.nativeRegisterOwnershipInfoAndResolveFuture(objectID, outerObjectID, ownerAddress)
}

func (n *NativeObjectStore) GetOwnerAddress(objectID *ids.ObjectID) ([]byte, error) {
	return n.nativeGetOwnerAddress(objectID)
}

func (n *NativeObjectStore) GetAllReferenceCounts() (map[ids.ObjectID][2]int64, error) {
	data, err := n.nativeGetAllReferenceCounts()
	if err != nil {
		return nil, err
	}

	var jsonMap map[string][2]int64
	if err := json.Unmarshal(data, &jsonMap); err != nil {
		return nil, fmt.Errorf("failed to parse reference counts: %w", err)
	}

	result := make(map[ids.ObjectID][2]int64)
	for hexID, counts := range jsonMap {
		objectID, err := ids.ObjectIDFromHex(hexID)
		if err != nil {
			continue
		}
		result[objectID] = counts
	}
	return result, nil
}

func (n *NativeObjectStore) nativePut(obj *object.NativeRayObject, ownerAddress []byte) (*ids.ObjectID, error) {
	n.cgoMu.Lock()
	defer n.cgoMu.Unlock()

	ownerAddrPtr, ownerAddrSize, ownerPinner := cgoByteSlice(ownerAddress)
	defer ownerPinner.Unpin()

	dataPtr, dataSize, dataPinner := cgoByteSlice(obj.Data)
	defer dataPinner.Unpin()

	metadataPtr, metadataSize, metadataPinner := cgoByteSlice(obj.Metadata)
	defer metadataPinner.Unpin()

	cResult := C.CObjectStore_Put(dataPtr, dataSize, metadataPtr, metadataSize, ownerAddrPtr, ownerAddrSize)
	defer C.free(unsafe.Pointer(cResult.data))

	if cResult.data == nil {
		return nil, fmt.Errorf("CObjectStore_Put failed")
	}

	objectIDBinary := C.GoBytes(unsafe.Pointer(cResult.data), cResult.size)
	objectID, err := ids.ObjectIDFromBinary(objectIDBinary)
	if err != nil {
		return nil, fmt.Errorf("failed to create ObjectID from binary: %w", err)
	}
	return &objectID, nil
}

func (n *NativeObjectStore) nativePutWithID(objectID *ids.ObjectID, obj *object.NativeRayObject) error {
	n.cgoMu.Lock()
	defer n.cgoMu.Unlock()

	cObjectIDData, cObjectIDSize, cleanupObjectID := cgoBytes(objectID.Binary())
	defer cleanupObjectID()

	dataPtr, dataSize, dataPinner := cgoByteSlice(obj.Data)
	defer dataPinner.Unpin()

	metadataPtr, metadataSize, metadataPinner := cgoByteSlice(obj.Metadata)
	defer metadataPinner.Unpin()

	result := C.CObjectStore_PutWithID(
		cObjectIDData, cObjectIDSize,
		dataPtr, dataSize,
		metadataPtr, metadataSize,
	)
	if result != 0 {
		return fmt.Errorf("CObjectStore_PutWithID failed with error code: %d", result)
	}
	return nil
}

func (n *NativeObjectStore) nativeGet(objectIDs []*ids.ObjectID, timeoutMs int64) ([]*object.NativeRayObject, error) {
	if len(objectIDs) == 0 {
		return []*object.NativeRayObject{}, nil
	}

	cObjectIDs, cObjectIDSizes, cleanupObjectIDs := cgoObjectIDArray(objectIDs)
	defer cleanupObjectIDs()

	cResult := C.CObjectStore_Get(
		(**C.char)(unsafe.Pointer(&cObjectIDs[0])),
		(*C.int)(unsafe.Pointer(&cObjectIDSizes[0])),
		C.int(len(objectIDs)),
		C.longlong(timeoutMs),
	)
	defer C.CObjectStore_FreeObjectArray(cResult)

	if cResult == nil || cResult.count == 0 {
		return []*object.NativeRayObject{}, nil
	}

	result := make([]*object.NativeRayObject, int(cResult.count))
	for i := 0; i < int(cResult.count); i++ {
		obj := *(*C.CObjectReference)(unsafe.Pointer(uintptr(unsafe.Pointer(cResult.objects)) + uintptr(i)*unsafe.Sizeof(*cResult.objects)))

		// Copy data buffer
		var data []byte
		if obj.data != nil && obj.size > 0 {
			data = C.GoBytes(unsafe.Pointer(obj.data), obj.size)
		}

		// Copy metadata buffer
		var metadata []byte
		if obj.metadata != nil && obj.metadata_size > 0 {
			metadata = C.GoBytes(unsafe.Pointer(obj.metadata), obj.metadata_size)
		}

		// Copy contained object IDs
		var containedObjectIds [][]byte
		if obj.contained_ids != nil && obj.contained_ids_count > 0 {
			containedObjectIds = make([][]byte, obj.contained_ids_count)
			for j := 0; j < int(obj.contained_ids_count); j++ {
				idPtr := (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(obj.contained_ids)) + uintptr(j)*unsafe.Sizeof(uintptr(0))))
				if idPtr != nil {
					containedObjectIds[j] = C.GoBytes(unsafe.Pointer(idPtr), C.int(RayObjectIDSize))
				}
			}
		}

		result[i] = &object.NativeRayObject{
			Data:               data,
			Metadata:           metadata,
			ContainedObjectIds: containedObjectIds,
		}
	}
	return result, nil
}

func (n *NativeObjectStore) nativeWait(objectIDs []*ids.ObjectID, numObjects int, timeoutMs int64, fetchLocal bool) ([]bool, error) {
	if len(objectIDs) == 0 {
		return []bool{}, nil
	}

	cObjectIDs, cObjectIDSizes, cleanupObjectIDs := cgoObjectIDArray(objectIDs)
	defer cleanupObjectIDs()

	cResult := C.CObjectStore_Wait(
		(**C.char)(unsafe.Pointer(&cObjectIDs[0])),
		(*C.int)(unsafe.Pointer(&cObjectIDSizes[0])),
		C.int(len(objectIDs)),
		C.int(numObjects),
		C.longlong(timeoutMs),
		C.bool(fetchLocal),
	)
	defer C.CObjectStore_FreeWaitResult(cResult)

	if cResult.count == 0 {
		return []bool{}, nil
	}

	result := make([]bool, cResult.count)
	for i := 0; i < int(cResult.count); i++ {
		ready := *(*C.bool)(unsafe.Pointer(uintptr(unsafe.Pointer(cResult.ready)) + uintptr(i)*unsafe.Sizeof(*cResult.ready)))
		result[i] = bool(ready)
	}
	return result, nil
}

func (n *NativeObjectStore) nativeDelete(objectIDs []*ids.ObjectID, localOnly bool) error {
	n.cgoMu.Lock()
	defer n.cgoMu.Unlock()
	if len(objectIDs) == 0 {
		return nil
	}

	cObjectIDs, cObjectIDSizes, freeObjectIDs := cgoObjectIDArray(objectIDs)
	defer freeObjectIDs()

	result := C.CObjectStore_Delete(
		(**C.char)(unsafe.Pointer(&cObjectIDs[0])),
		(*C.int)(unsafe.Pointer(&cObjectIDSizes[0])),
		C.int(len(objectIDs)),
		C.bool(localOnly),
	)

	if result != 0 {
		return fmt.Errorf("CObjectStore_Delete failed with error code: %d", result)
	}
	return nil
}

func (n *NativeObjectStore) nativeAddLocalReference(objectID *ids.ObjectID) error {
	n.cgoMu.Lock()
	defer n.cgoMu.Unlock()

	binary := objectID.Binary()
	cObjectIDData, cObjectIDSize, pinner := cgoByteSlice(binary)
	defer pinner.Unpin()

	result := C.CObjectStore_AddLocalReference(cObjectIDData, cObjectIDSize)
	if result != 0 {
		return fmt.Errorf("CObjectStore_AddLocalReference failed with error code: %d", result)
	}
	return nil
}

func (n *NativeObjectStore) nativeRemoveLocalReference(objectID *ids.ObjectID) error {
	n.cgoMu.Lock()
	defer n.cgoMu.Unlock()

	binary := objectID.Binary()
	cObjectIDData, cObjectIDSize, pinner := cgoByteSlice(binary)
	defer pinner.Unpin()

	result := C.CObjectStore_RemoveLocalReference(cObjectIDData, cObjectIDSize)
	if result != 0 {
		return fmt.Errorf("CObjectStore_RemoveLocalReference failed with error code: %d", result)
	}
	return nil
}

func (n *NativeObjectStore) nativeGetAllReferenceCounts() ([]byte, error) {
	cResult := C.CObjectStore_GetAllReferenceCounts()
	if cResult == nil {
		return []byte("{}"), nil
	}
	defer C.CObjectStore_FreeString(cResult)
	return []byte(C.GoString(cResult)), nil
}

func (n *NativeObjectStore) nativeGetOwnerAddress(objectID *ids.ObjectID) ([]byte, error) {
	binary := objectID.Binary()
	cObjectIDData := (*C.char)(C.CBytes(binary))
	defer C.free(unsafe.Pointer(cObjectIDData))

	cResult := C.CObjectStore_GetOwnerAddress(cObjectIDData, C.int(len(binary)))
	defer C.free(unsafe.Pointer(cResult.data))

	if cResult.data == nil {
		return nil, fmt.Errorf("CObjectStore_GetOwnerAddress returned null")
	}
	return C.GoBytes(unsafe.Pointer(cResult.data), cResult.size), nil
}

func (n *NativeObjectStore) nativeGetOwnershipInfo(objectID *ids.ObjectID) ([]byte, error) {
	binary := objectID.Binary()
	cObjectIDData := (*C.char)(C.CBytes(binary))
	defer C.free(unsafe.Pointer(cObjectIDData))

	cResult := C.CObjectStore_GetOwnershipInfo(cObjectIDData, C.int(len(binary)))
	defer C.free(unsafe.Pointer(cResult.data))

	if cResult.data == nil {
		return nil, fmt.Errorf("CObjectStore_GetOwnershipInfo returned null")
	}
	return C.GoBytes(unsafe.Pointer(cResult.data), cResult.size), nil
}

func (n *NativeObjectStore) nativeRegisterOwnershipInfoAndResolveFuture(
	objectID *ids.ObjectID,
	outerObjectID *ids.ObjectID,
	ownerAddress []byte,
) error {
	binary := objectID.Binary()
	cObjectIDData := (*C.char)(C.CBytes(binary))
	defer C.free(unsafe.Pointer(cObjectIDData))

	var cOuterObjectIDData *C.char
	var cOuterObjectIDSize C.int
	if outerObjectID != nil {
		var outerPinner runtime.Pinner
		cOuterObjectIDData, cOuterObjectIDSize, outerPinner = cgoByteSlice(outerObjectID.Binary())
		defer outerPinner.Unpin()
	}

	cOwnerAddress, cOwnerAddressSize, ownerPinner := cgoByteSlice(ownerAddress)
	defer ownerPinner.Unpin()

	result := C.CObjectStore_RegisterOwnershipInfoAndResolveFuture(
		cObjectIDData, C.int(len(binary)),
		cOuterObjectIDData, cOuterObjectIDSize,
		cOwnerAddress, cOwnerAddressSize,
	)
	if result != 0 {
		return fmt.Errorf("CObjectStore_RegisterOwnershipInfoAndResolveFuture failed with error code: %d", result)
	}
	return nil
}

var _ object.ObjectStore = (*NativeObjectStore)(nil)
