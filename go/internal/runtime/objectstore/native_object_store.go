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

// CObjectView - zero-copy object view. Mirrors native_object_store.h: the data
// and metadata pointers address the backing buffer directly (no copy) and the
// handles own the C++ std::shared_ptr<ray::Buffer> instances keeping them
// alive. A handle of 0 means there is nothing to release.
typedef struct {
	uint8_t* data;
	int size;
	uint8_t* metadata;
	int metadata_size;
	uint64_t buffer_handle;
	uint64_t metadata_handle;
	bool is_plasma;
	char** contained_ids;
	int contained_ids_count;
} CObjectView;

// CObjectCreateResult - zero-copy Create result. Mirrors native_object_store.h.
// It hands the caller a direct write pointer into the object-store buffer plus a
// handle owning a std::shared_ptr<ray::Buffer>. The caller must write the
// payload into data and then Seal the object; on any failure before Seal it must
// call CObjectStore_ReleaseBuffer(handle).
typedef struct {
	uint8_t* data;
	int size;
	uint64_t buffer_handle;
} CObjectCreateResult;

// CObjectViewArray - array of zero-copy object views. Mirrors native_object_store.h.
typedef struct {
	CObjectView* views;
	int count;
} CObjectViewArray;

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

// Zero-copy view APIs
CObjectViewArray* CObjectStore_GetView(const char** object_ids, int* object_id_sizes,
                                       int count, long long timeout_ms);
void CObjectStore_FreeObjectViewArray(CObjectViewArray* array);
void CObjectStore_ReleaseBuffer(uint64_t buffer_handle);

// Zero-copy Create/Write/Seal APIs
int CObjectStore_CreateOwned(const char* metadata, int metadata_size, int data_size,
                             const char* owner_address, int owner_address_size,
                             char** out_object_id, int* out_object_id_size,
                             CObjectCreateResult* out_result);
int CObjectStore_CreateExisting(const char* metadata, int metadata_size, int data_size,
                                const char* object_id_data, int object_id_size,
                                CObjectCreateResult* out_result);
int CObjectStore_WriteData(uint64_t buffer_handle, uint8_t* data_ptr,
                           const char* src, int src_size);
int CObjectStore_SealOwned(const char* object_id_data, int object_id_size,
                           const char* owner_address, int owner_address_size);
int CObjectStore_SealExisting(const char* object_id_data, int object_id_size,
                              const char* owner_address, int owner_address_size);
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
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"

	"github.com/ray-project/ray/go/pkg/runtime/object"
)

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

// cgoContainedObjectIds copies the contained (nested) object ID binary data
// from a C array into Go byte slices. Each contained ID is ids.ObjectIDSize
// bytes long. Returns nil when there are no contained IDs.
func cgoContainedObjectIds(containedIds **C.char, count C.int) [][]byte {
	if containedIds == nil || count <= 0 {
		return nil
	}
	idPtrs := unsafe.Slice(containedIds, int(count))
	containedObjectIds := make([][]byte, len(idPtrs))
	for j, idPtr := range idPtrs {
		if idPtr != nil {
			containedObjectIds[j] = C.GoBytes(unsafe.Pointer(idPtr), C.int(ids.ObjectIDSize))
		}
	}
	return containedObjectIds
}

// releasePlasmaBuffer drops a C++ shared_ptr<Buffer> handle via CGO, allowing
// the backing memory to be reclaimed. Releasing a zero handle is a no-op. It is
// the release callback injected into object.PlasmaBufferView.
func releasePlasmaBuffer(handle uint64) {
	C.CObjectStore_ReleaseBuffer(C.uint64_t(handle))
}

// nativeCreateOwned calls C++ CreateOwnedAndIncrementLocalRef and returns the
// derived ObjectID, the direct write pointer, and a buffer handle owning the
// shared_ptr<Buffer>. The caller must WriteData then SealOwned, or ReleaseBuffer
// on any failure.
func (n *NativeObjectStore) nativeCreateOwned(metadata, ownerAddress []byte, dataSize int) (*ids.ObjectID, uintptr, uint64, error) {
	metadataPtr, metadataSize, metadataPinner := cgoByteSlice(metadata)
	defer metadataPinner.Unpin()

	ownerPtr, ownerSize, ownerPinner := cgoByteSlice(ownerAddress)
	defer ownerPinner.Unpin()

	var cObjectID *C.char
	var cObjectIDSize C.int
	var cResult C.CObjectCreateResult

	rc := C.CObjectStore_CreateOwned(
		metadataPtr, metadataSize, C.int(dataSize),
		ownerPtr, ownerSize,
		&cObjectID, &cObjectIDSize,
		&cResult,
	)
	if rc != 0 {
		if cObjectID != nil {
			C.free(unsafe.Pointer(cObjectID))
		}
		return nil, 0, 0, fmt.Errorf("CObjectStore_CreateOwned failed with error code: %d", rc)
	}

	// Transfer ownership of the buffer handle to the caller; the C++ layer
	// keeps the shared_ptr alive until CObjectStore_ReleaseBuffer is called.
	objectIDBinary := C.GoBytes(unsafe.Pointer(cObjectID), cObjectIDSize)
	C.free(unsafe.Pointer(cObjectID))
	objectID, err := ids.ObjectIDFromBinary(objectIDBinary)
	if err != nil {
		releasePlasmaBuffer(uint64(cResult.buffer_handle))
		return nil, 0, 0, fmt.Errorf("failed to create ObjectID from binary: %w", err)
	}
	return &objectID, uintptr(unsafe.Pointer(cResult.data)), uint64(cResult.buffer_handle), nil
}

// nativeCreateExisting calls C++ CreateExisting for a caller-supplied ObjectID.
// Returns (writePtr, handle, error).
//
// The C++ contract collapses BOTH local-mode NotImplemented and an object that
// already exists in plasma to an all-zero result (buffer_handle==0, data==NULL,
// size==0): local mode throws std::logic_error (returning a zeroed result) and
// plasma-store Create folds ObjectExists to Status::OK() with *data==nullptr.
// The shim therefore maps any handle==0 result to exists=false, and the caller
// falls back to the idempotent copy path (ObjectExists is folded to OK in
// plasma_store_provider.cc), so functional correctness is unaffected.
func (n *NativeObjectStore) nativeCreateExisting(metadata []byte, dataSize int, objectID *ids.ObjectID) (uintptr, uint64, error) {
	metadataPtr, metadataSize, metadataPinner := cgoByteSlice(metadata)
	defer metadataPinner.Unpin()

	cObjectID, cObjectIDSize, cleanupObjectID := cgoBytes(objectID.Binary())
	defer cleanupObjectID()

	var cResult C.CObjectCreateResult
	rc := C.CObjectStore_CreateExisting(
		metadataPtr, metadataSize, C.int(dataSize),
		cObjectID, cObjectIDSize,
		&cResult,
	)
	if rc != 0 {
		return 0, 0, fmt.Errorf("CObjectStore_CreateExisting failed with error code: %d", rc)
	}
	if cResult.buffer_handle == 0 && cResult.data == nil && cResult.size == 0 {
		// Local-mode NotImplemented or object already exists in plasma (both
		// collapse to an all-zero result). Caller falls back to the existing
		// LocalMemoryBuffer(copy_data=true) / idempotent copy path.
		return 0, 0, nil
	}
	return uintptr(unsafe.Pointer(cResult.data)), uint64(cResult.buffer_handle), nil
}

// nativeWriteData copies src into the object-store buffer at writePtr.
func (n *NativeObjectStore) nativeWriteData(handle uint64, writePtr uintptr, src []byte) error {
	if handle == 0 {
		return fmt.Errorf("nativeWriteData: null handle")
	}
	if writePtr == 0 {
		return fmt.Errorf("nativeWriteData: null write pointer")
	}
	srcPtr, srcSize, srcPinner := cgoByteSlice(src)
	defer srcPinner.Unpin()

	rc := C.CObjectStore_WriteData(
		C.uint64_t(handle),
		(*C.uint8_t)(unsafe.Pointer(writePtr)),
		srcPtr, srcSize,
	)
	if rc != 0 {
		return fmt.Errorf("CObjectStore_WriteData failed with error code: %d", rc)
	}
	return nil
}

// nativeSeal finalizes an object created by CreateOwned or CreateExisting.
// Both Seal variants share the same cgoBytes/cgoByteSlice pinning and error
// wrapping; the owned flag selects the C++ entry point.
func (n *NativeObjectStore) nativeSeal(objectID *ids.ObjectID, ownerAddress []byte, owned bool) error {
	cObjectID, cObjectIDSize, cleanupObjectID := cgoBytes(objectID.Binary())
	defer cleanupObjectID()

	ownerPtr, ownerSize, ownerPinner := cgoByteSlice(ownerAddress)
	defer ownerPinner.Unpin()

	op := "SealExisting"
	var rc C.int
	if owned {
		op = "SealOwned"
		rc = C.CObjectStore_SealOwned(cObjectID, cObjectIDSize, ownerPtr, ownerSize)
	} else {
		rc = C.CObjectStore_SealExisting(cObjectID, cObjectIDSize, ownerPtr, ownerSize)
	}
	if rc != 0 {
		return fmt.Errorf("CObjectStore_%s failed with error code: %d", op, rc)
	}
	return nil
}

// nativeSealOwned finalizes an object created by nativeCreateOwned.
func (n *NativeObjectStore) nativeSealOwned(objectID *ids.ObjectID, ownerAddress []byte) error {
	return n.nativeSeal(objectID, ownerAddress, true)
}

// nativeSealExisting finalizes an object created by nativeCreateExisting.
func (n *NativeObjectStore) nativeSealExisting(objectID *ids.ObjectID, ownerAddress []byte) error {
	return n.nativeSeal(objectID, ownerAddress, false)
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

func (n *NativeObjectStore) CreateOwned(metadata *object.NativeRayObject, dataSize int) (*ids.ObjectID, uintptr, uint64, error) {
	n.cgoMu.Lock()
	defer n.cgoMu.Unlock()
	return n.nativeCreateOwned(metadata.Metadata, nil, dataSize)
}

func (n *NativeObjectStore) SealOwned(objectID *ids.ObjectID, handle uint64) error {
	n.cgoMu.Lock()
	defer n.cgoMu.Unlock()
	// C++ SealOwned does NOT release the shared_ptr buffer handle allocated by
	// CreateOwned; the Go side must release it here to avoid leaking the write
	// buffer. Releasing a zero handle is a no-op.
	defer releasePlasmaBuffer(handle)
	return n.nativeSealOwned(objectID, nil)
}

func (n *NativeObjectStore) CreateExisting(metadata *object.NativeRayObject, dataSize int, objectID *ids.ObjectID) (uintptr, uint64, error) {
	n.cgoMu.Lock()
	defer n.cgoMu.Unlock()
	return n.nativeCreateExisting(metadata.Metadata, dataSize, objectID)
}

func (n *NativeObjectStore) SealExisting(objectID *ids.ObjectID, handle uint64) error {
	n.cgoMu.Lock()
	defer n.cgoMu.Unlock()
	// C++ SealExisting does NOT release the shared_ptr buffer handle allocated
	// by CreateExisting; release it here. Releasing a zero handle is a no-op.
	defer releasePlasmaBuffer(handle)
	return n.nativeSealExisting(objectID, nil)
}

func (n *NativeObjectStore) GetRaw(objectIDs []*ids.ObjectID, timeoutMs int64, objectType string) ([]*object.NativeRayObject, error) {
	return n.nativeGetView(objectIDs, timeoutMs)
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
		result, err = n.nativeGetView(objectIDs, timeoutMs)
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

	objectID, writePtr, handle, err := n.nativeCreateOwned(obj.Metadata, ownerAddress, len(obj.Data))
	if err != nil {
		return nil, err
	}

	// data == nullptr: object already exists in plasma; Seal to finalize the
	// OBJECT_IN_PLASMA entry without writing.
	if handle == 0 {
		if err := n.nativeSealOwned(objectID, ownerAddress); err != nil {
			return nil, err
		}
		return objectID, nil
	}

	// Write the payload with a single memcpy into the object-store buffer.
	if err := n.nativeWriteData(handle, writePtr, obj.Data); err != nil {
		releasePlasmaBuffer(handle)
		return nil, err
	}

	// SealOwned finalizes the object but does NOT release the shared_ptr buffer
	// handle allocated by CreateOwned; release it here regardless to avoid a
	// per-Put leak of the write buffer.
	defer releasePlasmaBuffer(handle)
	if err := n.nativeSealOwned(objectID, ownerAddress); err != nil {
		return nil, err
	}
	return objectID, nil
}

func (n *NativeObjectStore) nativePutWithID(objectID *ids.ObjectID, obj *object.NativeRayObject) error {
	n.cgoMu.Lock()
	defer n.cgoMu.Unlock()

	writePtr, handle, err := n.nativeCreateExisting(obj.Metadata, len(obj.Data), objectID)
	if err != nil {
		return err
	}

	// The current C++ contract returns an all-zero CreateExisting result for BOTH
	// local-mode NotImplemented and an object that already exists in plasma; the
	// Go shim maps both to handle==0. Both cases fall back to the existing
	// idempotent copy path (ObjectExists is folded to OK in
	// plasma_store_provider.cc), so functional correctness is unaffected.
	if handle == 0 {
		return n.nativePutWithIDFallback(objectID, obj)
	}

	if err := n.nativeWriteData(handle, writePtr, obj.Data); err != nil {
		releasePlasmaBuffer(handle)
		return err
	}

	// SealExisting does NOT release the Create-allocated handle; release it here.
	defer releasePlasmaBuffer(handle)
	return n.nativeSealExisting(objectID, nil)
}

// nativePutWithIDFallback preserves the pre-existing explicit-ID copy path for
// local mode, where CreateExisting is not supported.
func (n *NativeObjectStore) nativePutWithIDFallback(objectID *ids.ObjectID, obj *object.NativeRayObject) error {
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

// nativeGetView retrieves objects in a single Get call and classifies each
// object individually: plasma-backed objects are wrapped in a NativeRayObject
// whose DataView provides zero-copy access, while non-plasma (small/inlined)
// objects are copied into the Go heap in the same pass. This avoids a second
// full Get for the common small-object case.
func (n *NativeObjectStore) nativeGetView(objectIDs []*ids.ObjectID, timeoutMs int64) ([]*object.NativeRayObject, error) {
	if len(objectIDs) == 0 {
		return []*object.NativeRayObject{}, nil
	}

	cObjectIDs, cObjectIDSizes, cleanupObjectIDs := cgoObjectIDArray(objectIDs)
	defer cleanupObjectIDs()

	cResult := C.CObjectStore_GetView(
		(**C.char)(unsafe.Pointer(&cObjectIDs[0])),
		(*C.int)(unsafe.Pointer(&cObjectIDSizes[0])),
		C.int(len(objectIDs)),
		C.longlong(timeoutMs),
	)
	if cResult == nil {
		// CgoErrorHandler returns nullptr when the C++ layer threw; surface it
		// as an error instead of an empty result so callers can distinguish an
		// object-store failure from "object not found".
		return []*object.NativeRayObject{}, fmt.Errorf("CObjectStore_GetView failed")
	}
	// The array itself is always released here; individual handles are released
	// only if ownership was not transferred to Go (see below).
	defer C.CObjectStore_FreeObjectViewArray(cResult)

	if cResult.count == 0 {
		return []*object.NativeRayObject{}, nil
	}

	result := make([]*object.NativeRayObject, int(cResult.count))
	for i := 0; i < int(cResult.count); i++ {
		// viewPtr points into the C array so handles can be zeroed once their
		// ownership moves to Go (prevents CObjectStore_FreeObjectViewArray from
		// double-releasing a handle we already took over).
		viewPtr := (*C.CObjectView)(unsafe.Pointer(uintptr(unsafe.Pointer(cResult.views)) +
			uintptr(i)*unsafe.Sizeof(*cResult.views)))
		cv := *viewPtr

		nativeObj := &object.NativeRayObject{}

		if cv.data != nil && cv.size > 0 {
			if cv.is_plasma {
				// Plasma-backed: hand the buffer to Go as a zero-copy view and
				// transfer ownership of the handle.
				nativeObj.DataView = object.NewPlasmaBufferView(
					unsafe.Pointer(cv.data),
					int(cv.size),
					uint64(cv.buffer_handle),
					releasePlasmaBuffer,
				)
				// Ownership moved to the view; clear the handle in the C array so
				// CObjectStore_FreeObjectViewArray does not release it a second time.
				viewPtr.buffer_handle = 0
			} else {
				// Small/inlined object: copy the payload into an aligned,
				// pool-owned buffer so NativeRayObject.Close() can safely return
				// it via PutBuffer(). A raw C.GoBytes allocation has arbitrary
				// capacity and is not 64-byte aligned, so returning it to the
				// tiered pool would violate the pool's alignment and size-class
				// accounting. The buffer handle stays in the C array and is
				// released by the single CObjectStore_FreeObjectViewArray call.
				buf := object.GetBuffer(int(cv.size))
				buf = append(buf, C.GoBytes(unsafe.Pointer(cv.data), C.int(cv.size))...)
				nativeObj.Data = buf
				// The buffer is pool-owned; Close() must return it via PutBuffer.
				nativeObj.MarkDataFromPool()
			}
		}

		// Metadata is small; copy it into Go before the C array is freed. The
		// handle stays in the C array and is released by
		// CObjectStore_FreeObjectViewArray.
		if cv.metadata != nil && cv.metadata_size > 0 {
			nativeObj.Metadata = C.GoBytes(unsafe.Pointer(cv.metadata), C.int(cv.metadata_size))
		}

		// Copy contained object IDs (nested references), matching the copy path.
		nativeObj.ContainedObjectIds = cgoContainedObjectIds(cv.contained_ids, C.int(cv.contained_ids_count))

		result[i] = nativeObj
	}

	log.Log.V(1).Info("GetRaw", "count", len(result), "timeoutMs", timeoutMs)
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
