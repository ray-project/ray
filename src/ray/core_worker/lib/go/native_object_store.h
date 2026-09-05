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

// CGO wrapper header for ObjectStore C++ API.
// This file provides C wrappers for CoreWorker ObjectStore to enable CGO binding.
// Pattern: Similar to io_ray_runtime_object_NativeObjectStore.h in Java runtime.

#ifndef SRC_RAY_CORE_WORKER_LIB_GO_NATIVE_OBJECT_STORE_H_
#define SRC_RAY_CORE_WORKER_LIB_GO_NATIVE_OBJECT_STORE_H_

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Type Definitions
// ============================================================================

// CObjectReference represents a reference to an object (typically an ObjectID).
// Caller is responsible for freeing the data, metadata, and contained_ids pointers.
typedef struct {
  char *data;         // Binary data (e.g., ObjectID or serialized address)
  int size;           // Size of data in bytes
  char *metadata;     // Metadata binary data (can be NULL if empty)
  int metadata_size;  // Size of metadata in bytes
  char *
      *contained_ids;  // Array of contained object ID binary data (can be NULL if empty)
  int contained_ids_count;  // Number of contained object IDs
} CObjectReference;

// CObjectArray represents an array of object references.
// Caller is responsible for freeing the entire structure and its contents.
typedef struct {
  CObjectReference *objects;  // Array of object references
  int count;                  // Number of objects
} CObjectArray;

// CWaitResult represents the result of a wait operation.
// Caller is responsible for freeing the ready array.
typedef struct {
  bool *ready;  // Array of boolean values indicating which objects are ready
  int count;    // Number of elements in ready array
} CWaitResult;

// ============================================================================
// ObjectStore Functions - Basic Operations
// ============================================================================

// CObjectStore_Put puts an object into the object store.
//
// Parameters:
//   data - Object data pointer
//   data_size - Size of object data in bytes
//   metadata - Object metadata pointer (can be NULL)
//   metadata_size - Size of metadata in bytes
//   owner_address - Serialized owner address (can be NULL)
//   owner_address_size - Size of owner address in bytes
//
// Returns:
//   CObjectReference containing the object ID, or empty reference on failure.
//   Caller is responsible for freeing the returned CObjectReference.
CObjectReference CObjectStore_Put(const char *data,
                                  int data_size,
                                  const char *metadata,
                                  int metadata_size,
                                  const char *owner_address,
                                  int owner_address_size);

// CObjectStore_PutWithID puts an object into the object store with a specific ID.
//
// Parameters:
//   object_id_data - Binary data of the desired object ID
//   object_id_size - Size of object ID binary data
//   data - Object data pointer
//   data_size - Size of object data in bytes
//   metadata - Object metadata pointer (can be NULL)
//   metadata_size - Size of metadata in bytes
//
// Returns:
//   0 on success, -1 on failure.
int CObjectStore_PutWithID(const char *object_id_data,
                           int object_id_size,
                           const char *data,
                           int data_size,
                           const char *metadata,
                           int metadata_size);

// CObjectStore_Get gets objects from the object store.
//
// Parameters:
//   object_ids - Array of object ID binary data pointers
//   object_id_sizes - Array of object ID sizes
//   count - Number of object IDs
//   timeout_ms - Timeout in milliseconds (-1 for infinite wait)
//
// Returns:
//   Pointer to CObjectArray containing the retrieved objects, or empty array on failure.
//   Caller is responsible for freeing the returned CObjectArray by calling
//   CObjectStore_FreeObjectArray.
CObjectArray *CObjectStore_Get(const char **object_ids,
                               int *object_id_sizes,
                               int count,
                               long long timeout_ms);

// CObjectStore_Wait waits for objects to be available in the object store.
//
// Parameters:
//   object_ids - Array of object ID binary data pointers
//   object_id_sizes - Array of object ID sizes
//   count - Number of object IDs
//   num_objects - Number of objects to wait for (0 for all)
//   timeout_ms - Timeout in milliseconds (-1 for infinite wait)
//   fetch_local - Whether to fetch objects locally
//
// Returns:
//   CWaitResult indicating which objects are ready, or empty result on failure.
//   Caller is responsible for freeing the returned CWaitResult.
CWaitResult CObjectStore_Wait(const char **object_ids,
                              int *object_id_sizes,
                              int count,
                              int num_objects,
                              long long timeout_ms,
                              bool fetch_local);

// CObjectStore_Delete deletes objects from the object store.
//
// Parameters:
//   object_ids - Array of object ID binary data pointers
//   object_id_sizes - Array of object ID sizes
//   count - Number of object IDs
//   local_only - Whether to delete only locally
//
// Returns:
//   0 on success, -1 on failure.
int CObjectStore_Delete(const char **object_ids,
                        int *object_id_sizes,
                        int count,
                        bool local_only);

// ============================================================================
// ObjectStore Functions - Reference Management
// ============================================================================

// CObjectStore_AddLocalReference adds a local reference to an object.
//
// Parameters:
//   object_id_data - Binary data of object ID
//   object_id_size - Size of object ID binary data
//
// Returns:
//   0 on success, -1 on failure.
int CObjectStore_AddLocalReference(const char *object_id_data, int object_id_size);

// CObjectStore_RemoveLocalReference removes a local reference from an object.
//
// Parameters:
//   object_id_data - Binary data of object ID
//   object_id_size - Size of object ID binary data
//
// Returns:
//   0 on success, -1 on failure.
int CObjectStore_RemoveLocalReference(const char *object_id_data, int object_id_size);

// CObjectStore_GetAllReferenceCounts gets all reference counts.
//
// Returns:
//   JSON string containing reference counts, or NULL on failure.
//   Format: {"object_id_hex":[local_count, submitted_count], ...}
//   Caller is responsible for freeing the returned string using CObjectStore_FreeString.
char *CObjectStore_GetAllReferenceCounts();

// CObjectStore_GetOwnerAddress gets the owner address for an object.
//
// Parameters:
//   object_id_data - Binary data of object ID
//   object_id_size - Size of object ID binary data
//
// Returns:
//   CObjectReference containing serialized owner address, or empty reference on failure.
//   Caller is responsible for freeing the returned CObjectReference.
CObjectReference CObjectStore_GetOwnerAddress(const char *object_id_data,
                                              int object_id_size);

// CObjectStore_GetOwnershipInfo gets ownership information for an object.
//
// Parameters:
//   object_id_data - Binary data of object ID
//   object_id_size - Size of object ID binary data
//
// Returns:
//   CObjectReference containing serialized ownership info, or empty reference on failure.
//   Caller is responsible for freeing the returned CObjectReference.
CObjectReference CObjectStore_GetOwnershipInfo(const char *object_id_data,
                                               int object_id_size);

// CObjectStore_RegisterOwnershipInfoAndResolveFuture registers ownership info and
// resolves a future.
//
// Parameters:
//   object_id_data - Binary data of object ID
//   object_id_size - Size of object ID binary data
//   outer_object_id_data - Binary data of outer object ID (can be NULL)
//   outer_object_id_size - Size of outer object ID binary data
//   owner_address - Serialized owner address
//   owner_address_size - Size of owner address in bytes
//
// Returns:
//   0 on success, -1 on failure.
int CObjectStore_RegisterOwnershipInfoAndResolveFuture(const char *object_id_data,
                                                       int object_id_size,
                                                       const char *outer_object_id_data,
                                                       int outer_object_id_size,
                                                       const char *owner_address,
                                                       int owner_address_size);

// ============================================================================
// Memory Management Helper Functions
// ============================================================================

// CObjectStore_FreeObjectReference frees a CObjectReference.
//
// Parameters:
//   ref - CObjectReference to free
void CObjectStore_FreeObjectReference(CObjectReference ref);

// CObjectStore_FreeObjectArray frees a CObjectArray.
//
// Parameters:
//   array - Pointer to CObjectArray to free
void CObjectStore_FreeObjectArray(CObjectArray *array);

// CObjectStore_FreeWaitResult frees a CWaitResult.
//
// Parameters:
//   result - CWaitResult to free
void CObjectStore_FreeWaitResult(CWaitResult result);

// CObjectStore_FreeString frees a string returned by ObjectStore functions.
//
// Parameters:
//   str - String to free
void CObjectStore_FreeString(char *str);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // SRC_RAY_CORE_WORKER_LIB_GO_NATIVE_OBJECT_STORE_H_
