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

// CGO Boundary Layer for Ray ObjectStore operations.
// This file contains only C interface functions and type conversions.
// All business logic is delegated to ObjectStoreOperations.

#include "native_object_store.h"

#include <cstring>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "cgo_wrapper.h"
#include "object_store_ops.h"
#include "ray/util/logging.h"

// Use nlohmann::json for type-safe JSON construction
#include "nlohmann/json.hpp"
using json = nlohmann::json;

using ray::go::CgoErrorHandler;
using ray::go::CgoTypeConverter;
using ray::go::CgoUniquePtr;

// ============================================================================
// Helper Functions - Memory Management
// ============================================================================

namespace {

// Free functions for C types defined in native_object_store.h
void FreeCObjectReference(CObjectReference *ref) {
  if (ref == nullptr) {
    return;
  }
  if (ref->data != nullptr) {
    free(ref->data);
  }
  if (ref->metadata != nullptr) {
    free(ref->metadata);
  }
  if (ref->contained_ids != nullptr) {
    for (int i = 0; i < ref->contained_ids_count; ++i) {
      free(ref->contained_ids[i]);
    }
    free(ref->contained_ids);
  }
}

void FreeCObjectArray(CObjectArray *array) {
  if (array == nullptr) {
    return;
  }
  // With arena allocation, all memory is in one contiguous block starting at 'array'.
  // The objects array and all data buffers are within this arena, so we just free
  // the arena once. Do NOT free individual fields as they point into the arena.
  free(array);
}

void FreeCWaitResult(CWaitResult *result) {
  if (result == nullptr) {
    return;
  }
  if (result->ready != nullptr) {
    free(result->ready);
  }
  // Do NOT free(result) - the result itself may be stack-allocated
  // Only the dynamically allocated ready array is freed
}

// RAII wrappers using CgoUniquePtr from cgo_wrapper.h
using CObjectReferencePtr = CgoUniquePtr<CObjectReference, &FreeCObjectReference>;
using CObjectArrayPtr = CgoUniquePtr<CObjectArray, &FreeCObjectArray>;
using CWaitResultPtr = CgoUniquePtr<CWaitResult, &FreeCWaitResult>;

// Helper function to create CObjectReference from ray::ObjectID
CObjectReference ObjectIDToCObjectReference(const ray::ObjectID &object_id) {
  CObjectReference result{};
  const auto &binary = object_id.Binary();
  result.size = static_cast<int>(binary.size());
  result.data = static_cast<char *>(malloc(result.size));
  if (result.data != nullptr) {
    memcpy(result.data, binary.data(), result.size);
  }
  result.metadata = nullptr;
  result.metadata_size = 0;
  result.contained_ids = nullptr;
  result.contained_ids_count = 0;
  return result;
}

// Helper function to create CObjectArray from
// std::vector<std::shared_ptr<ray::RayObject>> Uses arena allocation to reduce malloc
// calls from O(n) to O(1)
CObjectArray *CreateCObjectArray(
    const std::vector<std::shared_ptr<ray::RayObject>> &objects) {
  if (objects.empty()) {
    CObjectArray empty{};
    empty.objects = nullptr;
    empty.count = 0;
    return new CObjectArray(empty);
  }

  const size_t count = objects.size();

  // First pass: compute total size needed for arena allocation
  size_t total_size = 0;

  // Section 1: CObjectArray structure
  const size_t array_offset = 0;
  total_size += sizeof(CObjectArray);

  // Align for CObjectReference array
  total_size =
      (total_size + alignof(CObjectReference) - 1) & ~(alignof(CObjectReference) - 1);
  const size_t refs_offset = total_size;
  total_size += sizeof(CObjectReference) * count;

  // Section 2: Data buffers, metadata buffers, and contained_ids
  // Use stack-allocated arrays for small counts to avoid heap allocation overhead
  // For large counts, fall back to heap allocation
  constexpr size_t kStackArraySize = 64;
  size_t data_offsets_stack[kStackArraySize];
  size_t metadata_offsets_stack[kStackArraySize];
  size_t contained_ids_offsets_stack[kStackArraySize];

  size_t *data_offsets = data_offsets_stack;
  size_t *metadata_offsets = metadata_offsets_stack;
  size_t *contained_ids_offsets = contained_ids_offsets_stack;

  // Allocate on heap if count exceeds stack array size
  if (count > kStackArraySize) {
    data_offsets = new size_t[count]();
    metadata_offsets = new size_t[count]();
    contained_ids_offsets = new size_t[count]();
  } else {
    // Zero-initialize stack arrays
    std::fill(data_offsets, data_offsets + count, 0);
    std::fill(metadata_offsets, metadata_offsets + count, 0);
    std::fill(contained_ids_offsets, contained_ids_offsets + count, 0);
  }

  // Cleanup helper lambda to avoid code duplication
  auto cleanup = [&]() {
    if (count > kStackArraySize) {
      delete[] data_offsets;
      delete[] metadata_offsets;
      delete[] contained_ids_offsets;
    }
  };

  for (size_t i = 0; i < count; ++i) {
    const auto &obj = objects[i];
    if (obj == nullptr) {
      RAY_LOG(ERROR) << "Null object at index " << i;
      cleanup();
      return nullptr;
    }

    // Align for data buffer (char has alignment of 1, so this is a no-op but kept for
    // clarity)
    const auto &data_buffer = obj->GetData();
    size_t data_size = (data_buffer != nullptr) ? data_buffer->Size() : 0;
    if (data_size > 0) {
      // char alignment is 1, so no padding needed
      data_offsets[i] = total_size;
      total_size += data_size;
    }

    // Align for metadata buffer (same as data)
    const auto &metadata_buffer = obj->GetMetadata();
    size_t metadata_size = (metadata_buffer != nullptr) ? metadata_buffer->Size() : 0;
    if (metadata_size > 0) {
      metadata_offsets[i] = total_size;
      total_size += metadata_size;
    }

    // Align for contained_ids array (array of char* pointers)
    const auto &contained_refs = obj->GetNestedRefs();
    size_t contained_count = contained_refs.size();
    if (contained_count > 0) {
      // Align for pointer array
      total_size = (total_size + alignof(char *) - 1) & ~(alignof(char *) - 1);
      contained_ids_offsets[i] = total_size;
      total_size += sizeof(char *) * contained_count;

      // Add space for each contained ID's binary data
      for (const auto &contained_ref : contained_refs) {
        const auto &contained_binary = contained_ref.object_id();
        total_size += contained_binary.size();
      }
    }
  }

  // Allocate the entire arena in ONE malloc call
  uint8_t *arena = static_cast<uint8_t *>(malloc(total_size));
  if (!arena) {
    RAY_LOG(ERROR) << "Failed to allocate arena of size " << total_size;
    cleanup();
    return nullptr;
  }

  // Zero-initialize the entire arena
  memset(arena, 0, total_size);

  // Second pass: set up pointers and copy data
  CObjectArray *result = reinterpret_cast<CObjectArray *>(arena + array_offset);
  result->count = static_cast<int>(count);
  result->objects = reinterpret_cast<CObjectReference *>(arena + refs_offset);

  for (size_t i = 0; i < count; ++i) {
    const auto &obj = objects[i];

    // Set up data buffer
    const auto &data_buffer = obj->GetData();
    if (data_buffer != nullptr && data_buffer->Size() > 0) {
      result->objects[i].size = static_cast<int>(data_buffer->Size());
      result->objects[i].data = reinterpret_cast<char *>(arena + data_offsets[i]);
      memcpy(result->objects[i].data, data_buffer->Data(), data_buffer->Size());
    }

    // Set up metadata buffer
    const auto &metadata_buffer = obj->GetMetadata();
    if (metadata_buffer != nullptr && metadata_buffer->Size() > 0) {
      result->objects[i].metadata_size = static_cast<int>(metadata_buffer->Size());
      result->objects[i].metadata = reinterpret_cast<char *>(arena + metadata_offsets[i]);
      memcpy(
          result->objects[i].metadata, metadata_buffer->Data(), metadata_buffer->Size());
    }

    // Set up contained_ids
    const auto &contained_refs = obj->GetNestedRefs();
    if (!contained_refs.empty()) {
      result->objects[i].contained_ids_count = static_cast<int>(contained_refs.size());
      result->objects[i].contained_ids =
          reinterpret_cast<char **>(arena + contained_ids_offsets[i]);

      // Compute where to place the binary data for this object's contained IDs
      size_t contained_data_offset =
          contained_ids_offsets[i] + sizeof(char *) * contained_refs.size();

      for (size_t j = 0; j < contained_refs.size(); ++j) {
        const auto &contained_binary = contained_refs[j].object_id();
        result->objects[i].contained_ids[j] =
            reinterpret_cast<char *>(arena + contained_data_offset);
        memcpy(result->objects[i].contained_ids[j],
               contained_binary.data(),
               contained_binary.size());
        contained_data_offset += contained_binary.size();
      }
    }
  }

  // Clean up temporary offset arrays (heap-allocated only)
  cleanup();

  return result;
}

// Helper function to create CWaitResult from std::vector<bool>
CWaitResult *CreateCWaitResult(const std::vector<bool> &ready) {
  if (ready.empty()) {
    CWaitResult empty{};
    empty.ready = nullptr;
    empty.count = 0;
    return new CWaitResult(empty);
  }

  auto result_ptr =
      CWaitResultPtr(static_cast<CWaitResult *>(malloc(sizeof(CWaitResult))));
  if (!result_ptr) {
    RAY_LOG(ERROR) << "Failed to allocate memory for CWaitResult";
    return nullptr;
  }

  result_ptr->count = static_cast<int>(ready.size());
  result_ptr->ready = static_cast<bool *>(malloc(sizeof(bool) * result_ptr->count));
  if (!result_ptr->ready) {
    RAY_LOG(ERROR) << "Failed to allocate memory for ready array";
    return nullptr;
  }

  // Copy ready flags
  for (int i = 0; i < result_ptr->count; ++i) {
    result_ptr->ready[i] = ready[i];
  }

  return result_ptr.release();
}

// Helper function to parse object IDs from C arrays
std::vector<ray::ObjectID> ParseObjectIds(const char **object_ids,
                                          const int *object_id_sizes,
                                          int count) {
  std::vector<ray::ObjectID> ids;
  if (object_ids == nullptr || object_id_sizes == nullptr || count <= 0) {
    return ids;
  }

  for (int i = 0; i < count; ++i) {
    if (object_ids[i] != nullptr && object_id_sizes[i] > 0) {
      ids.push_back(
          ray::ObjectID::FromBinary(std::string(object_ids[i], object_id_sizes[i])));
    }
  }

  return ids;
}

// Helper function to parse single object ID from CByteArray
ray::ObjectID ParseObjectIDFromCByteArray(const char *data, int size) {
  if (data == nullptr || size <= 0) {
    throw std::invalid_argument("Invalid object ID data");
  }
  return ray::ObjectID::FromBinary(std::string(data, size));
}

// Helper function to convert reference counts map to JSON string using nlohmann::json
std::string ReferenceCountsToJSON(
    const std::unordered_map<ray::ObjectID, std::pair<size_t, size_t>> &ref_counts) {
  json j = json::object();
  for (const auto &pair : ref_counts) {
    // Use hex string of ObjectID as key, array of [local_count, submitted_count] as value
    j[pair.first.Hex()] = json::array({pair.second.first, pair.second.second});
  }
  return j.dump();
}

}  // anonymous namespace

// ============================================================================
// CGO Boundary Layer - C Interface Functions
// ============================================================================

extern "C" CObjectReference CObjectStore_Put(const char *data,
                                             int data_size,
                                             const char *metadata,
                                             int metadata_size,
                                             const char *owner_address,
                                             int owner_address_size) {
  return CgoErrorHandler::Execute("CObjectStore_Put", [&]() -> CObjectReference {
    // Create RayObject from data and metadata
    std::shared_ptr<ray::Buffer> data_buffer;
    if (data != nullptr && data_size > 0) {
      data_buffer = std::make_shared<ray::LocalMemoryBuffer>(
          const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(data)),
          static_cast<size_t>(data_size),
          /*copy_data=*/true);
    }

    std::shared_ptr<ray::Buffer> metadata_buffer;
    if (metadata != nullptr && metadata_size > 0) {
      metadata_buffer = std::make_shared<ray::LocalMemoryBuffer>(
          const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(metadata)),
          static_cast<size_t>(metadata_size),
          /*copy_data=*/true);
    }

    auto ray_object =
        std::make_shared<ray::RayObject>(data_buffer,
                                         metadata_buffer,
                                         std::vector<ray::rpc::ObjectReference>(),
                                         /*contains_data=*/true);

    // Call business logic
    auto &ops = ray::go::ObjectStoreOperations::GetInstance();
    ray::ObjectID object_id = ops.Put(ray_object);

    // Convert to C type
    return ObjectIDToCObjectReference(object_id);
  });
}

extern "C" int CObjectStore_PutWithID(const char *object_id_data,
                                      int object_id_size,
                                      const char *data,
                                      int data_size,
                                      const char *metadata,
                                      int metadata_size) {
  return CgoErrorHandler::ExecuteInt("CObjectStore_PutWithID", [&]() -> int {
    // Parse object ID
    ray::ObjectID object_id = ParseObjectIDFromCByteArray(object_id_data, object_id_size);

    // Create RayObject
    std::shared_ptr<ray::Buffer> data_buffer;
    if (data != nullptr && data_size > 0) {
      data_buffer = std::make_shared<ray::LocalMemoryBuffer>(
          const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(data)),
          static_cast<size_t>(data_size),
          /*copy_data=*/true);
    }

    std::shared_ptr<ray::Buffer> metadata_buffer;
    if (metadata != nullptr && metadata_size > 0) {
      metadata_buffer = std::make_shared<ray::LocalMemoryBuffer>(
          const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(metadata)),
          static_cast<size_t>(metadata_size),
          /*copy_data=*/true);
    }

    auto ray_object =
        std::make_shared<ray::RayObject>(data_buffer,
                                         metadata_buffer,
                                         std::vector<ray::rpc::ObjectReference>(),
                                         /*contains_data=*/true);

    // Call business logic
    auto &ops = ray::go::ObjectStoreOperations::GetInstance();
    ops.PutWithID(object_id, ray_object);
    return 0;  // Success
  });
}

extern "C" CObjectArray *CObjectStore_Get(const char **object_ids,
                                          int *object_id_sizes,
                                          int count,
                                          long long timeout_ms) {
  return CgoErrorHandler::Execute("CObjectStore_Get", [&]() -> CObjectArray * {
    if (object_ids == nullptr || object_id_sizes == nullptr || count <= 0) {
      throw std::invalid_argument("Invalid object_ids parameters");
    }

    // Parse object IDs
    std::vector<ray::ObjectID> ids = ParseObjectIds(object_ids, object_id_sizes, count);

    if (ids.empty()) {
      throw std::invalid_argument("No valid object IDs found");
    }

    // Call business logic
    auto &ops = ray::go::ObjectStoreOperations::GetInstance();
    auto objects = ops.Get(ids, static_cast<int>(timeout_ms));

    // Convert to C type - return pointer directly, not dereferenced
    return CreateCObjectArray(objects);
  });
}

extern "C" CWaitResult CObjectStore_Wait(const char **object_ids,
                                         int *object_id_sizes,
                                         int count,
                                         int num_objects,
                                         long long timeout_ms,
                                         bool fetch_local) {
  return CgoErrorHandler::Execute("CObjectStore_Wait", [&]() -> CWaitResult {
    if (object_ids == nullptr || object_id_sizes == nullptr || count <= 0) {
      throw std::invalid_argument("Invalid object_ids parameters");
    }

    // Parse object IDs
    std::vector<ray::ObjectID> ids = ParseObjectIds(object_ids, object_id_sizes, count);

    if (ids.empty()) {
      throw std::invalid_argument("No valid object IDs found");
    }

    // Call business logic
    auto &ops = ray::go::ObjectStoreOperations::GetInstance();
    auto ready = ops.Wait(ids, num_objects, timeout_ms, fetch_local);

    // Convert to C type
    CWaitResult *result = CreateCWaitResult(ready);
    if (result == nullptr) {
      CWaitResult empty{};
      empty.ready = nullptr;
      empty.count = 0;
      return empty;
    }
    return *result;
  });
}

extern "C" int CObjectStore_Delete(const char **object_ids,
                                   int *object_id_sizes,
                                   int count,
                                   bool local_only) {
  return CgoErrorHandler::ExecuteInt("CObjectStore_Delete", [&]() -> int {
    if (object_ids == nullptr || object_id_sizes == nullptr || count <= 0) {
      throw std::invalid_argument("Invalid object_ids parameters");
    }

    // Parse object IDs
    std::vector<ray::ObjectID> ids = ParseObjectIds(object_ids, object_id_sizes, count);

    if (ids.empty()) {
      throw std::invalid_argument("No valid object IDs found");
    }

    // Call business logic
    auto &ops = ray::go::ObjectStoreOperations::GetInstance();
    ops.Delete(ids, local_only);
    return 0;  // Success
  });
}

extern "C" int CObjectStore_AddLocalReference(const char *object_id_data,
                                              int object_id_size) {
  return CgoErrorHandler::ExecuteInt("CObjectStore_AddLocalReference", [&]() -> int {
    // Parse object ID
    ray::ObjectID id = ParseObjectIDFromCByteArray(object_id_data, object_id_size);

    // Call business logic
    auto &ops = ray::go::ObjectStoreOperations::GetInstance();
    ops.AddLocalReference(id);
    return 0;  // Success
  });
}

extern "C" int CObjectStore_RemoveLocalReference(const char *object_id_data,
                                                 int object_id_size) {
  return CgoErrorHandler::ExecuteInt("CObjectStore_RemoveLocalReference", [&]() -> int {
    // Parse object ID
    ray::ObjectID id = ParseObjectIDFromCByteArray(object_id_data, object_id_size);

    // Call business logic
    auto &ops = ray::go::ObjectStoreOperations::GetInstance();
    ops.RemoveLocalReference(id);
    return 0;  // Success
  });
}

extern "C" char *CObjectStore_GetAllReferenceCounts() {
  return CgoErrorHandler::Execute("CObjectStore_GetAllReferenceCounts", [&]() -> char * {
    // Call business logic
    auto &ops = ray::go::ObjectStoreOperations::GetInstance();
    auto ref_counts = ops.GetAllReferenceCounts();

    // Convert to JSON string
    std::string json_result = ReferenceCountsToJSON(ref_counts);

    // Convert to C string
    char *result = static_cast<char *>(malloc(json_result.size() + 1));
    if (result != nullptr) {
      memcpy(result, json_result.c_str(), json_result.size() + 1);
    }
    return result;
  });
}

extern "C" CObjectReference CObjectStore_GetOwnerAddress(const char *object_id_data,
                                                         int object_id_size) {
  return CgoErrorHandler::Execute(
      "CObjectStore_GetOwnerAddress", [&]() -> CObjectReference {
        // Parse object ID
        ray::ObjectID id = ParseObjectIDFromCByteArray(object_id_data, object_id_size);

        // Call business logic
        auto &ops = ray::go::ObjectStoreOperations::GetInstance();
        ray::rpc::Address owner_address = ops.GetOwnerAddress(id);

        // Serialize owner address to string
        std::string serialized;
        if (!owner_address.SerializeToString(&serialized)) {
          throw std::runtime_error("Failed to serialize owner address");
        }

        // Convert to C type (using data field to store serialized address)
        CObjectReference result{};
        result.size = static_cast<int>(serialized.size());
        result.data = static_cast<char *>(malloc(result.size));
        if (result.data != nullptr) {
          memcpy(result.data, serialized.data(), result.size);
        }
        result.metadata = nullptr;
        result.metadata_size = 0;
        result.contained_ids = nullptr;
        result.contained_ids_count = 0;
        return result;
      });
}

extern "C" CObjectReference CObjectStore_GetOwnershipInfo(const char *object_id_data,
                                                          int object_id_size) {
  return CgoErrorHandler::Execute(
      "CObjectStore_GetOwnershipInfo", [&]() -> CObjectReference {
        // Parse object ID
        ray::ObjectID id = ParseObjectIDFromCByteArray(object_id_data, object_id_size);

        // Call business logic
        auto &ops = ray::go::ObjectStoreOperations::GetInstance();
        std::string ownership_info = ops.GetOwnershipInfo(id);

        // Convert to C type (using data field to store serialized info)
        CObjectReference result{};
        result.size = static_cast<int>(ownership_info.size());
        result.data = static_cast<char *>(malloc(result.size));
        if (result.data != nullptr) {
          memcpy(result.data, ownership_info.c_str(), result.size);
        }
        result.metadata = nullptr;
        result.metadata_size = 0;
        result.contained_ids = nullptr;
        result.contained_ids_count = 0;
        return result;
      });
}

extern "C" int CObjectStore_RegisterOwnershipInfoAndResolveFuture(
    const char *object_id_data,
    int object_id_size,
    const char *outer_object_id_data,
    int outer_object_id_size,
    const char *owner_address,
    int owner_address_size) {
  return CgoErrorHandler::ExecuteInt(
      "CObjectStore_RegisterOwnershipInfoAndResolveFuture", [&]() -> int {
        // Parse object ID
        ray::ObjectID id = ParseObjectIDFromCByteArray(object_id_data, object_id_size);

        // Parse outer object ID (may be nil)
        ray::ObjectID outer_id;
        if (outer_object_id_data != nullptr && outer_object_id_size > 0) {
          outer_id =
              ParseObjectIDFromCByteArray(outer_object_id_data, outer_object_id_size);
        }

        // Parse owner address
        ray::rpc::Address owner_addr;
        if (owner_address != nullptr && owner_address_size > 0) {
          if (!owner_addr.ParseFromString(
                  std::string(owner_address, owner_address_size))) {
            throw std::runtime_error("Failed to parse owner address");
          }
        }

        // Call business logic
        auto &ops = ray::go::ObjectStoreOperations::GetInstance();
        ops.RegisterOwnershipInfoAndResolveFuture(id, outer_id, owner_addr);
        return 0;  // Success
      });
}

extern "C" void CObjectStore_FreeObjectReference(CObjectReference ref) {
  FreeCObjectReference(&ref);
}

extern "C" void CObjectStore_FreeObjectArray(CObjectArray *array) {
  FreeCObjectArray(array);
}

extern "C" void CObjectStore_FreeWaitResult(CWaitResult result) {
  FreeCWaitResult(&result);
}

extern "C" void CObjectStore_FreeString(char *str) { free(str); }
