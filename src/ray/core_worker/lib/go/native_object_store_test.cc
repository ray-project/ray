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

// Unit tests for native_object_store.cc - CGO wrapper for Ray Object Store C++ API.
// This test file verifies basic functionality that can be tested without full
// Ray infrastructure initialization.

#include "native_object_store.h"

#include <cstdlib>

#include "cgo_wrapper.h"  // for CSerializedObjectArray definition

// ============================================================================
// Mock implementation of Go CGO callback functions
// These weak symbols provide stub implementations for Go functions that are
// normally called from C++ via CGO. This allows the test to link without
// requiring the actual Go runtime.
// ============================================================================

extern "C" {

// Mock GoObjectRefHandle structure
typedef struct {
  void *data_ptr;
  size_t size;
  void *ref_handle;
} GoObjectRefHandle;

// Mock GoAllocateObject - returns a minimal valid handle
__attribute__((weak)) void *GoAllocateObject(const char *object_id_data,
                                             int object_id_size,
                                             const char *data,
                                             int data_size,
                                             const char *metadata,
                                             int metadata_size) {
  GoObjectRefHandle *handle = (GoObjectRefHandle *)malloc(sizeof(GoObjectRefHandle));
  if (handle) {
    handle->data_ptr = nullptr;
    handle->size = 0;
    handle->ref_handle = nullptr;
  }
  return handle;
}

// Mock GoReleaseObjectRef - does nothing
__attribute__((weak)) void GoReleaseObjectRef(void *handle) {
  // Stub
}

// Mock GoGetObjectData - returns null
__attribute__((weak)) void *GoGetObjectData(void *handle) { return nullptr; }

// Mock GoGetObjectSize - returns 0
__attribute__((weak)) size_t GoGetObjectSize(void *handle) { return 0; }

// Mock GoExecuteTask - returns null to indicate no task execution
// This stub allows the test to link without requiring the actual Go runtime
__attribute__((weak)) CSerializedObjectArray *GoExecuteTask(
    int task_type,
    const char **function_descriptor,
    int function_descriptor_count,
    const CFunctionArg *args,
    int args_count,
    int num_returns,
    const char *actor_id_data,
    int actor_id_size) {
  // Stub: return null to indicate no task execution
  return nullptr;
}

}  // extern "C"

#include <gtest/gtest.h>

#include <climits>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace ray {
namespace core_worker {
namespace go {

// ============================================================================
// Test Fixtures
// ============================================================================

class NativeObjectStoreCGOTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_data_ = "test_object_data";
    test_metadata_ = "test_metadata";
  }

  void TearDown() override {
    // Clean up any allocated resources
  }

  std::string test_data_;
  std::string test_metadata_;
};

// ============================================================================
// Memory Management Tests
// ============================================================================

TEST_F(NativeObjectStoreCGOTest, FreeObjectReferenceWithNullData) {
  // Test: FreeObjectReference with null data should not crash
  CObjectReference ref = {nullptr, 0};
  EXPECT_NO_THROW(CObjectStore_FreeObjectReference(ref));
}

TEST_F(NativeObjectStoreCGOTest, FreeObjectReferenceWithValidData) {
  // Test: FreeObjectReference with valid data
  CObjectReference ref{};  // Zero-initialize all fields
  ref.size = 10;
  ref.data = static_cast<char *>(malloc(ref.size));
  memset(ref.data, 0, ref.size);
  // Other fields (metadata, metadata_size, contained_ids, contained_ids_count)
  // are already zero-initialized by {} initialization

  EXPECT_NO_THROW(CObjectStore_FreeObjectReference(ref));
}

TEST_F(NativeObjectStoreCGOTest, FreeObjectArrayWithNullObjects) {
  // Test: FreeObjectArray with null objects should not crash
  CObjectArray array = {nullptr, 0};
  EXPECT_NO_THROW(CObjectStore_FreeObjectArray(&array));
}

TEST_F(NativeObjectStoreCGOTest, FreeObjectArrayWithValidObjects) {
  // Test: FreeObjectArray with valid objects
  CObjectArray array{};  // Zero-initialize
  array.count = 2;
  array.objects = static_cast<CObjectReference *>(malloc(sizeof(CObjectReference) * 2));
  if (array.objects == nullptr) {
    FAIL() << "Failed to allocate memory for objects array";
  }

  // Zero-initialize each CObjectReference
  memset(array.objects, 0, sizeof(CObjectReference) * 2);

  array.objects[0].data = static_cast<char *>(malloc(10));
  array.objects[0].size = 10;
  // metadata, metadata_size, contained_ids, contained_ids_count already zero

  array.objects[1].data = static_cast<char *>(malloc(20));
  array.objects[1].size = 20;
  // metadata, metadata_size, contained_ids, contained_ids_count already zero

  EXPECT_NO_THROW(CObjectStore_FreeObjectArray(&array));
}

TEST_F(NativeObjectStoreCGOTest, FreeWaitResultWithNullReady) {
  // Test: FreeWaitResult with null ready should not crash
  CWaitResult result = {nullptr, 0};
  EXPECT_NO_THROW(CObjectStore_FreeWaitResult(result));
}

TEST_F(NativeObjectStoreCGOTest, FreeWaitResultWithValidReady) {
  // Test: FreeWaitResult with valid ready array
  CWaitResult result;
  result.count = 3;
  result.ready = static_cast<bool *>(malloc(sizeof(bool) * 3));

  EXPECT_NO_THROW(CObjectStore_FreeWaitResult(result));
}

TEST_F(NativeObjectStoreCGOTest, FreeStringWithNullPointer) {
  // Test: FreeString with null pointer should not crash
  EXPECT_NO_THROW(CObjectStore_FreeString(nullptr));
}

TEST_F(NativeObjectStoreCGOTest, FreeStringWithValidPointer) {
  // Test: FreeString with valid pointer
  char *str = static_cast<char *>(malloc(20));
  strcpy(str, "test_string");

  EXPECT_NO_THROW(CObjectStore_FreeString(str));
}

// ============================================================================
// CObjectReference Struct Tests
// ============================================================================

TEST_F(NativeObjectStoreCGOTest, CObjectReference_StructSize) {
  // Test: Verify CObjectReference struct size
  // Note: Size includes padding for memory alignment
  EXPECT_GE(sizeof(CObjectReference), sizeof(char *));
  EXPECT_GE(sizeof(CObjectReference), sizeof(int));
}

TEST_F(NativeObjectStoreCGOTest, CObjectReference_NullInitialization) {
  // Test: Verify CObjectReference can be initialized to null
  CObjectReference ref = {nullptr, 0};
  EXPECT_EQ(ref.data, nullptr);
  EXPECT_EQ(ref.size, 0);
}

// ============================================================================
// CObjectArray Struct Tests
// ============================================================================

TEST_F(NativeObjectStoreCGOTest, CObjectArray_StructSize) {
  // Test: Verify CObjectArray struct size
  // Note: Size includes padding for memory alignment
  EXPECT_GE(sizeof(CObjectArray), sizeof(CObjectReference *));
  EXPECT_GE(sizeof(CObjectArray), sizeof(int));
}

TEST_F(NativeObjectStoreCGOTest, CObjectArray_NullInitialization) {
  // Test: Verify CObjectArray can be initialized to null
  CObjectArray array = {nullptr, 0};
  EXPECT_EQ(array.objects, nullptr);
  EXPECT_EQ(array.count, 0);
}

// ============================================================================
// CWaitResult Struct Tests
// ============================================================================

TEST_F(NativeObjectStoreCGOTest, CWaitResult_StructSize) {
  // Test: Verify CWaitResult struct size
  // Note: Size includes padding for memory alignment
  EXPECT_GE(sizeof(CWaitResult), sizeof(bool *));
  EXPECT_GE(sizeof(CWaitResult), sizeof(int));
}

TEST_F(NativeObjectStoreCGOTest, CWaitResult_NullInitialization) {
  // Test: Verify CWaitResult can be initialized to null
  CWaitResult result = {nullptr, 0};
  EXPECT_EQ(result.ready, nullptr);
  EXPECT_EQ(result.count, 0);
}

// ============================================================================
// Input Validation Tests (functions that should handle null inputs gracefully)
// ============================================================================
// Note: Tests that call CObjectStore_Put, CObjectStore_Get, CObjectStore_Wait,
// etc. require a fully initialized CoreWorker and cannot be run in isolation.
// These tests focus on functions that can operate without Ray infrastructure.

// Error code convention: CGO wrapper functions return -1 on error, 0 on success.

TEST_F(NativeObjectStoreCGOTest, DeleteWithNullPointers) {
  // Test: Delete with null pointers should fail (invalid parameters)
  int result = CObjectStore_Delete(nullptr, nullptr, 0, false);

  EXPECT_EQ(result, -1);  // -1 indicates error per CGO convention
}

TEST_F(NativeObjectStoreCGOTest, AddLocalReferenceWithNullPointer) {
  // Test: AddLocalReference with null pointer should fail
  int result = CObjectStore_AddLocalReference(nullptr, 0);

  EXPECT_EQ(result, -1);  // -1 indicates error per CGO convention
}

TEST_F(NativeObjectStoreCGOTest, AddLocalReferenceWithZeroSize) {
  // Test: AddLocalReference with zero size should fail
  int result = CObjectStore_AddLocalReference(test_data_.c_str(), 0);

  EXPECT_EQ(result, -1);  // -1 indicates error per CGO convention
}

TEST_F(NativeObjectStoreCGOTest, RemoveLocalReferenceWithNullPointer) {
  // Test: RemoveLocalReference with null pointer should fail
  int result = CObjectStore_RemoveLocalReference(nullptr, 0);

  EXPECT_EQ(result, -1);  // -1 indicates error per CGO convention
}

TEST_F(NativeObjectStoreCGOTest, GetOwnerAddressWithNullPointer) {
  // Test: GetOwnerAddress with null pointer should return empty result
  CObjectReference result = CObjectStore_GetOwnerAddress(nullptr, 0);

  EXPECT_EQ(result.data, nullptr);
  EXPECT_EQ(result.size, 0);
}

TEST_F(NativeObjectStoreCGOTest, GetOwnershipInfoWithNullPointer) {
  // Test: GetOwnershipInfo with null pointer should return empty result
  CObjectReference result = CObjectStore_GetOwnershipInfo(nullptr, 0);

  EXPECT_EQ(result.data, nullptr);
  EXPECT_EQ(result.size, 0);
}

TEST_F(NativeObjectStoreCGOTest, RegisterOwnershipInfoAndResolveFutureWithNullPointer) {
  // Test: RegisterOwnershipInfoAndResolveFuture with null pointer should fail
  int result = CObjectStore_RegisterOwnershipInfoAndResolveFuture(
      nullptr, 0, nullptr, 0, nullptr, 0);

  EXPECT_EQ(result, -1);  // -1 indicates error per CGO convention
}

TEST_F(NativeObjectStoreCGOTest, PutWithIDWithNullObjectId) {
  // Test: PutWithID with null object ID should fail
  int result = CObjectStore_PutWithID(nullptr,
                                      0,
                                      test_data_.c_str(),
                                      static_cast<int>(test_data_.size()),
                                      test_metadata_.c_str(),
                                      static_cast<int>(test_metadata_.size()));

  EXPECT_EQ(result, -1);  // -1 indicates error per CGO convention
}

TEST_F(NativeObjectStoreCGOTest, PutWithIDWithZeroSize) {
  // Test: PutWithID with zero size object ID should fail
  int result = CObjectStore_PutWithID(test_data_.c_str(),
                                      0,
                                      test_data_.c_str(),
                                      static_cast<int>(test_data_.size()),
                                      test_metadata_.c_str(),
                                      static_cast<int>(test_metadata_.size()));

  EXPECT_EQ(result, -1);  // -1 indicates error per CGO convention
}

TEST_F(NativeObjectStoreCGOTest, GetWithNullPointers) {
  // Test: Get with null pointers should return empty result
  CObjectArray *result = CObjectStore_Get(nullptr, nullptr, 0, 1000);

  EXPECT_NE(result, nullptr);  // Should return a valid pointer
  EXPECT_EQ(result->count, 0);
  EXPECT_EQ(result->objects, nullptr);

  // Clean up
  CObjectStore_FreeObjectArray(result);
}

TEST_F(NativeObjectStoreCGOTest, GetWithEmptyCount) {
  // Test: Get with count=0 should return empty result
  CObjectArray *result = CObjectStore_Get(nullptr, nullptr, 0, 1000);

  EXPECT_NE(result, nullptr);  // Should return a valid pointer
  EXPECT_EQ(result->count, 0);
  EXPECT_EQ(result->objects, nullptr);

  // Clean up
  CObjectStore_FreeObjectArray(result);
}

TEST_F(NativeObjectStoreCGOTest, WaitWithNullPointers) {
  // Test: Wait with null pointers should return empty result
  CWaitResult result = CObjectStore_Wait(nullptr, nullptr, 0, 1, 1000, true);

  EXPECT_EQ(result.count, 0);
  EXPECT_EQ(result.ready, nullptr);
}

// ============================================================================
// End of Tests
// ============================================================================
// Note: Additional tests for CObjectStore_Put, CObjectStore_Get, etc. would
// require full CoreWorker initialization and are better suited for integration
// tests with the complete Ray runtime environment.

}  // namespace go
}  // namespace core_worker
}  // namespace ray
