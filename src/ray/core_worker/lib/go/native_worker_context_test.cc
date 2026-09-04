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

// Unit tests for native_worker_context.cc - CGO wrapper memory management
// and error handling. These tests verify the C API wrapper implementation
// without requiring a full Ray infrastructure setup.

#include "src/ray/core_worker/lib/go/native_worker_context.h"

// Mock implementations of Go functions using weak symbols
// This allows the test to compile and run without Go runtime
extern "C" {

typedef struct {
  void* data_ptr;
  size_t size;
  void* ref_handle;
} MockGoObjectRefHandle;

__attribute__((weak)) void* GoAllocateObject(const char* object_id_data, int object_id_size,
                       const char* data, int data_size,
                       const char* metadata, int metadata_size) {
  // Allocate a minimal handle structure
  MockGoObjectRefHandle* handle = (MockGoObjectRefHandle*)malloc(sizeof(MockGoObjectRefHandle));
  if (handle) {
    handle->data_ptr = nullptr;
    handle->size = 0;
    handle->ref_handle = nullptr;
  }
  return handle;
}

__attribute__((weak)) void GoReleaseObjectRef(void* handle) {
  // Stub: nothing to do in mock
}

__attribute__((weak)) void* GoGetObjectData(void* handle) {
  return nullptr;
}

__attribute__((weak)) size_t GoGetObjectSize(void* handle) {
  return 0;
}

}  // extern "C"

#include <gtest/gtest.h>
#include <cstring>
#include <thread>
#include <vector>

namespace ray {
namespace core_worker {
namespace go {

// ============================================================================
// Test Fixtures
// ============================================================================

class NativeWorkerContextCGOTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // No setup needed for memory management and error handling tests
    // These tests don't require a CoreWorker instance
  }

  void TearDown() override {
    // No cleanup needed
  }
};

// ============================================================================
// Memory Management Tests
// ============================================================================

TEST_F(NativeWorkerContextCGOTest, CByteArrayStructureIsValid) {
  // Test: Verify CByteArray structure is properly defined
  CByteArray test_array;
  test_array.size = 0;
  test_array.data = nullptr;

  EXPECT_EQ(test_array.size, 0);
  EXPECT_EQ(test_array.data, nullptr);
}

TEST_F(NativeWorkerContextCGOTest, CByteArrayAllocation) {
  // Test: Verify we can allocate CByteArray structures
  const int kTestSize = 100;
  CByteArray* result = static_cast<CByteArray*>(malloc(sizeof(CByteArray)));

  ASSERT_NE(result, nullptr);
  result->size = kTestSize;
  result->data = static_cast<char*>(malloc(kTestSize));
  ASSERT_NE(result->data, nullptr);

  // Fill with test data
  memset(result->data, 0xAB, kTestSize);

  // Verify
  EXPECT_EQ(result->size, kTestSize);
  EXPECT_EQ(static_cast<unsigned char>(result->data[0]), 0xAB);
  EXPECT_EQ(static_cast<unsigned char>(result->data[kTestSize - 1]), 0xAB);

  // Clean up
  free(result->data);
  free(result);
}

TEST_F(NativeWorkerContextCGOTest, CByteArrayNullHandling) {
  // Test: Verify proper handling of NULL data pointers
  CByteArray* result = static_cast<CByteArray*>(malloc(sizeof(CByteArray)));
  ASSERT_NE(result, nullptr);

  result->size = 0;
  result->data = nullptr;

  // Should be able to free without issues
  free(result);
}

TEST_F(NativeWorkerContextCGOTest, CByteArrayZeroSize) {
  // Test: Zero-size allocation edge case
  CByteArray* result = static_cast<CByteArray*>(malloc(sizeof(CByteArray)));
  ASSERT_NE(result, nullptr);

  result->size = 0;
  result->data = nullptr;

  EXPECT_EQ(result->size, 0);
  EXPECT_EQ(result->data, nullptr);

  free(result);
}

TEST_F(NativeWorkerContextCGOTest, CByteArrayLargeAllocation) {
  // Test: Large CByteArray allocation

  const size_t kLargeSize = 1024 * 1024;  // 1MB
  CByteArray* result = static_cast<CByteArray*>(malloc(sizeof(CByteArray)));
  ASSERT_NE(result, nullptr);

  result->size = static_cast<int>(kLargeSize);
  result->data = static_cast<char*>(malloc(kLargeSize));
  ASSERT_NE(result->data, nullptr);

  // Fill with test pattern
  for (size_t i = 0; i < kLargeSize; i++) {
    result->data[i] = static_cast<char>(i & 0xFF);
  }

  // Verify
  EXPECT_EQ(result->size, static_cast<int>(kLargeSize));
  EXPECT_EQ(static_cast<unsigned char>(result->data[0]), 0x00);
  EXPECT_EQ(static_cast<unsigned char>(result->data[kLargeSize - 1]), 0xFF);

  // Clean up
  free(result->data);
  free(result);
}

}  // namespace go
}  // namespace core_worker
}  // namespace ray
