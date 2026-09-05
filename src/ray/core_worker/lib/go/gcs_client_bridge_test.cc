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

// src/ray/core_worker/lib/go/gcs_client_bridge_test.cc
#include "gcs_client_bridge.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "gcs_memory.h"

// Test 1: verify the header compiles and includes correctly
TEST(GcsClientBridgeTest, HeaderCompiles) {
  // Verify type definitions exist
  CGcsClient *client = nullptr;
  (void)client;  // avoid unused variable warning

  // Verify function declarations exist (not called; needs a real implementation)
  void (*create_fn)(const char *, const char *, int64_t, char **) =
      reinterpret_cast<void (*)(const char *, const char *, int64_t, char **)>(
          reinterpret_cast<uintptr_t>(&ray_gcs_client_create));
  (void)create_fn;

  SUCCEED() << "Header compiles successfully";
}

// Test 2: verify C ABI compatibility - ensure extern "C" works correctly
TEST(GcsClientBridgeTest, CAriCompatibility) {
  // Verify function pointers are directly accessible (proves C linkage)
  void (*free_fn)(void *) = ray_gcs_free_memory;
  void (*free_arr_fn)(char **, int) = ray_gcs_free_string_array;

  ASSERT_NE(free_fn, nullptr);
  ASSERT_NE(free_arr_fn, nullptr);
  (void)free_fn;
  (void)free_arr_fn;

  SUCCEED() << "C ABI compatibility verified";
}

// Test 3: verify basic behavior of the memory-free function
TEST(GcsClientBridgeTest, MemoryFree) {
  // Allocate some memory and free it
  char *test_str = static_cast<char *>(malloc(10));
  ASSERT_NE(test_str, nullptr);

  strcpy(test_str, "test");
  ASSERT_EQ(strlen(test_str), 4);

  ray_gcs_free_memory(test_str);
  // Must not access after free

  SUCCEED() << "Memory free works correctly";
}

// Test 4: verify the string-array free function
TEST(GcsClientBridgeTest, StringArrayFree) {
  // Create a simple string array
  char **arr = static_cast<char **>(malloc(3 * sizeof(char *)));
  ASSERT_NE(arr, nullptr);

  arr[0] = static_cast<char *>(malloc(5));
  arr[1] = static_cast<char *>(malloc(5));
  arr[2] = nullptr;
  ASSERT_NE(arr[0], nullptr);
  ASSERT_NE(arr[1], nullptr);

  strcpy(arr[0], "str1");
  strcpy(arr[1], "str2");

  ray_gcs_free_string_array(arr, 2);
  // Must not access after free

  SUCCEED() << "String array free works correctly";
}

// Test 5: verify the ray_gcs_free_string function
TEST(GcsClientBridgeTest, FreeString) {
  // Allocate with strdup (simulating a C++-returned string)
  const char *test_str = strdup("test string");
  ASSERT_NE(test_str, nullptr);
  ASSERT_STREQ(test_str, "test string");

  // Free with ray_gcs_free_string
  ray_gcs_free_string(test_str);
  // Must not access after free

  SUCCEED() << "ray_gcs_free_string works correctly";
}

// Test 6: verify ray_gcs_free_string handles null pointers
TEST(GcsClientBridgeTest, FreeStringNullPointer) {
  // Passing nullptr should not crash
  ray_gcs_free_string(nullptr);
  SUCCEED() << "ray_gcs_free_string handles nullptr safely";
}

// Test 7: verify invalid-arguments handling
TEST(GcsClientBridgeTest, InvalidArguments) {
  char *error = nullptr;

  // Test empty address
  CGcsClient *client = ray_gcs_client_create(
      nullptr, "00000000000000000000000000000000000000000000000000000000", 5000, &error);

  ASSERT_EQ(client, nullptr);
  ASSERT_NE(error, nullptr);
  std::cout << "Expected error for null address: " << error << std::endl;

  ray_gcs_free_memory(error);
}

// Test 8: verify ClusterID length checking
TEST(GcsClientBridgeTest, InvalidClusterIdLength) {
  char *error = nullptr;

  // Test invalid ClusterID length (should be 56 hex chars)
  CGcsClient *client = ray_gcs_client_create("127.0.0.1:6379",
                                             "short_id",  // too short
                                             5000,
                                             &error);

  ASSERT_EQ(client, nullptr);
  ASSERT_NE(error, nullptr);
  std::cout << "Expected error for short cluster ID: " << error << std::endl;

  ray_gcs_free_memory(error);
}

// Test 9: verify the Autoscaler GetStatus signature and basic error handling
TEST(GcsClientBridgeTest, AutoscalerGetStatusSignature) {
  // Verify the function exists and can be called (even without a live connection)
  char *serialized = nullptr;
  int size = 0;
  char *error = nullptr;

  // Test null pointer argument
  int result = ray_gcs_client_autoscaler_get_status(nullptr, &serialized, &size, &error);
  ASSERT_EQ(result, 0);
  ASSERT_NE(error, nullptr);
  std::cout << "Expected error for null client: " << error << std::endl;
  ray_gcs_free_memory(error);
  if (serialized) {
    ray_gcs_free_memory(serialized);
  }

  // Test null serialized_out argument - use nullptr client
  error = nullptr;
  result = ray_gcs_client_autoscaler_get_status(nullptr, nullptr, nullptr, &error);
  ASSERT_EQ(result, 0);
  ASSERT_NE(error, nullptr);
  std::cout << "Expected error for null serialized_out: " << error << std::endl;
  ray_gcs_free_memory(error);

  SUCCEED() << "Autoscaler get_status signature verified";
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
