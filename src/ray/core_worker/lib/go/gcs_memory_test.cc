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

#include "gcs_memory.h"

#include <gtest/gtest.h>

#include <cstring>

// Test freeing a single memory block
TEST(GcsMemoryTest, FreeMemory) {
  // Allocate memory
  void *ptr = malloc(100);
  ASSERT_NE(ptr, nullptr);

  // Write test data
  memset(ptr, 0xAB, 100);

  // Free memory - this is the function Go CGO calls
  ray_gcs_free_memory(ptr);

  // Note: accessing freed memory is undefined; here we only verify the function does not
  // crash
  SUCCEED();
}

// Test freeing a NULL pointer
TEST(GcsMemoryTest, FreeMemoryNullPointer) {
  // Passing a NULL pointer should be handled safely
  ray_gcs_free_memory(nullptr);
  SUCCEED();
}

// Test freeing a string array
TEST(GcsMemoryTest, FreeStringArray) {
  const int count = 3;
  char **arr = static_cast<char **>(malloc(count * sizeof(char *)));
  ASSERT_NE(arr, nullptr);

  // Allocate each string
  for (int i = 0; i < count; i++) {
    arr[i] = static_cast<char *>(malloc(10));
    ASSERT_NE(arr[i], nullptr);
    snprintf(arr[i], 10, "str%d", i);
  }

  // Free the string array
  ray_gcs_free_string_array(arr, count);

  SUCCEED();
}

// Test freeing an empty string array
TEST(GcsMemoryTest, FreeStringArrayNull) {
  // Passing a NULL array should be handled safely
  ray_gcs_free_string_array(nullptr, 5);
  SUCCEED();
}

// Test freeing a string array with NULL elements
TEST(GcsMemoryTest, FreeStringArrayWithNullElements) {
  const int count = 3;
  char **arr = static_cast<char **>(malloc(count * sizeof(char *)));
  ASSERT_NE(arr, nullptr);

  // Some elements are NULL
  arr[0] = static_cast<char *>(malloc(10));
  arr[1] = nullptr;  // NULL element
  arr[2] = static_cast<char *>(malloc(10));

  // Free the string array - should handle NULL elements correctly
  ray_gcs_free_string_array(arr, count);

  SUCCEED();
}
