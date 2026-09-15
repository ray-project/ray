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

// src/ray/core_worker/lib/go/gcs_memory.h
#pragma once
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Frees a C++-allocated memory block. Go must call this when done.
 *
 * @param ptr Pointer previously returned by a GCS accessor allocation.
 */
void ray_gcs_free_memory(void *ptr);

/**
 * @brief Frees a string array (each element plus the array itself).
 *
 * @param arr Array previously returned by a GCS accessor.
 * @param count Number of elements in the array.
 */
void ray_gcs_free_string_array(char **arr, int count);

#ifdef __cplusplus
}
#endif
