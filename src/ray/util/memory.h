// Copyright 2017 The Ray Authors.
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

#pragma once

#include <stdint.h>

namespace ray {

// A helper function for doing memcpy with multiple threads. This is required
// to saturate the memory bandwidth of modern cpus.
void parallel_memcopy(uint8_t *dst,
                      const uint8_t *src,
                      int64_t nbytes,
                      uintptr_t block_size,
                      int num_threads);

}  // namespace ray
