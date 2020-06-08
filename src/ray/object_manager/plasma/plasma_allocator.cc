// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/util/logging.h>

#include "plasma/malloc.h"
#include "plasma/plasma_allocator.h"

namespace plasma {

extern "C" {
void* dlmemalign(size_t alignment, size_t bytes);
void dlfree(void* mem);
}

int64_t PlasmaAllocator::footprint_limit_ = 0;
int64_t PlasmaAllocator::allocated_ = 0;

void* PlasmaAllocator::Memalign(size_t alignment, size_t bytes) {
  if (allocated_ + static_cast<int64_t>(bytes) > footprint_limit_) {
    return nullptr;
  }
  void* mem = dlmemalign(alignment, bytes);
  ARROW_CHECK(mem);
  allocated_ += bytes;
  return mem;
}

void PlasmaAllocator::Free(void* mem, size_t bytes) {
  dlfree(mem);
  allocated_ -= bytes;
}

void PlasmaAllocator::SetFootprintLimit(size_t bytes) {
  footprint_limit_ = static_cast<int64_t>(bytes);
}

int64_t PlasmaAllocator::GetFootprintLimit() { return footprint_limit_; }

int64_t PlasmaAllocator::Allocated() { return allocated_; }

}  // namespace plasma
