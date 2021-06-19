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

#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

#include "ray/object_manager/plasma/malloc.h"
#include "ray/object_manager/plasma/plasma_allocator.h"

namespace plasma {

bool IsOutsideInitialAllocation(void *ptr);

extern "C" {
void *dlmemalign(size_t alignment, size_t bytes);
void dlfree(void *mem);
int dlmallopt(int param_number, int value);
}

/* Copied from dlmalloc.c; make sure to keep in sync */
size_t MAX_SIZE_T = (size_t)-1;
const int M_MMAP_THRESHOLD = -3;

int64_t PlasmaAllocator::footprint_limit_ = 0;
int64_t PlasmaAllocator::allocated_ = 0;
int64_t PlasmaAllocator::fallback_allocated_ = 0;
absl::flat_hash_map<void *, PlasmaAllocator::AllocationInfo>
    PlasmaAllocator::cached_allocation_info_{};

namespace {
/// The invalid allocation info.
const PlasmaAllocator::AllocationInfo invalid_info{
    .fd = std::make_pair(INVALID_FD, INVALID_UNIQUE_FD_ID),
    .offset = 0,
    .device_num = 0,
    .mmap_size = 0};

PlasmaAllocator::AllocationInfo BuildAllocationInfo(void *addr) {
  PlasmaAllocator::AllocationInfo info{};
  if (detail::GetMallocMapinfo(addr, &info.fd, &info.mmap_size, &info.offset)) {
    return info;
  }
  return invalid_info;
}
}  // namespace

void *PlasmaAllocator::Memalign(size_t alignment, size_t bytes) {
  if (!RayConfig::instance().plasma_unlimited()) {
    // We only check against the footprint limit in limited allocation mode.
    // In limited mode: the check is done here; dlmemalign never returns nullptr.
    // In unlimited mode: dlmemalign returns nullptr once the initial /dev/shm block
    // fills.
    if (allocated_ + static_cast<int64_t>(bytes) > footprint_limit_) {
      return nullptr;
    }
  }
  RAY_LOG(DEBUG) << "allocating " << bytes;
  void *mem = dlmemalign(alignment, bytes);
  RAY_LOG(DEBUG) << "allocated " << bytes << " at " << mem;
  if (!mem) {
    return nullptr;
  }
  allocated_ += bytes;
  cached_allocation_info_.emplace(mem, BuildAllocationInfo(mem));
  return mem;
}

void *PlasmaAllocator::DiskMemalignUnlimited(size_t alignment, size_t bytes) {
  // Forces allocation as a separate file.
  RAY_CHECK(dlmallopt(M_MMAP_THRESHOLD, 0));
  void *mem = dlmemalign(alignment, bytes);
  // Reset to the default value.
  RAY_CHECK(dlmallopt(M_MMAP_THRESHOLD, MAX_SIZE_T));
  if (!mem) {
    return nullptr;
  }
  allocated_ += bytes;
  // The allocation was servicable using the initial region, no need to fallback.
  if (IsOutsideInitialAllocation(mem)) {
    fallback_allocated_ += bytes;
  }
  return mem;
}

void PlasmaAllocator::Free(void *mem, size_t bytes) {
  RAY_LOG(DEBUG) << "deallocating " << bytes << " at " << mem;
  dlfree(mem);
  allocated_ -= bytes;
  if (RayConfig::instance().plasma_unlimited() && IsOutsideInitialAllocation(mem)) {
    fallback_allocated_ -= bytes;
  }
  cached_allocation_info_.erase(mem);
}

void PlasmaAllocator::SetFootprintLimit(size_t bytes) {
  footprint_limit_ = static_cast<int64_t>(bytes);
}

int64_t PlasmaAllocator::GetFootprintLimit() { return footprint_limit_; }

int64_t PlasmaAllocator::Allocated() { return allocated_; }

int64_t PlasmaAllocator::FallbackAllocated() { return fallback_allocated_; }

const PlasmaAllocator::AllocationInfo &PlasmaAllocator::GetAllocationInfo(void *address) {
  auto it = cached_allocation_info_.find(address);
  if (it != cached_allocation_info_.end()) {
    return it->second;
  }
  return invalid_info;
}

}  // namespace plasma
