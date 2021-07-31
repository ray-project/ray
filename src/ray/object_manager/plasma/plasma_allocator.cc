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

namespace {
/* Copied from dlmalloc.c; make sure to keep in sync */
size_t MAX_SIZE_T = (size_t)-1;
const int M_MMAP_THRESHOLD = -3;

// We align the allocated region to a 64-byte boundary. This is not
// strictly necessary, but it is an optimization that could speed up the
// computation of a hash of the data (see compute_object_hash_parallel in
// plasma_client.cc). Note that even though this pointer is 64-byte aligned,
// it is not guaranteed that the corresponding pointer in the client will be
// 64-byte aligned, but in practice it often will be.
const size_t kAllocationAlignment = 64;

absl::optional<Allocation> BuildAllocation(void *addr, size_t size) {
  if (addr == nullptr) {
    return absl::nullopt;
  }
  MEMFD_TYPE fd;
  int64_t mmap_size;
  ptrdiff_t offset;

  if (GetMallocMapinfo(addr, &fd, &mmap_size, &offset)) {
    return Allocation(addr, static_cast<int64_t>(size), std::move(fd), offset,
                      0 /* device_number*/, mmap_size);
  }
  return absl::nullopt;
}

}  // namespace

/* static */ PlasmaAllocator &PlasmaAllocator::GetInstance() {
  static PlasmaAllocator instance(kAllocationAlignment);
  return instance;
}

PlasmaAllocator::PlasmaAllocator(size_t alignment)
    : kAlignment(alignment), allocated_(0), fallback_allocated_(0), footprint_limit_(0) {}

absl::optional<Allocation> PlasmaAllocator::Allocate(size_t bytes) {
  if (!RayConfig::instance().plasma_unlimited()) {
    // We only check against the footprint limit in limited allocation mode.
    // In limited mode: the check is done here; dlmemalign never returns nullptr.
    // In unlimited mode: dlmemalign returns nullptr once the initial /dev/shm block
    // fills.
    if (allocated_ + static_cast<int64_t>(bytes) > footprint_limit_) {
      return absl::nullopt;
    }
  }
  RAY_LOG(DEBUG) << "allocating " << bytes;
  void *mem = dlmemalign(kAlignment, bytes);
  RAY_LOG(DEBUG) << "allocated " << bytes << " at " << mem;
  if (!mem) {
    return absl::nullopt;
  }
  allocated_ += bytes;
  return BuildAllocation(mem, bytes);
}

absl::optional<Allocation> PlasmaAllocator::FallbackAllocate(size_t bytes) {
  // Forces allocation as a separate file.
  RAY_CHECK(dlmallopt(M_MMAP_THRESHOLD, 0));
  RAY_LOG(DEBUG) << "fallback allocating " << bytes;
  void *mem = dlmemalign(kAlignment, bytes);
  RAY_LOG(DEBUG) << "allocated " << bytes << " at " << mem;
  // Reset to the default value.
  RAY_CHECK(dlmallopt(M_MMAP_THRESHOLD, MAX_SIZE_T));

  if (!mem) {
    return absl::nullopt;
  }

  allocated_ += bytes;
  // The allocation was servicable using the initial region, no need to fallback.
  if (IsOutsideInitialAllocation(mem)) {
    fallback_allocated_ += bytes;
  }
  return BuildAllocation(mem, bytes);
}

void PlasmaAllocator::Free(const Allocation &allocation) {
  RAY_CHECK(allocation.address != nullptr) << "Cannot free the nullptr";
  RAY_LOG(DEBUG) << "deallocating " << allocation.size << " at " << allocation.address;
  dlfree(allocation.address);
  allocated_ -= allocation.size;
  if (RayConfig::instance().plasma_unlimited() &&
      IsOutsideInitialAllocation(allocation.address)) {
    fallback_allocated_ -= allocation.size;
  }
}

void PlasmaAllocator::SetFootprintLimit(size_t bytes) {
  footprint_limit_ = static_cast<int64_t>(bytes);
}

int64_t PlasmaAllocator::GetFootprintLimit() const { return footprint_limit_; }

int64_t PlasmaAllocator::Allocated() const { return allocated_; }

int64_t PlasmaAllocator::FallbackAllocated() const { return fallback_allocated_; }

}  // namespace plasma
