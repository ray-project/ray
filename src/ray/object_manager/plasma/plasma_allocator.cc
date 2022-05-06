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

#include "ray/object_manager/plasma/plasma_allocator.h"

#include "ray/common/ray_config.h"
#include "ray/object_manager/plasma/malloc.h"
#include "ray/util/logging.h"

namespace plasma {
namespace internal {
bool IsOutsideInitialAllocation(void *ptr);

void SetDLMallocConfig(const std::string &plasma_directory,
                       const std::string &fallback_directory,
                       bool hugepage_enabled,
                       bool fallback_enabled);
}  // namespace internal

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

// We are using a single memory-mapped file by mallocing and freeing a single
// large amount of space up front. According to the documentation,
// dlmalloc might need up to 128*sizeof(size_t) bytes for internal
// bookkeeping.
const int64_t kDlMallocReserved = 256 * sizeof(size_t);

}  // namespace

PlasmaAllocator::PlasmaAllocator(const std::string &plasma_directory,
                                 const std::string &fallback_directory,
                                 bool hugepage_enabled,
                                 int64_t footprint_limit)
    : kFootprintLimit(footprint_limit),
      kAlignment(kAllocationAlignment),
      allocated_(0),
      fallback_allocated_(0) {
  internal::SetDLMallocConfig(plasma_directory,
                              fallback_directory,
                              hugepage_enabled,
                              /*fallback_enabled=*/true);
  RAY_CHECK(kFootprintLimit > kDlMallocReserved)
      << "Footprint limit has to be greater than " << kDlMallocReserved;
  auto allocation = Allocate(kFootprintLimit - kDlMallocReserved);
  RAY_CHECK(allocation.has_value())
      << "PlasmaAllocator initialization failed."
      << " It's likely we don't have enought space in " << plasma_directory;
  // This will unmap the file, but the next one created will be as large
  // as this one (this is an implementation detail of dlmalloc).
  Free(std::move(allocation.value()));
}

absl::optional<Allocation> PlasmaAllocator::Allocate(size_t bytes) {
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
  if (internal::IsOutsideInitialAllocation(mem)) {
    fallback_allocated_ += bytes;
  }
  return BuildAllocation(mem, bytes);
}

void PlasmaAllocator::Free(Allocation allocation) {
  RAY_CHECK(allocation.address != nullptr) << "Cannot free the nullptr";
  RAY_LOG(DEBUG) << "deallocating " << allocation.size << " at " << allocation.address;
  dlfree(allocation.address);
  allocated_ -= allocation.size;
  if (internal::IsOutsideInitialAllocation(allocation.address)) {
    fallback_allocated_ -= allocation.size;
  }
}

int64_t PlasmaAllocator::GetFootprintLimit() const { return kFootprintLimit; }

int64_t PlasmaAllocator::Allocated() const { return allocated_; }

int64_t PlasmaAllocator::FallbackAllocated() const { return fallback_allocated_; }

absl::optional<Allocation> PlasmaAllocator::BuildAllocation(void *addr, size_t size) {
  if (addr == nullptr) {
    return absl::nullopt;
  }
  MEMFD_TYPE fd;
  int64_t mmap_size;
  ptrdiff_t offset;

  if (internal::GetMallocMapinfo(addr, &fd, &mmap_size, &offset)) {
    return Allocation(addr,
                      static_cast<int64_t>(size),
                      std::move(fd),
                      offset,
                      0 /* device_number*/,
                      mmap_size);
  }
  return absl::nullopt;
}
}  // namespace plasma
