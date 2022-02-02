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
#pragma once

#include "absl/types/optional.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/compat.h"

namespace plasma {

// IAllocator is responsible for allocating/deallocating memories.
// This class is not thread safe.
class IAllocator {
 public:
  virtual ~IAllocator() = default;

  /// Allocates size bytes and returns allocated memory.
  ///
  /// \param bytes Number of bytes.
  /// \return allocated memory. returns empty if not enough space.
  virtual absl::optional<Allocation> Allocate(size_t bytes) = 0;

  // Same as Allocate, but allocates pages from the filesystem. The footprint limit
  // is not enforced for these allocations, but allocations here are still tracked
  // and count towards the limit.
  //
  // TODO(scv119) ideally we should have mem/disk allocator implementations
  // so we don't need FallbackAllocate. However the dlmalloc has some limitation
  // prevents us from doing so.
  virtual absl::optional<Allocation> FallbackAllocate(size_t bytes) = 0;

  /// Frees the memory space pointed to by mem, which must have been returned by
  /// a previous call to Allocate/FallbackAllocate or it yield undefined behavior.
  ///
  /// \param allocation allocation to free.
  virtual void Free(Allocation allocation) = 0;

  /// Get the memory footprint limit for this allocator.
  ///
  /// \return memory footprint limit in bytes.
  virtual int64_t GetFootprintLimit() const = 0;

  /// Get the number of bytes allocated so far.
  virtual int64_t Allocated() const = 0;

  /// Get the number of bytes fallback allocated so far.
  virtual int64_t FallbackAllocated() const = 0;
};

}  // namespace plasma
