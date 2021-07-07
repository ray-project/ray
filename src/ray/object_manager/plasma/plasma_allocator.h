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

#include <cstddef>
#include <cstdint>

#include "ray/object_manager/plasma/common.h"

namespace plasma {

class IAllocator {
 public:
  virtual ~IAllocator() = default;
  /// Allocates size bytes and returns a pointer to the allocated memory. The
  /// memory address will be a multiple of alignment, which must be a power of two.
  ///
  /// \param alignment Memory alignment.
  /// \param bytes Number of bytes.
  /// \return Pointer to allocated memory.
  virtual Allocation Memalign(size_t alignment, size_t bytes) = 0;

  // Same as MemAlign, but allocates pages from the filesystem. The footprint limit
  // is not enforced for these allocations, but allocations here are still tracked
  // and count towards the limit.
  virtual Allocation DiskMemalignUnlimited(size_t alignment, size_t bytes) = 0;

  /// Frees the memory space pointed to by mem, which must have been returned by
  /// a previous call to Memalign()
  ///
  /// \param allocation allocation to free.
  virtual void Free(const Allocation &allocation) = 0;

  /// Sets the memory footprint limit for Plasma.
  ///
  /// \param bytes Plasma memory footprint limit in bytes.
  virtual void SetFootprintLimit(size_t bytes) = 0;

  /// Get the memory footprint limit for Plasma.
  ///
  /// \return Plasma memory footprint limit in bytes.
  virtual int64_t GetFootprintLimit() = 0;

  /// Get the number of bytes allocated by Plasma so far.
  /// \return Number of bytes allocated by Plasma so far.
  virtual int64_t Allocated() = 0;

  /// Get the number of bytes fallback allocated by Plasma so far.
  /// \return Number of bytes fallback allocated by Plasma so far.
  virtual int64_t FallbackAllocated() = 0;
};

class PlasmaAllocator : public IAllocator {
 public:
  /// Singleton factory.
  static PlasmaAllocator &GetInstance();

  /// Allocates size bytes and returns a pointer to the allocated memory. The
  /// memory address will be a multiple of alignment, which must be a power of two.
  ///
  /// \param alignment Memory alignment.
  /// \param bytes Number of bytes.
  /// \return Pointer to allocated memory.
  Allocation Memalign(size_t alignment, size_t bytes) override;

  // Same as MemAlign, but allocates pages from the filesystem. The footprint limit
  // is not enforced for these allocations, but allocations here are still tracked
  // and count towards the limit.
  Allocation DiskMemalignUnlimited(size_t alignment, size_t bytes) override;

  /// Frees the memory space pointed to by mem, which must have been returned by
  /// a previous call to Memalign()
  ///
  /// \param mem Pointer to memory to free.
  /// \param bytes Number of bytes to be freed.
  void Free(const Allocation &allocation) override;

  /// Sets the memory footprint limit for Plasma.
  ///
  /// \param bytes Plasma memory footprint limit in bytes.
  void SetFootprintLimit(size_t bytes) override;

  /// Get the memory footprint limit for Plasma.
  ///
  /// \return Plasma memory footprint limit in bytes.
  int64_t GetFootprintLimit() override;

  /// Get the number of bytes allocated by Plasma so far.
  /// \return Number of bytes allocated by Plasma so far.
  int64_t Allocated() override;

  /// Get the number of bytes fallback allocated by Plasma so far.
  /// \return Number of bytes fallback allocated by Plasma so far.
  int64_t FallbackAllocated() override;

 private:
  PlasmaAllocator();

 private:
  int64_t allocated_;
  int64_t fallback_allocated_;
  int64_t footprint_limit_;
};

}  // namespace plasma
