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

#include <inttypes.h>
#include <stddef.h>

#include "absl/container/flat_hash_map.h"
#include "ray/object_manager/plasma/compat.h"

namespace plasma {

/// Gap between two consecutive mmap regions allocated by fake_mmap.
/// This ensures that the segments of memory returned by
/// fake_mmap are never contiguous and dlmalloc does not coalesce it
/// (in the client we cannot guarantee that these mmaps are contiguous).
constexpr int64_t kMmapRegionsGap = sizeof(size_t);

struct MmapRecord {
  MEMFD_TYPE fd;
  int64_t size;
};

/// Hashtable that contains one entry per segment that we got from the OS
/// via mmap. Associates the address of that segment with its file descriptor
/// and size.
extern absl::flat_hash_map<void *, MmapRecord> mmap_records;

/// private function, only used by PlasmaAllocator
namespace internal {
bool GetMallocMapinfo(const void *const addr,
                      MEMFD_TYPE *fd,
                      int64_t *map_length,
                      ptrdiff_t *offset);
}
}  // namespace plasma
