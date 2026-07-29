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

#include "ray/object_manager/plasma/malloc.h"

#include <stddef.h>

#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/plasma.h"

namespace plasma {

absl::flat_hash_map<void *, MmapRecord> mmap_records;

namespace internal {
static void *pointer_advance(void *p, ptrdiff_t n) { return (unsigned char *)p + n; }

static ptrdiff_t pointer_distance(void const *pfrom, void const *pto) {
  return (unsigned char const *)pto - (unsigned char const *)pfrom;
}

bool GetMallocMapinfo(const void *const addr,
                      MEMFD_TYPE *fd,
                      int64_t *map_size,
                      ptrdiff_t *offset) {
  // TODO(rshin): Implement a more efficient search through mmap_records.
  for (const auto &[from_addr, record] : mmap_records) {
    if (addr >= from_addr && addr < pointer_advance(from_addr, record.size)) {
      fd->first = record.fd.first;
      fd->second = record.fd.second;
      *map_size = record.size;
      *offset = pointer_distance(from_addr, addr);
      return true;
    }
  }

  fd->first = INVALID_FD;
  fd->second = INVALID_UNIQUE_FD_ID;
  *map_size = 0;
  *offset = 0;

  return false;
}
}  // namespace internal
}  // namespace plasma
