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

#include "plasma/malloc.h"

#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <string>
#include <vector>

#include "plasma/common.h"
#include "plasma/plasma.h"

namespace plasma {

std::unordered_map<void*, MmapRecord> mmap_records;

static void* pointer_advance(void* p, ptrdiff_t n) { return (unsigned char*)p + n; }

static ptrdiff_t pointer_distance(void const* pfrom, void const* pto) {
  return (unsigned char const*)pto - (unsigned char const*)pfrom;
}

void GetMallocMapinfo(void* addr, int* fd, int64_t* map_size, ptrdiff_t* offset) {
  // TODO(rshin): Implement a more efficient search through mmap_records.
  for (const auto& entry : mmap_records) {
    if (addr >= entry.first && addr < pointer_advance(entry.first, entry.second.size)) {
      *fd = entry.second.fd;
      *map_size = entry.second.size;
      *offset = pointer_distance(entry.first, addr);
      return;
    }
  }
  *fd = -1;
  *map_size = 0;
  *offset = 0;
}

int64_t GetMmapSize(int fd) {
  for (const auto& entry : mmap_records) {
    if (entry.second.fd == fd) {
      return entry.second.size;
    }
  }
  ARROW_LOG(FATAL) << "failed to find entry in mmap_records for fd " << fd;
  return -1;  // This code is never reached.
}

}  // namespace plasma
