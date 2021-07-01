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

#include <assert.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <sys/mman.h>
#include <unistd.h>
#endif
#include <cerrno>
#include <string>
#include <vector>

#include "ray/common/ray_config.h"
#include "ray/object_manager/plasma/plasma.h"

namespace plasma {

void *fake_mmap(size_t);
int fake_munmap(void *, int64_t);

#define MMAP(s) fake_mmap(s)
#define MUNMAP(a, s) fake_munmap(a, s)
#define DIRECT_MMAP(s) fake_mmap(s)
#define DIRECT_MUNMAP(a, s) fake_munmap(a, s)
#define USE_DL_PREFIX
#define HAVE_MORECORE 0
#define DEFAULT_MMAP_THRESHOLD MAX_SIZE_T
#define DEFAULT_GRANULARITY ((size_t)128U * 1024U)

#include "ray/thirdparty/dlmalloc.c"  // NOLINT

#undef MMAP
#undef MUNMAP
#undef DIRECT_MMAP
#undef DIRECT_MUNMAP
#undef USE_DL_PREFIX
#undef HAVE_MORECORE
#undef DEFAULT_GRANULARITY

// dlmalloc.c defined DEBUG which will conflict with RAY_LOG(DEBUG).
#ifdef DEBUG
#undef DEBUG
#endif

#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif

constexpr int GRANULARITY_MULTIPLIER = 2;

// Ray allocates all plasma memory up-front at once to avoid runtime allocations.
// Combined with MAP_POPULATE, this can guarantee we never run into SIGBUS errors.
static bool allocated_once = false;

// Give each mmap record a unique id, so we can disambiguate fd reuse.
static int64_t next_mmap_unique_id = INVALID_UNIQUE_FD_ID + 1;

// Populated on the first allocation so we can track which allocations fall within
// the initial region vs outside.
static char *initial_region_ptr = nullptr;
static size_t initial_region_size = 0;

static void *pointer_advance(void *p, ptrdiff_t n) { return (unsigned char *)p + n; }

static void *pointer_retreat(void *p, ptrdiff_t n) { return (unsigned char *)p - n; }

#ifdef _WIN32
void create_and_mmap_buffer(int64_t size, void **pointer, HANDLE *handle) {
  *handle = CreateFileMapping(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE,
                              (DWORD)((uint64_t)size >> (CHAR_BIT * sizeof(DWORD))),
                              (DWORD)(uint64_t)size, NULL);
  RAY_CHECK(*handle != NULL) << "CreateFileMapping() failed. GetLastError() = "
                             << GetLastError();
  *pointer = MapViewOfFile(*handle, FILE_MAP_ALL_ACCESS, 0, 0, (size_t)size);
  if (*pointer == NULL) {
    RAY_LOG(ERROR) << "MapViewOfFile() failed. GetLastError() = " << GetLastError();
  }
}
#else
void create_and_mmap_buffer(int64_t size, void **pointer, int *fd) {
  // Create a buffer. This is creating a temporary file and then
  // immediately unlinking it so we do not leave traces in the system.
  std::string file_template = plasma_config->directory;

  // In never-OOM mode, fallback to allocating from the filesystem. Note that these
  // allocations will be run with dlmallopt(M_MMAP_THRESHOLD, 0) set by
  // plasma_allocator.cc.
  if (allocated_once && RayConfig::instance().plasma_unlimited()) {
    file_template = plasma_config->fallback_directory;
  }

  file_template += "/plasmaXXXXXX";
  RAY_LOG(INFO) << "create_and_mmap_buffer(" << size << ", " << file_template << ")";
  std::vector<char> file_name(file_template.begin(), file_template.end());
  file_name.push_back('\0');
  *fd = mkstemp(&file_name[0]);
  if (*fd < 0) {
    RAY_LOG(FATAL) << "create_buffer failed to open file " << &file_name[0];
  }
  // Immediately unlink the file so we do not leave traces in the system.
  if (unlink(&file_name[0]) != 0) {
    RAY_LOG(FATAL) << "failed to unlink file " << &file_name[0];
  }
  if (!plasma_config->hugepages_enabled) {
    // Increase the size of the file to the desired size. This seems not to be
    // needed for files that are backed by the huge page fs, see also
    // http://www.mail-archive.com/kvm-devel@lists.sourceforge.net/msg14737.html
    if (ftruncate(*fd, (off_t)size) != 0) {
      RAY_LOG(FATAL) << "failed to ftruncate file " << &file_name[0];
    }
  }

  // MAP_POPULATE can be used to pre-populate the page tables for this memory region
  // which avoids work when accessing the pages later. However it causes long pauses
  // when mmapping the files. Only supported on Linux.
  auto flags = MAP_SHARED;
  if (RayConfig::instance().preallocate_plasma_memory()) {
    if (!MAP_POPULATE) {
      RAY_LOG(FATAL) << "MAP_POPULATE is not supported on this platform.";
    }
    RAY_LOG(INFO) << "Preallocating all plasma memory using MAP_POPULATE.";
    flags |= MAP_POPULATE;
  }

#ifdef _GNU_SOURCE
  // For fallback allocation, use fallocate to ensure follow up access to this
  // mmaped file doesn't cause SIGBUS. Only supported on Linux.
  if (allocated_once && RayConfig::instance().plasma_unlimited()) {
    RAY_LOG(DEBUG) << "Preallocating fallback allocation using fallocate";
    int ret = fallocate(*fd, /*mode*/ 0, /*offset*/ 0, size);
    if (!ret) {
      if (ret == EOPNOTSUPP) {
        RAY_LOG(DEBUG) << "fallocate is not supported: " << std::strerror(errno);
      } else {
        RAY_LOG(ERROR) << "fallocate failed with error: " << std::strerror(errno);
      }
    }
  }
#endif

  *pointer = mmap(NULL, size, PROT_READ | PROT_WRITE, flags, *fd, 0);
  if (*pointer == MAP_FAILED) {
    RAY_LOG(ERROR) << "mmap failed with error: " << std::strerror(errno);
    if (errno == ENOMEM && plasma_config->hugepages_enabled) {
      RAY_LOG(ERROR)
          << "  (this probably means you have to increase /proc/sys/vm/nr_hugepages)";
    }
  } else if (!allocated_once) {
    initial_region_ptr = static_cast<char *>(*pointer);
    initial_region_size = size;
  }
}

#endif

void *fake_mmap(size_t size) {
  // In unlimited allocation mode, fail allocations done by PlasmaAllocator::Memalign()
  // after the initial allocation. Allow allocations done by
  // PlasmaAllocator::DiskMemalignUnlimited(), which sets mmap_threshold to zero prior to
  // calling dlmemalign().
  if (RayConfig::instance().plasma_unlimited() && allocated_once &&
      mparams.mmap_threshold > 0) {
    RAY_LOG(DEBUG) << "fake_mmap called once already, refusing to overcommit: " << size;
    return MFAIL;
  }

  // Add kMmapRegionsGap so that the returned pointer is deliberately not
  // page-aligned. This ensures that the segments of memory returned by
  // fake_mmap are never contiguous.
  size += kMmapRegionsGap;

  void *pointer;
  MEMFD_TYPE_NON_UNIQUE fd;
  create_and_mmap_buffer(size, &pointer, &fd);
  allocated_once = true;

  // Increase dlmalloc's allocation granularity directly.
  mparams.granularity *= GRANULARITY_MULTIPLIER;

  MmapRecord &record = mmap_records[pointer];
  record.fd = {fd, next_mmap_unique_id++};
  record.size = size;

  // We lie to dlmalloc about where mapped memory actually lives.
  pointer = pointer_advance(pointer, kMmapRegionsGap);
  RAY_LOG(DEBUG) << pointer << " = fake_mmap(" << size << ")";
  return pointer;
}

int fake_munmap(void *addr, int64_t size) {
  addr = pointer_retreat(addr, kMmapRegionsGap);
  size += kMmapRegionsGap;

  auto entry = mmap_records.find(addr);

  if (entry == mmap_records.end() || entry->second.size != size) {
    // Reject requests to munmap that don't directly match previous
    // calls to mmap, to prevent dlmalloc from trimming.
    return -1;
  }
  RAY_LOG(INFO) << "fake_munmap(" << addr << ", " << size << ")";

  int r;
#ifdef _WIN32
  r = UnmapViewOfFile(addr) ? 0 : -1;
  if (r == 0) {
    CloseHandle(entry->second.fd.first);
  }
#else
  r = munmap(addr, size);
  if (r == 0) {
    close(entry->second.fd.first);
  }
#endif

  mmap_records.erase(entry);
  return r;
}

void SetMallocGranularity(int value) { change_mparam(M_GRANULARITY, value); }

// Returns whether the given pointer is outside the initially allocated region.
bool IsOutsideInitialAllocation(void *p) {
  if (initial_region_ptr == nullptr) {
    return false;
  }
  return (p < initial_region_ptr) || (p >= (initial_region_ptr + initial_region_size));
}

const PlasmaStoreInfo *plasma_config;

}  // namespace plasma
