#include <assert.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <unordered_map>

#include "common.h"

extern "C" {
void *fake_mmap(size_t);
int fake_munmap(void *, size_t);

#define MMAP(s) fake_mmap(s)
#define MUNMAP(a, s) fake_munmap(a, s)
#define DIRECT_MMAP(s) fake_mmap(s)
#define DIRECT_MUNMAP(a, s) fake_munmap(a, s)
#define USE_DL_PREFIX
#define HAVE_MORECORE 0
#define DEFAULT_MMAP_THRESHOLD MAX_SIZE_T
#define DEFAULT_GRANULARITY ((size_t) 128U * 1024U)

#include "thirdparty/dlmalloc.c"

#undef MMAP
#undef MUNMAP
#undef DIRECT_MMAP
#undef DIRECT_MUNMAP
#undef USE_DL_PREFIX
#undef HAVE_MORECORE
#undef DEFAULT_GRANULARITY
}

struct mmap_record {
  int fd;
  int64_t size;
};

namespace {

/** Hashtable that contains one entry per segment that we got from the OS
 *  via mmap. Associates the address of that segment with its file descriptor
 *  and size. */
std::unordered_map<void *, mmap_record> mmap_records;

} /* namespace */

constexpr int GRANULARITY_MULTIPLIER = 2;

static void *pointer_advance(void *p, ptrdiff_t n) {
  return (unsigned char *) p + n;
}

static void *pointer_retreat(void *p, ptrdiff_t n) {
  return (unsigned char *) p - n;
}

static ptrdiff_t pointer_distance(void const *pfrom, void const *pto) {
  return (unsigned char const *) pto - (unsigned char const *) pfrom;
}

/* Create a buffer. This is creating a temporary file and then
 * immediately unlinking it so we do not leave traces in the system. */
int create_buffer(int64_t size) {
  int fd;
#ifdef _WIN32
  if (!CreateFileMapping(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE,
                         (DWORD)((uint64_t) size >> (CHAR_BIT * sizeof(DWORD))),
                         (DWORD)(uint64_t) size, NULL)) {
    fd = -1;
  }
#else
#ifdef __linux__
  constexpr char file_template[] = "/dev/shm/plasmaXXXXXX";
#else
  constexpr char file_template[] = "/tmp/plasmaXXXXXX";
#endif
  char file_name[32];
  strncpy(file_name, file_template, 32);
  fd = mkstemp(file_name);
  if (fd < 0)
    return -1;
  FILE *file = fdopen(fd, "a+");
  if (!file) {
    close(fd);
    return -1;
  }
  if (unlink(file_name) != 0) {
    LOG_ERROR("unlink error");
    return -1;
  }
  if (ftruncate(fd, (off_t) size) != 0) {
    LOG_ERROR("ftruncate error");
    return -1;
  }
#endif
  return fd;
}

void *fake_mmap(size_t size) {
  /* Add sizeof(size_t) so that the returned pointer is deliberately not
   * page-aligned. This ensures that the segments of memory returned by
   * fake_mmap are never contiguous. */
  size += sizeof(size_t);

  int fd = create_buffer(size);
  CHECKM(fd >= 0, "Failed to create buffer during mmap");
  void *pointer = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (pointer == MAP_FAILED) {
    return pointer;
  }

  /* Increase dlmalloc's allocation granularity directly. */
  mparams.granularity *= GRANULARITY_MULTIPLIER;

  mmap_record &record = mmap_records[pointer];
  record.fd = fd;
  record.size = size;

  /* We lie to dlmalloc about where mapped memory actually lives. */
  pointer = pointer_advance(pointer, sizeof(size_t));
  LOG_DEBUG("%p = fake_mmap(%lu)", pointer, size);
  return pointer;
}

int fake_munmap(void *addr, size_t size) {
  LOG_DEBUG("fake_munmap(%p, %lu)", addr, size);
  addr = pointer_retreat(addr, sizeof(size_t));
  size += sizeof(size_t);

  auto entry = mmap_records.find(addr);

  if (entry == mmap_records.end() || entry->second.size != size) {
    /* Reject requests to munmap that don't directly match previous
     * calls to mmap, to prevent dlmalloc from trimming. */
    return -1;
  }

  int r = munmap(addr, size);
  if (r == 0) {
    close(entry->second.fd);
  }

  mmap_records.erase(entry);
  return r;
}

void get_malloc_mapinfo(void *addr,
                        int *fd,
                        int64_t *map_size,
                        ptrdiff_t *offset) {
  /* TODO(rshin): Implement a more efficient search through mmap_records. */
  for (const auto &entry : mmap_records) {
    if (addr >= entry.first &&
        addr < pointer_advance(entry.first, entry.second.size)) {
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
