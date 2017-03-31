#include <assert.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "common.h"
#include "uthash.h"

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

struct mmap_record {
  int fd;
  void *pointer;
  int64_t size;
  UT_hash_handle hh_fd;
  UT_hash_handle hh_pointer;
};

/* TODO(rshin): Don't have two hash tables. */
struct mmap_record *records_by_fd = NULL;
struct mmap_record *records_by_pointer = NULL;

const int GRANULARITY_MULTIPLIER = 2;

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
  static char template[] = "/dev/shm/plasmaXXXXXX";
#else
  static char template[] = "/tmp/plasmaXXXXXX";
#endif
  char file_name[32];
  strncpy(file_name, template, 32);
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

  struct mmap_record *record = malloc(sizeof(struct mmap_record));
  record->fd = fd;
  record->pointer = pointer;
  record->size = size;
  HASH_ADD(hh_fd, records_by_fd, fd, sizeof(fd), record);
  HASH_ADD(hh_pointer, records_by_pointer, pointer, sizeof(pointer), record);

  /* We lie to dlmalloc about where mapped memory actually lives. */
  pointer = pointer_advance(pointer, sizeof(size_t));
  LOG_INFO("%p = fake_mmap(%lu)\n", pointer, size);
  return pointer;
}

int fake_munmap(void *addr, size_t size) {
  LOG_INFO("fake_munmap(%p, %lu)\n", addr, size);
  addr = pointer_retreat(addr, sizeof(size_t));
  size += sizeof(size_t);

  struct mmap_record *record;

  HASH_FIND(hh_pointer, records_by_pointer, &addr, sizeof(addr), record);
  if (record == NULL || record->size != size) {
    /* Reject requests to munmap that don't directly match previous
     * calls to mmap, to prevent dlmalloc from trimming. */
    return -1;
  }

  HASH_DELETE(hh_fd, records_by_fd, record);
  HASH_DELETE(hh_pointer, records_by_pointer, record);

  int r = munmap(addr, size);
  if (r == 0) {
    close(record->fd);
  }
  return r;
}

void get_malloc_mapinfo(void *addr,
                        int *fd,
                        int64_t *map_size,
                        ptrdiff_t *offset) {
  struct mmap_record *record;
  /* TODO(rshin): Implement a more efficient search through records_by_fd. */
  for (record = records_by_fd; record != NULL; record = record->hh_fd.next) {
    if (addr >= record->pointer &&
        addr < pointer_advance(record->pointer, record->size)) {
      *fd = record->fd;
      *map_size = record->size;
      *offset = pointer_distance(record->pointer, addr);
      return;
    }
  }
  *fd = -1;
  *map_size = 0;
  *offset = 0;
}
