#include <assert.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "plasma.h"
#include "uthash.h"

void *fake_mmap(size_t);
int fake_munmap(void *, size_t);

size_t dlmalloc_granularity = ((size_t) 128U * 1024U);

#define MMAP(s) fake_mmap(s)
#define MUNMAP(a, s) fake_munmap(a, s)
#define DIRECT_MMAP(s) fake_mmap(s)
#define DIRECT_MUNMAP(a, s) fake_munmap(a, s)
#define USE_DL_PREFIX
#define HAVE_MORECORE 0
#define DEFAULT_GRANULARITY (dlmalloc_granularity)

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

/* Create a buffer. This is creating a temporary file and then
 * immediately unlinking it so we do not leave traces in the system. */
int create_buffer(int64_t size) {
  static char template[] = "/tmp/plasmaXXXXXX";
  char file_name[32];
  strncpy(file_name, template, 32);
  int fd = mkstemp(file_name);
  if (fd < 0)
    return -1;
  FILE *file = fdopen(fd, "a+");
  if (!file) {
    close(fd);
    return -1;
  }
  if (unlink(file_name) != 0) {
    LOG_ERR("unlink error");
    return -1;
  }
  if (ftruncate(fd, (off_t) size) != 0) {
    LOG_ERR("ftruncate error");
    return -1;
  }
  return fd;
}

void *fake_mmap(size_t size) {
  /* Add sizeof(size_t) so that the returned pointer is deliberately not
   * page-aligned. This ensures that the segments of memory returned by
   * fake_mmap are never contiguous. */
  size += sizeof(size_t);

  int fd = create_buffer(size);
  void *pointer = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (pointer == MAP_FAILED) {
    return pointer;
  }

  /* Update dlmalloc's allocation granularity for future calls */
  dlmalloc_granularity *= GRANULARITY_MULTIPLIER;
  dlmallopt(M_GRANULARITY, dlmalloc_granularity);

  struct mmap_record *record = malloc(sizeof(struct mmap_record));
  record->fd = fd;
  record->pointer = pointer;
  record->size = size;
  HASH_ADD(hh_fd, records_by_fd, fd, sizeof(fd), record);
  HASH_ADD(hh_pointer, records_by_pointer, pointer, sizeof(pointer), record);

  /* We lie to dlmalloc about where mapped memory actually lives. */
  pointer += sizeof(size_t);
  LOG_DEBUG("%p = fake_mmap(%lu)", pointer, size);
  return pointer;
}

int fake_munmap(void *addr, size_t size) {
  LOG_DEBUG("fake_munmap(%p, %lu)", addr, size);
  addr -= sizeof(size_t);
  size += sizeof(size_t);

  struct mmap_record *record;

  HASH_FIND(hh_pointer, records_by_pointer, &addr, sizeof(addr), record);
  if (record == NULL || record->size != size) {
    /* Reject requests to munmap that don't directly match previous
     * calls to mmap, to prevent dlmalloc from trimming. */
    return -1;
  }
  close(record->fd);

  HASH_DELETE(hh_fd, records_by_fd, record);
  HASH_DELETE(hh_pointer, records_by_pointer, record);

  return munmap(addr, size);
}

void get_malloc_mapinfo(void *addr,
                        int *fd,
                        int64_t *map_size,
                        ptrdiff_t *offset) {
  struct mmap_record *record;
  /* TODO(rshin): Implement a more efficient search through records_by_fd. */
  for (record = records_by_fd; record != NULL; record = record->hh_fd.next) {
    if (addr >= record->pointer && addr < record->pointer + record->size) {
      *fd = record->fd;
      *map_size = record->size;
      *offset = addr - record->pointer;
      return;
    }
  }
  *fd = -1;
  *map_size = 0;
  *offset = 0;
}
