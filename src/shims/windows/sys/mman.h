#ifndef MMAN_H
#define MMAN_H

#include <unistd.h>

#define MAP_SHARED 0x0010 /* share changes */
#define MAP_FAILED ((void *)-1)
#define PROT_READ 0x04  /* pages can be read */
#define PROT_WRITE 0x02 /* pages can be written */
#define PROT_EXEC 0x01  /* pages can be executed */
#ifndef FILE_MAP_ALL_ACCESS
enum { FILE_MAP_ALL_ACCESS = 0xF001F };
#endif
EXTERN_C WINBASEAPI void *WINAPI MapViewOfFile(HANDLE hFileMappingObject,
                                               DWORD dwDesiredAccess,
                                               DWORD dwFileOffsetHigh,
                                               DWORD dwFileOffsetLow,
                                               SIZE_T dwNumberOfBytesToMap);
EXTERN_C WINBASEAPI BOOL WINAPI UnmapViewOfFile(void const *lpBaseAddress);
static void *mmap(void *addr, size_t len, int prot, int flags, int fd, off_t off) {
  void *result = (void *)(-1);
  if (!addr && (flags & MAP_SHARED)) {
    /* HACK: we're assuming handle sizes can't exceed 32 bits, which is wrong...
     * but works for now. */
    void *ptr = MapViewOfFile((HANDLE)(intptr_t)fd, FILE_MAP_ALL_ACCESS,
                              (DWORD)(off >> (CHAR_BIT * sizeof(DWORD))), (DWORD)off,
                              (SIZE_T)len);
    if (ptr) {
      result = ptr;
    }
  }
  return result;
}
static int munmap(void *addr, size_t length) {
  (void)length;
  return UnmapViewOfFile(addr) ? 0 : -1;
}

#endif /* MMAN_H */
