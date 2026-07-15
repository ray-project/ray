#include "ray/flight_store/fd_transfer.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>

#ifdef __linux__
#include <linux/memfd.h>
#include <sys/syscall.h>
#endif

namespace ray {
namespace fd_transfer {

int CreateSharedBuffer(size_t size) {
  int fd = -1;
#ifdef __linux__
  // Use the raw syscall to avoid depending on the glibc memfd_create wrapper
  // (which requires _GNU_SOURCE and a recent glibc).
  fd = static_cast<int>(syscall(SYS_memfd_create, "ray_flight", MFD_CLOEXEC));
  if (fd < 0) {
    return -1;
  }
#elif defined(__APPLE__)
  // macOS has no memfd_create and no SHM_ANON. Create a uniquely-named POSIX
  // shared memory object, then immediately unlink the name so only the fd (and
  // any fd passed via SCM_RIGHTS) keeps the memory alive. The name must fit in
  // PSHMNAMLEN (31), so keep it short.
  static std::atomic<uint64_t> counter{0};
  char name[32];
  for (int attempt = 0; attempt < 1000; ++attempt) {
    const uint64_t c = counter.fetch_add(1);
    std::snprintf(name,
                  sizeof(name),
                  "/ray_%d_%llu",
                  static_cast<int>(getpid()),
                  static_cast<unsigned long long>(c % 1000000ULL));
    fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd >= 0) {
      // The fd keeps the object alive after the name is removed.
      shm_unlink(name);
      break;
    }
    if (errno != EEXIST) {
      return -1;
    }
  }
  if (fd < 0) {
    return -1;
  }
#else
  (void)size;
  errno = ENOSYS;
  return -1;
#endif
  if (ftruncate(fd, static_cast<off_t>(size)) != 0) {
    const int saved_errno = errno;
    close(fd);
    errno = saved_errno;
    return -1;
  }
  return fd;
}

}  // namespace fd_transfer
}  // namespace ray
