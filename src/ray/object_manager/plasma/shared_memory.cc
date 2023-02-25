#include "ray/object_manager/plasma/shared_memory.h"

#include <cerrno>

#ifndef _WIN32
#include <sys/mman.h>
#include <unistd.h>
#endif

#include "ray/common/ray_config.h"
#include "ray/object_manager/plasma/malloc.h"
#include "ray/util/logging.h"

namespace plasma {

ClientMmapTableEntry::ClientMmapTableEntry(MEMFD_TYPE fd, int64_t map_size)
    : fd_(fd), pointer_(nullptr), length_(0) {
  // We subtract kMmapRegionsGap from the length that was added
  // in fake_mmap in malloc.h, to make map_size page-aligned again.
  length_ = map_size - kMmapRegionsGap;
#ifdef _WIN32
  pointer_ = reinterpret_cast<uint8_t *>(
      MapViewOfFile(fd.first, FILE_MAP_ALL_ACCESS, 0, 0, length_));
  // TODO(pcm): Don't fail here, instead return a Status.
  if (pointer_ == NULL) {
    RAY_LOG(FATAL) << "mmap failed";
  }
  CloseHandle(fd.first);  // Closing this fd has an effect on performance.
#else
  pointer_ = reinterpret_cast<uint8_t *>(
      mmap(NULL, length_, PROT_READ | PROT_WRITE, MAP_SHARED, fd.first, 0));
  // TODO(pcm): Don't fail here, instead return a Status.
  if (pointer_ == MAP_FAILED) {
    RAY_LOG(FATAL) << "mmap failed";
  }
  close(fd.first);  // Closing this fd has an effect on performance.

#endif

  MaybeMadviseDontdump();
}

void ClientMmapTableEntry::MaybeMadviseDontdump() {
  if (!RayConfig::instance().worker_core_dump_exclude_plasma_store()) {
    RAY_LOG(DEBUG) << "worker_core_dump_exclude_plasma_store disabled, worker coredumps "
                      "will contain the object store mappings.";
    return;
  }

#if !defined(__linux__)
  RAY_LOG(DEBUG)
      << "Filtering object store pages from coredumps only supported on linux.";
#else
  int rval = madvise(pointer_, length_, MADV_DONTDUMP);
  if (rval) {
    RAY_LOG(WARNING) << "madvise(MADV_DONTDUMP) call failed: " << rval << ", "
                     << strerror(errno);
  } else {
    RAY_LOG(DEBUG) << "madvise(MADV_DONTDUMP) call succeeded.";
  }
#endif
}

ClientMmapTableEntry::~ClientMmapTableEntry() {
  // At this point it is safe to unmap the memory, as the PlasmaBuffer
  // keeps the PlasmaClient (and therefore the ClientMmapTableEntry)
  // alive until it is destroyed.
  // We don't need to close the associated file, since it has
  // already been closed in the constructor.
  int r;
#ifdef _WIN32
  r = UnmapViewOfFile(pointer_) ? 0 : -1;
#else
  r = munmap(pointer_, length_);
#endif
  if (r != 0) {
    RAY_LOG(ERROR) << "munmap returned " << r << ", errno = " << errno;
  }
}

}  // namespace plasma
