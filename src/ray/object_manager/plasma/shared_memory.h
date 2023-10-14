#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

#include "ray/object_manager/plasma/compat.h"
#include "ray/util/macros.h"

namespace plasma {

class ClientMmapTableEntry {
 public:
  ClientMmapTableEntry(MEMFD_TYPE fd, int64_t map_size);

  ~ClientMmapTableEntry();

  uint8_t *pointer() { return pointer_; }

  MEMFD_TYPE fd() { return fd_; }

  void IncrementRefCount();
  void DecrementRefCount();
  bool SafeToUnmap();

 private:
  /// The associated file descriptor on the client.
  MEMFD_TYPE fd_;
  /// The result of mmap for this file descriptor.
  uint8_t *pointer_;
  /// The length of the memory-mapped file.
  size_t length_;
  /// Reference count to this mmap section. If it's zero, it can be safely unmapped.
  size_t ref_count_ = 0;

  void MaybeMadviseDontdump();

  RAY_DISALLOW_COPY_AND_ASSIGN(ClientMmapTableEntry);
};

}  // namespace plasma
