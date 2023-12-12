#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

#include "ray/object_manager/plasma/compat.h"
#include "ray/util/macros.h"

namespace plasma {

class ClientMmapTableEntry {
 public:
  ClientMmapTableEntry(MEMFD_TYPE fd, int64_t map_size, bool may_unmap);

  ~ClientMmapTableEntry();

  uint8_t *pointer() const { return reinterpret_cast<uint8_t *>(pointer_); }

  MEMFD_TYPE fd() const { return fd_; }

  bool may_unmap() const { return may_unmap_; }

 private:
  /// The associated file descriptor on the client.
  MEMFD_TYPE fd_;
  /// The result of mmap for this file descriptor.
  void *pointer_;
  /// The length of the memory-mapped file.
  size_t length_;
  /// This entry may be deleted and the mmap region unmapped throughout the lifetime of
  /// the plasma client.
  /// Will be `true` for the fallback-allocated entries, and `false` for the main memory
  /// entry.
  bool may_unmap_;

  void MaybeMadviseDontdump();

  RAY_DISALLOW_COPY_AND_ASSIGN(ClientMmapTableEntry);
};

}  // namespace plasma
