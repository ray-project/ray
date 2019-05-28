#ifndef RAY_COMMON_BUFFER_H
#define RAY_COMMON_BUFFER_H

#include <cstdint>
#include <cstdio>

namespace ray {

/// The interface that represents a buffer of bytes.
class Buffer {
 public:
  /// Pointer to the data.
  virtual uint8_t *Data() = 0;

  /// Size of this buffer.
  virtual size_t Size() = 0;

  virtual ~Buffer();
};

/// Represents a byte buffer in local memory.
class LocalMemoryBuffer : public Buffer {
 public:
  LocalMemoryBuffer(uint8_t *data, size_t size) : data_(data), size_(size) {}

  uint8_t *Data() override { return data_; }

  size_t Size() override { return size_; }

 private:
  uint8_t *data_;
  size_t size_;
};

}  // namespace ray

#endif  // RAY_COMMON_BUFFER_H
