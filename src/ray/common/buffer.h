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

  /// Length of this buffer.
  virtual size_t Length() = 0;

  virtual ~Buffer();
};

/// Represents a byte buffer in local memory.
class LocalMemoryBuffer : public Buffer {
 public:
  LocalMemoryBuffer(uint8_t *data, size_t length) : data_(data), length_(length) {}

  uint8_t *Data() { return data_; }

  size_t Length() { return length_; }

 private:
  uint8_t *data_;
  size_t length_;
};

}  // namespace ray

#endif  // RAY_COMMON_BUFFER_H
