#ifndef RAY_COMMON_BUFFER_H
#define RAY_COMMON_BUFFER_H

#include <cstdint>
#include <cstdio>
#include "plasma/client.h"

namespace arrow {
class Buffer;
}

namespace ray {

/// The interface that represents a buffer of bytes.
class Buffer {
 public:
  /// Pointer to the data.
  virtual uint8_t *Data() const = 0;

  /// Size of this buffer.
  virtual size_t Size() const = 0;

  virtual ~Buffer(){};

  bool operator==(const Buffer &rhs) const {
    return this->Data() == rhs.Data() && this->Size() == rhs.Size();
  }
};

/// Represents a byte buffer in local memory.
class LocalMemoryBuffer : public Buffer {
 public:
  LocalMemoryBuffer(uint8_t *data, size_t size, bool should_copy = false)
      : data_(data), size_(size) {
    if (should_copy) {
      buffer_.insert(buffer_.end(), data, data + size);
      data_ = buffer_.data();
      size_ = buffer_.size();
    }
  }

  uint8_t *Data() const override { return data_; }

  size_t Size() const override { return size_; }

  ~LocalMemoryBuffer() {}

 private:
  /// Pointer to the data.
  uint8_t *data_;
  /// Size of the buffer.
  size_t size_;
  /// This is only valid when `should_copy` is true.
  std::vector<uint8_t> buffer_;
};

/// Represents a byte buffer for plasma object.
class PlasmaBuffer : public Buffer {
 public:
  PlasmaBuffer(std::shared_ptr<arrow::Buffer> buffer) : buffer_(buffer) {}

  uint8_t *Data() const override { return const_cast<uint8_t *>(buffer_->data()); }

  size_t Size() const override { return buffer_->size(); }

 private:
  /// shared_ptr to arrow buffer which can potentially hold a reference
  /// for the object (when it's a plasma::PlasmaBuffer).
  std::shared_ptr<arrow::Buffer> buffer_;
};

}  // namespace ray

#endif  // RAY_COMMON_BUFFER_H
