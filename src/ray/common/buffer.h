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

  /// Whether this buffer owns the data.
  virtual bool OwnsData() const = 0;

  virtual ~Buffer(){};

  bool operator==(const Buffer &rhs) const {
    if (this->Size() != rhs.Size()) {
      return false;
    }

    return this->Size() == 0 || memcmp(Data(), rhs.Data(), Size()) == 0;
  }
};

/// Represents a byte buffer in local memory.
class LocalMemoryBuffer : public Buffer {
 public:
  /// Constructor.
  ///
  /// \param data The data pointer to the passed-in buffer.
  /// \param size The size of the passed in buffer.
  /// \param copy_data If true, data will be copied and owned by this buffer,
  /// otherwise the buffer only points to the given address.
  LocalMemoryBuffer(uint8_t *data, size_t size, bool copy_data = false)
      : has_data_copy_(copy_data) {
    if (copy_data) {
      buffer_.insert(buffer_.end(), data, data + size);
      data_ = buffer_.data();
      size_ = buffer_.size();
    } else {
      data_ = data;
      size_ = size;
    }
  }

  uint8_t *Data() const override { return data_; }

  size_t Size() const override { return size_; }

  bool OwnsData() const override { return has_data_copy_; }

  ~LocalMemoryBuffer() {}

 private:
  /// Disable copy constructor and assignment, as default copy will
  /// cause invalid data_.
  LocalMemoryBuffer &operator=(const LocalMemoryBuffer &) = delete;
  LocalMemoryBuffer(const LocalMemoryBuffer &) = delete;

  /// Pointer to the data.
  uint8_t *data_;
  /// Size of the buffer.
  size_t size_;
  /// Whether this buffer holds a copy of data.
  bool has_data_copy_;
  /// This is only valid when `should_copy` is true.
  std::vector<uint8_t> buffer_;
};

/// Represents a byte buffer for plasma object. This can be used to hold the
/// reference to a plasma object (via the underlying plasma::PlasmaBuffer).
class PlasmaBuffer : public Buffer {
 public:
  PlasmaBuffer(std::shared_ptr<arrow::Buffer> buffer) : buffer_(buffer) {}

  uint8_t *Data() const override { return const_cast<uint8_t *>(buffer_->data()); }

  size_t Size() const override { return buffer_->size(); }

  bool OwnsData() const override { return true; }

 private:
  /// shared_ptr to arrow buffer which can potentially hold a reference
  /// for the object (when it's a plasma::PlasmaBuffer).
  std::shared_ptr<arrow::Buffer> buffer_;
};

}  // namespace ray

#endif  // RAY_COMMON_BUFFER_H
