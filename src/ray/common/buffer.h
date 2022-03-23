// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>

#include "ray/common/status.h"
#include "ray/thirdparty/aligned_alloc.h"

#define BUFFER_ALIGNMENT 64

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

  virtual bool IsPlasmaBuffer() const = 0;

  virtual ~Buffer(){};

  bool operator==(const Buffer &rhs) const {
    if (this->Size() != rhs.Size()) {
      return false;
    }

    return this->Size() == 0 || memcmp(Data(), rhs.Data(), Size()) == 0;
  }
};

/// Represents a byte buffer in local memory.
/// TODO(suquark): In C++17, we can use std::aligned_alloc
class LocalMemoryBuffer : public Buffer {
 public:
  /// Constructor.
  ///
  /// By default when initializing a LocalMemoryBuffer with a data pointer and a length,
  /// it just assigns the pointer and length without coping the data content. This is
  /// for performance reasons. In this case the buffer cannot ensure data validity. It
  /// instead relies on the lifetime passed in data pointer.
  ///
  /// \param data The data pointer to the passed-in buffer.
  /// \param size The size of the passed in buffer.
  /// \param copy_data If true, data will be copied and owned by this buffer,
  ///                  otherwise the buffer only points to the given address.
  LocalMemoryBuffer(uint8_t *data, size_t size, bool copy_data = false)
      : has_data_copy_(copy_data) {
    if (copy_data) {
      RAY_CHECK(data != nullptr);
      buffer_ = reinterpret_cast<uint8_t *>(aligned_malloc(size, BUFFER_ALIGNMENT));
      std::copy(data, data + size, buffer_);
      data_ = buffer_;
      size_ = size;
    } else {
      data_ = data;
      size_ = size;
    }
  }

  /// Construct a LocalMemoryBuffer of all zeros of the given size.
  LocalMemoryBuffer(size_t size) : has_data_copy_(true) {
    buffer_ = reinterpret_cast<uint8_t *>(aligned_malloc(size, BUFFER_ALIGNMENT));
    data_ = buffer_;
    size_ = size;
  }

  uint8_t *Data() const override { return data_; }

  size_t Size() const override { return size_; }

  bool OwnsData() const override { return has_data_copy_; }

  bool IsPlasmaBuffer() const override { return false; }

  ~LocalMemoryBuffer() {
    size_ = 0;
    if (buffer_ != NULL) {
      aligned_free(buffer_);
    }
  }

 private:
  /// Disable copy constructor and assignment, as default copy will
  /// cause invalid data_.
  LocalMemoryBuffer &operator=(const LocalMemoryBuffer &) = delete;
  LocalMemoryBuffer(const LocalMemoryBuffer &) = delete;

  /// Pointer to the data.
  uint8_t *data_;
  /// Size of the buffer.
  size_t size_ = 0;
  /// Whether this buffer holds a copy of data.
  bool has_data_copy_ = false;
  /// This is only valid when `should_copy` is true.
  uint8_t *buffer_ = NULL;
};

/// Represents a byte buffer in shared memory.
class SharedMemoryBuffer : public Buffer {
 public:
  /// Constructor.
  ///
  /// By default when initializing a SharedMemoryBuffer with a data pointer and a length,
  /// it just assigns the pointer and length without coping the data content. This is
  /// for performance reasons. In this case the buffer cannot ensure data validity. It
  /// instead relies on the lifetime passed in data pointer.
  ///
  /// \param data The data pointer to the passed-in buffer.
  /// \param size The size of the passed in buffer.
  SharedMemoryBuffer(uint8_t *data, size_t size) {
    data_ = data;
    size_ = size;
  }

  /// Make a slice.
  SharedMemoryBuffer(const std::shared_ptr<Buffer> &buffer, int64_t offset, int64_t size)
      : size_(size), parent_(buffer) {
    data_ = buffer->Data() + offset;
    RAY_CHECK(size_ <= parent_->Size());
  }

  static std::shared_ptr<SharedMemoryBuffer> Slice(const std::shared_ptr<Buffer> &buffer,
                                                   int64_t offset,
                                                   int64_t size) {
    return std::make_shared<SharedMemoryBuffer>(buffer, offset, size);
  }

  uint8_t *Data() const override { return data_; }

  size_t Size() const override { return size_; }

  bool OwnsData() const override { return true; }

  bool IsPlasmaBuffer() const override { return true; }

  ~SharedMemoryBuffer() = default;

 private:
  /// Disable copy constructor and assignment, as default copy will
  /// cause invalid data_.
  SharedMemoryBuffer &operator=(const LocalMemoryBuffer &) = delete;
  SharedMemoryBuffer(const LocalMemoryBuffer &) = delete;

  /// Pointer to the data.
  uint8_t *data_;
  /// Size of the buffer.
  size_t size_;
  /// Keep the parent where the buffer is sliced from.
  std::shared_ptr<Buffer> parent_;
};

}  // namespace ray
