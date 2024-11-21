/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <tensorpipe/common/cpu_buffer.h>
#include <tensorpipe/common/device.h>

namespace tensorpipe {

class Buffer {
  class AbstractBufferWrapper {
   public:
    virtual Device device() const = 0;
    virtual void copyConstructInto(void* ptr) const = 0;
    virtual void moveConstructInto(void* ptr) = 0;
    virtual ~AbstractBufferWrapper() = default;
  };

  template <typename TBuffer>
  class BufferWrapper : public AbstractBufferWrapper {
    static_assert(
        std::is_trivially_copyable<TBuffer>::value,
        "wrapping non-trivially copyable class");

   public:
    TBuffer buffer;

    explicit BufferWrapper(TBuffer buffer) : buffer(std::move(buffer)) {}

    Device device() const override {
      return buffer.getDevice();
    }

    void copyConstructInto(void* ptr) const override {
      new (ptr) BufferWrapper(*this);
    }

    void moveConstructInto(void* ptr) override {
      new (ptr) BufferWrapper(std::move(*this));
    }
  };

 public:
  template <typename TBuffer>
  /* implicit */ Buffer(TBuffer b) {
    static_assert(
        sizeof(BufferWrapper<TBuffer>) <= kStructSize, "kStructSize too small");
    static_assert(
        alignof(BufferWrapper<TBuffer>) <= kStructAlign,
        "kStructAlign too small");
    new (&raw_) BufferWrapper<TBuffer>(std::move(b));
  }

  Buffer() : Buffer(CpuBuffer{}) {}

  Buffer(const Buffer& other) {
    other.ptr()->copyConstructInto(&raw_);
  }

  Buffer& operator=(const Buffer& other) {
    if (this != &other) {
      ptr()->~AbstractBufferWrapper();
      other.ptr()->copyConstructInto(&raw_);
    }
    return *this;
  }

  Buffer(Buffer&& other) noexcept {
    other.ptr()->moveConstructInto(&raw_);
  }

  Buffer& operator=(Buffer&& other) {
    if (this != &other) {
      ptr()->~AbstractBufferWrapper();
      other.ptr()->moveConstructInto(&raw_);
    }
    return *this;
  }

  ~Buffer() {
    ptr()->~AbstractBufferWrapper();
  }

  template <typename TBuffer>
  TBuffer& unwrap() {
    BufferWrapper<TBuffer>* wrapperPtr =
        dynamic_cast<BufferWrapper<TBuffer>*>(ptr());
    if (wrapperPtr == nullptr) {
      throw std::runtime_error("Invalid unwrapping of tensorpipe::Buffer");
    }
    return wrapperPtr->buffer;
  }

  template <typename TBuffer>
  const TBuffer& unwrap() const {
    const BufferWrapper<TBuffer>* wrapperPtr =
        dynamic_cast<const BufferWrapper<TBuffer>*>(ptr());
    if (wrapperPtr == nullptr) {
      throw std::runtime_error("Invalid unwrapping of tensorpipe::Buffer");
    }
    return wrapperPtr->buffer;
  }

  Device device() const {
    return ptr()->device();
  }

 private:
  static constexpr int kStructSize = 32;
  static constexpr int kStructAlign = 8;
  std::aligned_storage<kStructSize, kStructAlign>::type raw_{};

  const AbstractBufferWrapper* ptr() const {
    // FIXME: Once we go C++17, use std::launder on the returned pointer.
    return reinterpret_cast<const AbstractBufferWrapper*>(&raw_);
  }

  AbstractBufferWrapper* ptr() {
    // FIXME: Once we go C++17, use std::launder on the returned pointer.
    return reinterpret_cast<AbstractBufferWrapper*>(&raw_);
  }
};

} // namespace tensorpipe
