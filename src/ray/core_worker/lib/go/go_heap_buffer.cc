// Copyright 2025 The Ray Authors.
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

#include "go_heap_buffer.h"

extern "C" {
#include "native_runtime.h"
}

#include "ray/util/logging.h"

namespace ray::go {

GoHeapBuffer::GoHeapBuffer(GoObjectRefHandle* handle)
    : handle_(nullptr), data_(nullptr), size_(0) {
  RAY_CHECK(handle != nullptr) << "GoObjectRefHandle cannot be null";
  handle_ = handle;
  data_ = handle->data_ptr;
  size_ = handle->size;
}

GoHeapBuffer::~GoHeapBuffer() noexcept {
  if (handle_ != nullptr) {
    try {
      GoReleaseObjectRef(handle_->ref_handle);
    } catch (...) {
      // Swallow exceptions during destruction
    }
    free(handle_);
  }
}

GoHeapBuffer::GoHeapBuffer(GoHeapBuffer&& other) noexcept
    : handle_(other.handle_),
      data_(other.data_),
      size_(other.size_) {
  other.handle_ = nullptr;
  other.data_ = nullptr;
  other.size_ = 0;
}

GoHeapBuffer& GoHeapBuffer::operator=(GoHeapBuffer&& other) noexcept {
  if (this != &other) {
    if (handle_ != nullptr) {
      try {
        GoReleaseObjectRef(handle_->ref_handle);
      } catch (...) {
        // Swallow exceptions
      }
      free(handle_);
    }
    handle_ = other.handle_;
    data_ = other.data_;
    size_ = other.size_;
    other.handle_ = nullptr;
    other.data_ = nullptr;
    other.size_ = 0;
  }
  return *this;
}

uint8_t* GoHeapBuffer::Data() const {
  return const_cast<uint8_t*>(static_cast<const uint8_t*>(data_));
}

size_t GoHeapBuffer::Size() const {
  return size_;
}

bool GoHeapBuffer::OwnsData() const {
  return false;  // Go heap owns the data
}

bool GoHeapBuffer::IsPlasmaBuffer() const {
  return false;  // Go-managed memory, not plasma
}

std::shared_ptr<GoHeapBuffer> AllocateGoHeapBuffer(
    const char* object_id_data,
    int object_id_size,
    const char* data_ptr,
    int data_size,
    const char* metadata_ptr,
    int metadata_size) {
  // Call Go-side allocation function via CGO
  // Note: const_cast is necessary because Go exports functions with non-const parameters,
  // but C++ code maintains const correctness. The Go function does not modify the data.
  void* go_handle = GoAllocateObject(
      const_cast<char*>(object_id_data), object_id_size,
      const_cast<char*>(data_ptr), data_size,
      const_cast<char*>(metadata_ptr), metadata_size);

  if (go_handle == nullptr) {
    RAY_LOG(ERROR) << "Failed to allocate Go heap buffer: "
                   << "object_id_size=" << object_id_size
                   << ", data_size=" << data_size
                   << ", metadata_size=" << metadata_size;
    return nullptr;
  }

  // Wrap Go handle in GoHeapBuffer
  return std::make_shared<GoHeapBuffer>(
      static_cast<GoObjectRefHandle*>(go_handle));
}

}  // namespace ray::go
