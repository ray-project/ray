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

#ifndef RAY_CORE_WORKER_LIB_GO_GO_HEAP_BUFFER_H
#define RAY_CORE_WORKER_LIB_GO_GO_HEAP_BUFFER_H

/**
 * @file go_heap_buffer.h
 * @brief Go heap memory buffer management
 *
 * This file contains the GoHeapBuffer class that wraps Go-managed memory.
 * It provides a C++ Buffer interface to Go heap memory, coordinating with
 * Go's garbage collector for proper lifecycle management.
 *
 * Design principles:
 * - Zero-copy when possible
 * - Thread-safe reference counting
 * - Automatic cleanup via RAII
 * - Clear ownership semantics (Go owns data, C++ holds reference)
 */

#include <cstddef>
#include <cstdint>
#include <memory>

#include "ray/common/buffer.h"
#include "ray/util/logging.h"

namespace ray {
namespace go {

struct GoObjectRefHandle {
  void* data_ptr;
  size_t size;
  void* ref_handle;
};

/**
 * @brief A Buffer implementation that wraps Go-managed memory
 *
 * This class provides a C++ Buffer interface to memory that is managed by
 * Go's garbage collector. It coordinates with Go to ensure proper cleanup
 * when the C++ side releases all references.
 *
 * Thread safety:
 * - This class is NOT thread-safe for concurrent modifications
 * - Safe to pass between threads via std::shared_ptr
 * - The underlying Go object reference is thread-safe (handled by Go's GC)
 * - Do not access the same GoHeapBuffer instance from multiple threads simultaneously
 *
 * Ownership semantics:
 * - Go heap owns the actual data
 * - C++ holds a reference via GoObjectRefHandle
 * - When GoHeapBuffer is destroyed, it notifies Go to release the reference
 * - Go's GC can reclaim the data when all references are released
 */
class GoHeapBuffer : public ray::Buffer {
 public:
  /**
   * @brief Construct a GoHeapBuffer from a GoObjectRefHandle
   *
   * @param handle Go-side reference handle (must not be null)
   * @throws std::runtime_error if handle is null
   */
  explicit GoHeapBuffer(GoObjectRefHandle* handle);

  /**
   * @brief Destructor - notifies Go to release the reference
   *
   * This method calls GoReleaseObjectRef to notify Go's GC that this
   * reference is no longer needed. The actual memory may be reclaimed
   * when Go's GC determines it's safe.
   */
  ~GoHeapBuffer() noexcept override;

  // Disable copy constructor and assignment operator
  GoHeapBuffer(const GoHeapBuffer&) = delete;
  GoHeapBuffer& operator=(const GoHeapBuffer&) = delete;

  // Enable move semantics
  GoHeapBuffer(GoHeapBuffer&& other) noexcept;
  GoHeapBuffer& operator=(GoHeapBuffer&& other) noexcept;

  /**
   * @brief Get the data pointer
   * @return Pointer to the data (owned by Go)
   */
  uint8_t* Data() const override;

  /**
   * @brief Get the size of the buffer
   * @return Size in bytes
   */
  size_t Size() const override;

  /**
   * @brief Check if this buffer owns the data
   * @return false - Go heap owns the data
   */
  bool OwnsData() const override;

  /**
   * @brief Check if this is a plasma buffer
   * @return false - this is Go-managed memory
   */
  bool IsPlasmaBuffer() const override;

 private:
  GoObjectRefHandle* handle_;  // Go-side reference handle
  const void* data_;           // Data pointer (owned by Go)
  size_t size_;                // Data size in bytes
};

/**
 * @brief Create a GoHeapBuffer from raw data
 *
 * This function calls GoAllocateObject to allocate memory in Go heap,
 * then wraps it in a GoHeapBuffer.
 *
 * @param object_id_data ObjectID binary data
 * @param object_id_size ObjectID size
 * @param data_ptr Data pointer
 * @param data_size Data size
 * @param metadata_ptr Metadata pointer (may be null)
 * @param metadata_size Metadata size
 * @return std::shared_ptr<GoHeapBuffer> The allocated buffer, or nullptr on failure
 */
std::shared_ptr<GoHeapBuffer> AllocateGoHeapBuffer(
    const char* object_id_data,
    int object_id_size,
    const char* data_ptr,
    int data_size,
    const char* metadata_ptr,
    int metadata_size);

}  // namespace go
}  // namespace ray

#endif  // RAY_CORE_WORKER_LIB_GO_GO_HEAP_BUFFER_H
