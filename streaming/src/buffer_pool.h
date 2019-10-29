
#ifndef RAY_STREAMING_BUFFER_POOL_H
#define RAY_STREAMING_BUFFER_POOL_H

#include <tuple>
#include <vector>
#include "streaming.h"

namespace ray {
namespace streaming {

class Buffer {
 public:
  Buffer(uint8_t *data, uint64_t size) : data_(data), size_(size) {}

  Buffer() : data_(nullptr), size_(0) {}

  Buffer(const Buffer &buffer) noexcept = default;

  Buffer &operator=(const Buffer &buffer) noexcept = default;

  Buffer &operator=(Buffer &&buffer) noexcept = default;

  ~Buffer() = default;

  uint8_t *Data() const { return data_; }

  uint64_t Size() const { return size_; }

 private:
  uint8_t *data_;
  uint64_t size_;
  friend class BufferPool;
};

/// A SPSC sequential read/write/release buffer pool
class BufferPool {
 public:
  /// Buffer pool to allocate/release memory by self.
  BufferPool(uint64_t total_size, uint64_t min_buffer_size);

  /// Buffer pool on a provided buffer, provided buffer is never be released by this
  /// buffer pool, mainly for shared-memory case.
  BufferPool(uint8_t *external_buffer, uint64_t size);

  ~BufferPool();

  /// This method doesn't change current_writing_buffer_->data_end, so this buffer pool
  /// can't be used by multi producer.
  /// @param buffer result buffer, if buffer pool doesn't have enough memory, return an
  /// empty buffer
  StreamingStatus GetBuffer(Buffer *buffer);

  /// This method doesn't change current_writing_buffer_->data_end, so this buffer pool
  /// can't be used by multi producer.
  /// @param min_size minimum size of returned buffer
  /// @param buffer result buffer, if buffer pool doesn't have enough memory, return an
  /// empty buffer
  StreamingStatus GetBuffer(uint64_t min_size, Buffer *buffer);

  /// Return a buffer of minimal size `min_size`, blocks if pool doesn't have enough
  /// memory.
  /// @param min_size minimum size of returned buffer
  /// @param buffer result buffer
  StreamingStatus GetBufferBlocked(uint64_t min_size, Buffer *buffer);

  /// Mark a buffer as used
  /// @param address buffer start address
  /// @param size buffer size
  StreamingStatus MarkUsed(uint64_t address, uint64_t size);

  /// Mark a buffer as used
  /// @param address buffer address
  /// @param size buffer size
  StreamingStatus MarkUsed(const uint8_t *ptr, uint64_t size);

  /// Return a buffer to buffer pool
  /// @param address buffer start address
  /// @param size buffer size
  StreamingStatus Release(uint64_t address, uint64_t size);

  /// Return a buffer to buffer pool
  /// @param address buffer address
  /// @param size buffer size
  StreamingStatus Release(const uint8_t *ptr, uint64_t size);

  /// buffer pool usage info for debug
  std::string PrintUsage();

  /// Return how much memory buffer pool used.
  uint64_t memory_used() { return current_size_; }

  /// Return how much memory buffers used.
  uint64_t used() { return used_; }

  /// Return needed size of buffer when pool doesn't have enough space.
  uint64_t needed_size() { return needed_size_; }

 private:
  /// InnerBuf to record buffer usage info.
  /// when data_start == data_end, buffer is null; when data_end < data_start, buffer
  /// usage is wrap around; when data_end + 1 == data_start, buffer usage is wrap around
  /// full. We left a byte to differentiate forward full and wrap around full.
  ///
  /// buffer usage can be in two states:
  /// @code
  /// state 1:
  /// -----------------------------------------------------------------------------------
  /// |                         data_start           data_end                           |
  /// |------- left remained -------|------- used -------|------- right remained -------|
  /// -----------------------------------------------------------------------------------
  ///
  /// state 2 (wrap around):
  /// -----------------------------------------------------------------------------------
  /// |                       data_end                 data_start                       |
  /// |-------- left used --------|------- remained -------|-------- right used --------|
  /// -----------------------------------------------------------------------------------
  /// @endcode
  struct InnerBuf {
    // inclusive. it's also the address returned by malloc for this buffer
    u_int64_t buffer_start;
    u_int64_t buffer_end;  // exclusive
    u_int64_t data_start;  // inclusive
    u_int64_t data_end;    // exclusive
    // If set, it indicates buffer is not in use, release operation can free buffer
    // when buffer content is empty. buffer is used by writer if not set.
    bool marked;
  };

  StreamingStatus FindBuffer(u_int64_t address, InnerBuf **buffer);

  /// create a new InnerBuffer and add it in buffer pool.
  /// Return OutOfMemory if buffer pool is full.
  /// This method will ensure `current_size_ == pool_size_` when buffer pool is full
  /// finally.
  StreamingStatus NewBuffer(uint64_t size, InnerBuf **buffer);

  /// buffer usage info for debug
  static std::string PrintUsage(InnerBuf *buf);

  static bool GetRemained(InnerBuf *buf, uint64_t min_size, uint64_t *address,
                          uint64_t *remained);

  static uint64_t GetMaxRemained(InnerBuf *buf);

  /// pool size in bytes of this buffer pool
  uint64_t pool_size_;

  /// min size in bytes of an internal memory buffer
  uint64_t min_buffer_size_;

  /// provided buffer address
  uint8_t *external_buffer_;

  InnerBuf *current_writing_buffer_ = nullptr;

  InnerBuf *current_releasing_buffer_ = nullptr;

  /// buffers held by this buffer pool. buffers address increase monotonously.
  std::vector<InnerBuf *> buffers_;

  /// compare function to sort `buffers_``
  static bool compareFunc(const InnerBuf *left, const InnerBuf *right);

  /// Current size of buffer pool.
  /// When buffer pool is full, current_size_ will be equal to pool_size_
  uint64_t current_size_ = 0;

  /// Current usage of buffer pool.
  uint64_t used_ = 0;

  // Benchmark shows that use spin lock have better performance than mutex
  std::atomic_flag lock = ATOMIC_FLAG_INIT;
  uint64_t needed_size_ = 0;
  std::mutex m;
  std::condition_variable buffer_available_cv;
};

}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_BUFFER_POOL_H
