#ifndef RAY_RING_BUFFER_H
#define RAY_RING_BUFFER_H

#include <atomic>
#include <boost/circular_buffer.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

#include "message/message.h"
#include "ray/common/status.h"
#include "util/streaming_logging.h"

namespace ray {
namespace streaming {

/// Because the data cannot be successfully written to the channel every time, in
/// order not to serialize the message repeatedly, we designed a temporary buffer
/// area so that when the downstream is backpressured or the channel is blocked
/// due to memory limitations, it can be cached first and waited for the next use.
class StreamingTransientBuffer {
 private:
  std::shared_ptr<uint8_t> transient_buffer_;
  // BufferSize is length of last serialization data.
  uint32_t transient_buffer_size_ = 0;
  uint32_t max_transient_buffer_size_ = 0;
  bool transient_flag_ = false;

 public:
  inline size_t GetTransientBufferSize() const { return transient_buffer_size_; }

  inline void SetTransientBufferSize(uint32_t new_transient_buffer_size) {
    transient_buffer_size_ = new_transient_buffer_size;
  }

  inline size_t GetMaxTransientBufferSize() const { return max_transient_buffer_size_; }

  inline const uint8_t *GetTransientBuffer() const { return transient_buffer_.get(); }

  inline uint8_t *GetTransientBufferMutable() const { return transient_buffer_.get(); }

  ///  To reuse transient buffer, we will realloc buffer memory if size of needed
  ///  message bundle raw data is greater-than original buffer size.
  ///  \param size buffer size
  ///
  inline void ReallocTransientBuffer(uint32_t size) {
    transient_buffer_size_ = size;
    transient_flag_ = true;
    if (max_transient_buffer_size_ > size) {
      return;
    }
    max_transient_buffer_size_ = size;
    transient_buffer_.reset(new uint8_t[size], std::default_delete<uint8_t[]>());
  }

  inline bool IsTransientAvaliable() { return transient_flag_; }

  inline void FreeTransientBuffer(bool is_force = false) {
    transient_buffer_size_ = 0;
    transient_flag_ = false;

    // Transient buffer always holds max size buffer among all messages, which is
    // wasteful. So expiration time is considerable idea to release large buffer if this
    // transient buffer pointer hold it in long time.

    if (is_force) {
      max_transient_buffer_size_ = 0;
      transient_buffer_.reset();
    }
  }

  virtual ~StreamingTransientBuffer() = default;
};

template <class T>
class AbstractRingBuffer {
 public:
  virtual void Push(const T &) = 0;
  virtual void Pop() = 0;
  virtual T &Front() = 0;
  virtual bool Empty() const = 0;
  virtual bool Full() const = 0;
  virtual size_t Size() const = 0;
  virtual size_t Capacity() const = 0;
};

template <class T>
class RingBufferImplThreadSafe : public AbstractRingBuffer<T> {
 public:
  RingBufferImplThreadSafe(size_t size) : buffer_(size) {}
  virtual ~RingBufferImplThreadSafe() = default;
  void Push(const T &t) {
    boost::unique_lock<boost::shared_mutex> lock(ring_buffer_mutex_);
    buffer_.push_back(t);
  }
  void Pop() {
    boost::unique_lock<boost::shared_mutex> lock(ring_buffer_mutex_);
    buffer_.pop_front();
  }
  T &Front() {
    boost::shared_lock<boost::shared_mutex> lock(ring_buffer_mutex_);
    return buffer_.front();
  }
  bool Empty() const {
    boost::shared_lock<boost::shared_mutex> lock(ring_buffer_mutex_);
    return buffer_.empty();
  }
  bool Full() const {
    boost::shared_lock<boost::shared_mutex> lock(ring_buffer_mutex_);
    return buffer_.full();
  }
  size_t Size() const {
    boost::shared_lock<boost::shared_mutex> lock(ring_buffer_mutex_);
    return buffer_.size();
  }
  size_t Capacity() const { return buffer_.capacity(); }

 private:
  mutable boost::shared_mutex ring_buffer_mutex_;
  boost::circular_buffer<T> buffer_;
};

template <class T>
class RingBufferImplLockFree : public AbstractRingBuffer<T> {
 private:
  std::vector<T> buffer_;
  std::atomic<size_t> capacity_;
  std::atomic<size_t> read_index_;
  std::atomic<size_t> write_index_;

 public:
  RingBufferImplLockFree(size_t size)
      : buffer_(size, nullptr), capacity_(size), read_index_(0), write_index_(0) {}
  virtual ~RingBufferImplLockFree() = default;

  void Push(const T &t) {
    STREAMING_CHECK(!Full());
    buffer_[write_index_] = t;
    write_index_ = IncreaseIndex(write_index_);
  }

  void Pop() {
    STREAMING_CHECK(!Empty());
    read_index_ = IncreaseIndex(read_index_);
  }

  T &Front() {
    STREAMING_CHECK(!Empty());
    return buffer_[read_index_];
  }

  bool Empty() const { return write_index_ == read_index_; }

  bool Full() const { return IncreaseIndex(write_index_) == read_index_; }

  size_t Size() const { return (write_index_ + capacity_ - read_index_) % capacity_; }

  size_t Capacity() const { return capacity_; }

 private:
  size_t IncreaseIndex(size_t index) const { return (index + 1) % capacity_; }
};

enum class StreamingRingBufferType : uint8_t { SPSC_LOCK, SPSC };

/// StreamingRinggBuffer is factory to generate two different buffers. In data
/// writer, we use lock-free single producer single consumer (SPSC) ring buffer
/// to hold messages from user thread because SPSC has much better performance
/// than lock style. Since the SPSC_LOCK is useful to our event-driver model(
/// we will use that buffer to optimize our thread model in the future), so
/// it cann't be removed currently.
class StreamingRingBuffer {
 private:
  std::shared_ptr<AbstractRingBuffer<StreamingMessagePtr>> message_buffer_;

  StreamingTransientBuffer transient_buffer_;

 public:
  explicit StreamingRingBuffer(size_t buf_size, StreamingRingBufferType buffer_type =
                                                    StreamingRingBufferType::SPSC_LOCK);

  bool Push(const StreamingMessagePtr &msg);

  StreamingMessagePtr &Front();

  void Pop();

  bool IsFull() const;

  bool IsEmpty() const;

  size_t Size() const;

  size_t Capacity() const;

  size_t GetTransientBufferSize();

  void SetTransientBufferSize(uint32_t new_transient_buffer_size);

  size_t GetMaxTransientBufferSize() const;

  const uint8_t *GetTransientBuffer() const;

  uint8_t *GetTransientBufferMutable() const;

  void ReallocTransientBuffer(uint32_t size);

  bool IsTransientAvaliable();

  void FreeTransientBuffer(bool is_force = false);
};

typedef std::shared_ptr<StreamingRingBuffer> StreamingRingBufferPtr;
}  // namespace streaming
}  // namespace ray

#endif  // RAY_RING_BUFFER_H
