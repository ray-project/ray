#include "buffer_pool.h"
#include <algorithm>
#include <boost/range/adaptor/transformed.hpp>

namespace ray {
namespace streaming {

BufferPool::BufferPool(uint64_t pool_size, uint64_t min_buffer_size)
    : pool_size_(pool_size),
      min_buffer_size_(min_buffer_size),
      external_buffer_(nullptr) {
  STREAMING_LOG(INFO) << "create buffer pool with size " << pool_size
                      << ", min_buffer_size " << min_buffer_size;
  STREAMING_CHECK(pool_size >= min_buffer_size);
  // pool can have only one buffer.
  if (pool_size < 2 * min_buffer_size) {
    min_buffer_size_ = pool_size_;
    STREAMING_LOG(INFO) << "buffer pool size " << pool_size << " "
                        << "is less 2 times of min_buffer_size " << min_buffer_size
                        << " , set min_buffer_size to pool size.";
  }
}

BufferPool::BufferPool(uint8_t *external_buffer, uint64_t size)
    : pool_size_(size), min_buffer_size_(size), external_buffer_(external_buffer) {
  STREAMING_LOG(INFO) << "create buffer pool from external buffer with address "
                      << static_cast<void *>(external_buffer) << ", size " << size;
}

BufferPool::~BufferPool() {
  AutoSpinLock lk(lock);
  // memory is not allocated by buffer pool
  if (external_buffer_) {
    for (auto &buffer : buffers_) {
      delete buffer;
      buffer = nullptr;
    }
  } else {
    STREAMING_LOG(DEBUG) << "deallocate buffer pool. Usage: " << PrintUsage();
    for (auto &buffer : buffers_) {
      auto ptr = reinterpret_cast<uint8_t *>(buffer->buffer_start);
      delete[] ptr;
      delete buffer;
      buffer = nullptr;
    }
  }
}

void *to_ptr(uint64_t address) { return reinterpret_cast<void *>(address); }

StreamingStatus BufferPool::GetBuffer(Buffer *buffer) { return GetBuffer(1, buffer); }

StreamingStatus BufferPool::GetBuffer(uint64_t min_size, Buffer *buffer) {
  STREAMING_LOG(DEBUG) << "Get buffer of min size: " << min_size << " "
                       << "pool usage: " << PrintUsage();
  AutoSpinLock lk(lock);
  if (!current_writing_buffer_) {
    InnerBuf *inner_buf = nullptr;
    RETURN_IF_NOT_OK(NewBuffer(min_size, &inner_buf))
    current_writing_buffer_ = inner_buf;
  }

  uint64_t address;
  uint64_t remained;
  GetRemained(current_writing_buffer_, min_size, &address, &remained);

  if (min_size > remained) {
    if (current_size_ == pool_size_) {
      // next buffer may be usable, because when data_end = data_start, we can't free
      // buffer if mark is not set, and we maybe hold it to avoid malloc/free cost. Get
      // the buffer next to current_writing_buffer_, if current_writing_buffer_ is last
      // buffer, rewind to first buffer.
      auto pos = std::lower_bound(buffers_.begin(), buffers_.end(),
                                  current_writing_buffer_, compareFunc);
      if (pos != buffers_.end() - 1) {
        current_writing_buffer_ = *(pos + 1);
      } else {
        current_writing_buffer_ = buffers_.front();
      }
    } else {
      InnerBuf *inner_buf = nullptr;
      RETURN_IF_NOT_OK(NewBuffer(min_size, &inner_buf))
      current_writing_buffer_ = inner_buf;
    }
    GetRemained(current_writing_buffer_, min_size, &address, &remained);
  }

  if (min_size <= remained) {
    *buffer = {reinterpret_cast<uint8_t *>(address), remained};
    current_writing_buffer_->marked = false;
    return StreamingStatus::OK;
  } else {
    *buffer = {nullptr, 0};
    return StreamingStatus::OK;
  }
}

StreamingStatus BufferPool::GetBufferBlocked(uint64_t min_size, Buffer *buffer) {
  auto status = GetBuffer(min_size, buffer);
  RETURN_IF_NOT_OK(status)
  if (buffer->Size() < min_size) {
    AutoSpinLock lk(lock);
    auto iter = std::find_if(buffers_.begin(), buffers_.end(), [min_size](InnerBuf *buf) {
      return GetMaxRemained(buf) >= min_size;
    });
    if (iter != buffers_.end()) {
      current_writing_buffer_ = *iter;
      lk.unlock();
    } else {
      std::unique_lock<std::mutex> mutex_lk(m);
      // must lock mutex first, otherwise buffer_available_cv notification maybe missed
      lk.unlock();
      needed_size_ = min_size;
      buffer_available_cv.wait(mutex_lk, [&] { return needed_size_ == 0; });
      mutex_lk.unlock();
    }
    return GetBuffer(min_size, buffer);
  } else {
    return status;
  }
}

inline bool BufferPool::GetRemained(BufferPool::InnerBuf *buf, uint64_t min_size,
                                    uint64_t *address, uint64_t *remained) {
  u_int64_t buffer_start = buf->buffer_start, buffer_end = buf->buffer_end,
            data_start = buf->data_start, data_end = buf->data_end;
  // if wrap_around
  if (data_end < data_start) {
    // --------------------------------------------------------------------------------
    // |                       data_end                 data_start                     |
    // |-------- left used --------|------- remained -------|------- right used -------|
    // --------------------------------------------------------------------------------
    // left 1 byte to make `data_start == data_end` show buffer usage is null, rather
    // full.
    *remained = data_start - data_end - 1;
    *address = data_end;
    return true;
  } else {
    // --------------------------------------------------------------------------------
    // |                         data_start           data_end                         |
    // |------- left remained -------|------- used -------|------ right remained ------|
    // --------------------------------------------------------------------------------
    // Since buffer is consumed continuously, take right remained as remained
    auto right_remained = buffer_end - data_end;
    // if right remained is less than min_size, ignore right remained, take left
    // remained as remained, try to wrap around
    if (right_remained < min_size) {
      // left 1 byte to make `data_start == data_end` show buffer usage is null, rather
      // full
      // avoid unsigned *remained overflow
      if (data_start > buffer_start) {
        *remained = data_start - buffer_start - 1;
      } else {
        *remained = 0;
      }
      *address = buffer_start;
      return true;
    } else {
      *remained = right_remained;
      *address = data_end;
      return false;
    }
  }
}

inline uint64_t BufferPool::GetMaxRemained(BufferPool::InnerBuf *buf) {
  u_int64_t buffer_start = buf->buffer_start, buffer_end = buf->buffer_end,
            data_start = buf->data_start, data_end = buf->data_end;
  // if wrap_around
  if (data_end < data_start) {
    return data_start - data_end - 1;
  } else {
    auto right_remained = buffer_end - data_end;
    uint64_t left_remained = 0;
    // avoid u_int64_t overflow
    if (data_start > buffer_start) {
      left_remained = data_start - buffer_start - 1;
    }
    return std::max(right_remained, left_remained);
  }
}

// ensure continuous consume buffer
StreamingStatus BufferPool::MarkUsed(uint64_t address, uint64_t size) {
  STREAMING_LOG(DEBUG) << "Mark used range: [" << to_ptr(address) << ", "
                       << to_ptr(address + size) << "), "
                       << "pool usage: " << PrintUsage();
  InnerBuf *buffer;
  AutoSpinLock lk(lock);
  if (current_writing_buffer_ && current_writing_buffer_->buffer_start <= address &&
      address < current_writing_buffer_->buffer_end) {
    buffer = current_writing_buffer_;
  } else {
    RETURN_IF_NOT_OK(FindBuffer(address, &buffer))
    current_writing_buffer_ = buffer;
  }
  STREAMING_CHECK(buffer->buffer_start <= address &&
                  address + size <= buffer->buffer_end);
  if (buffer->data_start <= buffer->data_end) {
    // -----------------------------------------------------------------------------------
    // |                         data_start           data_end                           |
    // |------- left remained -------|------- used -------|------- right remained -------|
    // -----------------------------------------------------------------------------------
    // when wrap around, `address == buffer->buffer_start`
    STREAMING_CHECK(address == buffer->data_end || address == buffer->buffer_start);
    // when wrap around, right remained is taken used too, so releasing this right
    // remained needs more treatment.
    buffer->data_end = address + size;
  } else {
    STREAMING_CHECK(address == buffer->data_end);
    // ----------------------------------------------------------------------------------
    // |                       data_end                 data_start                       |
    // |-------- left used --------|------- remained -------|-------- right used --------|
    // ----------------------------------------------------------------------------------
    // when wrap around, address + size <= buffer->data_start - 1.
    STREAMING_CHECK(address + size < buffer->data_start)
        << "buffer overflow when in wrap around: "
        << "address " << address << " size " << size
        << "buffer usage: " << PrintUsage(buffer);
    buffer->data_end = address + size;
  }
  current_writing_buffer_->marked = true;
  used_ += size;
  return StreamingStatus::OK;
}

StreamingStatus BufferPool::MarkUsed(const uint8_t *ptr, uint64_t size) {
  auto addr = reinterpret_cast<uint64_t>(ptr);
  return MarkUsed(addr, size);
}

bool BufferPool::compareFunc(const BufferPool::InnerBuf *left,
                             const BufferPool::InnerBuf *right) {
  return left->buffer_start < right->buffer_start;
}

// ensure continuous release
StreamingStatus BufferPool::Release(uint64_t address, uint64_t size) {
  STREAMING_LOG(DEBUG) << "release range: [" << to_ptr(address) << ", "
                       << to_ptr(address + size) << ") "
                       << "usage: " << PrintUsage();
  InnerBuf *buffer;
  AutoSpinLock lk(lock);
  if (current_releasing_buffer_ && current_releasing_buffer_->buffer_start <= address &&
      address < current_releasing_buffer_->buffer_end) {
    buffer = current_releasing_buffer_;
  } else {
    RETURN_IF_NOT_OK(FindBuffer(address, &buffer))
    current_releasing_buffer_ = buffer;
  }

  bool wrap_around = buffer->data_end < buffer->data_start;
  if (wrap_around) {
    // ---------------------------------------------------------------------------------
    // |                       data_end                 data_start                      |
    // |-------- left used --------|------- remained -------|-------- right used -------|
    // ---------------------------------------------------------------------------------

    // wrap around case:
    // |--------- left remained ---------|------ used -------|-- msg1 --|--- ignored ---|
    // to
    // |-- msg2 --|--- used ---|--- remained ---|--- used ---|-- msg1 --|--- ignored ---|
    if (address == buffer->data_start) {
      // such as msg1
      STREAMING_CHECK(address + size <= buffer->buffer_end)
          << "release range: [" << to_ptr(address) << ", " << to_ptr(address + size)
          << "). pool usage: " << PrintUsage();
    } else if (address == buffer->buffer_start) {
      // such as msg2
      STREAMING_CHECK(address + size <= buffer->data_end)
          << "release range: [" << to_ptr(address) << ", " << to_ptr(address + size)
          << "). pool usage: " << PrintUsage();
    } else {
      STREAMING_LOG(FATAL) << "Invalid release range"
                           << ", release range: [" << to_ptr(address) << ", "
                           << to_ptr(address + size) << "]"
                           << ", buffer usage: " << PrintUsage(buffer);
      return StreamingStatus::Invalid;
    }
    buffer->data_start = address + size;
  } else {
    // buffer state 1
    // ----------------------------------------------------------------------------------
    // |                         data_start           data_end                           |
    // |------- left remained -------|------- used -------|------- right remained -------|
    // ----------------------------------------------------------------------------------
    STREAMING_CHECK(address == buffer->data_start)
        << "release range: [" << to_ptr(address) << ", " << to_ptr(address + size)
        << "). pool usage: " << PrintUsage();
    STREAMING_CHECK(address + size <= buffer->data_end)
        << "release range: [" << to_ptr(address) << ", " << to_ptr(address + size)
        << "). pool usage: " << PrintUsage();
  }
  buffer->data_start = address + size;
  used_ -= size;

  if (buffer->marked && buffer->data_start == buffer->data_end) {
    // For buffers_.size() == 1:
    // 1) if `current_size_ == min_buffer_size_`, reserve this buffer to avoid frequent
    // malloc/delete cost.
    // 2) if `current_size_ > min_buffer_size_`, release this buffer to
    // reduce memory usage.
    if (buffers_.size() > 1 || current_size_ > min_buffer_size_) {
      // binary search buffers_ to get position to delete from buffers_.
      auto pos = std::lower_bound(buffers_.begin(), buffers_.end(), buffer, compareFunc);
      buffers_.erase(pos);
      current_size_ -= (buffer->buffer_end - buffer->buffer_start);
      if (!external_buffer_) {
        auto ptr = reinterpret_cast<uint8_t *>(buffer->buffer_start);
        delete[] ptr;
      }
      delete buffer;
      if (current_writing_buffer_ == current_releasing_buffer_) {
        current_writing_buffer_ = nullptr;
      }
      current_releasing_buffer_ = nullptr;
    }
  }
  if (needed_size_ > 0) {
    if (current_releasing_buffer_ &&
        GetMaxRemained(current_releasing_buffer_) >= needed_size_) {
      {
        std::lock_guard<std::mutex> mutex_lk(m);
        current_writing_buffer_ = current_releasing_buffer_;
        needed_size_ = 0;
      }
      buffer_available_cv.notify_one();
    } else if (pool_size_ - current_size_ >= needed_size_) {
      {
        std::lock_guard<std::mutex> mutex_lk(m);
        current_writing_buffer_ = nullptr;
        needed_size_ = 0;
      }
      buffer_available_cv.notify_one();
    }
  }
  return StreamingStatus::OK;
}

StreamingStatus BufferPool::Release(const uint8_t *ptr, uint64_t size) {
  auto addr = reinterpret_cast<uint64_t>(ptr);
  return Release(addr, size);
}

std::string BufferPool::PrintUsage() {
  auto transformed =
      boost::adaptors::transform(buffers_, [](InnerBuf *buf) { return PrintUsage(buf); });
  auto buffers_usage =
      ray::streaming::StreamingUtility::join(transformed, ",\n", "[", "]");
  std::stringstream ss;
  ss << "pool size: " << pool_size_ << ", memory used: " << current_size_ << ", "
     << "used: " << used_ << ", buffers usage: " << buffers_usage;
  return ss.str();
}

std::string BufferPool::PrintUsage(BufferPool::InnerBuf *buf) {
  std::stringstream ss;
  ss << "(buffer range -> [" << to_ptr(buf->buffer_start) << ", "
     << to_ptr(buf->buffer_end) << "), "
     << "usage range -> [" << to_ptr(buf->data_start) << ", " << to_ptr(buf->data_end)
     << ")";
  bool wrap_around = buf->data_end < buf->data_start;
  if (wrap_around) {
    ss << ", wrap_around";
  }
  if (buf->marked) {
    ss << ", marked";
  }
  ss << ")";
  return ss.str();
}

StreamingStatus BufferPool::FindBuffer(u_int64_t address, BufferPool::InnerBuf **buffer) {
  // binary search buffer to get a buffer which `address` >= `buffer.start` && `address` <
  // `buffer.end`
  int buffer_nums = buffers_.size();
  int lower_bound = 0;
  int upper_bound = buffer_nums - 1;
  while (lower_bound <= upper_bound) {
    int mid = lower_bound + (upper_bound - lower_bound) / 2;
    auto inner_buffer = buffers_[mid];
    if (inner_buffer->buffer_start <= address && address < inner_buffer->buffer_end) {
      *buffer = inner_buffer;
      return StreamingStatus::OK;
    } else {
      if (address < inner_buffer->buffer_start) {
        upper_bound = mid - 1;
      }
      if (address >= inner_buffer->buffer_end) {
        lower_bound = mid + 1;
      }
    }
  }

  STREAMING_LOG(WARNING) << "Invalid address: " << to_ptr(address)
                         << ". Current pool usage: " << PrintUsage();
  return StreamingStatus::Invalid;
}

StreamingStatus BufferPool::NewBuffer(uint64_t min_size, BufferPool::InnerBuf **buffer) {
  uint64_t remained = pool_size_ - current_size_;
  uint64_t size = min_buffer_size_;
  if (min_buffer_size_ < min_size) {
    size = min_size;
  }
  if (remained < size) {
    STREAMING_LOG(FATAL) << "OutOfMemory: current memory size: " << current_size_
                         << ", still need: " << size;
    return StreamingStatus::OutOfMemory;
  }
  if (remained - size < min_buffer_size_) {
    // ensure `current_size_ == pool_size_` when buffer pool is full finally.
    size = remained;
  }

  uint8_t *data;
  if (external_buffer_) {
    data = external_buffer_ + current_size_;
  } else {
    data = static_cast<uint8_t *>(malloc(static_cast<size_t>(size)));
  }
  if (data) {
    auto buffer_start = reinterpret_cast<uint64_t>(data);
    *buffer =
        new InnerBuf{buffer_start, buffer_start + size, buffer_start, buffer_start, true};
    current_size_ += size;
    buffers_.push_back(*buffer);
    std::sort(buffers_.begin(), buffers_.end(), compareFunc);
    return StreamingStatus::OK;
  } else {
    return StreamingStatus::OutOfMemory;
  }
}

}  // namespace streaming
}  // namespace ray
