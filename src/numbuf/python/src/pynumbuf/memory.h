#ifndef PYNUMBUF_MEMORY_H
#define PYNUMBUF_MEMORY_H

#include <arrow/io/interfaces.h>

/* C++ includes */
#include <string>
#include <thread>
#include <vector>

#define THREADPOOL_SIZE 8
#define MEMCOPY_BLOCK_SIZE 64
#define BYTES_IN_MB (1 << 20)

using namespace std;

namespace numbuf {

class ParallelMemcopy {
 public:
  explicit ParallelMemcopy(uint64_t block_size, bool timeit, int threadpool_size)
      : timeit_(timeit),
        threadpool_(threadpool_size),
        block_size_(block_size),
        threadpool_size_(threadpool_size) {}

  void memcopy(uint8_t* dst, const uint8_t* src, uint64_t nbytes) {
    struct timeval tv1, tv2;
    if (timeit_) {
      // Start the timer.
      gettimeofday(&tv1, NULL);
    }
    if (nbytes >= BYTES_IN_MB) {
      memcopy_aligned(dst, src, nbytes, block_size_);
    } else {
      memcpy(dst, src, nbytes);
    }
    if (timeit_) {
      // Stop the timer and log the measured time.
      gettimeofday(&tv2, NULL);
      double elapsed =
          ((tv2.tv_sec - tv1.tv_sec) * 1000000 + (tv2.tv_usec - tv1.tv_usec)) / 1000000.0;
      // TODO: replace this with ARROW_LOG(ARROW_INFO) or better equivalent.
      printf("Copied %llu bytes in time = %8.4f MBps = %8.4f\n", nbytes, elapsed,
          nbytes / (BYTES_IN_MB * elapsed));
    }
  }

  ~ParallelMemcopy() {
    // Join threadpool threads just in case they are still running.
    for (auto& t : threadpool_) {
      if (t.joinable()) { t.join(); }
    }
  }

 private:
  /** Controls whether the memcopy operations are timed. */
  bool timeit_;
  /** Specifies the desired alignment in bytes, as a power of 2. */
  uint64_t block_size_;
  /** Number of threads to be used for parallel memcopy operations. */
  int threadpool_size_;
  /** Internal threadpool to be used in the fork/join pattern. */
  std::vector<std::thread> threadpool_;

  void memcopy_aligned(
      uint8_t* dst, const uint8_t* src, uint64_t nbytes, uint64_t block_size) {
    uint64_t num_threads = threadpool_size_;
    uint64_t dst_address = reinterpret_cast<uint64_t>(dst);
    uint64_t src_address = reinterpret_cast<uint64_t>(src);
    uint64_t left_address = (src_address + block_size - 1) & ~(block_size - 1);
    uint64_t right_address = (src_address + nbytes) & ~(block_size - 1);
    uint64_t num_blocks = (right_address - left_address) / block_size;
    // Update right address
    right_address = right_address - (num_blocks % num_threads) * block_size;
    // Now we divide these blocks between available threads. The remainder is
    // handled on the main thread.

    uint64_t chunk_size = (right_address - left_address) / num_threads;
    uint64_t prefix = left_address - src_address;
    uint64_t suffix = src_address + nbytes - right_address;
    // Now the data layout is | prefix | k * num_threads * block_size | suffix |.
    // We have chunk_size = k * block_size, therefore the data layout is
    // | prefix | num_threads * chunk_size | suffix |.
    // Each thread gets a "chunk" of k blocks.

    // Start all threads first and handle leftovers while threads run.
    for (int i = 0; i < num_threads; i++) {
      threadpool_[i] = std::thread(memcpy, dst + prefix + i * chunk_size,
          reinterpret_cast<uint8_t*>(left_address) + i * chunk_size, chunk_size);
    }

    memcpy(dst, src, prefix);
    memcpy(dst + prefix + num_threads * chunk_size,
        reinterpret_cast<uint8_t*>(right_address), suffix);

    for (auto& t : threadpool_) {
      if (t.joinable()) { t.join(); }
    }
  }
};

class FixedBufferStream : public arrow::io::OutputStream,
                          public arrow::io::ReadableFileInterface {
 public:
  virtual ~FixedBufferStream() {}

  explicit FixedBufferStream(uint8_t* data, int64_t nbytes)
      : data_(data),
        position_(0),
        size_(nbytes),
        memcopy_helper(MEMCOPY_BLOCK_SIZE, false, THREADPOOL_SIZE) {}

  arrow::Status Read(int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) override {
    DCHECK(out);
    if (position_ + nbytes > size_) {
      return arrow::Status::IOError("EOF");
    }
    *out = std::make_shared<arrow::Buffer>(data_ + position_, nbytes);
    position_ += nbytes;
    return arrow::Status::OK();
  }

  arrow::Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) override {
    assert(0);
    return arrow::Status::OK();
  }

  arrow::Status Seek(int64_t position) override {
    position_ = position;
    return arrow::Status::OK();
  }

  arrow::Status Close() override { return arrow::Status::OK(); }

  arrow::Status Tell(int64_t* position) override {
    *position = position_;
    return arrow::Status::OK();
  }

  arrow::Status Write(const uint8_t* data, int64_t nbytes) override {
    DCHECK(position_ >= 0 && position_ < size_);
    DCHECK(position_ + nbytes <= size_) << "position: " << position_
                                        << " nbytes: " << nbytes << "size: " << size_;
    uint8_t* dst = data_ + position_;
    memcopy_helper.memcopy(dst, data, nbytes);
    position_ += nbytes;
    return arrow::Status::OK();
  }

  arrow::Status GetSize(int64_t* size) override {
    *size = size_;
    return arrow::Status::OK();
  }

  bool supports_zero_copy() const override { return true; }

 private:
  uint8_t* data_;
  int64_t position_;
  int64_t size_;
  ParallelMemcopy memcopy_helper;
};

class MockBufferStream : public arrow::io::OutputStream {
 public:
  virtual ~MockBufferStream() {}

  explicit MockBufferStream() : position_(0) {}

  arrow::Status Close() override { return arrow::Status::OK(); }

  arrow::Status Tell(int64_t* position) override {
    *position = position_;
    return arrow::Status::OK();
  }

  arrow::Status Write(const uint8_t* data, int64_t nbytes) override {
    position_ += nbytes;
    return arrow::Status::OK();
  }

 private:
  int64_t position_;
};

}  // namespace numbuf

#endif  // PYNUMBUF_MEMORY_H
