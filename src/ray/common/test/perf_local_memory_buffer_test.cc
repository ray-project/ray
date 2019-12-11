#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/buffer.h"
#include <chrono>

static int perf_count = 10000000;

static int64_t current_sys_time_us() {
  std::chrono::microseconds mu_since_epoch =
  std::chrono::duration_cast<std::chrono::microseconds>(
  std::chrono::system_clock::now().time_since_epoch());
  return mu_since_epoch.count();
}

// Original LocalMemoryBuffer implemention
class LocalMemoryBuffer : public ray::Buffer {
 public:
  LocalMemoryBuffer(uint8_t *data, size_t size, bool copy_data = false)
      : has_data_copy_(copy_data) {
    if (copy_data) {
      RAY_CHECK(data != nullptr);
      buffer_.insert(buffer_.end(), data, data + size);
      data_ = buffer_.data();
      size_ = buffer_.size();
    } else {
      data_ = data;
      size_ = size;
    }
  }

  /// Construct a LocalMemoryBuffer of all zeros of the given size.
  LocalMemoryBuffer(size_t size) : has_data_copy_(true) {
    buffer_.resize(size, 0);
    data_ = buffer_.data();
    size_ = buffer_.size();
  }

  uint8_t *Data() const override { return data_; }

  size_t Size() const override { return size_; }

  bool OwnsData() const override { return has_data_copy_; }

  bool IsPlasmaBuffer() const override { return false; }

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

/// 
class LocalMemoryBufferUnique : public ray::Buffer {
 public:
  LocalMemoryBufferUnique(uint8_t *data, size_t size, bool copy_data = false)
      : has_data_copy_(copy_data) {
    if (copy_data) {
      RAY_CHECK(data != nullptr);
      buffer_ = std::unique_ptr<uint8_t[]>(new uint8_t[size]);
      memcpy(buffer_.get(), data, size);
      data_ = buffer_.get();
      size_ = size;
    } else {
      data_ = data;
      size_ = size;
    }
  }

  /// Construct a LocalMemoryBuffer of all zeros of the given size.
  LocalMemoryBufferUnique(size_t size) : has_data_copy_(true) {
    buffer_ = std::unique_ptr<uint8_t[]>(new uint8_t(size));
    data_ = buffer_.get();
    size_ = size;
  }

  uint8_t *Data() const override { return data_; }

  size_t Size() const override { return size_; }

  bool OwnsData() const override { return has_data_copy_; }

  bool IsPlasmaBuffer() const override { return false; }

  ~LocalMemoryBufferUnique() { size_ = 0; }

 private:
  /// Disable copy constructor and assignment, as default copy will
  /// cause invalid data_.
  LocalMemoryBufferUnique &operator=(const LocalMemoryBuffer &) = delete;
  LocalMemoryBufferUnique(const LocalMemoryBuffer &) = delete;

  /// Pointer to the data.
  uint8_t* data_;
  /// Size of the buffer.
  size_t size_;
  /// Whether this buffer holds a copy of data.
  bool has_data_copy_;
  std::unique_ptr<uint8_t[]> buffer_;
};


class LocalMemoryBufferReserve : public ray::Buffer {
 public:
  LocalMemoryBufferReserve(uint8_t *data, size_t size, bool copy_data = false)
      : has_data_copy_(copy_data) {
    if (copy_data) {
      RAY_CHECK(data != nullptr);
      buffer_.reserve(size);
      std::copy(data, data+size, buffer_.begin());
      data_ = buffer_.data();
      size_ = buffer_.size();
    } else {
      data_ = data;
      size_ = size;
    }
  }

  /// Construct a LocalMemoryBuffer of all zeros of the given size.
  LocalMemoryBufferReserve(size_t size) : has_data_copy_(true) {
    buffer_.resize(size, 0);
    // buffer_.reserve(size);
    // std::fill(buffer_.begin(), buffer_.end(), 0);
    data_ = buffer_.data();
    size_ = buffer_.size();
  }

  uint8_t *Data() const override { return data_; }

  size_t Size() const override { return size_; }

  bool OwnsData() const override { return has_data_copy_; }

  bool IsPlasmaBuffer() const override { return false; }

  ~LocalMemoryBufferReserve() {}

 private:
  /// Disable copy constructor and assignment, as default copy will
  /// cause invalid data_.
  LocalMemoryBufferReserve &operator=(const LocalMemoryBufferReserve &) = delete;
  LocalMemoryBufferReserve(const LocalMemoryBufferReserve &) = delete;

  /// Pointer to the data.
  uint8_t *data_;
  /// Size of the buffer.
  size_t size_;
  /// Whether this buffer holds a copy of data.
  bool has_data_copy_;
  /// This is only valid when `should_copy` is true.
  std::vector<uint8_t> buffer_;
};

// Benchmark for optimized LocalMemoryBuffer in ray/common/buffer.h
TEST(BufferPerfTest, OptimizedLocalMemoryBuffer) {
  uint8_t raw_buffer[1024] = {0};
  
  uint64_t start_time_us = current_sys_time_us();
  for(int i=0; i<perf_count; i++) {
    std::shared_ptr<ray::LocalMemoryBuffer> buffer = 
      std::make_shared<ray::LocalMemoryBuffer>(raw_buffer, 1024, true);
  }
  std::cout << "time cost: " << current_sys_time_us() - start_time_us << std::endl;
}

// Benchmark for original LocalMemoryBuffer
TEST(BufferPerfTest, LocalMemoryBuffer) {
  uint8_t raw_buffer[1024] = {0};
  
  uint64_t start_time_us = current_sys_time_us();
  for(int i=0; i<perf_count; i++) {
    std::shared_ptr<LocalMemoryBuffer> buffer = 
      std::make_shared<LocalMemoryBuffer>(raw_buffer, 1024, true);
  }
  std::cout << "time cost: " << current_sys_time_us() - start_time_us << std::endl;
}

TEST(BufferPerfTest, LocalMemoryBufferUnique) {
  uint8_t raw_buffer[1024] = {0};
  
  uint64_t start_time_us = current_sys_time_us();
  for(int i=0; i<perf_count; i++) {
    std::shared_ptr<LocalMemoryBufferUnique> buffer = 
      std::make_shared<LocalMemoryBufferUnique>(raw_buffer, 1024, true);
  }
  std::cout << "time cost: " << current_sys_time_us() - start_time_us << std::endl;
}

// Benchmark for LocalMemoryBuffer using buffer_.reserve
TEST(BufferPerfTest, LocalMemoryBufferReserve) {
  // uint8_t raw_buffer[1024] = {0};
  
  uint64_t start_time_us = current_sys_time_us();
  for(int i=0; i<perf_count; i++) {
    std::shared_ptr<LocalMemoryBufferReserve> buffer = 
      std::make_shared<LocalMemoryBufferReserve>(1024);
  }
  std::cout << "time cost: " << current_sys_time_us() - start_time_us << std::endl;
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}