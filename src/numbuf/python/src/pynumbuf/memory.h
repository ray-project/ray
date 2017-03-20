#ifndef PYNUMBUF_MEMORY_H
#define PYNUMBUF_MEMORY_H

#include <arrow/io/interfaces.h>

namespace numbuf {

class FixedBufferStream : public arrow::io::OutputStream,
                          public arrow::io::ReadableFileInterface {
 public:
  virtual ~FixedBufferStream() {}

  explicit FixedBufferStream(uint8_t* data, int64_t nbytes)
      : data_(data), position_(0), size_(nbytes) {}

  arrow::Status Read(int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) override {
    DCHECK(out);
    DCHECK(position_ + nbytes <= size_) << "position: " << position_
                                        << " nbytes: " << nbytes << "size: " << size_;
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
    memcpy(dst, data, nbytes);
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
