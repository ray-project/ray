#ifndef PYNUMBUF_MEMORY_H
#define PYNUMBUF_MEMORY_H

#include <arrow/ipc/memory.h>

namespace numbuf {

class BufferSource : public arrow::ipc::MemorySource {
 public:
  virtual ~BufferSource() {}

  explicit BufferSource(uint8_t* data, int64_t nbytes)
    : data_(data), size_(nbytes) {}

  arrow::Status ReadAt(int64_t position, int64_t nbytes,
                       std::shared_ptr<arrow::Buffer>* out) override {
    DCHECK(out);
    DCHECK(position + nbytes <= size_) << "position: " << position << " nbytes: " << nbytes << "size: " << size_;
    *out = std::make_shared<arrow::Buffer>(data_ + position, nbytes);
    return arrow::Status::OK();
  }

  arrow::Status Close() override {
    return arrow::Status::OK();
  }

  arrow::Status Write(int64_t position, const uint8_t* data,
                      int64_t nbytes) override {
    DCHECK(position >= 0 && position < size_);
    DCHECK(position + nbytes <= size_) << "position: " << position << " nbytes: " << nbytes << "size: " << size_;
    uint8_t* dst = data_ + position;
    memcpy(dst, data, nbytes);
    return arrow::Status::OK();
  }

  int64_t Size() const override {
    return size_;
  }

private:
 uint8_t* data_;
 int64_t size_;
};

} // namespace

#endif // PYNUMBUF_MEMORY_H
