#include "ray/common/buffer.h"

namespace ray {

LocalMemoryBuffer::LocalMemoryBuffer(uint8_t *data, size_t size, bool copy_data)
    : has_data_copy_(copy_data) {
  if (copy_data) {
    RAY_CHECK(data != nullptr);
    buffer_.resize(size);
    std::copy(data, data + size, buffer_.begin());
    data_ = buffer_.data();
    size_ = buffer_.size();
  } else {
    data_ = data;
    size_ = size;
  }
}

LocalMemoryBuffer::LocalMemoryBuffer(size_t size) : has_data_copy_(true) {
  buffer_.resize(size, 0);
  data_ = buffer_.data();
  size_ = buffer_.size();
}

PlasmaBuffer::~PlasmaBuffer() {}

PlasmaBuffer::PlasmaBuffer(const std::shared_ptr<arrow::Buffer> &buffer)
    : buffer_(buffer) {}

bool Buffer::operator==(const Buffer &rhs) const {
  if (this->Size() != rhs.Size()) {
    return false;
  }

  return this->Size() == 0 || memcmp(Data(), rhs.Data(), Size()) == 0;
}
uint8_t *PlasmaBuffer::Data() const { return const_cast<uint8_t *>(buffer_->data()); }
size_t PlasmaBuffer::Size() const { return buffer_->size(); }

Buffer::~Buffer() {}

}  // namespace ray
