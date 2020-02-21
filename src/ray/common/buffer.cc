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

}  // namespace ray
