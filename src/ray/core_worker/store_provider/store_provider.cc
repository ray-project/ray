#include "arrow/buffer.h"
#include "arrow/io/memory.h"

#include "ray/common/status.h"
#include "ray/core_worker/store_provider/store_provider.h"

namespace ray {

Status BufferedRayObject::WriteDataTo(std::shared_ptr<Buffer> buffer) const {
  if (data_->Size() > buffer->Size()) {
    return Status::Invalid(kBufferTooSmallErrMsg);
  }
  memcpy(buffer->Data(), data_->Data(), data_->Size());
  return Status::OK();
}

Status PyArrowRayObject::WriteDataTo(std::shared_ptr<Buffer> buffer) const {
  if (object_size_ > buffer->Size()) {
    return Status::Invalid(kBufferTooSmallErrMsg);
  }

  auto arrow_buffer =
      std::make_shared<arrow::MutableBuffer>(buffer->Data(), buffer->Size());
  arrow::io::FixedSizeBufferWriter buffer_writer(arrow_buffer);
  RAY_ARROW_RETURN_NOT_OK(object_->WriteTo(&buffer_writer));
  return Status::OK();
}

const std::shared_ptr<Buffer> &PyArrowRayObject::Data() {
  if (data_ == nullptr) {
    data_ = std::make_shared<LocalMemoryBuffer>(new uint8_t[object_size_], object_size_);
    auto status = this->WriteDataTo(data_);
  }
  return data_;
}

}  // namespace ray
