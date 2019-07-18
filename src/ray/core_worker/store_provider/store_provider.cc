#include "arrow/buffer.h"
#include "arrow/io/memory.h"

#include "ray/core_worker/store_provider/store_provider.h"
#include "ray/common/status.h"

namespace ray {

Status BufferedRayObject::WriteDataTo(std::shared_ptr<Buffer> buffer) const {
  memcpy(buffer->Data(), data_->Data(), data_->Size());
  return Status::OK();
}

Status PyArrowRayObject::WriteDataTo(std::shared_ptr<Buffer> buffer) const {
  auto arrow_buffer = std::make_shared<arrow::ResizableBuffer>(buffer->Data(), buffer->Size());
  arrow::io::BufferOutputStream buffer_stream(arrow_buffer);
  //arrow::io::OutputStream output_stream(arrow_buffer);
  RAY_ARROW_RETURN_NOT_OK(object_->WriteTo(&buffer_stream));
  return Status::OK();
}

} // namespace ray
