#include "ray/core_worker/store_provider/store_provider.h"
#include "ray/common/status.h"

namespace arrow {
  class MutableBuffer;
namespace io {
  class FixedSizeBufferWriter;
}
}

namespace ray {

Status BufferedRayObject::WriteDataTo(std::shared_ptr<Buffer> buffer) {
  memcpy(buffer->Data(), data_->Data(), data_->Size());
  return Status::OK();
}

Status PyRayObject::WriteDataTo(std::shared_ptr<Buffer> buffer) {
  arrow::MutableBuffer arrow_buffer(buffer->Data(), buffer->Size());
  arrow::io::FixedSizeBufferWriter buffer_writer(arrow_buffer);
  RAY_ARROW_RETURN_NOT_OK(data_->WriteTo(buffer_writer));
  return Status::OK();
}

} // namespace ray
