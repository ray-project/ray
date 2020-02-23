#include <cstdint>
#include "ray/util/memory.h"
#include "ray/python/buffer_reader.h"

namespace ray {
namespace python {

int PythonObjectParser::Parse(const std::shared_ptr<ray::Buffer>& buffer) {
  const uint8_t *data = buffer->Data();
  int64_t protobuf_offset = ((int64_t*)data)[0];
  if (protobuf_offset < 0) {
    return -1;
  }
  int64_t protobuf_size = ((int64_t*)data)[1];
  if (protobuf_size > INT32_MAX || protobuf_size < 0) {
    return -2;
  }
  bool status = python_object.ParseFromArray(data + protobuf_offset, (int32_t)protobuf_size);
  if (!status) {
    return -3;
  }
  inband_data.append((char*)(data + python_object.inband_data_offset()),
                     (size_t)python_object.inband_data_size());
  buffers_segment = data + python_object.raw_buffers_offset();
  return 0;
}

std::string PythonObjectParser::GetInbandData() {
  return std::move(inband_data);
}

int64_t PythonObjectParser::BuffersCount() {
  return python_object.buffer_size();
}

void PythonObjectParser::GetBuffer(
    int index, void **buf, ssize_t *length, int *readonly, ssize_t *itemsize, int *ndim, std::string *format,
    std::vector<ssize_t> *shape, std::vector<ssize_t> *strides) {
  auto buffer_meta = python_object.buffer(index);
  *buf = (void*)(buffers_segment + buffer_meta.address());
  *length = buffer_meta.length();
  *ndim = buffer_meta.ndim();
  *readonly = buffer_meta.readonly();
  *itemsize = buffer_meta.itemsize();
  *format = buffer_meta.format();
  const auto &shape_data = buffer_meta.shape().data();
  shape->assign(shape_data, shape_data + *ndim);
  const auto &strides_data = buffer_meta.strides().data();
  strides->assign(strides_data, strides_data + *ndim);
}

}  // namespace python
}  // namespace ray



