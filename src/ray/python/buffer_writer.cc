#include "ray/util/memory.h"
#include "ray/python/buffer_writer.h"
#include <cstdint>

namespace ray {
namespace python {

inline int64_t padded_length(int64_t offset, int64_t alignment) {
  return ((offset + alignment - 1) / alignment) * alignment;
}

inline int64_t padded_length_u64(uint64_t offset, uint64_t alignment) {
  return ((offset + alignment - 1) / alignment) * alignment;
}

inline void memcopy_fast(void* dst, const void* src, int64_t len, int memcopy_threads) {
  if (memcopy_threads > 1 && len > kMemcopyDefaultThreshold) {
     ray::parallel_memcopy(
       (uint8_t*)dst, (uint8_t*)src, len, kMemcopyDefaultBlocksize, memcopy_threads);
  } else {
    memcpy(dst, src, len);
  }
}

int RawDataWrite(const std::string &raw_data,
                 const std::shared_ptr<ray::Buffer> &data,
                 int memcopy_threads) {
  if (raw_data.length() > data->Size()) {
    return -1;
  }
  memcopy_fast(data->Data(), raw_data.data(), raw_data.length(), memcopy_threads);
  return 0;
}

PythonObjectBuilder::PythonObjectBuilder() {
  python_object.set_inband_data_offset(inband_data_offset);
}

void PythonObjectBuilder::AppendBuffer(
    uint8_t *buf, ssize_t length, ssize_t itemsize, int ndim,
    char* format, ssize_t *shape, ssize_t *strides) {
  buffer_ptrs.push_back(buf);
  PythonBuffer* buffer = python_object.add_buffer();
  buffer->set_length(length);
  buffer->set_ndim(ndim);
  // It should be 'view.readonly'. But for the sake of shared memory,
  // we have to make it immutable.
  buffer->set_readonly(1);
  buffer->set_itemsize(itemsize);
  if (format != nullptr) {
     buffer->set_format(format);
  }
  if (shape != nullptr) {
    for (int32_t i = 0; i < ndim; i++) {
      buffer->add_shape(shape[i]);
    }
  }
  if (strides != nullptr) {
    for (int32_t i = 0; i < ndim; i++) {
      buffer->add_strides(strides[i]);
    }
  }

  // Increase buffer address.
  if (length < kMajorBufferSize) {
    current_buffer_address = padded_length(
          current_buffer_address, kMinorBufferAlign);
  } else {
    current_buffer_address = padded_length(
          current_buffer_address, kMajorBufferAlign);
  }
  buffer->set_address(current_buffer_address);
  current_buffer_address += length;
  // update the protobuf and change the buffer address will change
  // the size.
  size_outdated = true;
}

void PythonObjectBuilder::SetInbandDataSize(int64_t size) {
  if (inband_data_size != size) {
    size_outdated = true;
  }
  inband_data_size = size;
}

int64_t PythonObjectBuilder::GetTotalBytes() {
  update_size();
  return total_bytes;
}

int PythonObjectBuilder::WriteTo(const std::string &inband,
                                  const std::shared_ptr<ray::Buffer> &data,
                                  int memcopy_threads) {
  if (inband_data_size < 0) {
    inband_data_size = inband.length();
    size_outdated = true;
  }
  update_size();
  auto total_bytes = GetTotalBytes();
  // check if the target has sufficient space
  if (total_bytes > data->Size()) {
    return -1;
  }
  // Check if the protobuf size is too large
  if (protobuf_size > INT32_MAX) {
    return -2;
  }

  uint8_t *ptr = data->Data();
  // Write protobuf size for deserialization.
  protobuf_size = python_object.GetCachedSize();
  ((int64_t*)ptr)[0] = protobuf_offset;
  ((int64_t*)ptr)[1] = protobuf_size;
  // Write protobuf data.
  python_object.SerializeWithCachedSizesToArray(ptr + protobuf_offset);
  // Write inband data.
  memcopy_fast(ptr + python_object.inband_data_offset(),
               (void*)inband.data(), inband.length(), memcopy_threads);
  // Write buffer data.
  ptr += python_object.raw_buffers_offset();
  for (int i = 0; i < python_object.buffer_size(); i++) {
    uint64_t buffer_addr = python_object.buffer(i).address();
    uint64_t buffer_len = python_object.buffer(i).length();
    memcopy_fast(ptr + buffer_addr, buffer_ptrs[i], buffer_len, memcopy_threads);
  }
  return 0;
}


void PythonObjectBuilder::update_size() {
  // Since calculating the output size is expensive, we will
  // reuse the cached size.
  if (inband_data_size < 0) {
    // The inband data is not ready.
    total_bytes = -1;
    size_outdated = true;
    return;
  }
  if (!size_outdated) return;
  uint64_t raw_buffers_offset = padded_length_u64(
    inband_data_offset + inband_data_size, kMajorBufferAlign);

  // Because protobuf makes use of data compression, updating
  // any field could change its size. So we have to update all
  // fields whose size could have changed.
  python_object.set_inband_data_size(inband_data_size);
  python_object.set_raw_buffers_offset(raw_buffers_offset);
  python_object.set_raw_buffers_size(current_buffer_address);
  protobuf_size = python_object.ByteSizeLong();
  protobuf_offset = padded_length_u64(
      raw_buffers_offset + current_buffer_address, kMinorBufferAlign);
  total_bytes = protobuf_offset + protobuf_size;
  size_outdated = false;
}

}  // namespace python
}  // namespace ray
