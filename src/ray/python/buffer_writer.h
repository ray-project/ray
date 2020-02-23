#ifndef RAY_PYTHON_BUFFER_WRITER_H
#define RAY_PYTHON_BUFFER_WRITER_H

#include <cinttypes>
#include <string>
#include "ray/protobuf/serialization.pb.h"
#include "ray/common/buffer.h"

namespace ray {
namespace python {

// This is the default alignment value for len(buffer) < 2048.
constexpr size_t kMinorBufferAlign = 8;
// This is the default alignment value for len(buffer) >= 2048.
// Some projects like Arrow use it for possible SIMD acceleration.
constexpr size_t kMajorBufferAlign = 64;
constexpr size_t kMajorBufferSize = 2048;
constexpr size_t kMemcopyDefaultBlocksize = 64;
constexpr size_t kMemcopyDefaultThreshold = 1024 * 1024;

using ray::serialization::PythonBuffer;
using ray::serialization::PythonObject;

int RawDataWrite(const std::string &raw_data,
                 const std::shared_ptr<ray::Buffer> &data,
                 int memcopy_threads);

class PythonObjectBuilder {
 public:
  PythonObjectBuilder();
  void AppendBuffer(uint8_t *buf, ssize_t length, ssize_t itemsize, int ndim,
                    char* format, ssize_t *shape, ssize_t *strides);
  // Get the total size of the current builder.
  int64_t GetTotalBytes();
  int64_t GetInbandDataSize() { return inband_data_size; };
  void SetInbandDataSize(int64_t size);
  int WriteTo(const std::string &inband, const std::shared_ptr<ray::Buffer> &data, int memcopy_threads);

 private:
  void update_size();
  PythonObject python_object;
  std::vector<uint8_t*> buffer_ptrs;
  bool size_outdated = true;
  uint64_t protobuf_offset;
  // Address of end of the current buffer, relative to the
  // begin offset of our buffers.
  uint64_t current_buffer_address = 0;
  // This is a constant in our layout.
  int64_t inband_data_offset = sizeof(int64_t) * 2;
  int64_t inband_data_size = -1;
  int64_t total_bytes = -1;
  int64_t protobuf_size = -1;
};

}  // namespace python
}  // namespace ray

#endif  // RAY_PYTHON_BUFFER_WRITER_H
