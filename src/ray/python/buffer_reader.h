#ifndef RAY_PYTHON_BUFFER_READER_H
#define RAY_PYTHON_BUFFER_READER_H

#include <cinttypes>
#include <string>
#include "ray/protobuf/serialization.pb.h"
#include "ray/common/buffer.h"

namespace ray {
namespace python {

#if defined(_WIN32)
using ssize_t = SSIZE_T;
#endif

using ray::serialization::PythonBuffer;
using ray::serialization::PythonObject;

class PythonObjectParser {
 public:
  int Parse(const std::shared_ptr<ray::Buffer>& buffer);
  // Get the inband data. This can only be called once.
  std::string GetInbandData();
  int64_t BuffersCount();
  void GetBuffer(int index, void **buf, ssize_t *length, int *readonly, ssize_t *itemsize, int *ndim, std::string *format,
                 std::vector<ssize_t> *shape, std::vector<ssize_t> *strides);
 private:
  PythonObject python_object;
  std::string inband_data;
  const uint8_t* buffers_segment;
};

}  // namespace python
}  // namespace ray

#endif  // RAY_PYTHON_BUFFER_READER_H