#include <collective.h>
#include <gloo/scatter.h>

namespace pygloo {

template <typename T>
void scatter(const std::shared_ptr<gloo::Context> &context,
             std::vector<intptr_t> sendbuf, intptr_t recvbuf, size_t size,
             int root, uint32_t tag) {

  std::vector<T *> input_ptr;
  for (size_t i = 0; i < sendbuf.size(); ++i)
    input_ptr.emplace_back(reinterpret_cast<T *>(sendbuf[i]));

  T *output_ptr = reinterpret_cast<T *>(recvbuf);

  // Configure ScatterOptions struct
  gloo::ScatterOptions opts_(context);
  opts_.setInputs(input_ptr, size);
  opts_.setOutput(output_ptr, size);
  opts_.setTag(tag);
  opts_.setRoot(root);

  gloo::scatter(opts_);
}

void scatter_wrapper(const std::shared_ptr<gloo::Context> &context,
                     std::vector<intptr_t> sendbuf, intptr_t recvbuf,
                     size_t size, glooDataType_t datatype, int root, uint32_t tag) {
  switch (datatype) {
  case glooDataType_t::glooInt8:
    scatter<int8_t>(context, sendbuf, recvbuf, size, root, tag);
    break;
  case glooDataType_t::glooUint8:
    scatter<uint8_t>(context, sendbuf, recvbuf, size, root, tag);
    break;
  case glooDataType_t::glooInt32:
    scatter<int32_t>(context, sendbuf, recvbuf, size, root, tag);
    break;
  case glooDataType_t::glooUint32:
    scatter<uint32_t>(context, sendbuf, recvbuf, size, root, tag);
    break;
  case glooDataType_t::glooInt64:
    scatter<int64_t>(context, sendbuf, recvbuf, size, root, tag);
    break;
  case glooDataType_t::glooUint64:
    scatter<uint64_t>(context, sendbuf, recvbuf, size, root, tag);
    break;
  case glooDataType_t::glooFloat16:
    scatter<gloo::float16>(context, sendbuf, recvbuf, size, root, tag);
    break;
  case glooDataType_t::glooFloat32:
    scatter<float_t>(context, sendbuf, recvbuf, size, root, tag);
    break;
  case glooDataType_t::glooFloat64:
    scatter<double_t>(context, sendbuf, recvbuf, size, root, tag);
    break;
  default:
    throw std::runtime_error("Unhandled dataType");
  }
}
} // namespace pygloo