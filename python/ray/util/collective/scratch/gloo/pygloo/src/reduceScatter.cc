#include <collective.h>
#include <gloo/reduce_scatter.h>

namespace pygloo {

template <typename T>
void reduce_scatter(const std::shared_ptr<gloo::Context> &context, intptr_t sendbuf,
               intptr_t recvbuf, size_t size, int root) {
  T *input_ptr = reinterpret_cast<T *>(sendbuf);
  T *output_ptr = reinterpret_cast<T *>(recvbuf);

  // Configure reduce_scatterOptions struct and call allreduec function
  gloo::reduce_scatterOptions opts_(context);
  opts_.setInput(input_ptr, size);
  opts_.setOutput(output_ptr, size);
  opts_.setRoot(root);
  opts_.setTag(0);

  gloo::reduce_scatter(opts_);
}

void reduce_scatter_wrapper(const std::shared_ptr<gloo::Context> &context,
                       intptr_t sendbuf, intptr_t recvbuf, size_t size,
                       glooDataType_t datatype, int root) {
  switch (datatype) {
  case glooDataType_t::glooInt8:
    reduce_scatter<int8_t>(context, sendbuf, recvbuf, size, root);
    break;
  case glooDataType_t::glooUint8:
    reduce_scatter<uint8_t>(context, sendbuf, recvbuf, size, root);
    break;
  case glooDataType_t::glooInt32:
    reduce_scatter<int32_t>(context, sendbuf, recvbuf, size, root);
    break;
  case glooDataType_t::glooUint32:
    reduce_scatter<uint32_t>(context, sendbuf, recvbuf, size, root);
    break;
  case glooDataType_t::glooInt64:
    reduce_scatter<int64_t>(context, sendbuf, recvbuf, size, root);
    break;
  case glooDataType_t::glooUint64:
    reduce_scatter<uint64_t>(context, sendbuf, recvbuf, size, root);
    break;
  case glooDataType_t::glooFloat16:
    reduce_scatter<gloo::float16>(context, sendbuf, recvbuf, size, root);
    break;
  case glooDataType_t::glooFloat32:
    reduce_scatter<float_t>(context, sendbuf, recvbuf, size, root);
    break;
  case glooDataType_t::glooFloat64:
    reduce_scatter<double_t>(context, sendbuf, recvbuf, size, root);
    break;
  default:
    throw std::runtime_error("Unhandled dataType");
  }
}

} // pygloo