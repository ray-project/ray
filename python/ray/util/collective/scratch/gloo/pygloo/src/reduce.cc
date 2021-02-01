#include <collective.h>
#include <gloo/reduce.h>

namespace pygloo {

template <typename T>
void reduce(const std::shared_ptr<gloo::Context> &context, intptr_t sendbuf,
            intptr_t recvbuf, size_t size, ReduceOp reduceop, int root, uint32_t tag) {
  T *input_ptr = reinterpret_cast<T *>(sendbuf);
  T *output_ptr = reinterpret_cast<T *>(recvbuf);

  // Configure reduceOptions struct
  gloo::ReduceOptions opts_(context);
  opts_.setInput(input_ptr, size);
  opts_.setOutput(output_ptr, size);
  gloo::ReduceOptions::Func fn = toFunction<T>(reduceop);
  opts_.setReduceFunction(fn);
  opts_.setRoot(root);
  opts_.setTag(tag);

  gloo::reduce(opts_);
}

void reduce_wrapper(const std::shared_ptr<gloo::Context> &context,
                    intptr_t sendbuf, intptr_t recvbuf, size_t size,
                    glooDataType_t datatype, ReduceOp reduceop, int root, uint32_t tag) {
  switch (datatype) {
  case glooDataType_t::glooInt8:
    reduce<int8_t>(context, sendbuf, recvbuf, size, reduceop, root, tag);
    break;
  case glooDataType_t::glooUint8:
    reduce<uint8_t>(context, sendbuf, recvbuf, size, reduceop, root, tag);
    break;
  case glooDataType_t::glooInt32:
    reduce<int32_t>(context, sendbuf, recvbuf, size, reduceop, root, tag);
    break;
  case glooDataType_t::glooUint32:
    reduce<uint32_t>(context, sendbuf, recvbuf, size, reduceop, root, tag);
    break;
  case glooDataType_t::glooInt64:
    reduce<int64_t>(context, sendbuf, recvbuf, size, reduceop, root, tag);
    break;
  case glooDataType_t::glooUint64:
    reduce<uint64_t>(context, sendbuf, recvbuf, size, reduceop, root, tag);
    break;
  case glooDataType_t::glooFloat16:
    reduce<gloo::float16>(context, sendbuf, recvbuf, size, reduceop, root, tag);
    break;
  case glooDataType_t::glooFloat32:
    reduce<float_t>(context, sendbuf, recvbuf, size, reduceop, root, tag);
    break;
  case glooDataType_t::glooFloat64:
    reduce<double_t>(context, sendbuf, recvbuf, size, reduceop, root, tag);
    break;
  default:
    throw std::runtime_error("Unhandled dataType");
  }
}

} // pygloo