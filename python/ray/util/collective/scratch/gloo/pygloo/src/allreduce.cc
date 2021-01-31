#include <collective.h>
#include <gloo/allreduce.h>
#include <gloo/reduce.h>

namespace pygloo {

template <typename T>
void allreduce(const std::shared_ptr<gloo::Context> &context, intptr_t sendbuf,
               intptr_t recvbuf, size_t size, ReduceOp reduceop,
               gloo::AllreduceOptions::Algorithm algorithm) {
  std::vector<T *> input_ptr;
  std::vector<T *> output_ptr;
  input_ptr.emplace_back(reinterpret_cast<T *>(sendbuf));
  output_ptr.emplace_back(reinterpret_cast<T *>(recvbuf));

  // Configure AllreduceOptions struct and call allreduec function
  gloo::AllreduceOptions opts_(context);
  opts_.setInputs(input_ptr, size);
  opts_.setOutputs(output_ptr, size);
  opts_.setAlgorithm(algorithm);
  gloo::ReduceOptions::Func fn = toFunction<T>(reduceop);
  opts_.setReduceFunction(fn);
  opts_.setTag(0);

  gloo::allreduce(opts_);
}

void allreduce_wrapper(const std::shared_ptr<gloo::Context> &context,
                       intptr_t sendbuf, intptr_t recvbuf, size_t size,
                       glooDataType_t datatype, ReduceOp reduceop,
                       gloo::AllreduceOptions::Algorithm algorithm) {
  switch (datatype) {
  case glooDataType_t::glooInt8:
    allreduce<int8_t>(context, sendbuf, recvbuf, size, reduceop, algorithm);
    break;
  case glooDataType_t::glooUint8:
    allreduce<uint8_t>(context, sendbuf, recvbuf, size, reduceop, algorithm);
    break;
  case glooDataType_t::glooInt32:
    allreduce<int32_t>(context, sendbuf, recvbuf, size, reduceop, algorithm);
    break;
  case glooDataType_t::glooUint32:
    allreduce<uint32_t>(context, sendbuf, recvbuf, size, reduceop, algorithm);
    break;
  case glooDataType_t::glooInt64:
    allreduce<int64_t>(context, sendbuf, recvbuf, size, reduceop, algorithm);
    break;
  case glooDataType_t::glooUint64:
    allreduce<uint64_t>(context, sendbuf, recvbuf, size, reduceop, algorithm);
    break;
  case glooDataType_t::glooFloat16:
    allreduce<gloo::float16>(context, sendbuf, recvbuf, size, reduceop,
                             algorithm);
    break;
  case glooDataType_t::glooFloat32:
    allreduce<float_t>(context, sendbuf, recvbuf, size, reduceop, algorithm);
    break;
  case glooDataType_t::glooFloat64:
    allreduce<double_t>(context, sendbuf, recvbuf, size, reduceop, algorithm);
    break;
  default:
    throw std::runtime_error("Unhandled dataType");
  }
}

} // namespace pygloo