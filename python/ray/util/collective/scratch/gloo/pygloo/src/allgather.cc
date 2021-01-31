#include <collective.h>

#include <gloo/allgather.h>
#include <gloo/allgatherv.h>
#include <gloo/context.h>

namespace pygloo {

template <typename T>
void allgather(const std::shared_ptr<gloo::Context> &context, intptr_t sendbuf,
               intptr_t recvbuf, size_t size) {
  T *input_ptr = reinterpret_cast<T *>(sendbuf);
  T *output_ptr = reinterpret_cast<T *>(recvbuf);

  // Configure AllgatherOptions struct and call allgather function
  gloo::AllgatherOptions opts_(context);
  opts_.setInput(input_ptr, size);
  opts_.setOutput(output_ptr, size * context->size);
  opts_.setTag(0);

  gloo::allgather(opts_);
}

void allgather_wrapper(const std::shared_ptr<gloo::Context> &context,
                       intptr_t sendbuf, intptr_t recvbuf, size_t size,
                       glooDataType_t datatype) {
  switch (datatype) {
  case glooDataType_t::glooInt8:
    allgather<int8_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooUint8:
    allgather<uint8_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooInt32:
    allgather<int32_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooUint32:
    allgather<uint32_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooInt64:
    allgather<int64_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooUint64:
    allgather<uint64_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooFloat16:
    allgather<gloo::float16>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooFloat32:
    allgather<float_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooFloat64:
    allgather<double_t>(context, sendbuf, recvbuf, size);
    break;
  default:
    throw std::runtime_error("Unhandled dataType");
  }
}

template <typename T>
void allgatherv(const std::shared_ptr<gloo::Context> &context, intptr_t sendbuf,
                intptr_t recvbuf, size_t size) {
  T *input_ptr = reinterpret_cast<T *>(sendbuf);
  T *output_ptr = reinterpret_cast<T *>(recvbuf);

  // Configure AllgatherOptions struct and call allgather function
  gloo::AllgatherOptions opts_(context);
  opts_.setInput(input_ptr, size);
  opts_.setOutput(output_ptr, size * context->size);
  opts_.setTag(0);

  gloo::allgather(opts_);
}

void allgatherv_wrapper(const std::shared_ptr<gloo::Context> &context,
                        intptr_t sendbuf, intptr_t recvbuf, size_t size,
                        glooDataType_t datatype) {
  switch (datatype) {
  case glooDataType_t::glooInt8:
    allgather<int8_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooUint8:
    allgather<uint8_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooInt32:
    allgather<int32_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooUint32:
    allgather<uint32_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooInt64:
    allgather<int64_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooUint64:
    allgather<uint64_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooFloat16:
    allgather<gloo::float16>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooFloat32:
    allgather<float_t>(context, sendbuf, recvbuf, size);
    break;
  case glooDataType_t::glooFloat64:
    allgather<double_t>(context, sendbuf, recvbuf, size);
    break;
  default:
    throw std::runtime_error("Unhandled dataType");
  }
}
} // namespace pygloo
