#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <vector>

#include <gloo/allreduce.h>
#include <gloo/context.h>
#include <gloo/math.h>
#include <gloo/types.h>

namespace pygloo {

enum class ReduceOp : std::uint8_t {
  SUM = 0,
  PRODUCT,
  MIN,
  MAX,
  BAND, // Bitwise AND
  BOR,  // Bitwise OR
  BXOR, // Bitwise XOR
  UNUSED,
};

typedef void (*ReduceFunc)(void *, const void *, const void *, size_t);

template <typename T> ReduceFunc toFunction(const ReduceOp &r) {
  switch (r) {
  case ReduceOp::SUM:
    return ReduceFunc(&gloo::sum<T>);
  case ReduceOp::PRODUCT:
    return ReduceFunc(&gloo::product<T>);
  case ReduceOp::MIN:
    return ReduceFunc(&gloo::min<T>);
  case ReduceOp::MAX:
    return ReduceFunc(&gloo::max<T>);
  case ReduceOp::BAND:
    throw std::runtime_error(
        "Cannot use ReduceOp.BAND with non-integral dtype");
    break;
  case ReduceOp::BOR:
    throw std::runtime_error("Cannot use ReduceOp.BOR with non-integral dtype");
    break;
  case ReduceOp::BXOR:
    throw std::runtime_error(
        "Cannot use ReduceOp.BXOR with non-integral dtype");
    break;
  case ReduceOp::UNUSED:
    break;
  }

  throw std::runtime_error("Unhandled ReduceOp");
}

enum class glooDataType_t : std::uint8_t {
  glooInt8 = 0,
  glooUint8,
  glooInt32,
  glooUint32,
  glooInt64,
  glooUint64,
  glooFloat16,
  glooFloat32,
  glooFloat64,
};

void allreduce_wrapper(const std::shared_ptr<gloo::Context> &context,
                       intptr_t sendbuf, intptr_t recvbuf, size_t size,
                       glooDataType_t datatype,
                       ReduceOp reduceop = ReduceOp::SUM,
                       gloo::AllreduceOptions::Algorithm algorithm =
                           gloo::AllreduceOptions::Algorithm::RING);

void allgather_wrapper(const std::shared_ptr<gloo::Context> &context,
                       intptr_t sendbuf, intptr_t recvbuf, size_t size,
                       glooDataType_t datatype);

void allgatherv_wrapper(const std::shared_ptr<gloo::Context> &context,
                        intptr_t sendbuf, intptr_t recvbuf, size_t size,
                        glooDataType_t datatype);

void reduce_wrapper(const std::shared_ptr<gloo::Context> &context,
                    intptr_t sendbuf, intptr_t recvbuf, size_t size,
                    glooDataType_t datatype, ReduceOp reduceop = pygloo::ReduceOp::SUM, int root=0);

void scatter_wrapper(const std::shared_ptr<gloo::Context> &context,
                     std::vector<intptr_t> sendbuf, intptr_t recvbuf,
                     size_t size, glooDataType_t datatype, int root=0);

void send_wrapper(const std::shared_ptr<gloo::Context> &context,
                  intptr_t sendbuf, size_t size, glooDataType_t datatype,
                  int peer);

void recv_wrapper(const std::shared_ptr<gloo::Context> &context,
                  intptr_t recvbuf, size_t size, glooDataType_t datatype,
                  int peer);

void broadcast_wrapper(const std::shared_ptr<gloo::Context> &context,
                       intptr_t sendbuf, intptr_t recvbuf, size_t size,
                       glooDataType_t datatype, int root=0);



void barrier(const std::shared_ptr<gloo::Context> &context);

} // pygloo