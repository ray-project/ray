#include <condition_variable>

#include <gloo/math.h>
#include <gloo/reduce.h>
#include <gloo/allreduce.h>
#include <gloo/allreduce_ring.h>
#include <collective.h>

namespace pygloo{

typedef void (*ReduceFunc)(void*, const void*, const void*, size_t);

template <
    typename T,
    typename std::enable_if<!std::is_integral<T>::value, int>::type = 0>
ReduceFunc toFunction(const ReduceOp& r) {
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
      throw std::runtime_error(
          "Cannot use ReduceOp.BOR with non-integral dtype");
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


void allreduce(const std::shared_ptr<gloo::Context> &context, pybind11::array_t<double, pybind11::array::c_style> &sendbuf,
        pybind11::array_t<double, pybind11::array::c_style> &recvbuf,
        size_t count, int datatype, ReduceOp reduceop){
    auto* sendbuf_ptr = sendbuf.mutable_unchecked<>().mutable_data(0);
    auto* recvbuf_ptr = recvbuf.mutable_unchecked<>().mutable_data(0);
    // allocate std::vector (to pass to the C++ function)
    std::vector<double*> inputBuf(sendbuf.size());
    std::vector<double*> outputBuf(recvbuf.size());

    for(size_t i=0; i<count; ++i){
        inputBuf[i] = sendbuf_ptr++;
        outputBuf[i] = recvbuf_ptr++;
    }

    // Configure AllreduceOptions struct and call allreduec function
    gloo::AllreduceOptions opts_(context);
    opts_.setInputs(inputBuf, 1);
    opts_.setOutputs(outputBuf, 1);
    opts_.setAlgorithm(gloo::AllreduceOptions::Algorithm::RING);
    gloo::ReduceOptions::Func fn = toFunction<double>(reduceop);
    opts_.setReduceFunction(fn);
    opts_.setTag(0);

    gloo::allreduce(opts_);
}

}// pygloo