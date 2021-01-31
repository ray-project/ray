#include <collective.h>
#include <gloo/algorithm.h>
#include <gloo/reduce.h>
#include <gloo/reduce_scatter.h>

namespace pygloo {

template <typename T>
const gloo::ReductionFunction<T> *getReductionFunction(ReduceOp reduceop) {
  switch (reduceop) {
  case ReduceOp::SUM:
    return gloo::ReductionFunction<T>::sum;
    break;
  case ReduceOp::PRODUCT:
    return gloo::ReductionFunction<T>::product;
    break;
  case ReduceOp::MIN:
    return gloo::ReductionFunction<T>::min;
    break;
  case ReduceOp::MAX:
    return gloo::ReductionFunction<T>::max;
    break;
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

template <typename T>
void reduce_scatter(const std::shared_ptr<gloo::Context> &context,
                    intptr_t sendbuf, intptr_t recvbuf, size_t size,
                    std::vector<int> recvElems, ReduceOp reduceop) {
  T *input_ptr = reinterpret_cast<T *>(sendbuf);

  std::vector<T> inputbuf(size);

  memcpy(inputbuf.data(), input_ptr, size * sizeof(T));

  std::vector<T *> dataPtrs{inputbuf.data()};

  const gloo::ReductionFunction<T> *fn = getReductionFunction<T>(reduceop);

  gloo::ReduceScatterHalvingDoubling<T> algorithm(context, dataPtrs, size,
                                                  recvElems, fn);
  algorithm.run();

  memcpy(reinterpret_cast<T *>(recvbuf), inputbuf.data(), recvElems[context->rank]*sizeof(T));
}

void reduce_scatter_wrapper(const std::shared_ptr<gloo::Context> &context,
                            intptr_t sendbuf, intptr_t recvbuf, size_t size,
                            std::vector<int> recvElems, glooDataType_t datatype,
                            ReduceOp reduceop) {
  switch (datatype) {
  case glooDataType_t::glooInt8:
    reduce_scatter<int8_t>(context, sendbuf, recvbuf, size, recvElems,
                           reduceop);
    break;
  case glooDataType_t::glooUint8:
    reduce_scatter<uint8_t>(context, sendbuf, recvbuf, size, recvElems,
                            reduceop);
    break;
  case glooDataType_t::glooInt32:
    reduce_scatter<int32_t>(context, sendbuf, recvbuf, size, recvElems,
                            reduceop);
    break;
  case glooDataType_t::glooUint32:
    reduce_scatter<uint32_t>(context, sendbuf, recvbuf, size, recvElems,
                             reduceop);
    break;
  case glooDataType_t::glooInt64:
    reduce_scatter<int64_t>(context, sendbuf, recvbuf, size, recvElems,
                            reduceop);
    break;
  case glooDataType_t::glooUint64:
    reduce_scatter<uint64_t>(context, sendbuf, recvbuf, size, recvElems,
                             reduceop);
    break;
  case glooDataType_t::glooFloat16:
    reduce_scatter<gloo::float16>(context, sendbuf, recvbuf, size, recvElems,
                                  reduceop);
    break;
  case glooDataType_t::glooFloat32:
    reduce_scatter<float_t>(context, sendbuf, recvbuf, size, recvElems,
                            reduceop);
    break;
  case glooDataType_t::glooFloat64:
    reduce_scatter<double_t>(context, sendbuf, recvbuf, size, recvElems,
                             reduceop);
    break;
  default:
    throw std::runtime_error("Unhandled dataType");
  }
}

} // namespace pygloo