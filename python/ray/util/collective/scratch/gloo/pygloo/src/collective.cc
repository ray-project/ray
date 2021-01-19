#include <condition_variable>

#include <gloo/math.h>
// #include <gloo/context.h>
#include <gloo/reduce.h>
#include <gloo/allreduce.h>
#include <gloo/allreduce_ring.h>
#include <collective.h>

enum class ReduceOp: std::uint8_t {
  SUM = 0,
  PRODUCT,
  MIN,
  MAX,
  BAND, // Bitwise AND
  BOR, // Bitwise OR
  BXOR, // Bitwise XOR
  UNUSED,
};

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



void mysum(void* c_, const void* a_, const void* b_, int n) {
  printf("n=%d\r\n", n);
  int* c = static_cast<int*>(c_);
  const int* a = static_cast<const int*>(a_);
  const int* b = static_cast<const int*>(b_);
  for (auto i = 0; i < n; i++) {
    printf("a[%d]=%d\r\n", i, a[i]);
    printf("b[%d]=%d\r\n", i, b[i]);
    c[i] = a[i] + b[i];
    printf("c[%d]=%d\r\n", i, c[i]);
  }
}


// wrap C++ function with NumPy array IO
// pybind11::array py_allreduce(const std::shared_ptr<gloo::Context>& context, pybind11::array_t<double, pybind11::array::c_style | pybind11::array::forcecast> array, ReduceOp reduceop = ReduceOp::SUM )
// {
//     // check input dimensions
//     if ( array.ndim()     != 2 )
//         throw std::runtime_error("Input should be 2-D NumPy array");
//     if ( array.shape()[1] != 2 )
//         throw std::runtime_error("Input should have size [N,2]");

//     // allocate std::vector (to pass to the C++ function)
//     std::vector<double*> inputArray(array.size());

//     // copy py::array -> std::vector
//     std::memcpy(inputArray.data(), array.data(), array.size()*sizeof(double));

//     // // call pure C++ function
//     // std::vector<double> result = length(inputArray);

//     // Configure AllreduceOptions struct and call allreduec function
//     gloo::AllreduceOptions opts_(context);
//     opts_.setInputs(inputArray, 1);
//     opts_.setOutputs(inputArray, 1);
//     opts_.setAlgorithm(gloo::AllreduceOptions::Algorithm::RING);
//     // void (*fn)(void*, const void*, const void*, int) = &mysum;
//     gloo::ReduceOptions::Func fn = toFunction(reduceop)
//     opts_.setReduceFunction(fn);
//     gloo::allreduce(opts_);

//     ssize_t ndim = 2;
//     std::vector<ssize_t> shape = {array.shape()[0], 3};
//     std::vector<ssize_t> strides = {sizeof(double) * 3, sizeof(double)};

//     // return 2-D NumPy array
//     return pybind11::array(pybind11::buffer_info(
//         result.data(),                           /* data as contiguous array  */
//         sizeof(double),                          /* size of one scalar        */
//         pybind11::format_descriptor<double>::format(), /* data type                 */
//         ndim,                                    /* number of dimensions      */
//         shape,                                   /* shape of the matrix       */
//         strides                                  /* strides for each axis     */
//     ));
// }