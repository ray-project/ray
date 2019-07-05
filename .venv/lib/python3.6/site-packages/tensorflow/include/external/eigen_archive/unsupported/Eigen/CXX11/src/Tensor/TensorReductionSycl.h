// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Mehdi Goli    Codeplay Software Ltd.
// Ralph Potter  Codeplay Software Ltd.
// Luke Iwanski  Codeplay Software Ltd.
// Contact: <eigen@codeplay.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

/*****************************************************************
 * TensorSyclPlaceHolderExpr.h
 *
 * \brief:
 *  This is the specialisation of the placeholder expression based on the
 * operation type
 *
*****************************************************************/

#ifndef UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSOR_REDUCTION_SYCL_HPP
#define UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSOR_REDUCTION_SYCL_HPP

namespace Eigen {
namespace internal {

template<typename OP, typename CoeffReturnType> struct syclGenericBufferReducer{
template<typename BufferTOut, typename BufferTIn>
static void run(OP op, BufferTOut& bufOut, ptrdiff_t out_offset, BufferTIn& bufI, const Eigen::SyclDevice& dev, size_t length, size_t local){
  do {
          auto f = [length, local, op, out_offset, &bufOut, &bufI](cl::sycl::handler& h) mutable {
            cl::sycl::nd_range<1> r{cl::sycl::range<1>{std::max(length, local)},
                                    cl::sycl::range<1>{std::min(length, local)}};
            /* Two accessors are used: one to the buffer that is being reduced,
             * and a second to local memory, used to store intermediate data. */
            auto aI =bufI.template get_access<cl::sycl::access::mode::read_write>(h);
            auto aOut =bufOut.template get_access<cl::sycl::access::mode::write>(h);
            typedef decltype(aI) InputAccessor;
            typedef decltype(aOut) OutputAccessor;
            typedef cl::sycl::accessor<CoeffReturnType, 1, cl::sycl::access::mode::read_write,cl::sycl::access::target::local> LocalAccessor;
            LocalAccessor scratch(cl::sycl::range<1>(local), h);

            /* The parallel_for invocation chosen is the variant with an nd_item
             * parameter, since the code requires barriers for correctness. */
            h.parallel_for(r, TensorSycl::internal::GenericKernelReducer<CoeffReturnType, OP, OutputAccessor, InputAccessor, LocalAccessor>(op, aOut, out_offset, aI, scratch,  length, local));
          };
            dev.sycl_queue().submit(f);
            dev.asynchronousExec();

          /* At this point, you could queue::wait_and_throw() to ensure that
           * errors are caught quickly. However, this would likely impact
           * performance negatively. */
          length = length / local;

        } while (length > 1);
}

};

template<typename CoeffReturnType> struct syclGenericBufferReducer<Eigen::internal::MeanReducer<CoeffReturnType>, CoeffReturnType>{
template<typename BufferTOut, typename BufferTIn>
static void run(Eigen::internal::MeanReducer<CoeffReturnType>, BufferTOut& bufOut,ptrdiff_t out_offset, BufferTIn& bufI, const Eigen::SyclDevice& dev, size_t length, size_t local){
   syclGenericBufferReducer<Eigen::internal::SumReducer<CoeffReturnType>, CoeffReturnType>::run(Eigen::internal::SumReducer<CoeffReturnType>(),
    bufOut, out_offset, bufI, dev, length, local);
}
};

/// Self is useless here because in expression construction we are going to treat reduction as a leafnode.
/// we want to take reduction child and then build a construction and apply the full reducer function on it. Fullreducre applies the
/// reduction operation on the child of the reduction. once it is done the reduction is an empty shell and can be thrown away and treated as
// a leafNode.

template <typename Self, typename Op, bool Vectorizable>
struct FullReducer<Self, Op, const Eigen::SyclDevice, Vectorizable> {

  typedef typename Self::CoeffReturnType CoeffReturnType;
  static const bool HasOptimizedImplementation = false;

  static void run(const Self& self, Op& reducer, const Eigen::SyclDevice& dev, CoeffReturnType* output) {
    typedef const typename Self::ChildType HostExpr; /// this is the child of reduction
    typedef Eigen::TensorSycl::internal::FunctorExtractor<TensorEvaluator<HostExpr, const Eigen::SyclDevice> > FunctorExpr;
    FunctorExpr functors = TensorSycl::internal::extractFunctors(self.impl());
    int red_factor =256; /// initial reduction. If the size is less than red_factor we only creates one thread.
    size_t inputSize =self.impl().dimensions().TotalSize();
    size_t rng = inputSize/red_factor; // the total number of thread initially is half the size of the input
    size_t remaining = inputSize% red_factor;
    if(rng ==0) {
      red_factor=1;
    };
    size_t tileSize =dev.sycl_queue().get_device(). template get_info<cl::sycl::info::device::max_work_group_size>()/2;
    size_t GRange=std::max((size_t )1, rng);

    // convert global range to power of 2 for redecution
    GRange--;
    GRange |= GRange >> 1;
    GRange |= GRange >> 2;
    GRange |= GRange >> 4;
    GRange |= GRange >> 8;
    GRange |= GRange >> 16;
#if __x86_64__ || __ppc64__ || _WIN64
    GRange |= GRange >> 32;
#endif
    GRange++;
    size_t  outTileSize = tileSize;
    /// if the shared memory is less than the GRange, we set shared_mem size to the TotalSize and in this case one kernel would be created for recursion to reduce all to one.
    if (GRange < outTileSize) outTileSize=GRange;
    /// creating the shared memory for calculating reduction.
    /// This one is used to collect all the reduced value of shared memory as we don't have global barrier on GPU. Once it is saved we can
    /// recursively apply reduction on it in order to reduce the whole.
    auto temp_global_buffer =cl::sycl::buffer<CoeffReturnType, 1>(cl::sycl::range<1>(GRange));
    typedef typename Eigen::internal::remove_all<decltype(self.xprDims())>::type Dims;
  //  Dims dims= self.xprDims();
    //Op functor = reducer;
    dev.sycl_queue().submit([&](cl::sycl::handler &cgh) {
      // this is a workaround for gcc 4.8 bug
      typedef decltype(TensorSycl::internal::createTupleOfAccessors(cgh, self.impl())) TupleType;
      // create a tuple of accessors from Evaluator
      TupleType tuple_of_accessors =  TensorSycl::internal::createTupleOfAccessors(cgh, self.impl());
      auto tmp_global_accessor = temp_global_buffer. template get_access<cl::sycl::access::mode::read_write, cl::sycl::access::target::global_buffer>(cgh);
      typedef decltype(tmp_global_accessor) OutAccessor;
      cgh.parallel_for( cl::sycl::nd_range<1>(cl::sycl::range<1>(GRange), cl::sycl::range<1>(outTileSize)),
        TensorSycl::internal::FullReductionKernelFunctor<CoeffReturnType, OutAccessor, HostExpr, FunctorExpr, Op, Dims, size_t, TupleType>
       (tmp_global_accessor, rng, remaining, red_factor, reducer, self.xprDims(), functors,  tuple_of_accessors));
    });
    dev.asynchronousExec();

    // getting final out buffer at the moment the created buffer is true because there is no need for assign
    auto out_buffer =dev.get_sycl_buffer(output);
    ptrdiff_t out_offset = dev.get_offset(output);
    /// This is used to recursively reduce the tmp value to an element of 1;
    syclGenericBufferReducer<Op, CoeffReturnType>::run(reducer, out_buffer, out_offset, temp_global_buffer,dev, GRange,  outTileSize);
  }

};


template <typename Self, typename Op>
struct InnerReducer<Self, Op, const Eigen::SyclDevice> {

  typedef typename Self::CoeffReturnType CoeffReturnType;
  static const bool HasOptimizedImplementation = false;

  static bool run(const Self& self, Op& reducer, const Eigen::SyclDevice& dev, CoeffReturnType* output, typename Self::Index num_values_to_reduce, typename Self::Index num_coeffs_to_preserve) {
    typedef const typename Self::ChildType HostExpr; /// this is the child of reduction
    typedef Eigen::TensorSycl::internal::FunctorExtractor<TensorEvaluator<HostExpr, const Eigen::SyclDevice> > FunctorExpr;
    FunctorExpr functors = TensorSycl::internal::extractFunctors(self.impl());
    typename Self::Index range, GRange, tileSize;
    typedef typename Eigen::internal::remove_all<decltype(self.xprDims())>::type Dims;

    // getting final out buffer at the moment the created buffer is true because there is no need for assign
    /// creating the shared memory for calculating reduction.
    /// This one is used to collect all the reduced value of shared memory as we don't have global barrier on GPU. Once it is saved we can
    /// recursively apply reduction on it in order to reduce the whole.
      dev.parallel_for_setup(num_coeffs_to_preserve, tileSize, range, GRange);
      dev.sycl_queue().submit([&](cl::sycl::handler &cgh) {
      // this is workaround for gcc 4.8 bug.
      typedef decltype(TensorSycl::internal::createTupleOfAccessors(cgh, self.impl())) Tuple_of_Acc;
      // create a tuple of accessors from Evaluator
      Tuple_of_Acc tuple_of_accessors =  TensorSycl::internal::createTupleOfAccessors(cgh, self.impl());
      auto output_accessor = dev.template get_sycl_accessor<cl::sycl::access::mode::write>(cgh, output);
      ptrdiff_t out_offset = dev.get_offset(output);
      Index red_size = (num_values_to_reduce!=0)? num_values_to_reduce : static_cast<Index>(1);
      cgh.parallel_for( cl::sycl::nd_range<1>(cl::sycl::range<1>(GRange), cl::sycl::range<1>(tileSize)),
      TensorSycl::internal::ReductionFunctor<HostExpr, FunctorExpr, Tuple_of_Acc, Dims, Op, typename Self::Index>
      (output_accessor, out_offset, functors, tuple_of_accessors, self.xprDims(), reducer, range, red_size));

    });
    dev.asynchronousExec();
    return false;
  }
};

}  // end namespace internal
}  // namespace Eigen

#endif  // UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSOR_REDUCTION_SYCL_HPP
