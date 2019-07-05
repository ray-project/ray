// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Mehdi Goli    Codeplay Software Ltd.
// Ralph Potter  Codeplay Software Ltd.
// Luke Iwanski  Codeplay Software Ltd.
// Cummins Chris PhD student at The University of Edinburgh.
// Contact: <eigen@codeplay.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

/*****************************************************************
 * TensorSyclRun.h
 *
 * \brief:
 * Schedule_kernel invoke an specialised version of kernel struct. The
 * specialisation is based on the data dimension in sycl buffer
 *
*****************************************************************/

#ifndef UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_SYCLRUN_HPP
#define UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_SYCLRUN_HPP

namespace Eigen {
namespace TensorSycl {
template<typename Expr, typename FunctorExpr, typename TupleType > struct ExecExprFunctorKernel{
  typedef  typename internal::createPlaceHolderExpression<Expr>::Type PlaceHolderExpr;

  typedef typename Expr::Index Index;
  FunctorExpr functors;
  TupleType tuple_of_accessors;
  Index range;
  ExecExprFunctorKernel(Index range_, FunctorExpr functors_, TupleType tuple_of_accessors_)
    : functors(functors_), tuple_of_accessors(tuple_of_accessors_), range(range_){}
  void operator()(cl::sycl::nd_item<1> itemID) {
    typedef  typename internal::ConvertToDeviceExpression<Expr>::Type DevExpr;
    auto device_expr =internal::createDeviceExpression<DevExpr, PlaceHolderExpr>(functors, tuple_of_accessors);
    auto device_evaluator = Eigen::TensorEvaluator<decltype(device_expr.expr), Eigen::SyclKernelDevice>(device_expr.expr, Eigen::SyclKernelDevice());
    typename DevExpr::Index gId = static_cast<typename DevExpr::Index>(itemID.get_global_linear_id());
    if (gId < range)
      device_evaluator.evalScalar(gId);
  }
};

/// The run function in tensor sycl convert the expression tree to a buffer
/// based expression tree;
/// creates the expression tree for the device with accessor to buffers;
/// construct the kernel and submit it to the sycl queue.
/// std::array does not have TotalSize. So I have to get the size through template specialisation.
template<typename , typename Dimensions> struct DimensionSize{
  static auto getDimSize(const Dimensions& dim)->decltype(dim.TotalSize()){
    return dim.TotalSize();
  }
};
#define DIMSIZEMACRO(CVQual)\
template<typename Index, size_t NumDims> struct DimensionSize<Index, CVQual std::array<Index, NumDims>>{\
  static inline Index getDimSize(const std::array<Index, NumDims>& dim){\
    return (NumDims == 0) ? 1 : ::Eigen::internal::array_prod(dim);\
  }\
};

DIMSIZEMACRO(const)
DIMSIZEMACRO()
#undef DIMSIZEMACRO


template <typename Expr, typename Dev>
void run(Expr &expr, Dev &dev) {
  Eigen::TensorEvaluator<Expr, Dev> evaluator(expr, dev);
  const bool needs_assign = evaluator.evalSubExprsIfNeeded(NULL);
  if (needs_assign) {
    typedef Eigen::TensorSycl::internal::FunctorExtractor<Eigen::TensorEvaluator<Expr, Dev> > FunctorExpr;
    FunctorExpr functors = internal::extractFunctors(evaluator);
    dev.sycl_queue().submit([&](cl::sycl::handler &cgh) {
      // create a tuple of accessors from Evaluator
      typedef decltype(internal::createTupleOfAccessors<Eigen::TensorEvaluator<Expr, Dev> >(cgh, evaluator)) TupleType;
      TupleType tuple_of_accessors = internal::createTupleOfAccessors<Eigen::TensorEvaluator<Expr, Dev> >(cgh, evaluator);
      typename Expr::Index range, GRange, tileSize;
      typename Expr::Index total_size = static_cast<typename Expr::Index>(DimensionSize<typename Expr::Index, typename Eigen::TensorEvaluator<Expr, Dev>::Dimensions>::getDimSize(evaluator.dimensions()));
      dev.parallel_for_setup(total_size, tileSize, range, GRange);

      cgh.parallel_for(cl::sycl::nd_range<1>(cl::sycl::range<1>(GRange), cl::sycl::range<1>(tileSize)),
      ExecExprFunctorKernel<Expr,FunctorExpr,TupleType>(range
        , functors, tuple_of_accessors
      ));
    });
      dev.asynchronousExec();
  }
  evaluator.cleanup();
}
}  // namespace TensorSycl
}  // namespace Eigen

#endif  // UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_SYCLRUN_HPP
