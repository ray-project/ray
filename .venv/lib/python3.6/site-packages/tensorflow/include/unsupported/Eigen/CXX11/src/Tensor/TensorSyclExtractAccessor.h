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
 * TensorSyclExtractAccessor.h
 *
 * \brief:
 * ExtractAccessor takes Expression placeHolder expression and the tuple of sycl
 * buffers as an input. Using pre-order tree traversal, ExtractAccessor
 * recursively calls itself for its children in the expression tree. The
 * leaf node in the PlaceHolder expression is nothing but a container preserving
 * the order of the actual data in the tuple of sycl buffer. By invoking the
 * extract accessor for the PlaceHolder<N>, an accessor is created for the Nth
 * buffer in the tuple of buffers. This accessor is then added as an Nth
 * element in the tuple of accessors. In this case we preserve the order of data
 * in the expression tree.
 *
 * This is the specialisation of extract accessor method for different operation
 * type in the PlaceHolder expression.
 *
*****************************************************************/

#ifndef UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_EXTRACT_ACCESSOR_HPP
#define UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_EXTRACT_ACCESSOR_HPP

namespace Eigen {
namespace TensorSycl {
namespace internal {
#define RETURN_CPP11(expr) ->decltype(expr) {return expr;}

/// struct ExtractAccessor: Extract Accessor Class is used to extract the
/// accessor from a buffer.
/// Depending on the type of the leaf node we can get a read accessor or a
/// read_write accessor
template <typename Evaluator>
struct ExtractAccessor;

struct AccessorConstructor{
  template<typename Arg> static inline auto getTuple(cl::sycl::handler& cgh, const Arg& eval)
  RETURN_CPP11(ExtractAccessor<Arg>::getTuple(cgh, eval))

  template<typename Arg1, typename Arg2> static inline auto getTuple(cl::sycl::handler& cgh, const Arg1& eval1, const Arg2& eval2)
  RETURN_CPP11(utility::tuple::append(ExtractAccessor<Arg1>::getTuple(cgh, eval1), ExtractAccessor<Arg2>::getTuple(cgh, eval2)))

  template<typename Arg1, typename Arg2, typename Arg3>	static inline auto getTuple(cl::sycl::handler& cgh, const Arg1& eval1 , const Arg2& eval2 , const Arg3& eval3)
  RETURN_CPP11(utility::tuple::append(ExtractAccessor<Arg1>::getTuple(cgh, eval1),utility::tuple::append(ExtractAccessor<Arg2>::getTuple(cgh, eval2), ExtractAccessor<Arg3>::getTuple(cgh, eval3))))

  template< cl::sycl::access::mode AcM, typename Arg> static inline auto getAccessor(cl::sycl::handler& cgh, const Arg& eval)
  RETURN_CPP11(utility::tuple::make_tuple(eval.device().template get_sycl_accessor<AcM>(cgh,eval.data())))
};

/// specialisation of the \ref ExtractAccessor struct when the node type is
///  TensorCwiseNullaryOp,  TensorCwiseUnaryOp and  TensorBroadcastingOp
#define SYCLUNARYCATEGORYEXTACC(CVQual)\
template <template<class, class> class UnaryCategory, typename OP, typename RHSExpr, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual UnaryCategory<OP, RHSExpr>, Dev> > {\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual UnaryCategory<OP, RHSExpr>, Dev>& eval)\
RETURN_CPP11(AccessorConstructor::getTuple(cgh, eval.impl()))\
};

SYCLUNARYCATEGORYEXTACC(const)
SYCLUNARYCATEGORYEXTACC()
#undef SYCLUNARYCATEGORYEXTACC


/// specialisation of the \ref ExtractAccessor struct when the node type is TensorCwiseBinaryOp
#define SYCLBINARYCATEGORYEXTACC(CVQual)\
template <template<class, class, class> class BinaryCategory, typename OP,  typename LHSExpr, typename RHSExpr, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual BinaryCategory<OP, LHSExpr, RHSExpr>, Dev> > {\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual BinaryCategory<OP, LHSExpr, RHSExpr>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::getTuple(cgh, eval.left_impl(), eval.right_impl()))\
};

SYCLBINARYCATEGORYEXTACC(const)
SYCLBINARYCATEGORYEXTACC()
#undef SYCLBINARYCATEGORYEXTACC

/// specialisation of the \ref ExtractAccessor struct when the node type is
/// const TensorCwiseTernaryOp
#define SYCLTERNARYCATEGORYEXTACC(CVQual)\
template <template<class, class, class, class> class TernaryCategory, typename OP, typename Arg1Expr, typename Arg2Expr, typename Arg3Expr, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual TernaryCategory<OP, Arg1Expr, Arg2Expr, Arg3Expr>, Dev> > {\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual TernaryCategory<OP, Arg1Expr, Arg2Expr, Arg3Expr>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::getTuple(cgh, eval.arg1Impl(), eval.arg2Impl(), eval.arg3Impl()))\
};

SYCLTERNARYCATEGORYEXTACC(const)
SYCLTERNARYCATEGORYEXTACC()
#undef SYCLTERNARYCATEGORYEXTACC


/// specialisation of the \ref ExtractAccessor struct when the node type is
/// TensorCwiseSelectOp. This is a special case where there is no OP
#define SYCLSELECTOPEXTACC(CVQual)\
template <typename IfExpr, typename ThenExpr, typename ElseExpr, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual TensorSelectOp<IfExpr, ThenExpr, ElseExpr>, Dev> > {\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual TensorSelectOp<IfExpr, ThenExpr, ElseExpr>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::getTuple(cgh, eval.cond_impl(), eval.then_impl(), eval.else_impl()))\
};

SYCLSELECTOPEXTACC(const)
SYCLSELECTOPEXTACC()
#undef SYCLSELECTOPEXTACC

/// specialisation of the \ref ExtractAccessor struct when the node type is TensorAssignOp
#define SYCLTENSORASSIGNOPEXTACC(CVQual)\
template <typename LHSExpr, typename RHSExpr, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual TensorAssignOp<LHSExpr, RHSExpr>, Dev> > {\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual TensorAssignOp<LHSExpr, RHSExpr>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::getTuple(cgh, eval.left_impl(), eval.right_impl()))\
};

 SYCLTENSORASSIGNOPEXTACC(const)
 SYCLTENSORASSIGNOPEXTACC()
 #undef SYCLTENSORASSIGNOPEXTACC

/// specialisation of the \ref ExtractAccessor struct when the node type is const TensorMap
#define TENSORMAPEXPR(CVQual, ACCType)\
template <typename PlainObjectType, int Options_, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual TensorMap<PlainObjectType, Options_>, Dev> > {\
  static inline auto getTuple(cl::sycl::handler& cgh,const TensorEvaluator<CVQual TensorMap<PlainObjectType, Options_>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::template getAccessor<ACCType>(cgh, eval))\
};

TENSORMAPEXPR(const, cl::sycl::access::mode::read)
TENSORMAPEXPR(, cl::sycl::access::mode::read_write)
#undef TENSORMAPEXPR

/// specialisation of the \ref ExtractAccessor struct when the node type is TensorForcedEvalOp
#define SYCLFORCEDEVALEXTACC(CVQual)\
template <typename Expr, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual TensorForcedEvalOp<Expr>, Dev> > {\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual TensorForcedEvalOp<Expr>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::template getAccessor<cl::sycl::access::mode::read>(cgh, eval))\
};

SYCLFORCEDEVALEXTACC(const)
SYCLFORCEDEVALEXTACC()
#undef SYCLFORCEDEVALEXTACC

//TensorCustomUnaryOp
#define SYCLCUSTOMUNARYOPEXTACC(CVQual)\
template <typename CustomUnaryFunc, typename XprType, typename Dev >\
struct ExtractAccessor<TensorEvaluator<CVQual TensorCustomUnaryOp<CustomUnaryFunc, XprType>, Dev> > {\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual TensorCustomUnaryOp<CustomUnaryFunc, XprType>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::template getAccessor<cl::sycl::access::mode::read>(cgh, eval))\
};


SYCLCUSTOMUNARYOPEXTACC(const)
SYCLCUSTOMUNARYOPEXTACC()
#undef SYCLCUSTOMUNARYOPEXTACC

//TensorCustomBinaryOp
#define SYCLCUSTOMBINARYOPEXTACC(CVQual)\
template <typename CustomBinaryFunc, typename LhsXprType, typename RhsXprType , typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual TensorCustomBinaryOp<CustomBinaryFunc, LhsXprType, RhsXprType>, Dev> > {\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual TensorCustomBinaryOp<CustomBinaryFunc, LhsXprType, RhsXprType>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::template getAccessor<cl::sycl::access::mode::read>(cgh, eval))\
};

SYCLCUSTOMBINARYOPEXTACC(const)
SYCLCUSTOMBINARYOPEXTACC()
#undef SYCLCUSTOMBIBARYOPEXTACC

/// specialisation of the \ref ExtractAccessor struct when the node type is TensorEvalToOp
#define SYCLEVALTOEXTACC(CVQual)\
template <typename Expr, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual TensorEvalToOp<Expr>, Dev> > {\
  static inline auto getTuple(cl::sycl::handler& cgh,const TensorEvaluator<CVQual TensorEvalToOp<Expr>, Dev>& eval)\
  RETURN_CPP11(utility::tuple::append(AccessorConstructor::template getAccessor<cl::sycl::access::mode::write>(cgh, eval), AccessorConstructor::getTuple(cgh, eval.impl())))\
};

SYCLEVALTOEXTACC(const)
SYCLEVALTOEXTACC()
#undef SYCLEVALTOEXTACC

/// specialisation of the \ref ExtractAccessor struct when the node type is TensorReductionOp
#define SYCLREDUCTIONEXTACC(CVQual, ExprNode)\
template <typename OP, typename Dim, typename Expr, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual ExprNode<OP, Dim, Expr>, Dev> > {\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual ExprNode<OP, Dim, Expr>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::template getAccessor<cl::sycl::access::mode::read>(cgh, eval))\
};
// TensorReductionOp
SYCLREDUCTIONEXTACC(const,TensorReductionOp)
SYCLREDUCTIONEXTACC(,TensorReductionOp)

// TensorTupleReducerOp
SYCLREDUCTIONEXTACC(const,TensorTupleReducerOp)
SYCLREDUCTIONEXTACC(,TensorTupleReducerOp)
#undef SYCLREDUCTIONEXTACC

/// specialisation of the \ref ExtractAccessor struct when the node type is TensorContractionOp and TensorConvolutionOp
#define SYCLCONTRACTIONCONVOLUTIONEXTACC(CVQual, ExprNode)\
template<typename Indices, typename LhsXprType, typename RhsXprType, typename Dev>\
 struct ExtractAccessor<TensorEvaluator<CVQual ExprNode<Indices, LhsXprType, RhsXprType>, Dev> > {\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual ExprNode<Indices, LhsXprType, RhsXprType>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::template getAccessor<cl::sycl::access::mode::read>(cgh, eval))\
};
//TensorContractionOp
SYCLCONTRACTIONCONVOLUTIONEXTACC(const,TensorContractionOp)
SYCLCONTRACTIONCONVOLUTIONEXTACC(,TensorContractionOp)
//TensorConvolutionOp
SYCLCONTRACTIONCONVOLUTIONEXTACC(const,TensorConvolutionOp)
SYCLCONTRACTIONCONVOLUTIONEXTACC(,TensorConvolutionOp)
#undef SYCLCONTRACTIONCONVOLUTIONEXTACC

/// specialisation of the \ref ExtractAccessor struct when the node type is
/// const TensorSlicingOp.
#define SYCLSLICEOPEXTACC(CVQual)\
template <typename StartIndices, typename Sizes, typename XprType, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual TensorSlicingOp<StartIndices, Sizes, XprType>, Dev> > {\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual TensorSlicingOp<StartIndices, Sizes, XprType>, Dev>& eval)\
  RETURN_CPP11( AccessorConstructor::getTuple(cgh, eval.impl()))\
};

SYCLSLICEOPEXTACC(const)
SYCLSLICEOPEXTACC()
#undef SYCLSLICEOPEXTACC
// specialisation of the \ref ExtractAccessor struct when the node type is
///  TensorStridingSlicingOp.
#define SYCLSLICESTRIDEOPEXTACC(CVQual)\
template<typename StartIndices, typename StopIndices, typename Strides, typename XprType, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual TensorStridingSlicingOp<StartIndices, StopIndices, Strides, XprType>, Dev> >{\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual TensorStridingSlicingOp<StartIndices, StopIndices, Strides, XprType>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::getTuple(cgh, eval.impl()))\
};

SYCLSLICESTRIDEOPEXTACC(const)
SYCLSLICESTRIDEOPEXTACC()
#undef SYCLSLICESTRIDEOPEXTACC

// specialisation of the \ref ExtractAccessor struct when the node type is
/// TensorChippingOp.
#define SYCLTENSORCHIPPINGOPEXTACC(CVQual)\
template<DenseIndex DimId, typename XprType, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual TensorChippingOp<DimId, XprType>, Dev> >{\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual TensorChippingOp<DimId, XprType>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::getTuple(cgh, eval.impl()))\
};

SYCLTENSORCHIPPINGOPEXTACC(const)
SYCLTENSORCHIPPINGOPEXTACC()
#undef SYCLTENSORCHIPPINGOPEXTACC

// specialisation of the \ref ExtractAccessor struct when the node type is
/// TensorImagePatchOp.
#define SYCLTENSORIMAGEPATCHOPEXTACC(CVQual)\
template<DenseIndex Rows, DenseIndex Cols, typename XprType, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual TensorImagePatchOp<Rows, Cols, XprType>, Dev> >{\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual TensorImagePatchOp<Rows, Cols, XprType>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::getTuple(cgh, eval.impl()))\
};

SYCLTENSORIMAGEPATCHOPEXTACC(const)
SYCLTENSORIMAGEPATCHOPEXTACC()
#undef SYCLTENSORIMAGEPATCHOPEXTACC

// specialisation of the \ref ExtractAccessor struct when the node type is
/// TensorVolumePatchOp.
#define SYCLTENSORVOLUMEPATCHOPEXTACC(CVQual)\
template<DenseIndex Planes, DenseIndex Rows, DenseIndex Cols, typename XprType, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual TensorVolumePatchOp<Planes, Rows, Cols, XprType>, Dev> >{\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual TensorVolumePatchOp<Planes, Rows, Cols, XprType>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::getTuple(cgh, eval.impl()))\
};

SYCLTENSORVOLUMEPATCHOPEXTACC(const)
SYCLTENSORVOLUMEPATCHOPEXTACC()
#undef SYCLTENSORVOLUMEPATCHOPEXTACC

// specialisation of the \ref ExtractAccessor struct when the node type is
/// TensorLayoutSwapOp, TensorIndexTupleOp
#define SYCLTENSORLAYOUTSWAPINDEXTUPLEOPEXTACC(CVQual, ExprNode)\
template<typename XprType, typename Dev>\
struct ExtractAccessor<TensorEvaluator<CVQual ExprNode<XprType>, Dev> >{\
  static inline auto getTuple(cl::sycl::handler& cgh, const TensorEvaluator<CVQual ExprNode<XprType>, Dev>& eval)\
  RETURN_CPP11(AccessorConstructor::getTuple(cgh, eval.impl()))\
};

// TensorLayoutSwapOp
SYCLTENSORLAYOUTSWAPINDEXTUPLEOPEXTACC(const,TensorLayoutSwapOp)
SYCLTENSORLAYOUTSWAPINDEXTUPLEOPEXTACC(,TensorLayoutSwapOp)
//TensorIndexTupleOp
SYCLTENSORLAYOUTSWAPINDEXTUPLEOPEXTACC(const,TensorIndexTupleOp)
SYCLTENSORLAYOUTSWAPINDEXTUPLEOPEXTACC(,TensorIndexTupleOp)

#undef SYCLTENSORLAYOUTSWAPINDEXTUPLEOPEXTACC

/// template deduction for \ref ExtractAccessor
template <typename Evaluator>
auto createTupleOfAccessors(cl::sycl::handler& cgh, const Evaluator& eval)
-> decltype(ExtractAccessor<Evaluator>::getTuple(cgh, eval)) {
  return ExtractAccessor<Evaluator>::getTuple(cgh, eval);
}

} /// namespace TensorSycl
} /// namespace internal
} /// namespace Eigen
#endif  // UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_EXTRACT_ACCESSOR_HPP
