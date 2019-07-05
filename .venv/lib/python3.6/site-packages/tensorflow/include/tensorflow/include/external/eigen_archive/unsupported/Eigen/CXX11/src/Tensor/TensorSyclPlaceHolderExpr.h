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

#ifndef UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_PLACEHOLDER_EXPR_HPP
#define UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_PLACEHOLDER_EXPR_HPP

namespace Eigen {
namespace TensorSycl {
namespace internal {

/// \struct PlaceHolder
/// \brief PlaceHolder is used to replace the \ref TensorMap in the expression
/// tree.
/// PlaceHolder contains the order of the leaf node in the expression tree.
template <typename Scalar, size_t N>
struct PlaceHolder {
  static constexpr size_t I = N;
  typedef Scalar Type;
};

/// \sttruct PlaceHolderExpression
/// \brief it is used to create the PlaceHolder expression. The PlaceHolder
/// expression is a copy of expression type in which the TensorMap of the has
/// been replaced with PlaceHolder.
template <typename Expr, size_t N>
struct PlaceHolderExpression;

template<size_t N, typename... Args>
struct CalculateIndex;

template<size_t N, typename Arg>
struct CalculateIndex<N, Arg>{
  typedef typename PlaceHolderExpression<Arg, N>::Type ArgType;
  typedef utility::tuple::Tuple<ArgType> ArgsTuple;
};

template<size_t N, typename Arg1, typename Arg2>
struct CalculateIndex<N, Arg1, Arg2>{
  static const size_t Arg2LeafCount = LeafCount<Arg2>::Count;
  typedef typename PlaceHolderExpression<Arg1, N - Arg2LeafCount>::Type Arg1Type;
  typedef typename PlaceHolderExpression<Arg2, N>::Type Arg2Type;
  typedef utility::tuple::Tuple<Arg1Type, Arg2Type> ArgsTuple;
};

template<size_t N, typename Arg1, typename Arg2, typename Arg3>
struct CalculateIndex<N, Arg1, Arg2, Arg3> {
  static const size_t Arg3LeafCount = LeafCount<Arg3>::Count;
  static const size_t Arg2LeafCount = LeafCount<Arg2>::Count;
  typedef typename PlaceHolderExpression<Arg1, N - Arg3LeafCount - Arg2LeafCount>::Type Arg1Type;
  typedef typename PlaceHolderExpression<Arg2, N - Arg3LeafCount>::Type Arg2Type;
  typedef typename PlaceHolderExpression<Arg3, N>::Type Arg3Type;
  typedef utility::tuple::Tuple<Arg1Type, Arg2Type, Arg3Type> ArgsTuple;
};

template<template<class...> class Category , class OP, class TPL>
struct CategoryHelper;

template<template<class...> class Category , class OP, class ...T >
struct CategoryHelper<Category, OP, utility::tuple::Tuple<T...> > {
  typedef Category<OP, T... > Type;
};

template<template<class...> class Category , class ...T >
struct CategoryHelper<Category, NoOP, utility::tuple::Tuple<T...> > {
  typedef Category<T... > Type;
};

/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensorCwiseNullaryOp, TensorCwiseUnaryOp, TensorBroadcastingOp, TensorCwiseBinaryOp,  TensorCwiseTernaryOp
#define OPEXPRCATEGORY(CVQual)\
template <template <class, class... > class Category, typename OP, typename... SubExpr, size_t N>\
struct PlaceHolderExpression<CVQual Category<OP, SubExpr...>, N>{\
  typedef CVQual typename CategoryHelper<Category, OP, typename CalculateIndex<N, SubExpr...>::ArgsTuple>::Type Type;\
};

OPEXPRCATEGORY(const)
OPEXPRCATEGORY()
#undef OPEXPRCATEGORY

/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensorCwiseSelectOp
#define SELECTEXPR(CVQual)\
template <typename IfExpr, typename ThenExpr, typename ElseExpr, size_t N>\
struct PlaceHolderExpression<CVQual TensorSelectOp<IfExpr, ThenExpr, ElseExpr>, N> {\
  typedef CVQual typename CategoryHelper<TensorSelectOp, NoOP, typename CalculateIndex<N, IfExpr, ThenExpr, ElseExpr>::ArgsTuple>::Type Type;\
};

SELECTEXPR(const)
SELECTEXPR()
#undef SELECTEXPR

/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensorAssignOp
#define ASSIGNEXPR(CVQual)\
template <typename LHSExpr, typename RHSExpr, size_t N>\
struct PlaceHolderExpression<CVQual TensorAssignOp<LHSExpr, RHSExpr>, N> {\
  typedef CVQual typename CategoryHelper<TensorAssignOp, NoOP, typename CalculateIndex<N, LHSExpr, RHSExpr>::ArgsTuple>::Type Type;\
};

ASSIGNEXPR(const)
ASSIGNEXPR()
#undef ASSIGNEXPR

/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensorMap
#define TENSORMAPEXPR(CVQual)\
template <typename T, int Options_, template <class> class MakePointer_, size_t N>\
struct PlaceHolderExpression< CVQual TensorMap< T, Options_, MakePointer_>, N> {\
  typedef CVQual PlaceHolder<CVQual TensorMap<T, Options_, MakePointer_>, N> Type;\
};

TENSORMAPEXPR(const)
TENSORMAPEXPR()
#undef TENSORMAPEXPR

/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensorForcedEvalOp
#define FORCEDEVAL(CVQual)\
template <typename Expr, size_t N>\
struct PlaceHolderExpression<CVQual TensorForcedEvalOp<Expr>, N> {\
  typedef CVQual PlaceHolder<CVQual TensorForcedEvalOp<Expr>, N> Type;\
};

FORCEDEVAL(const)
FORCEDEVAL()
#undef FORCEDEVAL


/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensorForcedEvalOp
#define CUSTOMUNARYOPEVAL(CVQual)\
template <typename CustomUnaryFunc, typename XprType, size_t N>\
struct PlaceHolderExpression<CVQual TensorCustomUnaryOp<CustomUnaryFunc, XprType>, N> {\
  typedef CVQual PlaceHolder<CVQual TensorCustomUnaryOp<CustomUnaryFunc, XprType>, N> Type;\
};

CUSTOMUNARYOPEVAL(const)
CUSTOMUNARYOPEVAL()
#undef CUSTOMUNARYOPEVAL


/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensorForcedEvalOp
#define CUSTOMBINARYOPEVAL(CVQual)\
template <typename CustomBinaryFunc, typename LhsXprType, typename RhsXprType, size_t N>\
struct PlaceHolderExpression<CVQual TensorCustomBinaryOp<CustomBinaryFunc, LhsXprType, RhsXprType>, N> {\
  typedef CVQual PlaceHolder<CVQual TensorCustomBinaryOp<CustomBinaryFunc, LhsXprType, RhsXprType>, N> Type;\
};

CUSTOMBINARYOPEVAL(const)
CUSTOMBINARYOPEVAL()
#undef CUSTOMBINARYOPEVAL


/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensoroOp, TensorLayoutSwapOp, and TensorIndexTupleOp
#define EVALTOLAYOUTSWAPINDEXTUPLE(CVQual, ExprNode)\
template <typename Expr, size_t N>\
struct PlaceHolderExpression<CVQual ExprNode<Expr>, N> {\
  typedef CVQual ExprNode<typename CalculateIndex <N, Expr>::ArgType> Type;\
};

// TensorEvalToOp
EVALTOLAYOUTSWAPINDEXTUPLE(const, TensorEvalToOp)
EVALTOLAYOUTSWAPINDEXTUPLE(, TensorEvalToOp)
//TensorLayoutSwapOp
EVALTOLAYOUTSWAPINDEXTUPLE(const, TensorLayoutSwapOp)
EVALTOLAYOUTSWAPINDEXTUPLE(, TensorLayoutSwapOp)
//TensorIndexTupleOp
EVALTOLAYOUTSWAPINDEXTUPLE(const, TensorIndexTupleOp)
EVALTOLAYOUTSWAPINDEXTUPLE(, TensorIndexTupleOp)

#undef EVALTOLAYOUTSWAPINDEXTUPLE


/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensorChippingOp
#define CHIPPINGOP(CVQual)\
template <DenseIndex DimId, typename Expr, size_t N>\
struct PlaceHolderExpression<CVQual TensorChippingOp<DimId, Expr>, N> {\
  typedef CVQual TensorChippingOp< DimId, typename CalculateIndex <N, Expr>::ArgType> Type;\
};

CHIPPINGOP(const)
CHIPPINGOP()
#undef CHIPPINGOP

/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensorReductionOp and TensorTupleReducerOp (Argmax)
#define SYCLREDUCTION(CVQual, ExprNode)\
template <typename OP, typename Dims, typename Expr, size_t N>\
struct PlaceHolderExpression<CVQual ExprNode<OP, Dims, Expr>, N>{\
  typedef CVQual PlaceHolder<CVQual ExprNode<OP, Dims,Expr>, N> Type;\
};

// tensor reduction
SYCLREDUCTION(const, TensorReductionOp)
SYCLREDUCTION(, TensorReductionOp)

// tensor Argmax -TensorTupleReducerOp
SYCLREDUCTION(const, TensorTupleReducerOp)
SYCLREDUCTION(, TensorTupleReducerOp)
#undef SYCLREDUCTION



/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensorReductionOp
#define SYCLCONTRACTIONCONVOLUTIONPLH(CVQual, ExprNode)\
template <typename Indices, typename LhsXprType, typename RhsXprType, size_t N>\
struct PlaceHolderExpression<CVQual ExprNode<Indices, LhsXprType, RhsXprType>, N>{\
  typedef CVQual PlaceHolder<CVQual ExprNode<Indices, LhsXprType, RhsXprType>, N> Type;\
};
SYCLCONTRACTIONCONVOLUTIONPLH(const, TensorContractionOp)
SYCLCONTRACTIONCONVOLUTIONPLH(,TensorContractionOp)
SYCLCONTRACTIONCONVOLUTIONPLH(const, TensorConvolutionOp)
SYCLCONTRACTIONCONVOLUTIONPLH(,TensorConvolutionOp)
#undef SYCLCONTRACTIONCONVOLUTIONPLH


/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensorCwiseSelectOp
#define SLICEOPEXPR(CVQual)\
template <typename StartIndices, typename Sizes, typename XprType, size_t N>\
struct PlaceHolderExpression<CVQual TensorSlicingOp<StartIndices, Sizes, XprType>, N> {\
  typedef CVQual TensorSlicingOp<StartIndices, Sizes, typename CalculateIndex<N, XprType>::ArgType> Type;\
};

SLICEOPEXPR(const)
SLICEOPEXPR()
#undef SLICEOPEXPR


#define SYCLSLICESTRIDEOPPLH(CVQual)\
template<typename StartIndices, typename StopIndices, typename Strides, typename XprType, size_t N>\
struct PlaceHolderExpression<CVQual TensorStridingSlicingOp<StartIndices, StopIndices, Strides, XprType>, N> {\
  typedef CVQual TensorStridingSlicingOp<StartIndices, StopIndices, Strides, typename CalculateIndex<N, XprType>::ArgType> Type;\
};

SYCLSLICESTRIDEOPPLH(const)
SYCLSLICESTRIDEOPPLH()
#undef SYCLSLICESTRIDEOPPLH



/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensorImagePatchOp
#define SYCLTENSORIMAGEPATCHOP(CVQual)\
template<DenseIndex Rows, DenseIndex Cols, typename XprType, size_t N>\
struct PlaceHolderExpression<CVQual TensorImagePatchOp<Rows, Cols, XprType>, N> {\
  typedef CVQual TensorImagePatchOp<Rows, Cols, typename CalculateIndex <N, XprType>::ArgType> Type;\
};

SYCLTENSORIMAGEPATCHOP(const)
SYCLTENSORIMAGEPATCHOP()
#undef SYCLTENSORIMAGEPATCHOP



/// specialisation of the \ref PlaceHolderExpression when the node is
/// TensorVolumePatchOp
#define SYCLTENSORVOLUMEPATCHOP(CVQual)\
template<DenseIndex Planes, DenseIndex Rows, DenseIndex Cols, typename XprType, size_t N>\
struct PlaceHolderExpression<CVQual TensorVolumePatchOp<Planes,Rows, Cols, XprType>, N> {\
  typedef CVQual TensorVolumePatchOp<Planes,Rows, Cols, typename CalculateIndex <N, XprType>::ArgType> Type;\
};

SYCLTENSORVOLUMEPATCHOP(const)
SYCLTENSORVOLUMEPATCHOP()
#undef SYCLTENSORVOLUMEPATCHOP


/// template deduction for \ref PlaceHolderExpression struct
template <typename Expr>
struct createPlaceHolderExpression {
  static const size_t TotalLeaves = LeafCount<Expr>::Count;
  typedef typename PlaceHolderExpression<Expr, TotalLeaves - 1>::Type Type;
};

}  // internal
}  // TensorSycl
}  // namespace Eigen

#endif  // UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_PLACEHOLDER_EXPR_HPP
