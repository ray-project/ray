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
 * TensorSyclextractFunctors.h
 *
 * \brief:
 *  Used to extract all the functors allocated to each node of the expression
*tree.
 *
*****************************************************************/

#ifndef UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_EXTRACT_FUNCTORS_HPP
#define UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_EXTRACT_FUNCTORS_HPP

namespace Eigen {
namespace TensorSycl {
namespace internal {
/// struct FunctorExtractor:  This struct is used to extract the functors
/// constructed on
/// the host-side, to pack them and reuse them in reconstruction of the
/// expression on the device.
/// We have to do that as in Eigen the functors are not stateless so we cannot
/// re-instantiate them on the device.
/// We have to pass instantiated functors to the device.
// This struct is used for leafNode (TensorMap) and nodes behaving like leafNode (TensorForcedEval).
#define DEFALTACTION(Evaluator)\
typedef typename Evaluator::Dimensions Dimensions;\
const Dimensions m_dimensions;\
EIGEN_STRONG_INLINE const Dimensions& dimensions() const { return m_dimensions; }\
FunctorExtractor(const Evaluator& expr): m_dimensions(expr.dimensions()) {}

template <typename Evaluator> struct FunctorExtractor{
  DEFALTACTION(Evaluator)
};


/// specialisation of the \ref FunctorExtractor struct when the node type does not require anything
///TensorConversionOp
#define SYCLEXTRFUNCCONVERSION(ExprNode, CVQual)\
template <typename ArgType1, typename ArgType2, typename Dev>\
struct FunctorExtractor<TensorEvaluator<CVQual ExprNode<ArgType1, ArgType2>, Dev> > {\
  FunctorExtractor<TensorEvaluator<ArgType2, Dev> > subExpr;\
  FunctorExtractor(const TensorEvaluator<CVQual ExprNode<ArgType1, ArgType2>, Dev>& expr)\
  : subExpr(expr.impl()) {}\
};

SYCLEXTRFUNCCONVERSION(TensorConversionOp, const)
SYCLEXTRFUNCCONVERSION(TensorConversionOp, )
#undef SYCLEXTRFUNCCONVERSION

#define SYCLEXTRTENSORMAPFIXEDSIZE(CVQual)\
template <typename Scalar_, typename Dimensions_, int Options_2, typename IndexType, int Options_, template <class> class MakePointer_, typename Dev>\
struct FunctorExtractor< TensorEvaluator <CVQual TensorMap<TensorFixedSize<Scalar_, Dimensions_, Options_2, IndexType>, Options_, MakePointer_> , Dev> >{\
FunctorExtractor(const TensorEvaluator <CVQual TensorMap<TensorFixedSize<Scalar_, Dimensions_, Options_2, IndexType>, Options_, MakePointer_> , Dev>& ){}\
};

SYCLEXTRTENSORMAPFIXEDSIZE(const)
SYCLEXTRTENSORMAPFIXEDSIZE()
#undef SYCLEXTRTENSORMAPFIXEDSIZE

/// specialisation of the \ref FunctorExtractor struct when the node type is
/// TensorCwiseNullaryOp,  TensorCwiseUnaryOp, and  TensorBroadcastingOp
#define SYCLEXTRFUNCUNARY(CVQual)\
template <template <class, class> class UnaryCategory, typename OP, typename RHSExpr, typename Dev>\
struct FunctorExtractor<TensorEvaluator<CVQual UnaryCategory<OP, RHSExpr>, Dev> > {\
  FunctorExtractor<TensorEvaluator<RHSExpr, Dev> > rhsExpr;\
  const OP func;\
  FunctorExtractor(const TensorEvaluator<CVQual UnaryCategory<OP, RHSExpr>, Dev>& expr)\
  : rhsExpr(expr.impl()), func(expr.functor()) {}\
};

SYCLEXTRFUNCUNARY(const)
SYCLEXTRFUNCUNARY()
#undef SYCLEXTRFUNCUNARY

/// specialisation of the \ref FunctorExtractor struct when the node type is
/// TensorCwiseBinaryOp
#define SYCLEXTRFUNCBIINARY(CVQual)\
template <template<class, class, class> class BinaryCategory, typename OP, typename LHSExpr, typename RHSExpr, typename Dev>\
struct FunctorExtractor<TensorEvaluator<CVQual BinaryCategory<OP, LHSExpr, RHSExpr>, Dev> > {\
  FunctorExtractor<TensorEvaluator<LHSExpr, Dev> > lhsExpr;\
  FunctorExtractor<TensorEvaluator<RHSExpr, Dev> > rhsExpr;\
  const OP func;\
  FunctorExtractor(const TensorEvaluator<CVQual BinaryCategory<OP, LHSExpr, RHSExpr>, Dev>& expr)\
  : lhsExpr(expr.left_impl()),rhsExpr(expr.right_impl()),func(expr.functor()) {}\
};

SYCLEXTRFUNCBIINARY(const)
SYCLEXTRFUNCBIINARY()
#undef SYCLEXTRFUNCBIINARY

/// specialisation of the \ref FunctorExtractor struct when the node type is TensorCwiseTernaryOp
#define SYCLEXTRFUNCTERNARY(CVQual)\
template <template <class, class, class, class> class TernaryCategory, typename OP, typename Arg1Expr, typename Arg2Expr, typename Arg3Expr,typename Dev>\
struct FunctorExtractor<TensorEvaluator<CVQual TernaryCategory<OP, Arg1Expr, Arg2Expr, Arg3Expr>, Dev> > {\
  FunctorExtractor<TensorEvaluator<Arg1Expr, Dev> > arg1Expr;\
  FunctorExtractor<TensorEvaluator<Arg2Expr, Dev> > arg2Expr;\
  FunctorExtractor<TensorEvaluator<Arg3Expr, Dev> > arg3Expr;\
  const OP func;\
  FunctorExtractor(const TensorEvaluator<CVQual TernaryCategory<OP, Arg1Expr, Arg2Expr, Arg3Expr>, Dev>& expr)\
  : arg1Expr(expr.arg1Impl()), arg2Expr(expr.arg2Impl()), arg3Expr(expr.arg3Impl()), func(expr.functor()) {}\
};

SYCLEXTRFUNCTERNARY(const)
SYCLEXTRFUNCTERNARY()
#undef SYCLEXTRFUNCTERNARY



//TensorCustomOp must be specialised otherwise it will be captured by UnaryCategory while its action is different
//from the UnaryCategory and it is similar to the general FunctorExtractor.
/// specialisation of TensorCustomOp
#define SYCLEXTRFUNCCUSTOMUNARYOP(CVQual)\
template <typename CustomUnaryFunc, typename ArgType, typename Dev >\
struct FunctorExtractor<TensorEvaluator<CVQual TensorCustomUnaryOp<CustomUnaryFunc, ArgType>, Dev> > {\
  typedef TensorEvaluator<CVQual TensorCustomUnaryOp<CustomUnaryFunc, ArgType>, Dev> Evaluator;\
  DEFALTACTION(Evaluator)\
};
//TensorCustomUnaryOp
SYCLEXTRFUNCCUSTOMUNARYOP(const)
SYCLEXTRFUNCCUSTOMUNARYOP()
#undef SYCLEXTRFUNCCUSTOMUNARYOP

//TensorCustomBinaryOp
#define SYCLEXTRFUNCCUSTOMBIBARYOP(CVQual)\
template <typename CustomBinaryFunc, typename ArgType1, typename ArgType2, typename Dev >\
struct FunctorExtractor<TensorEvaluator<CVQual TensorCustomBinaryOp<CustomBinaryFunc, ArgType1, ArgType2>, Dev> > {\
  typedef TensorEvaluator<CVQual TensorCustomBinaryOp<CustomBinaryFunc, ArgType1, ArgType2>, Dev> Evaluator;\
  DEFALTACTION(Evaluator)\
};
//TensorCustomBinaryOp
SYCLEXTRFUNCCUSTOMBIBARYOP(const)
SYCLEXTRFUNCCUSTOMBIBARYOP()
#undef SYCLEXTRFUNCCUSTOMBIBARYOP



/// specialisation of the \ref FunctorExtractor struct when the node type is
/// TensorCwiseSelectOp. This is an specialisation without OP so it has to be separated.
#define SYCLEXTRFUNCSELECTOP(CVQual)\
template <typename IfExpr, typename ThenExpr, typename ElseExpr, typename Dev>\
struct FunctorExtractor< TensorEvaluator<CVQual TensorSelectOp<IfExpr, ThenExpr, ElseExpr>, Dev> > {\
  FunctorExtractor<TensorEvaluator<IfExpr, Dev> > ifExpr;\
  FunctorExtractor<TensorEvaluator<ThenExpr, Dev> > thenExpr;\
  FunctorExtractor<TensorEvaluator<ElseExpr, Dev> > elseExpr;\
  FunctorExtractor(const TensorEvaluator<CVQual TensorSelectOp<IfExpr, ThenExpr, ElseExpr>, Dev>& expr)\
  : ifExpr(expr.cond_impl()), thenExpr(expr.then_impl()), elseExpr(expr.else_impl()) {}\
};

SYCLEXTRFUNCSELECTOP(const)
SYCLEXTRFUNCSELECTOP()
#undef SYCLEXTRFUNCSELECTOP

/// specialisation of the \ref FunctorExtractor struct when the node type is
/// const TensorAssignOp. This is an specialisation without OP so it has to be separated.
#define SYCLEXTRFUNCASSIGNOP(CVQual)\
template <typename LHSExpr, typename RHSExpr, typename Dev>\
struct FunctorExtractor<TensorEvaluator<CVQual TensorAssignOp<LHSExpr, RHSExpr>, Dev> > {\
  FunctorExtractor<TensorEvaluator<LHSExpr, Dev> > lhsExpr;\
  FunctorExtractor<TensorEvaluator<RHSExpr, Dev> > rhsExpr;\
  FunctorExtractor(const TensorEvaluator<CVQual TensorAssignOp<LHSExpr, RHSExpr>, Dev>& expr)\
  : lhsExpr(expr.left_impl()), rhsExpr(expr.right_impl()) {}\
};
SYCLEXTRFUNCASSIGNOP(const)
SYCLEXTRFUNCASSIGNOP()
#undef SYCLEXTRFUNCASSIGNOP

/// specialisation of the \ref FunctorExtractor struct when the node types are
/// TensorEvalToOp, TensorLayoutSwapOp. This is an specialisation without OP so it has to be separated.
#define SYCLEXTRFUNCEVALTOOPSWAPLAYOUTINDEXTUPLE(CVQual, ExprNode)\
template <typename Expr, typename Dev>\
struct FunctorExtractor<TensorEvaluator<CVQual ExprNode<Expr>, Dev> > {\
  FunctorExtractor<TensorEvaluator<Expr, Dev> > xprExpr;\
  FunctorExtractor(const TensorEvaluator<CVQual ExprNode<Expr>, Dev>& expr)\
  : xprExpr(expr.impl()) {}\
};
//TensorEvalToOp
SYCLEXTRFUNCEVALTOOPSWAPLAYOUTINDEXTUPLE(const, TensorEvalToOp)
SYCLEXTRFUNCEVALTOOPSWAPLAYOUTINDEXTUPLE(, TensorEvalToOp)
// TensorLayoutSwapOp
SYCLEXTRFUNCEVALTOOPSWAPLAYOUTINDEXTUPLE(const, TensorLayoutSwapOp)
SYCLEXTRFUNCEVALTOOPSWAPLAYOUTINDEXTUPLE(, TensorLayoutSwapOp)
// TensorIndexTupleOp
SYCLEXTRFUNCEVALTOOPSWAPLAYOUTINDEXTUPLE(const, TensorIndexTupleOp)
SYCLEXTRFUNCEVALTOOPSWAPLAYOUTINDEXTUPLE(, TensorIndexTupleOp)

#undef SYCLEXTRFUNCEVALTOOPSWAPLAYOUTINDEXTUPLE

template<typename Dim, size_t NumOutputDim> struct DimConstr {
template<typename InDim>
  static EIGEN_STRONG_INLINE Dim getDim(InDim dims ) {return dims;}
};

template<typename Dim> struct DimConstr<Dim, 0> {
  template<typename InDim>
    static EIGEN_STRONG_INLINE Dim getDim(InDim dims ) {return Dim(static_cast<Dim>(dims.TotalSize()));}
};
//TensorReductionOp
#define SYCLEXTRFUNCREDUCTIONOP(CVQual)\
template<typename Op, typename Dims, typename ArgType, template <class> class MakePointer_, typename Device>\
struct FunctorExtractor<TensorEvaluator<CVQual TensorReductionOp<Op, Dims, ArgType, MakePointer_>, Device> >{\
  typedef TensorEvaluator<CVQual TensorReductionOp<Op, Dims, ArgType, MakePointer_>, Device> Evaluator;\
  typedef typename Eigen::internal::conditional<Evaluator::NumOutputDims==0, DSizes<typename Evaluator::Index, 1>, typename Evaluator::Dimensions >::type Dimensions;\
  const Dimensions m_dimensions;\
  EIGEN_STRONG_INLINE const Dimensions& dimensions() const { return m_dimensions; }\
  FunctorExtractor(const TensorEvaluator<CVQual TensorReductionOp<Op, Dims, ArgType, MakePointer_>, Device>& expr)\
  : m_dimensions(DimConstr<Dimensions, Evaluator::NumOutputDims>::getDim(expr.dimensions())) {}\
};
SYCLEXTRFUNCREDUCTIONOP(const)
SYCLEXTRFUNCREDUCTIONOP()
#undef SYCLEXTRFUNCREDUCTIONOP

//TensorTupleReducerOp
#define SYCLEXTRFUNCTUPLEREDUCTIONOP(CVQual)\
template<typename ReduceOp, typename Dims, typename ArgType, typename Device>\
 struct FunctorExtractor<TensorEvaluator<CVQual TensorTupleReducerOp<ReduceOp, Dims, ArgType>, Device> >{\
 typedef TensorEvaluator<CVQual TensorTupleReducerOp<ReduceOp, Dims, ArgType>, Device> Evaluator;\
 static const int  NumOutputDims= Eigen::internal::traits<TensorTupleReducerOp<ReduceOp, Dims, ArgType> >::NumDimensions;\
 typedef typename Evaluator::StrideDims StrideDims;\
 typedef typename Evaluator::Index Index;\
 typedef typename Eigen::internal::conditional<NumOutputDims==0, DSizes<Index, 1>, typename Evaluator::Dimensions >::type Dimensions;\
 const Dimensions m_dimensions;\
 const Index m_return_dim;\
 const StrideDims m_strides;\
 const Index m_stride_mod;\
 const Index m_stride_div;\
 EIGEN_STRONG_INLINE const Dimensions& dimensions() const { return m_dimensions; }\
 EIGEN_STRONG_INLINE  Index return_dim() const {return m_return_dim;}\
 EIGEN_STRONG_INLINE const StrideDims strides() const {return m_strides;}\
 EIGEN_STRONG_INLINE const Index stride_mod() const {return m_stride_mod;}\
 EIGEN_STRONG_INLINE const Index stride_div() const {return m_stride_div;}\
 FunctorExtractor(const TensorEvaluator<CVQual TensorTupleReducerOp<ReduceOp, Dims, ArgType>, Device>& expr)\
 : m_dimensions(DimConstr<Dimensions, NumOutputDims>::getDim(expr.dimensions())), m_return_dim(expr.return_dim()),\
   m_strides(expr.strides()), m_stride_mod(expr.stride_mod()), m_stride_div(expr.stride_div()){}\
};

SYCLEXTRFUNCTUPLEREDUCTIONOP(const)
SYCLEXTRFUNCTUPLEREDUCTIONOP()
#undef SYCLEXTRFUNCTUPLEREDUCTIONOP

//TensorContractionOp and TensorConvolutionOp
#define SYCLEXTRFUNCCONTRACTCONVOLUTIONOP(CVQual, ExprNode)\
template<typename Indices, typename LhsXprType, typename RhsXprType, typename Device>\
struct FunctorExtractor<TensorEvaluator<CVQual ExprNode<Indices, LhsXprType, RhsXprType>, Device>>{\
  typedef TensorEvaluator<CVQual ExprNode<Indices, LhsXprType, RhsXprType>, Device> Evaluator;\
  typedef typename Evaluator::Dimensions Dimensions;\
  const Dimensions m_dimensions;\
  EIGEN_STRONG_INLINE const Dimensions& dimensions() const { return m_dimensions; }\
  FunctorExtractor(const TensorEvaluator<CVQual ExprNode<Indices, LhsXprType, RhsXprType>, Device>& expr)\
  : m_dimensions(expr.dimensions()) {}\
};

//TensorContractionOp
SYCLEXTRFUNCCONTRACTCONVOLUTIONOP(const,TensorContractionOp)
SYCLEXTRFUNCCONTRACTCONVOLUTIONOP(,TensorContractionOp)
//TensorConvolutionOp
SYCLEXTRFUNCCONTRACTCONVOLUTIONOP(const,TensorConvolutionOp)
SYCLEXTRFUNCCONTRACTCONVOLUTIONOP(,TensorConvolutionOp)
#undef SYCLEXTRFUNCCONTRACTCONVOLUTIONOP

/// specialisation of the \ref FunctorExtractor struct when the node type is
/// const TensorSlicingOp. This is an specialisation without OP so it has to be separated.
#define SYCLEXTRFUNCTSLICEOP(CVQual)\
template <typename StartIndices, typename Sizes, typename XprType, typename Dev>\
struct FunctorExtractor<TensorEvaluator<CVQual TensorSlicingOp<StartIndices, Sizes, XprType>, Dev> > {\
  FunctorExtractor<TensorEvaluator<XprType, Dev> > xprExpr;\
  const StartIndices m_offsets;\
  const Sizes m_dimensions;\
  FunctorExtractor(const TensorEvaluator<CVQual  TensorSlicingOp<StartIndices, Sizes, XprType>, Dev>& expr)\
  : xprExpr(expr.impl()), m_offsets(expr.startIndices()), m_dimensions(expr.dimensions()) {}\
  EIGEN_STRONG_INLINE const StartIndices& startIndices() const {return m_offsets;}\
  EIGEN_STRONG_INLINE const Sizes& dimensions() const {return m_dimensions;}\
};

SYCLEXTRFUNCTSLICEOP(const)
SYCLEXTRFUNCTSLICEOP()
#undef SYCLEXTRFUNCTSLICEOP

//TensorStridingSlicingOp
#define SYCLEXTRFUNCTSLICESTRIDEOP(CVQual)\
template<typename StartIndices, typename StopIndices, typename Strides, typename XprType, typename Dev>\
struct FunctorExtractor<TensorEvaluator<CVQual TensorStridingSlicingOp<StartIndices, StopIndices, Strides, XprType>, Dev> >{\
  FunctorExtractor<TensorEvaluator<XprType, Dev> > xprExpr;\
  const StartIndices m_startIndices;\
  const StopIndices m_stopIndices;\
  const Strides m_strides;\
  FunctorExtractor(const TensorEvaluator<CVQual  TensorStridingSlicingOp<StartIndices, StopIndices,Strides, XprType>, Dev>& expr)\
  : xprExpr(expr.impl()), m_startIndices(expr.exprStartIndices()), m_stopIndices(expr.exprStopIndices()), m_strides(expr.strides()) {}\
  EIGEN_STRONG_INLINE  const StartIndices& startIndices() const { return m_startIndices; }\
  EIGEN_STRONG_INLINE  const StartIndices& stopIndices() const { return m_stopIndices; }\
  EIGEN_STRONG_INLINE  const StartIndices& strides() const { return m_strides; }\
};

SYCLEXTRFUNCTSLICESTRIDEOP(const)
SYCLEXTRFUNCTSLICESTRIDEOP()
#undef SYCLEXTRFUNCTSLICESTRIDEOP

// Had to separate TensorReshapingOp and TensorShufflingOp. Otherwise it will be mistaken by UnaryCategory
#define SYCLRESHAPEANDSHUFFLEOPFUNCEXT(OPEXPR, FUNCCALL, CVQual)\
template<typename Param, typename XprType, typename Dev>\
struct FunctorExtractor<Eigen::TensorEvaluator<CVQual Eigen::OPEXPR<Param, XprType>, Dev> > {\
  FunctorExtractor<Eigen::TensorEvaluator<XprType, Dev> > xprExpr;\
  const Param m_param;\
  EIGEN_STRONG_INLINE const Param& param() const { return m_param; }\
  FunctorExtractor(const Eigen::TensorEvaluator<CVQual Eigen::OPEXPR<Param, XprType>, Dev>& expr)\
  : xprExpr(expr.impl()), m_param(expr.FUNCCALL) {}\
};

//TensorReshapingOp
SYCLRESHAPEANDSHUFFLEOPFUNCEXT(TensorReshapingOp, dimensions(), const)
SYCLRESHAPEANDSHUFFLEOPFUNCEXT(TensorReshapingOp, dimensions(), )

//TensorShufflingOp
SYCLRESHAPEANDSHUFFLEOPFUNCEXT(TensorShufflingOp, shufflePermutation(), const)
SYCLRESHAPEANDSHUFFLEOPFUNCEXT(TensorShufflingOp, shufflePermutation(), )
#undef SYCLRESHAPEANDSHUFFLEOPFUNCEXT

// Had to separate reshapeOP otherwise it will be mistaken by UnaryCategory
#define PADDINGOPFUNCEXT(OPEXPR, FUNCCALL, SCALARFUNCCALL, CVQual)\
template<typename Param, typename XprType, typename Dev>\
struct FunctorExtractor<Eigen::TensorEvaluator<CVQual Eigen::OPEXPR<Param, XprType>, Dev> > {\
  FunctorExtractor<Eigen::TensorEvaluator<XprType, Dev> > xprExpr;\
  const Param m_param;\
  typedef typename Eigen::TensorEvaluator<CVQual Eigen::OPEXPR<Param, XprType>, Dev>::Scalar Scalar;\
  const Scalar m_scalar_param;\
  EIGEN_STRONG_INLINE const Param& param() const { return m_param; }\
  EIGEN_STRONG_INLINE const Scalar& scalar_param() const { return m_scalar_param; }\
  FunctorExtractor(const Eigen::TensorEvaluator<CVQual Eigen::OPEXPR<Param, XprType>, Dev>& expr)\
  : xprExpr(expr.impl()), m_param(expr.FUNCCALL), m_scalar_param(expr.SCALARFUNCCALL)  {}\
};

PADDINGOPFUNCEXT(TensorPaddingOp, padding(), padding_value(), const)
PADDINGOPFUNCEXT(TensorPaddingOp, padding(), padding_value(), )
#undef PADDINGOPFUNCEXT

/// specialisation of the \ref FunctorExtractor struct when the node type is TensorContractionOp and TensorConcatenationOp
/// for TensorContractionOp the LHS and RHS here are the original one no need to apply condition on their type.
#define SYCLEXTRFUNCCONTRACTCONCAT(OPEXPR, FUNCCALL, CVQual)\
template <typename Param, typename LHSExpr, typename RHSExpr, typename Dev>\
struct FunctorExtractor<TensorEvaluator<CVQual OPEXPR<Param, LHSExpr, RHSExpr>, Dev> > {\
  FunctorExtractor<TensorEvaluator<LHSExpr, Dev> > lhsExpr;\
  FunctorExtractor<TensorEvaluator<RHSExpr, Dev> > rhsExpr;\
  const Param func;\
  FunctorExtractor(const TensorEvaluator<CVQual OPEXPR<Param, LHSExpr, RHSExpr>, Dev>& expr)\
  : lhsExpr(expr.left_impl()),rhsExpr(expr.right_impl()),func(expr.FUNCCALL) {}\
};

// TensorConcatenationOp
SYCLEXTRFUNCCONTRACTCONCAT(TensorConcatenationOp, axis(), const)
SYCLEXTRFUNCCONTRACTCONCAT(TensorConcatenationOp, axis(),)
#undef SYCLEXTRFUNCCONTRACTCONCAT

//TensorChippingOp
#define SYCLEXTRFUNCCHIPPINGOP(CVQual)\
template<DenseIndex DimId, typename XprType, typename Device>\
struct FunctorExtractor<TensorEvaluator<CVQual TensorChippingOp<DimId, XprType>, Device> >{\
  FunctorExtractor<Eigen::TensorEvaluator<XprType, Device> > xprExpr;\
  const DenseIndex m_dim;\
  const DenseIndex m_offset;\
  EIGEN_STRONG_INLINE const DenseIndex& dimId() const { return m_dim; }\
  EIGEN_STRONG_INLINE const DenseIndex& offset() const { return m_offset; }\
  FunctorExtractor(const TensorEvaluator<CVQual TensorChippingOp<DimId, XprType>, Device>& expr)\
  : xprExpr(expr.impl()), m_dim(expr.dimId()), m_offset(expr.offset()) {}\
};

SYCLEXTRFUNCCHIPPINGOP(const)
SYCLEXTRFUNCCHIPPINGOP()
#undef SYCLEXTRFUNCCHIPPINGOP

//TensorImagePatchOp
#define SYCLEXTRFUNCIMAGEPATCHOP(CVQual)\
template<DenseIndex Rows, DenseIndex Cols, typename XprType, typename Device>\
struct FunctorExtractor<TensorEvaluator<CVQual TensorImagePatchOp<Rows, Cols, XprType>, Device> >{\
typedef CVQual TensorImagePatchOp<Rows, Cols, XprType> Self;\
FunctorExtractor<Eigen::TensorEvaluator<XprType, Device> > xprExpr;\
const DenseIndex m_patch_rows;\
const DenseIndex m_patch_cols;\
const DenseIndex m_row_strides;\
const DenseIndex m_col_strides;\
const DenseIndex m_in_row_strides;\
const DenseIndex m_in_col_strides;\
const DenseIndex m_row_inflate_strides;\
const DenseIndex m_col_inflate_strides;\
const bool m_padding_explicit;\
const DenseIndex m_padding_top;\
const DenseIndex m_padding_bottom;\
const DenseIndex m_padding_left;\
const DenseIndex m_padding_right;\
const PaddingType m_padding_type;\
const typename Self::Scalar m_padding_value;\
FunctorExtractor(const TensorEvaluator<Self, Device>& expr)\
: xprExpr(expr.impl()), m_patch_rows(expr.xpr().patch_rows()), m_patch_cols(expr.xpr().patch_cols()),\
  m_row_strides(expr.xpr().row_strides()), m_col_strides(expr.xpr().col_strides()),\
  m_in_row_strides(expr.xpr().in_row_strides()), m_in_col_strides(expr.xpr().in_col_strides()),\
  m_row_inflate_strides(expr.xpr().row_inflate_strides()), m_col_inflate_strides(expr.xpr().col_inflate_strides()),\
  m_padding_explicit(expr.xpr().padding_explicit()),m_padding_top(expr.xpr().padding_top()),\
  m_padding_bottom(expr.xpr().padding_bottom()), m_padding_left(expr.xpr().padding_left()),\
  m_padding_right(expr.xpr().padding_right()), m_padding_type(expr.xpr().padding_type()),\
  m_padding_value(expr.xpr().padding_value()){}\
};

SYCLEXTRFUNCIMAGEPATCHOP(const)
SYCLEXTRFUNCIMAGEPATCHOP()
#undef SYCLEXTRFUNCIMAGEPATCHOP

/// TensorVolumePatchOp
#define SYCLEXTRFUNCVOLUMEPATCHOP(CVQual)\
template<DenseIndex Planes, DenseIndex Rows, DenseIndex Cols, typename XprType, typename Device>\
struct FunctorExtractor<TensorEvaluator<CVQual TensorVolumePatchOp<Planes, Rows, Cols, XprType>, Device> >{\
typedef CVQual TensorVolumePatchOp<Planes, Rows, Cols, XprType> Self;\
FunctorExtractor<Eigen::TensorEvaluator<XprType, Device> > xprExpr;\
const DenseIndex m_patch_planes;\
const DenseIndex m_patch_rows;\
const DenseIndex m_patch_cols;\
const DenseIndex m_plane_strides;\
const DenseIndex m_row_strides;\
const DenseIndex m_col_strides;\
const DenseIndex m_in_plane_strides;\
const DenseIndex m_in_row_strides;\
const DenseIndex m_in_col_strides;\
const DenseIndex m_plane_inflate_strides;\
const DenseIndex m_row_inflate_strides;\
const DenseIndex m_col_inflate_strides;\
const bool m_padding_explicit;\
const DenseIndex m_padding_top_z;\
const DenseIndex m_padding_bottom_z;\
const DenseIndex m_padding_top;\
const DenseIndex m_padding_bottom;\
const DenseIndex m_padding_left;\
const DenseIndex m_padding_right;\
const PaddingType m_padding_type;\
const typename Self::Scalar m_padding_value;\
FunctorExtractor(const TensorEvaluator<Self, Device>& expr)\
: xprExpr(expr.impl()), m_patch_planes(expr.xpr().patch_planes()), m_patch_rows(expr.xpr().patch_rows()), m_patch_cols(expr.xpr().patch_cols()),\
  m_plane_strides(expr.xpr().plane_strides()), m_row_strides(expr.xpr().row_strides()), m_col_strides(expr.xpr().col_strides()),\
  m_in_plane_strides(expr.xpr().in_plane_strides()), m_in_row_strides(expr.xpr().in_row_strides()), m_in_col_strides(expr.xpr().in_col_strides()),\
  m_plane_inflate_strides(expr.xpr().plane_inflate_strides()),m_row_inflate_strides(expr.xpr().row_inflate_strides()),\
  m_col_inflate_strides(expr.xpr().col_inflate_strides()), m_padding_explicit(expr.xpr().padding_explicit()),\
  m_padding_top_z(expr.xpr().padding_top_z()), m_padding_bottom_z(expr.xpr().padding_bottom_z()), \
  m_padding_top(expr.xpr().padding_top()), m_padding_bottom(expr.xpr().padding_bottom()), m_padding_left(expr.xpr().padding_left()),\
  m_padding_right(expr.xpr().padding_right()), m_padding_type(expr.xpr().padding_type()),m_padding_value(expr.xpr().padding_value()){}\
};
SYCLEXTRFUNCVOLUMEPATCHOP(const)
SYCLEXTRFUNCVOLUMEPATCHOP()
#undef SYCLEXTRFUNCVOLUMEPATCHOP


/// template deduction function for FunctorExtractor
template <typename Evaluator>
auto inline extractFunctors(const Evaluator& evaluator)-> FunctorExtractor<Evaluator> {
  return FunctorExtractor<Evaluator>(evaluator);
}
}  // namespace internal
}  // namespace TensorSycl
}  // namespace Eigen

#endif  // UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_EXTRACT_FUNCTORS_HPP
