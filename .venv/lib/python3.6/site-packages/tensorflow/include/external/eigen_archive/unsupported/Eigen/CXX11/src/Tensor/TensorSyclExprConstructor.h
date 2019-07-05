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
 * TensorSyclExprConstructor.h
 *
 * \brief:
 *  This file re-create an expression on the SYCL device in order
 *  to use the original tensor evaluator.
 *
*****************************************************************/

#ifndef UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_EXPR_CONSTRUCTOR_HPP
#define UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_EXPR_CONSTRUCTOR_HPP

namespace Eigen {
namespace TensorSycl {
namespace internal {

template <typename Expr, typename Dims>
struct DeviceFixedSizeTensor;

template <typename Expr, typename std::ptrdiff_t... Indices>
struct DeviceFixedSizeTensor<Expr, Eigen::Sizes<Indices...>>{
  template<typename Data>
  static EIGEN_ALWAYS_INLINE Expr instantiate(Data& dt) {return Expr(ConvertToActualTypeSycl(typename Expr::Scalar, dt), Indices...);}
};
/// this class is used by EvalToOp in order to create an lhs expression which is
/// a pointer from an accessor on device-only buffer
template <typename PtrType, size_t N, typename... Params>
struct EvalToLHSConstructor {
  PtrType expr;
  EvalToLHSConstructor(const utility::tuple::Tuple<Params...> &t) : expr(ConvertToActualTypeSycl(typename Eigen::internal::remove_all<PtrType>::type, utility::tuple::get<N>(t))) {}
};

/// struct ExprConstructor is used to reconstruct the expression on the device and
/// recreate the expression with MakeGlobalPointer containing the device address
/// space for the TensorMap pointers used in eval function.
/// It receives the original expression type, the functor of the node, the tuple
/// of accessors, and the device expression type to re-instantiate the
/// expression tree for the device
template <typename OrigExpr, typename IndexExpr, typename... Params>
struct ExprConstructor;

/// specialisation of the \ref ExprConstructor struct when the node type is
/// TensorMap
#define TENSORMAP(CVQual)\
template <typename T,  int Options_,\
template <class> class MakePointer_, size_t N, typename... Params>\
struct ExprConstructor< CVQual TensorMap<T, Options_, MakeGlobalPointer>,\
CVQual PlaceHolder<CVQual TensorMap<T, Options_, MakePointer_>, N>, Params...>{\
  typedef  CVQual TensorMap<T, Options_, MakeGlobalPointer>  Type;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &fd, const utility::tuple::Tuple<Params...> &t)\
  : expr(Type(ConvertToActualTypeSycl(typename Type::Scalar, utility::tuple::get<N>(t)), fd.dimensions())){}\
};

TENSORMAP(const)
TENSORMAP()
#undef TENSORMAP

/// specialisation of the \ref ExprConstructor struct when the node type is
/// TensorMap
#define TENSORMAPFIXEDSIZE(CVQual)\
template <typename Scalar_, typename Dimensions_, int Options_2, typename IndexType, int Options_,\
template <class> class MakePointer_, size_t N, typename... Params>\
struct ExprConstructor< CVQual TensorMap<TensorFixedSize<Scalar_, Dimensions_, Options_2, IndexType>, Options_, MakeGlobalPointer>,\
CVQual PlaceHolder<CVQual TensorMap<TensorFixedSize<Scalar_, Dimensions_, Options_2, IndexType>, Options_, MakePointer_>, N>, Params...>{\
  typedef  CVQual TensorMap<TensorFixedSize<Scalar_, Dimensions_, Options_2, IndexType>, Options_, MakeGlobalPointer>  Type;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &, const utility::tuple::Tuple<Params...> &t)\
  : expr(DeviceFixedSizeTensor<Type,Dimensions_>::instantiate(utility::tuple::get<N>(t))){}\
};

TENSORMAPFIXEDSIZE(const)
TENSORMAPFIXEDSIZE()
#undef TENSORMAPFIXEDSIZE

#define UNARYCATEGORY(CVQual)\
template <template<class, class> class UnaryCategory, typename OP, typename OrigRHSExpr, typename RHSExpr, typename... Params>\
struct ExprConstructor<CVQual UnaryCategory<OP, OrigRHSExpr>, CVQual UnaryCategory<OP, RHSExpr>, Params...> {\
  typedef  ExprConstructor<OrigRHSExpr, RHSExpr, Params...> my_type;\
  my_type rhsExpr;\
  typedef CVQual UnaryCategory<OP, typename my_type::Type> Type;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
  : rhsExpr(funcD.rhsExpr, t), expr(rhsExpr.expr, funcD.func) {}\
};

UNARYCATEGORY(const)
UNARYCATEGORY()
#undef UNARYCATEGORY

/// specialisation of the \ref ExprConstructor struct when the node type is
/// TensorBinaryOp
#define BINARYCATEGORY(CVQual)\
template <template<class, class, class> class BinaryCategory, typename OP, typename OrigLHSExpr, typename OrigRHSExpr, typename LHSExpr,\
typename RHSExpr, typename... Params>\
struct ExprConstructor<CVQual BinaryCategory<OP, OrigLHSExpr, OrigRHSExpr>,  CVQual BinaryCategory<OP, LHSExpr, RHSExpr>, Params...> {\
  typedef  ExprConstructor<OrigLHSExpr, LHSExpr, Params...> my_left_type;\
  typedef  ExprConstructor<OrigRHSExpr, RHSExpr, Params...> my_right_type;\
  typedef  CVQual BinaryCategory<OP, typename my_left_type::Type, typename my_right_type::Type> Type;\
  my_left_type lhsExpr;\
  my_right_type rhsExpr;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
  : lhsExpr(funcD.lhsExpr, t),rhsExpr(funcD.rhsExpr, t), expr(lhsExpr.expr, rhsExpr.expr, funcD.func) {}\
};

BINARYCATEGORY(const)
BINARYCATEGORY()
#undef BINARYCATEGORY

/// specialisation of the \ref ExprConstructor struct when the node type is
/// TensorCwiseTernaryOp
#define TERNARYCATEGORY(CVQual)\
template <template <class, class, class, class> class TernaryCategory, typename OP, typename OrigArg1Expr, typename OrigArg2Expr,typename OrigArg3Expr,\
typename Arg1Expr, typename Arg2Expr, typename Arg3Expr, typename... Params>\
struct ExprConstructor<CVQual TernaryCategory<OP, OrigArg1Expr, OrigArg2Expr, OrigArg3Expr>, CVQual TernaryCategory<OP, Arg1Expr, Arg2Expr, Arg3Expr>, Params...> {\
  typedef ExprConstructor<OrigArg1Expr, Arg1Expr, Params...> my_arg1_type;\
  typedef ExprConstructor<OrigArg2Expr, Arg2Expr, Params...> my_arg2_type;\
  typedef ExprConstructor<OrigArg3Expr, Arg3Expr, Params...> my_arg3_type;\
  typedef  CVQual TernaryCategory<OP, typename my_arg1_type::Type, typename my_arg2_type::Type, typename my_arg3_type::Type> Type;\
  my_arg1_type arg1Expr;\
  my_arg2_type arg2Expr;\
  my_arg3_type arg3Expr;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD,const utility::tuple::Tuple<Params...> &t)\
  : arg1Expr(funcD.arg1Expr, t), arg2Expr(funcD.arg2Expr, t), arg3Expr(funcD.arg3Expr, t), expr(arg1Expr.expr, arg2Expr.expr, arg3Expr.expr, funcD.func) {}\
};

TERNARYCATEGORY(const)
TERNARYCATEGORY()
#undef TERNARYCATEGORY

/// specialisation of the \ref ExprConstructor struct when the node type is
/// TensorCwiseSelectOp
#define SELECTOP(CVQual)\
template <typename OrigIfExpr, typename OrigThenExpr, typename OrigElseExpr, typename IfExpr, typename ThenExpr, typename ElseExpr, typename... Params>\
struct ExprConstructor< CVQual TensorSelectOp<OrigIfExpr, OrigThenExpr, OrigElseExpr>, CVQual TensorSelectOp<IfExpr, ThenExpr, ElseExpr>, Params...> {\
  typedef  ExprConstructor<OrigIfExpr, IfExpr, Params...> my_if_type;\
  typedef  ExprConstructor<OrigThenExpr, ThenExpr, Params...> my_then_type;\
  typedef  ExprConstructor<OrigElseExpr, ElseExpr, Params...> my_else_type;\
  typedef CVQual TensorSelectOp<typename my_if_type::Type, typename my_then_type::Type, typename my_else_type::Type> Type;\
  my_if_type ifExpr;\
  my_then_type thenExpr;\
  my_else_type elseExpr;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
  : ifExpr(funcD.ifExpr, t), thenExpr(funcD.thenExpr, t), elseExpr(funcD.elseExpr, t), expr(ifExpr.expr, thenExpr.expr, elseExpr.expr) {}\
};

SELECTOP(const)
SELECTOP()
#undef SELECTOP

/// specialisation of the \ref ExprConstructor struct when the node type is
/// const TensorAssignOp
#define ASSIGN(CVQual)\
template <typename OrigLHSExpr, typename OrigRHSExpr, typename LHSExpr, typename RHSExpr, typename... Params>\
struct ExprConstructor<CVQual TensorAssignOp<OrigLHSExpr, OrigRHSExpr>,  CVQual TensorAssignOp<LHSExpr, RHSExpr>, Params...> {\
  typedef ExprConstructor<OrigLHSExpr, LHSExpr, Params...> my_left_type;\
  typedef ExprConstructor<OrigRHSExpr, RHSExpr, Params...> my_right_type;\
  typedef CVQual TensorAssignOp<typename my_left_type::Type, typename my_right_type::Type>  Type;\
  my_left_type lhsExpr;\
  my_right_type rhsExpr;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
  : lhsExpr(funcD.lhsExpr, t), rhsExpr(funcD.rhsExpr, t), expr(lhsExpr.expr, rhsExpr.expr) {}\
 };

 ASSIGN(const)
 ASSIGN()
 #undef ASSIGN

 /// specialisation of the \ref ExprConstructor struct when the node type is
 /// const TensorAssignOp
 #define CONVERSIONEXPRCONST(CVQual)\
 template <typename OrigNestedExpr, typename ConvertType, typename NestedExpr, typename... Params>\
 struct ExprConstructor<CVQual TensorConversionOp<ConvertType, OrigNestedExpr>,  CVQual TensorConversionOp<ConvertType, NestedExpr>, Params...> {\
   typedef ExprConstructor<OrigNestedExpr, NestedExpr, Params...> my_nested_type;\
   typedef CVQual TensorConversionOp<ConvertType, typename my_nested_type::Type>  Type;\
   my_nested_type nestedExpr;\
   Type expr;\
   template <typename FuncDetector>\
   ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
   : nestedExpr(funcD.subExpr, t), expr(nestedExpr.expr) {}\
  };

  CONVERSIONEXPRCONST(const)
  CONVERSIONEXPRCONST()
  #undef CONVERSIONEXPRCONST

/// specialisation of the \ref ExprConstructor struct when the node type is
///  TensorEvalToOp /// 0 here is the output number in the buffer
#define EVALTO(CVQual)\
template <typename OrigExpr, typename Expr, typename... Params>\
struct ExprConstructor<CVQual TensorEvalToOp<OrigExpr, MakeGlobalPointer>, CVQual TensorEvalToOp<Expr>, Params...> {\
  typedef ExprConstructor<OrigExpr, Expr, Params...> my_expr_type;\
  typedef typename TensorEvalToOp<OrigExpr, MakeGlobalPointer>::PointerType my_buffer_type;\
  typedef CVQual TensorEvalToOp<typename my_expr_type::Type, MakeGlobalPointer> Type;\
  my_expr_type nestedExpression;\
  EvalToLHSConstructor<my_buffer_type, 0, Params...> buffer;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
  : nestedExpression(funcD.xprExpr, t), buffer(t), expr(buffer.expr, nestedExpression.expr) {}\
};

EVALTO(const)
EVALTO()
#undef EVALTO

/// specialisation of the \ref ExprConstructor struct when the node type is
/// TensorForcedEvalOp
#define FORCEDEVAL(CVQual)\
template <typename OrigExpr, typename DevExpr, size_t N, typename... Params>\
struct ExprConstructor<CVQual TensorForcedEvalOp<OrigExpr>,\
CVQual PlaceHolder<CVQual TensorForcedEvalOp<DevExpr>, N>, Params...> {\
  typedef  TensorForcedEvalOp<OrigExpr> XprType;\
  typedef CVQual TensorMap<\
                            Tensor<typename XprType::Scalar,XprType::NumDimensions, Eigen::internal::traits<XprType>::Layout,typename XprType::Index>,\
                            Eigen::internal::traits<XprType>::Layout, \
                            MakeGlobalPointer\
                          > Type;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &fd, const utility::tuple::Tuple<Params...> &t)\
  : expr(Type(ConvertToActualTypeSycl(typename Type::Scalar, utility::tuple::get<N>(t)), fd.dimensions())) {}\
};

FORCEDEVAL(const)
FORCEDEVAL()
#undef FORCEDEVAL

#define TENSORCUSTOMUNARYOP(CVQual)\
template <typename CustomUnaryFunc, typename OrigExpr, typename DevExpr, size_t N, typename... Params>\
struct ExprConstructor<CVQual TensorCustomUnaryOp<CustomUnaryFunc, OrigExpr>,\
CVQual PlaceHolder<CVQual TensorCustomUnaryOp<CustomUnaryFunc, DevExpr>, N>, Params...> {\
  typedef TensorCustomUnaryOp<CustomUnaryFunc, OrigExpr> XprType;\
  typedef CVQual TensorMap<\
                            Tensor<typename XprType::Scalar,XprType::NumDimensions, Eigen::internal::traits<XprType>::Layout,typename XprType::Index>,\
                            Eigen::internal::traits<XprType>::Layout, \
                            MakeGlobalPointer\
                          > Type;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &fd, const utility::tuple::Tuple<Params...> &t)\
  : expr(Type(ConvertToActualTypeSycl(typename Type::Scalar, utility::tuple::get<N>(t)), fd.dimensions())) {}\
};

TENSORCUSTOMUNARYOP(const)
TENSORCUSTOMUNARYOP()
#undef TENSORCUSTOMUNARYOP

/// specialisation of the \ref ExprConstructor struct when the node type is TensorReductionOp
#define SYCLREDUCTIONEXPR(CVQual)\
template <typename OP, typename Dim, typename OrigExpr, typename DevExpr, size_t N, typename... Params>\
struct ExprConstructor<CVQual TensorReductionOp<OP, Dim, OrigExpr, MakeGlobalPointer>,\
CVQual PlaceHolder<CVQual TensorReductionOp<OP, Dim, DevExpr>, N>, Params...> {\
  static const auto NumIndices= ValueCondition< TensorReductionOp<OP, Dim, DevExpr, MakeGlobalPointer>::NumDimensions==0,  1, TensorReductionOp<OP, Dim, DevExpr, MakeGlobalPointer>::NumDimensions >::Res;\
  typedef CVQual TensorMap<Tensor<typename TensorReductionOp<OP, Dim, DevExpr, MakeGlobalPointer>::Scalar,\
  NumIndices, Eigen::internal::traits<TensorReductionOp<OP, Dim, DevExpr, MakeGlobalPointer>>::Layout, typename TensorReductionOp<OP, Dim, DevExpr>::Index>, Eigen::internal::traits<TensorReductionOp<OP, Dim, DevExpr, MakeGlobalPointer>>::Layout, MakeGlobalPointer> Type;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &fd, const utility::tuple::Tuple<Params...> &t)\
  :expr(Type(ConvertToActualTypeSycl(typename Type::Scalar, utility::tuple::get<N>(t)), fd.dimensions())) {}\
};

SYCLREDUCTIONEXPR(const)
SYCLREDUCTIONEXPR()
#undef SYCLREDUCTIONEXPR

/// specialisation of the \ref ExprConstructor struct when the node type is TensorTupleReducerOp
/// use reductionOp instead of the TensorTupleReducerOp in order to build the tensor map. Because the tensorMap is the output of Tensor ReductionOP.
#define SYCLTUPLEREDUCTIONEXPR(CVQual)\
template <typename OP, typename Dim, typename OrigExpr, typename DevExpr, size_t N, typename... Params>\
struct ExprConstructor<CVQual TensorTupleReducerOp<OP, Dim, OrigExpr>,\
CVQual PlaceHolder<CVQual TensorTupleReducerOp<OP, Dim, DevExpr>, N>, Params...> {\
  static const auto NumRedDims= TensorReductionOp<OP, Dim, const TensorIndexTupleOp<OrigExpr> , MakeGlobalPointer>::NumDimensions;\
  static const auto NumIndices= ValueCondition<NumRedDims==0, 1, NumRedDims>::Res;\
static const int Layout =static_cast<int>(Eigen::internal::traits<TensorReductionOp<OP, Dim, const TensorIndexTupleOp<OrigExpr>, MakeGlobalPointer>>::Layout);\
  typedef CVQual TensorMap<\
                          Tensor<typename TensorIndexTupleOp<OrigExpr>::CoeffReturnType,NumIndices, Layout, typename TensorTupleReducerOp<OP, Dim, OrigExpr>::Index>,\
                          Layout,\
                          MakeGlobalPointer\
                          > XprType;\
  typedef typename TensorEvaluator<const TensorIndexTupleOp<OrigExpr> , SyclKernelDevice>::Dimensions InputDimensions;\
  static const int NumDims = Eigen::internal::array_size<InputDimensions>::value;\
  typedef array<Index, NumDims> StrideDims;\
  typedef const TensorTupleReducerDeviceOp<StrideDims, XprType> Type;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &fd, const utility::tuple::Tuple<Params...> &t)\
  :expr(Type(XprType(ConvertToActualTypeSycl(typename XprType::CoeffReturnType, utility::tuple::get<N>(t)), fd.dimensions()),\
    fd.return_dim(), fd.strides(), fd.stride_mod(), fd.stride_div())) {\
    }\
};

SYCLTUPLEREDUCTIONEXPR(const)
SYCLTUPLEREDUCTIONEXPR()
#undef SYCLTUPLEREDUCTIONEXPR

/// specialisation of the \ref ExprConstructor struct when the node type is
/// TensorContractionOp, TensorConvolutionOp TensorCustomBinaryOp
#define SYCLCONTRACTCONVCUSBIOPS(CVQual, ExprNode)\
template <typename Indices, typename OrigLhsXprType, typename OrigRhsXprType, typename LhsXprType, typename RhsXprType, size_t N, typename... Params>\
struct ExprConstructor<CVQual ExprNode<Indices, OrigLhsXprType, OrigRhsXprType>,\
CVQual PlaceHolder<CVQual ExprNode<Indices, LhsXprType,  RhsXprType>, N>, Params...> {\
  typedef ExprNode<Indices, OrigLhsXprType, OrigRhsXprType> XprTyp;\
  static const auto NumIndices= Eigen::internal::traits<XprTyp>::NumDimensions;\
  typedef CVQual TensorMap<\
                            Tensor<typename XprTyp::Scalar,NumIndices, Eigen::internal::traits<XprTyp>::Layout, typename XprTyp::Index>,\
                            Eigen::internal::traits<XprTyp>::Layout, \
                            MakeGlobalPointer\
                          > Type;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &fd, const utility::tuple::Tuple<Params...> &t)\
  :expr(Type(ConvertToActualTypeSycl(typename Type::Scalar, utility::tuple::get<N>(t)), fd.dimensions())) {}\
};

//TensorContractionOp
SYCLCONTRACTCONVCUSBIOPS(const, TensorContractionOp)
SYCLCONTRACTCONVCUSBIOPS(, TensorContractionOp)
//TensorConvolutionOp
SYCLCONTRACTCONVCUSBIOPS(const, TensorConvolutionOp)
SYCLCONTRACTCONVCUSBIOPS(, TensorConvolutionOp)
//TensorCustomBinaryOp
SYCLCONTRACTCONVCUSBIOPS(const, TensorCustomBinaryOp)
SYCLCONTRACTCONVCUSBIOPS(, TensorCustomBinaryOp)
#undef SYCLCONTRACTCONVCUSBIOPS

//TensorSlicingOp
#define SYCLSLICEOPEXPR(CVQual)\
template<typename StartIndices, typename Sizes, typename OrigXprType, typename XprType, typename... Params>\
struct ExprConstructor<CVQual TensorSlicingOp <StartIndices, Sizes, OrigXprType> , CVQual TensorSlicingOp<StartIndices, Sizes, XprType>, Params... >{\
  typedef ExprConstructor<OrigXprType, XprType, Params...> my_xpr_type;\
  typedef CVQual TensorSlicingOp<StartIndices, Sizes, typename my_xpr_type::Type> Type;\
  my_xpr_type xprExpr;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
  : xprExpr(funcD.xprExpr, t), expr(xprExpr.expr, funcD.startIndices(), funcD.dimensions()) {}\
};

SYCLSLICEOPEXPR(const)
SYCLSLICEOPEXPR()
#undef SYCLSLICEOPEXPR

//TensorStridingSlicingOp
#define SYCLSLICESTRIDEOPEXPR(CVQual)\
template<typename StartIndices, typename StopIndices, typename Strides, typename OrigXprType, typename XprType, typename... Params>\
struct ExprConstructor<CVQual TensorStridingSlicingOp<StartIndices, StopIndices, Strides, OrigXprType>, CVQual TensorStridingSlicingOp<StartIndices, StopIndices, Strides, XprType>, Params... >{\
  typedef ExprConstructor<OrigXprType, XprType, Params...> my_xpr_type;\
  typedef CVQual TensorStridingSlicingOp<StartIndices, StopIndices, Strides, typename my_xpr_type::Type> Type;\
  my_xpr_type xprExpr;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
  : xprExpr(funcD.xprExpr, t), expr(xprExpr.expr, funcD.startIndices(), funcD.stopIndices(),funcD.strides()) {}\
};

SYCLSLICESTRIDEOPEXPR(const)
SYCLSLICESTRIDEOPEXPR()
#undef SYCLSLICESTRIDEOPEXPR

//TensorReshapingOp and TensorShufflingOp
#define SYCLRESHAPEANDSHUFFLEOPEXPRCONST(OPEXPR, CVQual)\
template<typename Param, typename OrigXprType, typename XprType, typename... Params>\
struct ExprConstructor<CVQual OPEXPR <Param, OrigXprType> , CVQual OPEXPR <Param, XprType>, Params... >{\
  typedef ExprConstructor<OrigXprType, XprType, Params...> my_xpr_type;\
  typedef CVQual OPEXPR <Param, typename my_xpr_type::Type> Type ;\
  my_xpr_type xprExpr;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
  : xprExpr(funcD.xprExpr, t), expr(xprExpr.expr, funcD.param()) {}\
};

// TensorReshapingOp
SYCLRESHAPEANDSHUFFLEOPEXPRCONST(TensorReshapingOp, const)
SYCLRESHAPEANDSHUFFLEOPEXPRCONST(TensorReshapingOp, )
// TensorShufflingOp
SYCLRESHAPEANDSHUFFLEOPEXPRCONST(TensorShufflingOp, const)
SYCLRESHAPEANDSHUFFLEOPEXPRCONST(TensorShufflingOp, )
#undef SYCLRESHAPEANDSHUFFLEOPEXPRCONST

//TensorPaddingOp
#define SYCLPADDINGOPEXPRCONST(OPEXPR, CVQual)\
template<typename Param, typename OrigXprType, typename XprType, typename... Params>\
struct ExprConstructor<CVQual OPEXPR <Param, OrigXprType> , CVQual OPEXPR <Param, XprType>, Params... >{\
  typedef ExprConstructor<OrigXprType, XprType, Params...> my_xpr_type;\
  typedef CVQual OPEXPR <Param, typename my_xpr_type::Type> Type ;\
  my_xpr_type xprExpr;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
  : xprExpr(funcD.xprExpr, t), expr(xprExpr.expr, funcD.param() , funcD.scalar_param()) {}\
};

//TensorPaddingOp
SYCLPADDINGOPEXPRCONST(TensorPaddingOp, const)
SYCLPADDINGOPEXPRCONST(TensorPaddingOp, )
#undef SYCLPADDINGOPEXPRCONST

// TensorChippingOp
#define SYCLTENSORCHIPPINGOPEXPR(CVQual)\
template<DenseIndex DimId, typename OrigXprType, typename XprType, typename... Params>\
struct ExprConstructor<CVQual TensorChippingOp <DimId, OrigXprType> , CVQual TensorChippingOp<DimId, XprType>, Params... >{\
  typedef ExprConstructor<OrigXprType, XprType, Params...> my_xpr_type;\
  typedef CVQual TensorChippingOp<DimId, typename my_xpr_type::Type> Type;\
  my_xpr_type xprExpr;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
  : xprExpr(funcD.xprExpr, t), expr(xprExpr.expr, funcD.offset(), funcD.dimId()) {}\
};

SYCLTENSORCHIPPINGOPEXPR(const)
SYCLTENSORCHIPPINGOPEXPR()
#undef SYCLTENSORCHIPPINGOPEXPR

// TensorImagePatchOp
#define SYCLTENSORIMAGEPATCHOPEXPR(CVQual)\
template<DenseIndex Rows, DenseIndex Cols, typename OrigXprType, typename XprType, typename... Params>\
struct  ExprConstructor<CVQual TensorImagePatchOp<Rows, Cols, OrigXprType>, CVQual TensorImagePatchOp<Rows, Cols, XprType>, Params... > {\
  typedef ExprConstructor<OrigXprType, XprType, Params...> my_xpr_type;\
  typedef CVQual TensorImagePatchOp<Rows, Cols, typename my_xpr_type::Type> Type;\
  my_xpr_type xprExpr;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
  : xprExpr(funcD.xprExpr, t), expr(xprExpr.expr, funcD.m_patch_rows, funcD.m_patch_cols, funcD.m_row_strides, funcD.m_col_strides,\
    funcD.m_in_row_strides, funcD.m_in_col_strides, funcD.m_row_inflate_strides, funcD.m_col_inflate_strides, funcD.m_padding_explicit, \
    funcD.m_padding_top, funcD.m_padding_bottom, funcD.m_padding_left, funcD.m_padding_right, funcD.m_padding_type, funcD.m_padding_value){}\
};

SYCLTENSORIMAGEPATCHOPEXPR(const)
SYCLTENSORIMAGEPATCHOPEXPR()
#undef SYCLTENSORIMAGEPATCHOPEXPR

// TensorVolumePatchOp
#define SYCLTENSORVOLUMEPATCHOPEXPR(CVQual)\
template<DenseIndex Planes, DenseIndex Rows, DenseIndex Cols, typename OrigXprType, typename XprType, typename... Params>\
struct  ExprConstructor<CVQual TensorVolumePatchOp<Planes, Rows, Cols, OrigXprType>, CVQual TensorVolumePatchOp<Planes, Rows, Cols, XprType>, Params... > {\
  typedef ExprConstructor<OrigXprType, XprType, Params...> my_xpr_type;\
  typedef CVQual TensorVolumePatchOp<Planes, Rows, Cols, typename my_xpr_type::Type> Type;\
  my_xpr_type xprExpr;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
  : xprExpr(funcD.xprExpr, t), expr(xprExpr.expr, funcD.m_patch_planes, funcD.m_patch_rows, funcD.m_patch_cols, funcD.m_plane_strides, funcD.m_row_strides, funcD.m_col_strides,\
    funcD.m_in_plane_strides, funcD.m_in_row_strides, funcD.m_in_col_strides,funcD.m_plane_inflate_strides, funcD.m_row_inflate_strides, funcD.m_col_inflate_strides, \
    funcD.m_padding_explicit, funcD.m_padding_top_z, funcD.m_padding_bottom_z, funcD.m_padding_top, funcD.m_padding_bottom, funcD.m_padding_left, funcD.m_padding_right, \
    funcD.m_padding_type, funcD.m_padding_value ){\
    }\
};

SYCLTENSORVOLUMEPATCHOPEXPR(const)
SYCLTENSORVOLUMEPATCHOPEXPR()
#undef SYCLTENSORVOLUMEPATCHOPEXPR

// TensorLayoutSwapOp and TensorIndexTupleOp
#define SYCLTENSORLAYOUTSWAPINDEXTUPLEOPEXPR(CVQual, ExprNode)\
template<typename OrigXprType, typename XprType, typename... Params>\
struct ExprConstructor<CVQual ExprNode <OrigXprType> , CVQual ExprNode<XprType>, Params... >{\
  typedef ExprConstructor<OrigXprType, XprType, Params...> my_xpr_type;\
  typedef CVQual ExprNode<typename my_xpr_type::Type> Type;\
  my_xpr_type xprExpr;\
  Type expr;\
  template <typename FuncDetector>\
  ExprConstructor(FuncDetector &funcD, const utility::tuple::Tuple<Params...> &t)\
  : xprExpr(funcD.xprExpr, t), expr(xprExpr.expr) {}\
};

//TensorLayoutSwapOp
SYCLTENSORLAYOUTSWAPINDEXTUPLEOPEXPR(const, TensorLayoutSwapOp)
SYCLTENSORLAYOUTSWAPINDEXTUPLEOPEXPR(, TensorLayoutSwapOp)
//TensorIndexTupleOp
SYCLTENSORLAYOUTSWAPINDEXTUPLEOPEXPR(const, TensorIndexTupleOp)
SYCLTENSORLAYOUTSWAPINDEXTUPLEOPEXPR(, TensorIndexTupleOp)

#undef SYCLTENSORLAYOUTSWAPINDEXTUPLEOPEXPR

/// template deduction for \ref ExprConstructor struct
template <typename OrigExpr, typename IndexExpr, typename FuncD, typename... Params>
auto createDeviceExpression(FuncD &funcD, const utility::tuple::Tuple<Params...> &t)
    -> decltype(ExprConstructor<OrigExpr, IndexExpr, Params...>(funcD, t)) {
  return ExprConstructor<OrigExpr, IndexExpr, Params...>(funcD, t);
}

} /// namespace TensorSycl
} /// namespace internal
} /// namespace Eigen


#endif  // UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_EXPR_CONSTRUCTOR_HPP
