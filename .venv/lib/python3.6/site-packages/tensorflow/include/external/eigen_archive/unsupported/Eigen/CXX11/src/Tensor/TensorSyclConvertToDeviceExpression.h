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
 * TensorSyclConvertToDeviceExpression.h
 *
 * \brief:
 *  Conversion from host pointer to device pointer
 *  inside leaf nodes of the expression.
 *
*****************************************************************/

#ifndef UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_CONVERT_TO_DEVICE_EXPRESSION_HPP
#define UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSORSYCL_CONVERT_TO_DEVICE_EXPRESSION_HPP

namespace Eigen {
namespace TensorSycl {
namespace internal {

/// \struct ConvertToDeviceExpression
/// \brief This struct is used to convert the MakePointer in the host expression
/// to the MakeGlobalPointer for the device expression. For the leafNodes
/// containing the pointer. This is due to the fact that the address space of
/// the pointer T* is different on the host and the device.
template <typename Expr>
struct ConvertToDeviceExpression;

template<template<class...> class NonOpCategory, bool IsConst, typename... Args>
struct NonOpConversion{
  typedef typename GetType<IsConst, NonOpCategory<typename ConvertToDeviceExpression<Args>::Type...> >::Type Type;
};


template<template<class, template <class> class > class NonOpCategory, bool IsConst, typename Args>
struct DeviceConvertor{
  typedef typename GetType<IsConst, NonOpCategory<typename ConvertToDeviceExpression<Args>::Type, MakeGlobalPointer> >::Type Type;
};

/// specialisation of the \ref ConvertToDeviceExpression struct when the node
/// type is TensorMap
#define TENSORMAPCONVERT(CVQual)\
template <typename T,  int Options_, template <class> class MakePointer_>\
struct ConvertToDeviceExpression<CVQual TensorMap<T, Options_, MakePointer_> > {\
  typedef CVQual TensorMap<T, Options_, MakeGlobalPointer> Type;\
};

TENSORMAPCONVERT(const)
TENSORMAPCONVERT()
#undef TENSORMAPCONVERT

/// specialisation of the \ref ConvertToDeviceExpression struct when the node
/// type is TensorCwiseNullaryOp, TensorCwiseUnaryOp, TensorCwiseBinaryOp, TensorCwiseTernaryOp, TensorBroadcastingOp
#define CATEGORYCONVERT(CVQual)\
template <template<class, class...> class Category, typename OP, typename... subExprs>\
struct ConvertToDeviceExpression<CVQual Category<OP, subExprs...> > {\
  typedef CVQual Category<OP, typename ConvertToDeviceExpression<subExprs>::Type... > Type;\
};
CATEGORYCONVERT(const)
CATEGORYCONVERT()
#undef CATEGORYCONVERT


/// specialisation of the \ref ConvertToDeviceExpression struct when the node
/// type is  TensorCwiseSelectOp
#define SELECTOPCONVERT(CVQual, Res)\
template <typename IfExpr, typename ThenExpr, typename ElseExpr>\
struct ConvertToDeviceExpression<CVQual TensorSelectOp<IfExpr, ThenExpr, ElseExpr> >\
: NonOpConversion<TensorSelectOp, Res, IfExpr, ThenExpr, ElseExpr> {};
SELECTOPCONVERT(const, true)
SELECTOPCONVERT(, false)
#undef SELECTOPCONVERT

/// specialisation of the \ref ConvertToDeviceExpression struct when the node
/// type is const AssingOP
#define ASSIGNCONVERT(CVQual, Res)\
template <typename LHSExpr, typename RHSExpr>\
struct ConvertToDeviceExpression<CVQual TensorAssignOp<LHSExpr, RHSExpr> >\
: NonOpConversion<TensorAssignOp, Res, LHSExpr, RHSExpr>{};

ASSIGNCONVERT(const, true)
ASSIGNCONVERT(, false)
#undef ASSIGNCONVERT

/// specialisation of the \ref ConvertToDeviceExpression struct when the node
/// type is  TensorEvalToOp
#define KERNELBROKERCONVERT(CVQual, Res, ExprNode)\
template <typename Expr>\
struct ConvertToDeviceExpression<CVQual ExprNode<Expr> > \
: DeviceConvertor<ExprNode, Res, Expr>{};


KERNELBROKERCONVERT(const, true, TensorEvalToOp)
KERNELBROKERCONVERT(, false, TensorEvalToOp)
#undef KERNELBROKERCONVERT

/// specialisation of the \ref ConvertToDeviceExpression struct when the node types are TensorForcedEvalOp and TensorLayoutSwapOp
#define KERNELBROKERCONVERTFORCEDEVALLAYOUTSWAPINDEXTUPLEOP(CVQual, ExprNode)\
template <typename Expr>\
struct ConvertToDeviceExpression<CVQual ExprNode<Expr> > {\
  typedef CVQual ExprNode< typename ConvertToDeviceExpression<Expr>::Type> Type;\
};


// TensorForcedEvalOp
KERNELBROKERCONVERTFORCEDEVALLAYOUTSWAPINDEXTUPLEOP(const,TensorForcedEvalOp)
KERNELBROKERCONVERTFORCEDEVALLAYOUTSWAPINDEXTUPLEOP(,TensorForcedEvalOp)

// TensorLayoutSwapOp
KERNELBROKERCONVERTFORCEDEVALLAYOUTSWAPINDEXTUPLEOP(const,TensorLayoutSwapOp)
KERNELBROKERCONVERTFORCEDEVALLAYOUTSWAPINDEXTUPLEOP(,TensorLayoutSwapOp)

//TensorIndexTupleOp
KERNELBROKERCONVERTFORCEDEVALLAYOUTSWAPINDEXTUPLEOP(const,TensorIndexTupleOp)
KERNELBROKERCONVERTFORCEDEVALLAYOUTSWAPINDEXTUPLEOP(,TensorIndexTupleOp)
#undef KERNELBROKERCONVERTFORCEDEVALLAYOUTSWAPINDEXTUPLEOP

/// specialisation of the \ref ConvertToDeviceExpression struct when the node type is TensorReductionOp
#define KERNELBROKERCONVERTREDUCTION(CVQual)\
template <typename OP, typename Dim, typename subExpr, template <class> class MakePointer_>\
struct ConvertToDeviceExpression<CVQual TensorReductionOp<OP, Dim, subExpr, MakePointer_> > {\
  typedef CVQual TensorReductionOp<OP, Dim, typename ConvertToDeviceExpression<subExpr>::Type, MakeGlobalPointer> Type;\
};

KERNELBROKERCONVERTREDUCTION(const)
KERNELBROKERCONVERTREDUCTION()
#undef KERNELBROKERCONVERTREDUCTION

/// specialisation of the \ref ConvertToDeviceExpression struct when the node type is TensorReductionOp
#define KERNELBROKERCONVERTTUPLEREDUCTION(CVQual)\
template <typename OP, typename Dim, typename subExpr>\
struct ConvertToDeviceExpression<CVQual TensorTupleReducerOp<OP, Dim, subExpr> > {\
  typedef CVQual TensorTupleReducerOp<OP, Dim, typename ConvertToDeviceExpression<subExpr>::Type> Type;\
};

KERNELBROKERCONVERTTUPLEREDUCTION(const)
KERNELBROKERCONVERTTUPLEREDUCTION()
#undef KERNELBROKERCONVERTTUPLEREDUCTION

//TensorSlicingOp
#define KERNELBROKERCONVERTSLICEOP(CVQual)\
template<typename StartIndices, typename Sizes, typename XprType>\
struct ConvertToDeviceExpression<CVQual TensorSlicingOp <StartIndices, Sizes, XprType> >{\
  typedef CVQual TensorSlicingOp<StartIndices, Sizes, typename ConvertToDeviceExpression<XprType>::Type> Type;\
};

KERNELBROKERCONVERTSLICEOP(const)
KERNELBROKERCONVERTSLICEOP()
#undef KERNELBROKERCONVERTSLICEOP

//TensorStridingSlicingOp
#define KERNELBROKERCONVERTERSLICESTRIDEOP(CVQual)\
template<typename StartIndices, typename StopIndices, typename Strides, typename XprType>\
struct ConvertToDeviceExpression<CVQual TensorStridingSlicingOp<StartIndices, StopIndices, Strides, XprType> >{\
  typedef CVQual TensorStridingSlicingOp<StartIndices, StopIndices, Strides, typename ConvertToDeviceExpression<XprType>::Type> Type;\
};

KERNELBROKERCONVERTERSLICESTRIDEOP(const)
KERNELBROKERCONVERTERSLICESTRIDEOP()
#undef KERNELBROKERCONVERTERSLICESTRIDEOP

/// specialisation of the \ref ConvertToDeviceExpression struct when the node type is TensorChippingOp
#define KERNELBROKERCONVERTCHIPPINGOP(CVQual)\
template <DenseIndex DimId, typename Expr>\
struct ConvertToDeviceExpression<CVQual TensorChippingOp<DimId, Expr> > {\
  typedef CVQual TensorChippingOp<DimId, typename ConvertToDeviceExpression<Expr>::Type> Type;\
};
KERNELBROKERCONVERTCHIPPINGOP(const)
KERNELBROKERCONVERTCHIPPINGOP()
#undef KERNELBROKERCONVERTCHIPPINGOP

/// specialisation of the \ref ConvertToDeviceExpression struct when the node type is TensorImagePatchOp
#define KERNELBROKERCONVERTIMAGEPATCHOP(CVQual)\
template<DenseIndex Rows, DenseIndex Cols, typename XprType>\
struct ConvertToDeviceExpression<CVQual TensorImagePatchOp<Rows, Cols, XprType> >{\
  typedef CVQual TensorImagePatchOp<Rows, Cols, typename ConvertToDeviceExpression<XprType>::Type> Type;\
};
KERNELBROKERCONVERTIMAGEPATCHOP(const)
KERNELBROKERCONVERTIMAGEPATCHOP()
#undef KERNELBROKERCONVERTIMAGEPATCHOP


/// specialisation of the \ref ConvertToDeviceExpression struct when the node type is TensorVolumePatchOp
#define KERNELBROKERCONVERTVOLUMEPATCHOP(CVQual)\
template<DenseIndex Plannes, DenseIndex Rows, DenseIndex Cols, typename XprType>\
struct ConvertToDeviceExpression<CVQual TensorVolumePatchOp<Plannes, Rows, Cols, XprType> >{\
  typedef CVQual TensorVolumePatchOp<Plannes, Rows, Cols, typename ConvertToDeviceExpression<XprType>::Type> Type;\
};
KERNELBROKERCONVERTVOLUMEPATCHOP(const)
KERNELBROKERCONVERTVOLUMEPATCHOP()
#undef KERNELBROKERCONVERTVOLUMEPATCHOP

}  // namespace internal
}  // namespace TensorSycl
}  // namespace Eigen

#endif  // UNSUPPORTED_EIGEN_CXX1
