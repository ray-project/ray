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
 *  TensorArgMaxSycl.h
 * \brief:
 *  TensorArgMaxSycl
 *
*****************************************************************/

#ifndef UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSOR_ARGMAX_SYCL_HPP
#define UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSOR_ARGMAX_SYCL_HPP
namespace Eigen {
namespace internal {
  template<typename Dims, typename XprType>
  struct eval<TensorTupleReducerDeviceOp<Dims, XprType>, Eigen::Dense>
  {
    typedef const TensorTupleReducerDeviceOp<Dims, XprType>& type;
  };

  template<typename Dims, typename XprType>
  struct nested<TensorTupleReducerDeviceOp<Dims, XprType>, 1,
                typename eval<TensorTupleReducerDeviceOp<Dims, XprType> >::type>
  {
    typedef TensorTupleReducerDeviceOp<Dims, XprType> type;
  };

template<typename StrideDims, typename XprType>
struct traits<TensorTupleReducerDeviceOp<StrideDims, XprType> > : public traits<XprType>
{
  typedef traits<XprType> XprTraits;
  typedef typename XprTraits::StorageKind StorageKind;
  typedef typename XprTraits::Index Index;
  typedef Index Scalar;
  typedef typename XprType::Nested Nested;
  typedef typename remove_reference<Nested>::type _Nested;
  static const int NumDimensions = XprTraits::NumDimensions;
  static const int Layout = XprTraits::Layout;
};


}// end namespace internal
template<typename StrideDims, typename XprType>
class TensorTupleReducerDeviceOp : public TensorBase<TensorTupleReducerDeviceOp<StrideDims, XprType>, ReadOnlyAccessors>
{
  public:
  typedef typename Eigen::internal::traits<TensorTupleReducerDeviceOp>::Scalar Scalar;
  typedef typename Eigen::NumTraits<Scalar>::Real RealScalar;
  typedef typename Eigen::internal::nested<TensorTupleReducerDeviceOp>::type Nested;
  typedef typename Eigen::internal::traits<TensorTupleReducerDeviceOp>::StorageKind StorageKind;
  typedef typename Eigen::internal::traits<TensorTupleReducerDeviceOp>::Index Index;
  typedef typename XprType::CoeffReturnType TupleType;
  typedef Index CoeffReturnType;

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE TensorTupleReducerDeviceOp(XprType expr,
                                                              const Index return_dim,
                                                              const StrideDims strides,
                                                              const Index stride_mod, const Index stride_div)
          :m_xpr(expr), m_return_dim(return_dim), m_strides(strides), m_stride_mod(stride_mod), m_stride_div(stride_div) {}

  EIGEN_DEVICE_FUNC
  const typename internal::remove_all<typename XprType::Nested>::type&
  expression() const { return m_xpr; }

  EIGEN_DEVICE_FUNC
  Index return_dim() const { return m_return_dim; }

  EIGEN_DEVICE_FUNC
  const StrideDims& strides() const { return m_strides; }

  EIGEN_DEVICE_FUNC
  const Index& stride_mod() const { return m_stride_mod; }

  EIGEN_DEVICE_FUNC
  const Index& stride_div() const { return m_stride_div; }

  protected:
    typename Eigen::internal::remove_all<typename
    XprType::Nested
    >::type m_xpr;
    const Index m_return_dim;
    const StrideDims m_strides;
    const Index m_stride_mod;
    const Index m_stride_div;
};


// Eval as rvalue
template<typename StrideDims, typename ArgType>
struct TensorEvaluator<const TensorTupleReducerDeviceOp<StrideDims, ArgType>, SyclKernelDevice>
{
  typedef TensorTupleReducerDeviceOp<StrideDims, ArgType> XprType;
  typedef typename XprType::Index Index;
  typedef typename XprType::Scalar Scalar;
  typedef typename XprType::CoeffReturnType CoeffReturnType;
  typedef typename XprType::TupleType TupleType;
  typedef typename TensorEvaluator<ArgType, SyclKernelDevice>::Dimensions Dimensions;

  enum {
    IsAligned =  false,
    PacketAccess = false,
    BlockAccess = false,
    PreferBlockAccess = false,
    Layout = TensorEvaluator<ArgType, SyclKernelDevice>::Layout,
    CoordAccess = false,
    RawAccess = false
  };

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE TensorEvaluator(const XprType& op,  const SyclKernelDevice& device)
      : m_impl(op.expression(), device), m_return_dim(op.return_dim()), m_strides(op.strides()), m_stride_mod(op.stride_mod()),
      m_stride_div(op.stride_div()){}
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE const Dimensions& dimensions() const {
    return m_impl.dimensions();
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE bool evalSubExprsIfNeeded(Scalar*) {
    m_impl.evalSubExprsIfNeeded(NULL);
    return true;
  }
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void cleanup() {
    m_impl.cleanup();
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE CoeffReturnType coeff(Index index) const {
    const TupleType v = m_impl.coeff(index);
    return (m_return_dim < 0) ? v.first : (v.first % m_stride_mod) / m_stride_div;
  }
typedef typename MakeGlobalPointer<typename TensorEvaluator<ArgType , SyclKernelDevice>::CoeffReturnType >::Type ptr_Dev_type;
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE ptr_Dev_type  data() const { return const_cast<ptr_Dev_type>(m_impl.data()); }

protected:
 TensorEvaluator<ArgType , SyclKernelDevice> m_impl;
 const Index m_return_dim;
 const StrideDims m_strides;
 const Index m_stride_mod;
 const Index m_stride_div;
};
} // end namespace Eigen
#endif //UNSUPPORTED_EIGEN_CXX11_SRC_TENSOR_TENSOR_ARGMAX_SYCL_HPP
