// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2014 Benoit Steiner <benoit.steiner.goog@gmail.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#ifndef EIGEN_CXX11_TENSOR_TENSOR_BROADCASTING_H
#define EIGEN_CXX11_TENSOR_TENSOR_BROADCASTING_H

namespace Eigen {

/** \class TensorBroadcasting
  * \ingroup CXX11_Tensor_Module
  *
  * \brief Tensor broadcasting class.
  *
  *
  */
namespace internal {
template<typename Broadcast, typename XprType>
struct traits<TensorBroadcastingOp<Broadcast, XprType> > : public traits<XprType>
{
  typedef typename XprType::Scalar Scalar;
  typedef traits<XprType> XprTraits;
  typedef typename XprTraits::StorageKind StorageKind;
  typedef typename XprTraits::Index Index;
  typedef typename XprType::Nested Nested;
  typedef typename remove_reference<Nested>::type _Nested;
  static const int NumDimensions = XprTraits::NumDimensions;
  static const int Layout = XprTraits::Layout;
  typedef typename XprTraits::PointerType PointerType;
};

template<typename Broadcast, typename XprType>
struct eval<TensorBroadcastingOp<Broadcast, XprType>, Eigen::Dense>
{
  typedef const TensorBroadcastingOp<Broadcast, XprType>& type;
};

template<typename Broadcast, typename XprType>
struct nested<TensorBroadcastingOp<Broadcast, XprType>, 1, typename eval<TensorBroadcastingOp<Broadcast, XprType> >::type>
{
  typedef TensorBroadcastingOp<Broadcast, XprType> type;
};

template <typename Dims>
struct is_input_scalar {
  static const bool value = false;
};
template <>
struct is_input_scalar<Sizes<> > {
  static const bool value = true;
};
#ifndef EIGEN_EMULATE_CXX11_META_H
template <typename std::ptrdiff_t... Indices>
struct is_input_scalar<Sizes<Indices...> > {
  static const bool value = (Sizes<Indices...>::total_size == 1);
};
#endif

}  // end namespace internal



template<typename Broadcast, typename XprType>
class TensorBroadcastingOp : public TensorBase<TensorBroadcastingOp<Broadcast, XprType>, ReadOnlyAccessors>
{
  public:
  typedef typename Eigen::internal::traits<TensorBroadcastingOp>::Scalar Scalar;
  typedef typename Eigen::NumTraits<Scalar>::Real RealScalar;
  typedef typename XprType::CoeffReturnType CoeffReturnType;
  typedef typename Eigen::internal::nested<TensorBroadcastingOp>::type Nested;
  typedef typename Eigen::internal::traits<TensorBroadcastingOp>::StorageKind StorageKind;
  typedef typename Eigen::internal::traits<TensorBroadcastingOp>::Index Index;

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE TensorBroadcastingOp(const XprType& expr, const Broadcast& broadcast)
      : m_xpr(expr), m_broadcast(broadcast) {}

    EIGEN_DEVICE_FUNC
    const Broadcast& broadcast() const { return m_broadcast; }

    EIGEN_DEVICE_FUNC
    const typename internal::remove_all<typename XprType::Nested>::type&
    expression() const { return m_xpr; }

  protected:
    typename XprType::Nested m_xpr;
    const Broadcast m_broadcast;
};


// Eval as rvalue
template<typename Broadcast, typename ArgType, typename Device>
struct TensorEvaluator<const TensorBroadcastingOp<Broadcast, ArgType>, Device>
{
  typedef TensorBroadcastingOp<Broadcast, ArgType> XprType;
  typedef typename XprType::Index Index;
  static const int NumDims = internal::array_size<typename TensorEvaluator<ArgType, Device>::Dimensions>::value;
  typedef DSizes<Index, NumDims> Dimensions;
  typedef typename XprType::Scalar Scalar;
  typedef typename TensorEvaluator<ArgType, Device>::Dimensions InputDimensions;
  typedef typename XprType::CoeffReturnType CoeffReturnType;
  typedef typename PacketType<CoeffReturnType, Device>::type PacketReturnType;
  static const int PacketSize = PacketType<CoeffReturnType, Device>::size;
  bool isCopy, nByOne, oneByN;

  enum {
    IsAligned         = true,
    PacketAccess      = TensorEvaluator<ArgType, Device>::PacketAccess,
    BlockAccess       = TensorEvaluator<ArgType, Device>::BlockAccess,
    PreferBlockAccess = true,
    Layout            = TensorEvaluator<ArgType, Device>::Layout,
    RawAccess         = false
  };

  typedef typename internal::remove_const<Scalar>::type ScalarNoConst;

  // Block based access to the XprType (input) tensor.
  typedef internal::TensorBlock<ScalarNoConst, Index, NumDims, Layout>
      TensorBlock;
  typedef internal::TensorBlockReader<ScalarNoConst, Index, NumDims, Layout>
      TensorBlockReader;

  // We do block based broadcasting using a trick with 2x tensor rank and 0
  // strides. See block method implementation for details.
  typedef DSizes<Index, 2 * NumDims> BroadcastDimensions;
  typedef internal::TensorBlock<ScalarNoConst, Index, 2 * NumDims, Layout>
      BroadcastTensorBlock;
  typedef internal::TensorBlockReader<ScalarNoConst, Index, 2 * NumDims, Layout>
      BroadcastTensorBlockReader;

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE TensorEvaluator(const XprType& op,
                                                        const Device& device)
      : isCopy(false), nByOne(false), oneByN(false),
        m_device(device), m_broadcast(op.broadcast()), m_impl(op.expression(), device)
  {

    // The broadcasting op doesn't change the rank of the tensor. One can't broadcast a scalar
    // and store the result in a scalar. Instead one should reshape the scalar into a a N-D
    // tensor with N >= 1 of 1 element first and then broadcast.
    EIGEN_STATIC_ASSERT((NumDims > 0), YOU_MADE_A_PROGRAMMING_MISTAKE);
    const InputDimensions& input_dims = m_impl.dimensions();
    isCopy = true;
    for (int i = 0; i < NumDims; ++i) {
      eigen_assert(input_dims[i] > 0);
      m_dimensions[i] = input_dims[i] * m_broadcast[i];
      if (m_broadcast[i] != 1) {
        isCopy = false;
      }
    }

    if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
      m_inputStrides[0] = 1;
      m_outputStrides[0] = 1;
      for (int i = 1; i < NumDims; ++i) {
        m_inputStrides[i] = m_inputStrides[i-1] * input_dims[i-1];
        m_outputStrides[i] = m_outputStrides[i-1] * m_dimensions[i-1];
      }
    } else {
      m_inputStrides[NumDims-1] = 1;
      m_outputStrides[NumDims-1] = 1;
      for (int i = NumDims-2; i >= 0; --i) {
        m_inputStrides[i] = m_inputStrides[i+1] * input_dims[i+1];
        m_outputStrides[i] = m_outputStrides[i+1] * m_dimensions[i+1];
      }
    }

    if (input_dims[0] == 1) {
      oneByN = true;
      for (int i = 1; i < NumDims; ++i) {
        if (m_broadcast[i] != 1) {
          oneByN = false;
          break;
        }
      }
    } else if (input_dims[NumDims-1] == 1) {
      nByOne = true;
      for (int i = 0; i < NumDims-1; ++i) {
        if (m_broadcast[i] != 1) {
          nByOne = false;
          break;
        }
      }
    }

    // Handle special format like NCHW, its input shape is '[1, N..., 1]' and
    // broadcast shape is '[N, 1..., N]'
    if (!oneByN && !nByOne) {
      if (input_dims[0] == 1 && input_dims[NumDims-1] == 1 && NumDims > 2) {
        nByOne = true;
        oneByN = true;
        for (int i = 1; i < NumDims-1; ++i) {
          if (m_broadcast[i] != 1) {
            nByOne = false;
            oneByN = false;
            break;
          }
        }
      }
    }
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE const Dimensions& dimensions() const { return m_dimensions; }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE bool evalSubExprsIfNeeded(Scalar* /*data*/) {
    m_impl.evalSubExprsIfNeeded(NULL);
    return true;
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void cleanup() {
    m_impl.cleanup();
  }

  EIGEN_DEVICE_FUNC EIGEN_ALWAYS_INLINE CoeffReturnType coeff(Index index) const
  {
    if (internal::is_input_scalar<typename internal::remove_all<InputDimensions>::type>::value) {
      return m_impl.coeff(0);
    }

    if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
      if (isCopy) {
        return m_impl.coeff(index);
      } else {
        return coeffColMajor(index);
      }
    } else {
      if (isCopy) {
        return m_impl.coeff(index);
      } else {
        return coeffRowMajor(index);
      }
    }
  }

  // TODO: attempt to speed this up. The integer divisions and modulo are slow
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE Index indexColMajor(Index index) const {
    Index inputIndex = 0;
    for (int i = NumDims - 1; i > 0; --i) {
      const Index idx = index / m_outputStrides[i];
      if (internal::index_statically_eq<Broadcast>(i, 1)) {
        eigen_assert(idx < m_impl.dimensions()[i]);
        inputIndex += idx * m_inputStrides[i];
      } else {
        if (internal::index_statically_eq<InputDimensions>(i, 1)) {
          eigen_assert(idx % m_impl.dimensions()[i] == 0);
        } else {
          inputIndex += (idx % m_impl.dimensions()[i]) * m_inputStrides[i];
        }
      }
      index -= idx * m_outputStrides[i];
    }
    if (internal::index_statically_eq<Broadcast>(0, 1)) {
      eigen_assert(index < m_impl.dimensions()[0]);
      inputIndex += index;
    } else {
      if (internal::index_statically_eq<InputDimensions>(0, 1)) {
        eigen_assert(index % m_impl.dimensions()[0] == 0);
      } else {
        inputIndex += (index % m_impl.dimensions()[0]);
      }
    }
    return inputIndex;
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE CoeffReturnType coeffColMajor(Index index) const
  {
    return m_impl.coeff(indexColMajor(index));
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE Index indexRowMajor(Index index) const {
    Index inputIndex = 0;
    for (int i = 0; i < NumDims - 1; ++i) {
      const Index idx = index / m_outputStrides[i];
      if (internal::index_statically_eq<Broadcast>(i, 1)) {
        eigen_assert(idx < m_impl.dimensions()[i]);
        inputIndex += idx * m_inputStrides[i];
      } else {
        if (internal::index_statically_eq<InputDimensions>(i, 1)) {
          eigen_assert(idx % m_impl.dimensions()[i] == 0);
        } else {
          inputIndex += (idx % m_impl.dimensions()[i]) * m_inputStrides[i];
        }
      }
      index -= idx * m_outputStrides[i];
    }
    if (internal::index_statically_eq<Broadcast>(NumDims - 1, 1)) {
      eigen_assert(index < m_impl.dimensions()[NumDims - 1]);
      inputIndex += index;
    } else {
      if (internal::index_statically_eq<InputDimensions>(NumDims - 1, 1)) {
        eigen_assert(index % m_impl.dimensions()[NumDims - 1] == 0);
      } else {
        inputIndex += (index % m_impl.dimensions()[NumDims - 1]);
      }
    }
    return inputIndex;
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE CoeffReturnType coeffRowMajor(Index index) const
  {
    return m_impl.coeff(indexRowMajor(index));
  }

  template<int LoadMode>
  EIGEN_DEVICE_FUNC EIGEN_ALWAYS_INLINE PacketReturnType packet(Index index) const
  {
    if (internal::is_input_scalar<typename internal::remove_all<InputDimensions>::type>::value) {
      return internal::pset1<PacketReturnType>(m_impl.coeff(0));
    }

    if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
      if (isCopy) {
        #ifdef EIGEN_GPU_COMPILE_PHASE
        // See PR 437: on NVIDIA P100 and K20m we observed a x3-4 speed up by enforcing
        // unaligned loads here. The reason is unclear though.
        return m_impl.template packet<Unaligned>(index);
        #else
        return m_impl.template packet<LoadMode>(index);
        #endif
      } else if (oneByN && !nByOne) {
        return packetNByOne<LoadMode>(index);
      } else if (!oneByN && nByOne) {
        return packetOneByN<LoadMode>(index);
      } else if (oneByN && nByOne) {
        return packetOneByNByOne<LoadMode>(index);
      } else {
        return packetColMajor<LoadMode>(index);
      }
    } else {
      if (isCopy) {
        #ifdef EIGEN_GPU_COMPILE_PHASE
        // See above.
        return m_impl.template packet<Unaligned>(index);
        #else
        return m_impl.template packet<LoadMode>(index);
        #endif
      } else if (oneByN && !nByOne) {
        return packetOneByN<LoadMode>(index);
      } else if (!oneByN && nByOne) {
        return packetNByOne<LoadMode>(index);
      } else if (oneByN && nByOne) {
        return packetOneByNByOne<LoadMode>(index);
      } else {
        return packetRowMajor<LoadMode>(index);
      }
    }
  }

  template<int LoadMode>
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE PacketReturnType packetOneByNByOne
  (Index index) const
  {
    EIGEN_STATIC_ASSERT((PacketSize > 1), YOU_MADE_A_PROGRAMMING_MISTAKE)
    eigen_assert(index+PacketSize-1 < dimensions().TotalSize());

    EIGEN_ALIGN_MAX typename internal::remove_const<CoeffReturnType>::type values[PacketSize];
    Index startDim, endDim;
    Index inputIndex, outputOffset, batchedIndex;

    if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
      startDim = NumDims - 1;
      endDim = 1;
    } else {
      startDim = 0;
      endDim = NumDims - 2;
    }

    batchedIndex = index % m_outputStrides[startDim];
    inputIndex   = batchedIndex / m_outputStrides[endDim];
    outputOffset = batchedIndex % m_outputStrides[endDim];

    if (outputOffset + PacketSize <= m_outputStrides[endDim]) {
      values[0] = m_impl.coeff(inputIndex);
      return internal::pload1<PacketReturnType>(values);
    } else {
      for (int i = 0, cur = 0; i < PacketSize; ++i, ++cur) {
        if (outputOffset + cur < m_outputStrides[endDim]) {
          values[i] = m_impl.coeff(inputIndex);
        } else {
          ++inputIndex;
          inputIndex = (inputIndex == m_inputStrides[startDim] ? 0 : inputIndex);
          values[i] = m_impl.coeff(inputIndex);
          outputOffset = 0;
          cur = 0;
        }
      }
      return internal::pload<PacketReturnType>(values);
    }
  }

  template<int LoadMode>
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE PacketReturnType packetOneByN(Index index) const
  {
    EIGEN_STATIC_ASSERT((PacketSize > 1), YOU_MADE_A_PROGRAMMING_MISTAKE)
    eigen_assert(index+PacketSize-1 < dimensions().TotalSize());

    Index dim, inputIndex;

    if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
      dim = NumDims - 1;
    } else {
      dim = 0;
    }

    inputIndex = index % m_inputStrides[dim];
    if (inputIndex + PacketSize <= m_inputStrides[dim]) {
      return m_impl.template packet<Unaligned>(inputIndex);
    } else {
      EIGEN_ALIGN_MAX typename internal::remove_const<CoeffReturnType>::type values[PacketSize];
      for (int i = 0; i < PacketSize; ++i) {
        if (inputIndex > m_inputStrides[dim]-1) {
          inputIndex = 0;
        }
        values[i] = m_impl.coeff(inputIndex++);
      }
      return internal::pload<PacketReturnType>(values);
    }
  }

  template<int LoadMode>
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE PacketReturnType packetNByOne(Index index) const
  {
    EIGEN_STATIC_ASSERT((PacketSize > 1), YOU_MADE_A_PROGRAMMING_MISTAKE)
    eigen_assert(index+PacketSize-1 < dimensions().TotalSize());

    EIGEN_ALIGN_MAX typename internal::remove_const<CoeffReturnType>::type values[PacketSize];
    Index dim, inputIndex, outputOffset;

    if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
      dim = 1;
    } else {
      dim = NumDims - 2;
    }

    inputIndex   = index / m_outputStrides[dim];
    outputOffset = index % m_outputStrides[dim];
    if (outputOffset + PacketSize <= m_outputStrides[dim]) {
      values[0] = m_impl.coeff(inputIndex);
      return internal::pload1<PacketReturnType>(values);
    } else {
      for (int i = 0, cur = 0; i < PacketSize; ++i, ++cur) {
        if (outputOffset + cur < m_outputStrides[dim]) {
          values[i] = m_impl.coeff(inputIndex);
        } else {
          values[i] = m_impl.coeff(++inputIndex);
          outputOffset = 0;
          cur = 0;
        }
      }
      return internal::pload<PacketReturnType>(values);
    }
  }

  // Ignore the LoadMode and always use unaligned loads since we can't guarantee
  // the alignment at compile time.
  template<int LoadMode>
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE PacketReturnType packetColMajor(Index index) const
  {
    EIGEN_STATIC_ASSERT((PacketSize > 1), YOU_MADE_A_PROGRAMMING_MISTAKE)
    eigen_assert(index+PacketSize-1 < dimensions().TotalSize());

    const Index originalIndex = index;

    Index inputIndex = 0;
    for (int i = NumDims - 1; i > 0; --i) {
      const Index idx = index / m_outputStrides[i];
      if (internal::index_statically_eq<Broadcast>(i, 1)) {
        eigen_assert(idx < m_impl.dimensions()[i]);
        inputIndex += idx * m_inputStrides[i];
      } else {
        if (internal::index_statically_eq<InputDimensions>(i, 1)) {
          eigen_assert(idx % m_impl.dimensions()[i] == 0);
        } else {
          inputIndex += (idx % m_impl.dimensions()[i]) * m_inputStrides[i];
        }
      }
      index -= idx * m_outputStrides[i];
    }
    Index innermostLoc;
    if (internal::index_statically_eq<Broadcast>(0, 1)) {
      eigen_assert(index < m_impl.dimensions()[0]);
      innermostLoc = index;
    } else {
      if (internal::index_statically_eq<InputDimensions>(0, 1)) {
        eigen_assert(index % m_impl.dimensions()[0] == 0);
        innermostLoc = 0;
      } else {
        innermostLoc = index % m_impl.dimensions()[0];
      }
    }
    inputIndex += innermostLoc;

    // Todo: this could be extended to the second dimension if we're not
    // broadcasting alongside the first dimension, and so on.
    if (innermostLoc + PacketSize <= m_impl.dimensions()[0]) {
      return m_impl.template packet<Unaligned>(inputIndex);
    } else {
      EIGEN_ALIGN_MAX typename internal::remove_const<CoeffReturnType>::type values[PacketSize];
      values[0] = m_impl.coeff(inputIndex);
      for (int i = 1; i < PacketSize; ++i) {
        if (innermostLoc + i < m_impl.dimensions()[0]) {
          values[i] = m_impl.coeff(inputIndex+i);
        } else {
          values[i] = coeffColMajor(originalIndex+i);
        }
      }
      PacketReturnType rslt = internal::pload<PacketReturnType>(values);
      return rslt;
    }
  }

  template<int LoadMode>
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE PacketReturnType packetRowMajor(Index index) const
  {
    EIGEN_STATIC_ASSERT((PacketSize > 1), YOU_MADE_A_PROGRAMMING_MISTAKE)
    eigen_assert(index+PacketSize-1 < dimensions().TotalSize());

    const Index originalIndex = index;

    Index inputIndex = 0;
    for (int i = 0; i < NumDims - 1; ++i) {
      const Index idx = index / m_outputStrides[i];
      if (internal::index_statically_eq<Broadcast>(i, 1)) {
        eigen_assert(idx < m_impl.dimensions()[i]);
        inputIndex += idx * m_inputStrides[i];
      } else {
        if (internal::index_statically_eq<InputDimensions>(i, 1)) {
          eigen_assert(idx % m_impl.dimensions()[i] == 0);
        } else {
          inputIndex += (idx % m_impl.dimensions()[i]) * m_inputStrides[i];
        }
      }
      index -= idx * m_outputStrides[i];
    }
    Index innermostLoc;
    if (internal::index_statically_eq<Broadcast>(NumDims-1, 1)) {
      eigen_assert(index < m_impl.dimensions()[NumDims-1]);
      innermostLoc = index;
    } else {
      if (internal::index_statically_eq<InputDimensions>(NumDims-1, 1)) {
        eigen_assert(index % m_impl.dimensions()[NumDims-1] == 0);
        innermostLoc = 0;
      } else {
        innermostLoc = index % m_impl.dimensions()[NumDims-1];
      }
    }
    inputIndex += innermostLoc;

    // Todo: this could be extended to the second dimension if we're not
    // broadcasting alongside the first dimension, and so on.
    if (innermostLoc + PacketSize <= m_impl.dimensions()[NumDims-1]) {
      return m_impl.template packet<Unaligned>(inputIndex);
    } else {
      EIGEN_ALIGN_MAX typename internal::remove_const<CoeffReturnType>::type values[PacketSize];
      values[0] = m_impl.coeff(inputIndex);
      for (int i = 1; i < PacketSize; ++i) {
        if (innermostLoc + i < m_impl.dimensions()[NumDims-1]) {
          values[i] = m_impl.coeff(inputIndex+i);
        } else {
          values[i] = coeffRowMajor(originalIndex+i);
        }
      }
      PacketReturnType rslt = internal::pload<PacketReturnType>(values);
      return rslt;
    }
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE TensorOpCost
  costPerCoeff(bool vectorized) const {
    double compute_cost = TensorOpCost::AddCost<Index>();
    if (!isCopy && NumDims > 0) {
      for (int i = NumDims - 1; i > 0; --i) {
        compute_cost += TensorOpCost::DivCost<Index>();
        if (internal::index_statically_eq<Broadcast>(i, 1)) {
          compute_cost +=
              TensorOpCost::MulCost<Index>() + TensorOpCost::AddCost<Index>();
        } else {
          if (!internal::index_statically_eq<InputDimensions>(i, 1)) {
            compute_cost += TensorOpCost::MulCost<Index>() +
                            TensorOpCost::ModCost<Index>() +
                            TensorOpCost::AddCost<Index>();
          }
        }
        compute_cost +=
            TensorOpCost::MulCost<Index>() + TensorOpCost::AddCost<Index>();
      }
    }
    return m_impl.costPerCoeff(vectorized) +
           TensorOpCost(0, 0, compute_cost, vectorized, PacketSize);
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void getResourceRequirements(
      std::vector<internal::TensorOpResourceRequirements>* resources) const {
    // TODO(wuke): Targeting L1 size is 30% faster than targeting L{-1} on large
    // tensors. But this might need further tuning.
    Eigen::Index block_total_size_max = numext::maxi<Eigen::Index>(
        1, m_device.firstLevelCacheSize() / sizeof(Scalar));

    resources->push_back(internal::TensorOpResourceRequirements(
        internal::kSkewedInnerDims, block_total_size_max));

    m_impl.getResourceRequirements(resources);
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void block(
      TensorBlock* output_block) const {
    if (NumDims <= 0) {
      output_block->data()[0] = m_impl.coeff(0);
      return;
    }

    // Because we only support kSkewedInnerDims blocking, block size should be
    // equal to m_dimensions for inner dims, a smaller than m_dimensions[i] size
    // for the first outer dim, and 1 for other outer dims. This is guaranteed
    // by MergeResourceRequirements() in TensorBlock.h.
    const Dimensions& output_block_sizes = output_block->block_sizes();
    const Dimensions& output_block_strides = output_block->block_strides();

    // Find where outer dims start.
    int outer_dim_start = 0;
    Index outer_dim_size = 1, inner_dim_size = 1;
    for (int i = 0; i < NumDims; ++i) {
      const int dim = static_cast<int>(Layout) == static_cast<int>(ColMajor)
                          ? i
                          : NumDims - i - 1;
      if (i > outer_dim_start) {
        eigen_assert(output_block_sizes[dim] == 1);
      } else if (output_block_sizes[dim] != m_dimensions[dim]) {
        eigen_assert(output_block_sizes[dim] < m_dimensions[dim]);
        outer_dim_size = output_block_sizes[dim];
      } else {
        inner_dim_size *= output_block_sizes[dim];
        ++outer_dim_start;
      }
    }

    if (inner_dim_size == 0 || outer_dim_size == 0) {
      return;
    }

    const Dimensions& input_dims = Dimensions(m_impl.dimensions());

    // Pre-fill input_block_sizes, broadcast_block_sizes,
    // broadcast_block_strides, and broadcast_tensor_strides. Later on we will
    // only modify the outer_dim_start-th dimension on these arrays.

    // Calculate the input block size for looking into the input.
    Dimensions input_block_sizes;
    if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
      for (int i = 0; i < outer_dim_start; ++i) {
        input_block_sizes[i] = input_dims[i];
      }
      for (int i = outer_dim_start; i < NumDims; ++i) {
        input_block_sizes[i] = 1;
      }
    } else {
      for (int i = 0; i < outer_dim_start; ++i) {
        input_block_sizes[NumDims - i - 1] = input_dims[NumDims - i - 1];
      }
      for (int i = outer_dim_start; i < NumDims; ++i) {
        input_block_sizes[NumDims - i - 1] = 1;
      }
    }

    // Broadcast with the 0-stride trick: Create 1 extra dim for each
    // broadcast, set the input stride to 0.
    //
    // When ColMajor:
    // - broadcast_block_sizes is [d_0, b_0, d_1, b_1, ...].
    //
    // - broadcast_block_strides is [output_block_strides[0],
    //                               output_block_strides[0] * d_0,
    //                               output_block_strides[1],
    //                               output_block_strides[1] * d_1,
    //                               ...].
    //
    // - broadcast_tensor_strides is [output_block_strides[0],
    //                                0,
    //                                output_block_strides[1],
    //                                0,
    //                                ...].
    BroadcastDimensions broadcast_block_sizes, broadcast_block_strides,
        broadcast_tensor_strides;

    for (int i = 0; i < outer_dim_start; ++i) {
      const int dim = static_cast<int>(Layout) == static_cast<int>(ColMajor)
                          ? i
                          : NumDims - i - 1;
      const int copy_dim =
          static_cast<int>(Layout) == static_cast<int>(ColMajor)
              ? 2 * i
              : 2 * NumDims - 2 * i - 1;
      const int broadcast_dim =
          static_cast<int>(Layout) == static_cast<int>(ColMajor) ? copy_dim + 1
                                                                 : copy_dim - 1;
      broadcast_block_sizes[copy_dim] = input_dims[dim];
      broadcast_block_sizes[broadcast_dim] = m_broadcast[dim];
      broadcast_block_strides[copy_dim] = output_block_strides[dim];
      broadcast_block_strides[broadcast_dim] =
          output_block_strides[dim] * input_dims[dim];
      broadcast_tensor_strides[copy_dim] = m_inputStrides[dim];
      broadcast_tensor_strides[broadcast_dim] = 0;
    }
    for (int i = 2 * outer_dim_start; i < 2 * NumDims; ++i) {
      const int dim = static_cast<int>(Layout) == static_cast<int>(ColMajor)
                          ? i
                          : 2 * NumDims - i - 1;
      broadcast_block_sizes[dim] = 1;
      broadcast_block_strides[dim] = 0;
      broadcast_tensor_strides[dim] = 0;
    }

    const int outer_dim = static_cast<int>(Layout) == static_cast<int>(ColMajor)
                              ? outer_dim_start
                              : NumDims - outer_dim_start - 1;

    if (outer_dim_size == 1) {
      // We just need one block read using the ready-set values above.
      BroadcastBlock(input_block_sizes, broadcast_block_sizes,
                     broadcast_block_strides, broadcast_tensor_strides, 0,
                     output_block);
    } else if (input_dims[outer_dim] == 1) {
      // Broadcast outer_dim_start-th dimension (< NumDims) by outer_dim_size.
      const int broadcast_outer_dim =
          static_cast<int>(Layout) == static_cast<int>(ColMajor)
              ? 2 * outer_dim_start + 1
              : 2 * NumDims - 2 * outer_dim_start - 2;
      broadcast_block_sizes[broadcast_outer_dim] = outer_dim_size;
      broadcast_tensor_strides[broadcast_outer_dim] = 0;
      broadcast_block_strides[broadcast_outer_dim] =
          output_block_strides[outer_dim];
      BroadcastBlock(input_block_sizes, broadcast_block_sizes,
                     broadcast_block_strides, broadcast_tensor_strides, 0,
                     output_block);
    } else {
      // The general case. Let's denote the output block as x[...,
      // a:a+outer_dim_size, :, ..., :], where a:a+outer_dim_size is a slice on
      // the outer_dim_start-th dimension (< NumDims). We need to split the
      // a:a+outer_dim_size into possibly 3 sub-blocks:
      //
      // (1) a:b, where b is the smallest multiple of
      // input_dims[outer_dim_start] in [a, a+outer_dim_size].
      //
      // (2) b:c, where c is the largest multiple of input_dims[outer_dim_start]
      // in [a, a+outer_dim_size].
      //
      // (3) c:a+outer_dim_size .
      //
      // Or, when b and c do not exist, we just need to process the whole block
      // together.

      // Find a.
      const Index outer_dim_left_index =
          output_block->first_coeff_index() / m_outputStrides[outer_dim];

      // Find b and c.
      const Index input_outer_dim_size = input_dims[outer_dim];

      // First multiple after a. This is b when <= outer_dim_left_index +
      // outer_dim_size.
      const Index first_multiple =
          divup<Index>(outer_dim_left_index, input_outer_dim_size) *
          input_outer_dim_size;

      if (first_multiple <= outer_dim_left_index + outer_dim_size) {
        // b exists, so does c. Find it.
        const Index last_multiple = (outer_dim_left_index + outer_dim_size) /
                                    input_outer_dim_size * input_outer_dim_size;
        const int copy_outer_dim =
            static_cast<int>(Layout) == static_cast<int>(ColMajor)
                ? 2 * outer_dim_start
                : 2 * NumDims - 2 * outer_dim_start - 1;
        const int broadcast_outer_dim =
            static_cast<int>(Layout) == static_cast<int>(ColMajor)
                ? 2 * outer_dim_start + 1
                : 2 * NumDims - 2 * outer_dim_start - 2;
        if (first_multiple > outer_dim_left_index) {
          const Index head_size = first_multiple - outer_dim_left_index;
          input_block_sizes[outer_dim] = head_size;
          broadcast_block_sizes[copy_outer_dim] = head_size;
          broadcast_tensor_strides[copy_outer_dim] = m_inputStrides[outer_dim];
          broadcast_block_strides[copy_outer_dim] =
              output_block_strides[outer_dim];
          broadcast_block_sizes[broadcast_outer_dim] = 1;
          broadcast_tensor_strides[broadcast_outer_dim] = 0;
          broadcast_block_strides[broadcast_outer_dim] =
              output_block_strides[outer_dim] * input_dims[outer_dim];
          BroadcastBlock(input_block_sizes, broadcast_block_sizes,
                         broadcast_block_strides, broadcast_tensor_strides, 0,
                         output_block);
        }
        if (first_multiple < last_multiple) {
          input_block_sizes[outer_dim] = input_outer_dim_size;
          broadcast_block_sizes[copy_outer_dim] = input_outer_dim_size;
          broadcast_tensor_strides[copy_outer_dim] = m_inputStrides[outer_dim];
          broadcast_block_strides[copy_outer_dim] =
              output_block_strides[outer_dim];
          broadcast_block_sizes[broadcast_outer_dim] =
              (last_multiple - first_multiple) / input_outer_dim_size;
          broadcast_tensor_strides[broadcast_outer_dim] = 0;
          broadcast_block_strides[broadcast_outer_dim] =
              output_block_strides[outer_dim] * input_dims[outer_dim];
          const Index offset = (first_multiple - outer_dim_left_index) *
                               m_outputStrides[outer_dim];
          BroadcastBlock(input_block_sizes, broadcast_block_sizes,
                         broadcast_block_strides, broadcast_tensor_strides,
                         offset, output_block);
        }
        if (last_multiple < outer_dim_left_index + outer_dim_size) {
          const Index tail_size =
              outer_dim_left_index + outer_dim_size - last_multiple;
          input_block_sizes[outer_dim] = tail_size;
          broadcast_block_sizes[copy_outer_dim] = tail_size;
          broadcast_tensor_strides[copy_outer_dim] = m_inputStrides[outer_dim];
          broadcast_block_strides[copy_outer_dim] =
              output_block_strides[outer_dim];
          broadcast_block_sizes[broadcast_outer_dim] = 1;
          broadcast_tensor_strides[broadcast_outer_dim] = 0;
          broadcast_block_strides[broadcast_outer_dim] =
              output_block_strides[outer_dim] * input_dims[outer_dim];
          const Index offset = (last_multiple - outer_dim_left_index) *
                               m_outputStrides[outer_dim];
          BroadcastBlock(input_block_sizes, broadcast_block_sizes,
                         broadcast_block_strides, broadcast_tensor_strides,
                         offset, output_block);
        }
      } else {
        // b and c do not exist.
        const int copy_outer_dim =
            static_cast<int>(Layout) == static_cast<int>(ColMajor)
                ? 2 * outer_dim_start
                : 2 * NumDims - 2 * outer_dim_start - 1;
        input_block_sizes[outer_dim] = outer_dim_size;
        broadcast_block_sizes[copy_outer_dim] = outer_dim_size;
        broadcast_tensor_strides[copy_outer_dim] = m_inputStrides[outer_dim];
        broadcast_block_strides[copy_outer_dim] =
            output_block_strides[outer_dim];
        BroadcastBlock(input_block_sizes, broadcast_block_sizes,
                       broadcast_block_strides, broadcast_tensor_strides, 0,
                       output_block);
      }
    }
  }

  EIGEN_DEVICE_FUNC typename Eigen::internal::traits<XprType>::PointerType data() const { return NULL; }

  const TensorEvaluator<ArgType, Device>& impl() const { return m_impl; }

  Broadcast functor() const { return m_broadcast; }

 private:
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void BroadcastBlock(
      const Dimensions& input_block_sizes,
      const BroadcastDimensions& broadcast_block_sizes,
      const BroadcastDimensions& broadcast_block_strides,
      const BroadcastDimensions& broadcast_tensor_strides, Index offset,
      TensorBlock* output_block) const {
    TensorBlock input_view_block(
        static_cast<int>(Layout) == static_cast<int>(ColMajor)
            ? indexColMajor(output_block->first_coeff_index() + offset)
            : indexRowMajor(output_block->first_coeff_index() + offset),
        input_block_sizes, Dimensions(m_inputStrides),
        Dimensions(m_inputStrides), NULL);

    internal::TensorBlockView<ArgType, Device> input_block(m_device, m_impl,
                                                           input_view_block);
    BroadcastTensorBlock broadcast_block(
        0, broadcast_block_sizes, broadcast_block_strides,
        broadcast_tensor_strides, output_block->data() + offset);

    BroadcastTensorBlockReader::Run(&broadcast_block, input_block.data());
  }

 protected:
  const Device& m_device;
  const Broadcast m_broadcast;
  Dimensions m_dimensions;
  array<Index, NumDims> m_outputStrides;
  array<Index, NumDims> m_inputStrides;
  TensorEvaluator<ArgType, Device> m_impl;
};


} // end namespace Eigen

#endif // EIGEN_CXX11_TENSOR_TENSOR_BROADCASTING_H
