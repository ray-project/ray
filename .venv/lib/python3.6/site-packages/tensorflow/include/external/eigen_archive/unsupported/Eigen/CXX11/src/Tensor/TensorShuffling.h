// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2014 Benoit Steiner <benoit.steiner.goog@gmail.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#ifndef EIGEN_CXX11_TENSOR_TENSOR_SHUFFLING_H
#define EIGEN_CXX11_TENSOR_TENSOR_SHUFFLING_H

namespace Eigen {

/** \class TensorShuffling
  * \ingroup CXX11_Tensor_Module
  *
  * \brief Tensor shuffling class.
  *
  *
  */
namespace internal {
template<typename Shuffle, typename XprType>
struct traits<TensorShufflingOp<Shuffle, XprType> > : public traits<XprType>
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

template<typename Shuffle, typename XprType>
struct eval<TensorShufflingOp<Shuffle, XprType>, Eigen::Dense>
{
  typedef const TensorShufflingOp<Shuffle, XprType>& type;
};

template<typename Shuffle, typename XprType>
struct nested<TensorShufflingOp<Shuffle, XprType>, 1, typename eval<TensorShufflingOp<Shuffle, XprType> >::type>
{
  typedef TensorShufflingOp<Shuffle, XprType> type;
};

}  // end namespace internal



template<typename Shuffle, typename XprType>
class TensorShufflingOp : public TensorBase<TensorShufflingOp<Shuffle, XprType> >
{
  public:
  typedef typename Eigen::internal::traits<TensorShufflingOp>::Scalar Scalar;
  typedef typename Eigen::NumTraits<Scalar>::Real RealScalar;
  typedef typename XprType::CoeffReturnType CoeffReturnType;
  typedef typename Eigen::internal::nested<TensorShufflingOp>::type Nested;
  typedef typename Eigen::internal::traits<TensorShufflingOp>::StorageKind StorageKind;
  typedef typename Eigen::internal::traits<TensorShufflingOp>::Index Index;

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE TensorShufflingOp(const XprType& expr, const Shuffle& shfl)
      : m_xpr(expr), m_shuffle(shfl) {}

    EIGEN_DEVICE_FUNC
    const Shuffle& shufflePermutation() const { return m_shuffle; }

    EIGEN_DEVICE_FUNC
    const typename internal::remove_all<typename XprType::Nested>::type&
    expression() const { return m_xpr; }

    EIGEN_DEVICE_FUNC
    EIGEN_STRONG_INLINE TensorShufflingOp& operator = (const TensorShufflingOp& other)
    {
      typedef TensorAssignOp<TensorShufflingOp, const TensorShufflingOp> Assign;
      Assign assign(*this, other);
      internal::TensorExecutor<const Assign, DefaultDevice>::run(assign, DefaultDevice());
      return *this;
    }

    template<typename OtherDerived>
    EIGEN_DEVICE_FUNC
    EIGEN_STRONG_INLINE TensorShufflingOp& operator = (const OtherDerived& other)
    {
      typedef TensorAssignOp<TensorShufflingOp, const OtherDerived> Assign;
      Assign assign(*this, other);
      internal::TensorExecutor<const Assign, DefaultDevice>::run(assign, DefaultDevice());
      return *this;
    }

  protected:
    typename XprType::Nested m_xpr;
    const Shuffle m_shuffle;
};


// Eval as rvalue
template<typename Shuffle, typename ArgType, typename Device>
struct TensorEvaluator<const TensorShufflingOp<Shuffle, ArgType>, Device>
{
  typedef TensorEvaluator<const TensorShufflingOp<Shuffle, ArgType>, Device> Self;
  typedef TensorShufflingOp<Shuffle, ArgType> XprType;
  typedef typename XprType::Index Index;
  static const int NumDims = internal::array_size<typename TensorEvaluator<ArgType, Device>::Dimensions>::value;
  typedef DSizes<Index, NumDims> Dimensions;
  typedef typename XprType::Scalar Scalar;
  typedef typename XprType::CoeffReturnType CoeffReturnType;
  typedef typename PacketType<CoeffReturnType, Device>::type PacketReturnType;
  static const int PacketSize = PacketType<CoeffReturnType, Device>::size;

  enum {
    IsAligned         = false,
    PacketAccess      = (PacketType<CoeffReturnType, Device>::size > 1),
    BlockAccess       = TensorEvaluator<ArgType, Device>::BlockAccess,
    PreferBlockAccess = true,
    Layout            = TensorEvaluator<ArgType, Device>::Layout,
    CoordAccess       = false,  // to be implemented
    RawAccess         = false
  };

  typedef typename internal::remove_const<Scalar>::type ScalarNoConst;

  typedef internal::TensorBlock<ScalarNoConst, Index, NumDims, Layout>
      TensorBlock;
  typedef internal::TensorBlockReader<ScalarNoConst, Index, NumDims, Layout>
      TensorBlockReader;

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE TensorEvaluator(const XprType& op,
                                                        const Device& device)
      : m_device(device),
        m_impl(op.expression(), device),
        m_shuffle(op.shufflePermutation())
  {
    const typename TensorEvaluator<ArgType, Device>::Dimensions& input_dims = m_impl.dimensions();
    const Shuffle& shuffle = op.shufflePermutation();
    m_is_identity = true;
    for (int i = 0; i < NumDims; ++i) {
      m_dimensions[i] = input_dims[shuffle[i]];
      m_inverseShuffle[shuffle[i]] = i;
      if (m_is_identity && shuffle[i] != i) {
        m_is_identity = false;
      }
    }

    if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
      m_unshuffledInputStrides[0] = 1;
      m_outputStrides[0] = 1;

      for (int i = 1; i < NumDims; ++i) {
        m_unshuffledInputStrides[i] =
            m_unshuffledInputStrides[i - 1] * input_dims[i - 1];
        m_outputStrides[i] = m_outputStrides[i - 1] * m_dimensions[i - 1];
        m_fastOutputStrides[i] = internal::TensorIntDivisor<Index>(m_outputStrides[i]);
      }
    } else {
      m_unshuffledInputStrides[NumDims - 1] = 1;
      m_outputStrides[NumDims - 1] = 1;
      for (int i = NumDims - 2; i >= 0; --i) {
        m_unshuffledInputStrides[i] =
            m_unshuffledInputStrides[i + 1] * input_dims[i + 1];
        m_outputStrides[i] = m_outputStrides[i + 1] * m_dimensions[i + 1];
        m_fastOutputStrides[i] = internal::TensorIntDivisor<Index>(m_outputStrides[i]);
      }
    }

    for (int i = 0; i < NumDims; ++i) {
      m_inputStrides[i] = m_unshuffledInputStrides[shuffle[i]];
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

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE CoeffReturnType coeff(Index index) const
  {
    if (m_is_identity) {
      return m_impl.coeff(index);
    } else {
      return m_impl.coeff(srcCoeff(index));
    }
  }

  template <int LoadMode, typename Self, bool ImplPacketAccess>
  struct PacketLoader {
    EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE
    static PacketReturnType Run(const Self& self, Index index) {
      EIGEN_ALIGN_MAX typename internal::remove_const<CoeffReturnType>::type values[PacketSize];
      for (int i = 0; i < PacketSize; ++i) {
        values[i] = self.coeff(index + i);
      }
      PacketReturnType rslt = internal::pload<PacketReturnType>(values);
      return rslt;
    }
  };

  template<int LoadMode, typename Self>
  struct PacketLoader<LoadMode, Self, true> {
    EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE
    static PacketReturnType Run(const Self& self, Index index) {
      if (self.m_is_identity) {
        return self.m_impl.template packet<LoadMode>(index);
      } else {
        EIGEN_ALIGN_MAX typename internal::remove_const<CoeffReturnType>::type values[PacketSize];
        for (int i = 0; i < PacketSize; ++i) {
          values[i] = self.coeff(index + i);
        }
        PacketReturnType rslt = internal::pload<PacketReturnType>(values);
        return rslt;
      }
    }
  };

  template<int LoadMode>
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE PacketReturnType packet(Index index) const
  {
    EIGEN_STATIC_ASSERT((PacketSize > 1), YOU_MADE_A_PROGRAMMING_MISTAKE)
        eigen_assert(index + PacketSize - 1 < dimensions().TotalSize());
    return PacketLoader<LoadMode, Self, TensorEvaluator<ArgType, Device>::PacketAccess>::Run(*this, index);
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void getResourceRequirements(
      std::vector<internal::TensorOpResourceRequirements>* resources) const {
    Eigen::Index block_total_size_max = numext::maxi<Eigen::Index>(
        1, m_device.firstLevelCacheSize() / sizeof(Scalar));
    resources->push_back(internal::TensorOpResourceRequirements(
        internal::kUniformAllDims, block_total_size_max));
    m_impl.getResourceRequirements(resources);
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void block(
      TensorBlock* output_block) const {
    if (m_impl.data() != NULL) {
      // Fast path: we have direct access to the data, so shuffle as we read.
      TensorBlockReader::Run(output_block,
                             srcCoeff(output_block->first_coeff_index()),
                             m_inverseShuffle,
                             m_unshuffledInputStrides,
                             m_impl.data());
      return;
    }

    // Slow path: read unshuffled block from the input and shuffle in-place.
    // Initialize input block sizes using input-to-output shuffle map.
    DSizes<Index, NumDims> input_block_sizes;
    for (Index i = 0; i < NumDims; ++i) {
      input_block_sizes[i] = output_block->block_sizes()[m_inverseShuffle[i]];
    }

    // Calculate input block strides.
    DSizes<Index, NumDims> input_block_strides;
    if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
      input_block_strides[0] = 1;
      for (int i = 1; i < NumDims; ++i) {
        input_block_strides[i] =
            input_block_strides[i - 1] * input_block_sizes[i - 1];
      }
    } else {
      input_block_strides[NumDims - 1] = 1;
      for (int i = NumDims - 2; i >= 0; --i) {
        input_block_strides[i] =
            input_block_strides[i + 1] * input_block_sizes[i + 1];
      }
    }

    // Read input block.
    TensorBlock input_block(srcCoeff(output_block->first_coeff_index()),
                            input_block_sizes,
                            input_block_strides,
                            Dimensions(m_unshuffledInputStrides),
                            output_block->data());

    m_impl.block(&input_block);

    // Naive In-place shuffle: random IO but block size is O(L1 cache size).
    // TODO(andydavis) Improve the performance of this in-place shuffle.
    const Index total_size = input_block_sizes.TotalSize();
    std::vector<bool> bitmap(total_size, false);
    ScalarNoConst* data = const_cast<ScalarNoConst*>(output_block->data());
    const DSizes<Index, NumDims>& output_block_strides =
        output_block->block_strides();
    for (Index input_index = 0; input_index < total_size; ++input_index) {
      if (bitmap[input_index]) {
        // Coefficient at this index has already been shuffled.
        continue;
      }

      Index output_index = GetBlockOutputIndex(input_index, input_block_strides,
                                               output_block_strides);
      if (output_index == input_index) {
        // Coefficient already in place.
        bitmap[output_index] = true;
        continue;
      }

      // The following loop starts at 'input_index', and shuffles
      // coefficients into their shuffled location at 'output_index'.
      // It skips through the array shuffling coefficients by following
      // the shuffle cycle starting and ending a 'start_index'.
      ScalarNoConst evicted_value;
      ScalarNoConst shuffled_value = data[input_index];
      do {
        evicted_value = data[output_index];
        data[output_index] = shuffled_value;
        shuffled_value = evicted_value;
        bitmap[output_index] = true;
        output_index = GetBlockOutputIndex(output_index, input_block_strides,
                                           output_block_strides);
      } while (output_index != input_index);

      data[output_index] = shuffled_value;
      bitmap[output_index] = true;
    }
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE TensorOpCost costPerCoeff(bool vectorized) const {
    const double compute_cost = m_is_identity ? TensorOpCost::AddCost<Index>() :
                                NumDims * (2 * TensorOpCost::AddCost<Index>() +
                                           2 * TensorOpCost::MulCost<Index>() +
                                           TensorOpCost::DivCost<Index>());
    return m_impl.costPerCoeff(vectorized) +
           TensorOpCost(0, 0, compute_cost, m_is_identity /* vectorized */, PacketSize);
  }

  EIGEN_DEVICE_FUNC typename Eigen::internal::traits<XprType>::PointerType data() const { return NULL; }

  // required by sycl
  EIGEN_STRONG_INLINE const Shuffle& shufflePermutation() const {return m_shuffle;}
  // required by sycl
  EIGEN_STRONG_INLINE const TensorEvaluator<ArgType, Device>& impl() const {return m_impl;}

 protected:
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE Index GetBlockOutputIndex(
      Index input_index,
      const DSizes<Index, NumDims>& input_block_strides,
      const DSizes<Index, NumDims>& output_block_strides) const {
    Index output_index = 0;
    if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
      for (int i = NumDims - 1; i > 0; --i) {
        const Index idx = input_index / input_block_strides[i];
        output_index += idx * output_block_strides[m_inverseShuffle[i]];
        input_index -= idx * input_block_strides[i];
      }
      return output_index + input_index *
          output_block_strides[m_inverseShuffle[0]];
    } else {
      for (int i = 0; i < NumDims - 1; ++i) {
        const Index idx = input_index / input_block_strides[i];
        output_index += idx * output_block_strides[m_inverseShuffle[i]];
        input_index -= idx * input_block_strides[i];
      }
      return output_index + input_index *
          output_block_strides[m_inverseShuffle[NumDims - 1]];
    }
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE Index srcCoeff(Index index) const {
    Index inputIndex = 0;
    if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
      for (int i = NumDims - 1; i > 0; --i) {
        const Index idx = index / m_fastOutputStrides[i];
        inputIndex += idx * m_inputStrides[i];
        index -= idx * m_outputStrides[i];
      }
      return inputIndex + index * m_inputStrides[0];
    } else {
      for (int i = 0; i < NumDims - 1; ++i) {
        const Index idx = index / m_fastOutputStrides[i];
        inputIndex += idx * m_inputStrides[i];
        index -= idx * m_outputStrides[i];
      }
      return inputIndex + index * m_inputStrides[NumDims - 1];
    }
  }

  Dimensions m_dimensions;
  bool m_is_identity;
  array<Index, NumDims> m_inverseShuffle;
  array<Index, NumDims> m_outputStrides;
  array<internal::TensorIntDivisor<Index>, NumDims> m_fastOutputStrides;
  array<Index, NumDims> m_inputStrides;
  array<Index, NumDims> m_unshuffledInputStrides;

  const Device& m_device;
  TensorEvaluator<ArgType, Device> m_impl;
  /// required by sycl
  Shuffle m_shuffle;
};


// Eval as lvalue
template<typename Shuffle, typename ArgType, typename Device>
struct TensorEvaluator<TensorShufflingOp<Shuffle, ArgType>, Device>
    : public TensorEvaluator<const TensorShufflingOp<Shuffle, ArgType>, Device>
{
  typedef TensorEvaluator<const TensorShufflingOp<Shuffle, ArgType>, Device> Base;

  typedef TensorShufflingOp<Shuffle, ArgType> XprType;
  typedef typename XprType::Index Index;
  static const int NumDims = internal::array_size<typename TensorEvaluator<ArgType, Device>::Dimensions>::value;
  typedef DSizes<Index, NumDims> Dimensions;
  typedef typename XprType::Scalar Scalar;
  typedef typename XprType::CoeffReturnType CoeffReturnType;
  typedef typename PacketType<CoeffReturnType, Device>::type PacketReturnType;
  static const int PacketSize = PacketType<CoeffReturnType, Device>::size;

  enum {
    IsAligned         = false,
    PacketAccess      = (PacketType<CoeffReturnType, Device>::size > 1),
    BlockAccess       = TensorEvaluator<ArgType, Device>::BlockAccess,
    PreferBlockAccess = true,
    Layout            = TensorEvaluator<ArgType, Device>::Layout,
    RawAccess         = false
  };

  typedef typename internal::remove_const<Scalar>::type ScalarNoConst;

  typedef internal::TensorBlock<ScalarNoConst, Index, NumDims, Layout>
      TensorBlock;
  typedef internal::TensorBlockWriter<ScalarNoConst, Index, NumDims, Layout>
      TensorBlockWriter;

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE TensorEvaluator(const XprType& op, const Device& device)
      : Base(op, device)
  { }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE CoeffReturnType& coeffRef(Index index)
  {
    return this->m_impl.coeffRef(this->srcCoeff(index));
  }

  template <int StoreMode> EIGEN_STRONG_INLINE
  void writePacket(Index index, const PacketReturnType& x)
  {
    EIGEN_STATIC_ASSERT((PacketSize > 1), YOU_MADE_A_PROGRAMMING_MISTAKE)

    EIGEN_ALIGN_MAX typename internal::remove_const<CoeffReturnType>::type values[PacketSize];
    internal::pstore<CoeffReturnType, PacketReturnType>(values, x);
    for (int i = 0; i < PacketSize; ++i) {
      this->coeffRef(index+i) = values[i];
    }
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void writeBlock(
      const TensorBlock& block) {
    eigen_assert(this->m_impl.data() != NULL);
    TensorBlockWriter::Run(block, this->srcCoeff(block.first_coeff_index()),
                           this->m_inverseShuffle,
                           this->m_unshuffledInputStrides, this->m_impl.data());
  }
};


} // end namespace Eigen

#endif // EIGEN_CXX11_TENSOR_TENSOR_SHUFFLING_H
