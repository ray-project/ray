// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2018 Andy Davis <andydavis@google.com>
// Copyright (C) 2018 Eugene Zhulenev <ezhulenev@google.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#ifndef EIGEN_CXX11_TENSOR_TENSOR_BLOCK_H
#define EIGEN_CXX11_TENSOR_TENSOR_BLOCK_H

namespace Eigen {
namespace internal {

namespace {

// Helper template to choose between ColMajor and RowMajor values.
template <int Layout>
struct cond;

template <>
struct cond<ColMajor> {
  template <typename T>
  EIGEN_STRONG_INLINE const T& operator()(const T& col,
                                          const T& /*row*/) const {
    return col;
  }
};

template <>
struct cond<RowMajor> {
  template <typename T>
  EIGEN_STRONG_INLINE const T& operator()(const T& /*col*/,
                                          const T& row) const {
    return row;
  }
};

}  // namespace

/**
 * \class TensorBlockShapeType
 * \ingroup CXX11_Tensor_Module
 *
 * \brief Tensor block shape type.
 *
 * Tensor block shape type defines what are the shape preference for the blocks
 * extracted from the larger tensor.
 *
 * Example:
 *
 * We want to extract blocks of 100 elements from the large 100x100 tensor:
 *  - tensor: 100x100
 *  - target_block_size: 100
 *
 * TensorBlockShapeType:
 *  - kUniformAllDims: 100 blocks of size 10x10
 *  - kSkewedInnerDims: 100 blocks of size 100x1 (or 1x100 depending on a column
 *                      or row major layout)
 */
enum TensorBlockShapeType {
  kUniformAllDims,
  kSkewedInnerDims
};

struct TensorOpResourceRequirements {
  TensorBlockShapeType block_shape;
  Index block_total_size;
  // TODO(andydavis) Add 'target_num_threads' to support communication of
  // thread-resource requirements. This will allow ops deep in the
  // expression tree (like reductions) to communicate resources
  // requirements based on local state (like the total number of reductions
  // to be computed).
  TensorOpResourceRequirements(TensorBlockShapeType shape,
                               const Index size)
      : block_shape(shape), block_total_size(size) {}
};

// Tries to merge multiple resource requirements.
EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void MergeResourceRequirements(
    const std::vector<TensorOpResourceRequirements>& resources,
    TensorBlockShapeType* block_shape, Index* block_total_size) {
  if (resources.empty()) {
    return;
  }
  // TODO(andydavis) Implement different policies (i.e. revert to a default
  // policy if block shapes/sizes conflict).
  *block_shape = resources[0].block_shape;
  *block_total_size = resources[0].block_total_size;
  for (std::vector<TensorOpResourceRequirements>::size_type i = 1; i < resources.size(); ++i) {
    if (resources[i].block_shape == kSkewedInnerDims &&
        *block_shape != kSkewedInnerDims) {
      *block_shape = kSkewedInnerDims;
    }
    *block_total_size =
        numext::maxi(*block_total_size, resources[i].block_total_size);
  }
}

/**
 * \class TensorBlock
 * \ingroup CXX11_Tensor_Module
 *
 * \brief Tensor block class.
 *
 * This class represents a tensor block specified by the index of the
 * first block coefficient, and the size of the block in each dimension.
 */
template <typename Scalar, typename StorageIndex, int NumDims, int Layout>
class TensorBlock {
 public:
  typedef DSizes<StorageIndex, NumDims> Dimensions;

  TensorBlock(const StorageIndex first_coeff_index, const Dimensions& block_sizes,
              const Dimensions& block_strides, const Dimensions& tensor_strides,
              Scalar* data)
      : m_first_coeff_index(first_coeff_index),
        m_block_sizes(block_sizes),
        m_block_strides(block_strides),
        m_tensor_strides(tensor_strides),
        m_data(data) {}

  StorageIndex first_coeff_index() const { return m_first_coeff_index; }

  const Dimensions& block_sizes() const { return m_block_sizes; }

  const Dimensions& block_strides() const { return m_block_strides; }

  const Dimensions& tensor_strides() const { return m_tensor_strides; }

  Scalar* data() { return m_data; }

  const Scalar* data() const { return m_data; }

 private:
  StorageIndex m_first_coeff_index;
  Dimensions m_block_sizes;
  Dimensions m_block_strides;
  Dimensions m_tensor_strides;
  Scalar* m_data;  // Not owned.
};

template <typename Scalar, typename StorageIndex>
struct TensorBlockCopyOp {

  typedef typename packet_traits<Scalar>::type Packet;
  enum {
    Vectorizable = internal::packet_traits<Scalar>::Vectorizable,
    PacketSize   = internal::packet_traits<Scalar>::size
  };

  static EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void Run(
      const StorageIndex num_coeff_to_copy, const StorageIndex dst_index,
      const StorageIndex dst_stride, Scalar* EIGEN_RESTRICT dst_data,
      const StorageIndex src_index, const StorageIndex src_stride,
      const Scalar* EIGEN_RESTRICT src_data) {
    const Scalar* src = &src_data[src_index];
    Scalar* dst = &dst_data[dst_index];

    if (!Vectorizable) {
      for (Index i = 0; i < num_coeff_to_copy; ++i) {
        dst[i * dst_stride] = src[i * src_stride];
      }
      return;
    }

    if (src_stride == 1) {
      const StorageIndex vectorized_size = (num_coeff_to_copy / PacketSize) * PacketSize;
      if (dst_stride == 1) {
        // LINEAR
        for (StorageIndex i = 0; i < vectorized_size; i += PacketSize) {
          Packet p = internal::ploadu<Packet>(src + i);
          internal::pstoreu<Scalar, Packet>(dst + i, p);
        }
        for (StorageIndex i = vectorized_size; i < num_coeff_to_copy; ++i) {
          dst[i] = src[i];
        }
      } else {
        // SCATTER
        for (StorageIndex i = 0; i < vectorized_size; i += PacketSize) {
          Packet p = internal::ploadu<Packet>(src + i);
          internal::pscatter<Scalar, Packet>(dst + i * dst_stride, p, dst_stride);
        }
        for (StorageIndex i = vectorized_size; i < num_coeff_to_copy; ++i) {
          dst[i * dst_stride] = src[i];
        }
      }
    } else if (src_stride == 0) {
      const StorageIndex vectorized_size = (num_coeff_to_copy / PacketSize) * PacketSize;
      if (dst_stride == 1) {
        // LINEAR
        for (StorageIndex i = 0; i < vectorized_size; i += PacketSize) {
          Packet p = internal::pload1<Packet>(src);
          internal::pstoreu<Scalar, Packet>(dst + i, p);
        }
        for (StorageIndex i = vectorized_size; i < num_coeff_to_copy; ++i) {
          dst[i] = *src;
        }
      } else {
        // SCATTER
        for (StorageIndex i = 0; i < vectorized_size; i += PacketSize) {
          Packet p = internal::pload1<Packet>(src);
          internal::pscatter<Scalar, Packet>(dst + i * dst_stride, p, dst_stride);
        }
        for (StorageIndex i = vectorized_size; i < num_coeff_to_copy; ++i) {
          dst[i * dst_stride] = *src;
        }
      }
    } else {
      if (dst_stride == 1) {
        // GATHER
        const StorageIndex vectorized_size = (num_coeff_to_copy / PacketSize) * PacketSize;
        for (StorageIndex i = 0; i < vectorized_size; i += PacketSize) {
          Packet p = internal::pgather<Scalar, Packet>(src + i * src_stride, src_stride);
          internal::pstoreu<Scalar, Packet>(dst + i, p);
        }
        for (StorageIndex i = vectorized_size; i < num_coeff_to_copy; ++i) {
          dst[i] = src[i * src_stride];
        }
      } else {
        // RANDOM
        for (StorageIndex i = 0; i < num_coeff_to_copy; ++i) {
          dst[i * dst_stride] = src[i * src_stride];
        }
      }
    }
  }
};

/**
 * \class TensorBlockIO
 * \ingroup CXX11_Tensor_Module
 *
 * \brief Tensor block IO class.
 *
 * This class is responsible for copying data between a tensor and a tensor
 * block.
 */
template <typename Scalar, typename StorageIndex, int NumDims, int Layout,
          bool BlockRead>
class TensorBlockIO {
 public:
  typedef TensorBlock<Scalar, StorageIndex, NumDims, Layout> Block;
  typedef TensorBlockCopyOp<Scalar, StorageIndex> BlockCopyOp;

 protected:
  typedef array<StorageIndex, NumDims> Dimensions;

  struct BlockIteratorState {
    StorageIndex input_stride;
    StorageIndex output_stride;
    StorageIndex input_span;
    StorageIndex output_span;
    StorageIndex size;
    StorageIndex count;
    BlockIteratorState()
        : input_stride(0),
          output_stride(0),
          input_span(0),
          output_span(0),
          size(0),
          count(0) {}
  };

  // Compute how many inner dimensions it's allowed to squeeze when doing IO
  // between a tensor and a block. It's safe to squeeze inner dimensions, only
  // if they are not reordered.
  static int NumSqueezableInnerDims(const Dimensions& tensor_to_block_dim_map) {
    int num_squeezable_dims = 0;
    if (Layout == ColMajor) {
      for (int i = 0; i < NumDims; ++i) {
        if (tensor_to_block_dim_map[i] == i) num_squeezable_dims++;
        else break;
      }
    } else {
      for (int i = NumDims - 1; i >= 0; --i) {
        if (tensor_to_block_dim_map[i] == i) num_squeezable_dims++;
        else break;
      }
    }
    return num_squeezable_dims;
  }

  static EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void Copy(
      const Block& block, StorageIndex first_coeff_index,
      const Dimensions& tensor_to_block_dim_map,
      const Dimensions& tensor_strides,
      const Scalar* src_data,
      Scalar* dst_data) {
    // Do not squeeze reordered inner dimensions.
    int num_squeezable_dims = NumSqueezableInnerDims(tensor_to_block_dim_map);

    // Find the innermost tensor dimension whose size is not 1. This is the
    // effective inner dim. If all dimensions are of size 1, then fallback to
    // using the actual innermost dim to avoid out-of-bound access.
    StorageIndex num_size_one_inner_dims = 0;
    for (int i = 0; i < num_squeezable_dims; ++i) {
      const int dim = cond<Layout>()(i, NumDims - i - 1);
      if (block.block_sizes()[tensor_to_block_dim_map[dim]] != 1) {
        num_size_one_inner_dims = i;
        break;
      }
    }

    // Calculate strides and dimensions.
    const StorageIndex tensor_stride1_dim = cond<Layout>()(
        num_size_one_inner_dims, NumDims - num_size_one_inner_dims - 1);
    const StorageIndex block_dim_for_tensor_stride1_dim =
        NumDims == 0 ? 1 : tensor_to_block_dim_map[tensor_stride1_dim];
    StorageIndex block_inner_dim_size =
        NumDims == 0 ? 1
                     : block.block_sizes()[block_dim_for_tensor_stride1_dim];

    // Squeeze multiple inner dims into one for larger inner dim size.
    for (Index i = num_size_one_inner_dims + 1; i < num_squeezable_dims; ++i) {
      const Index dim = cond<Layout>()(i, NumDims - i - 1);
      const StorageIndex block_stride =
          block.block_strides()[tensor_to_block_dim_map[dim]];
      if (block_inner_dim_size == block_stride &&
          block_stride == tensor_strides[dim]) {
        block_inner_dim_size *=
            block.block_sizes()[tensor_to_block_dim_map[dim]];
        ++num_size_one_inner_dims;
      } else {
        break;
      }
    }

    StorageIndex inputIndex;
    StorageIndex outputIndex;
    StorageIndex input_stride;
    StorageIndex output_stride;

    // Setup strides to read/write along the tensor's stride1 dimension.
    if (BlockRead) {
      inputIndex = first_coeff_index;
      outputIndex = 0;
      input_stride = NumDims == 0 ? 1 : tensor_strides[tensor_stride1_dim];
      output_stride =
          NumDims == 0
              ? 1
              : block.block_strides()[block_dim_for_tensor_stride1_dim];
    } else {
      inputIndex = 0;
      outputIndex = first_coeff_index;
      input_stride =
          NumDims == 0
              ? 1
              : block.block_strides()[block_dim_for_tensor_stride1_dim];
      output_stride = NumDims == 0 ? 1 : tensor_strides[tensor_stride1_dim];
    }

    const int at_least_1_dim = NumDims <= 1 ? 1 : NumDims - 1;
    array<BlockIteratorState, at_least_1_dim> block_iter_state;

    // Initialize block iterator state. Squeeze away any dimension of size 1.
    Index num_squeezed_dims = 0;
    for (Index i = num_size_one_inner_dims; i < NumDims - 1; ++i) {
      const Index dim = cond<Layout>()(i + 1, NumDims - i - 2);
      const StorageIndex size = block.block_sizes()[tensor_to_block_dim_map[dim]];
      if (size == 1) {
        continue;
      }
      block_iter_state[num_squeezed_dims].size = size;
      if (BlockRead) {
        block_iter_state[num_squeezed_dims].input_stride = tensor_strides[dim];
        block_iter_state[num_squeezed_dims].output_stride =
            block.block_strides()[tensor_to_block_dim_map[dim]];
      } else {
        block_iter_state[num_squeezed_dims].input_stride =
            block.block_strides()[tensor_to_block_dim_map[dim]];
        block_iter_state[num_squeezed_dims].output_stride = tensor_strides[dim];
      }
      block_iter_state[num_squeezed_dims].input_span =
          block_iter_state[num_squeezed_dims].input_stride *
          (block_iter_state[num_squeezed_dims].size - 1);
      block_iter_state[num_squeezed_dims].output_span =
          block_iter_state[num_squeezed_dims].output_stride *
          (block_iter_state[num_squeezed_dims].size - 1);
      ++num_squeezed_dims;
    }

    // Iterate copying data from src to dst.
    const StorageIndex block_total_size =
        NumDims == 0 ? 1 : block.block_sizes().TotalSize();
    for (StorageIndex i = 0; i < block_total_size; i += block_inner_dim_size) {
      BlockCopyOp::Run(block_inner_dim_size, outputIndex, output_stride,
                       dst_data, inputIndex, input_stride, src_data);
      // Update index.
      for (int j = 0; j < num_squeezed_dims; ++j) {
        if (++block_iter_state[j].count < block_iter_state[j].size) {
          inputIndex += block_iter_state[j].input_stride;
          outputIndex += block_iter_state[j].output_stride;
          break;
        }
        block_iter_state[j].count = 0;
        inputIndex -= block_iter_state[j].input_span;
        outputIndex -= block_iter_state[j].output_span;
      }
    }
  }
};

/**
 * \class TensorBlockReader
 * \ingroup CXX11_Tensor_Module
 *
 * \brief Tensor block reader class.
 *
 * This class is responsible for reading a tensor block.
 *
 */
template <typename Scalar, typename StorageIndex, int NumDims, int Layout>
class TensorBlockReader : public TensorBlockIO<Scalar, StorageIndex, NumDims,
                                               Layout, /*BlockRead=*/true> {
 public:
  typedef TensorBlock<Scalar, StorageIndex, NumDims, Layout> Block;
  typedef TensorBlockIO<Scalar, StorageIndex, NumDims, Layout, /*BlockRead=*/true> Base;

  static EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void Run(
      Block* block, const Scalar* src_data) {
    array<StorageIndex, NumDims> tensor_to_block_dim_map;
    for (int i = 0; i < NumDims; ++i) {
      tensor_to_block_dim_map[i] = i;
    }
    Base::Copy(*block, block->first_coeff_index(), tensor_to_block_dim_map,
               block->tensor_strides(), src_data, block->data());
  }

  static EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void Run(
      Block* block, StorageIndex first_coeff_index,
      const array<StorageIndex, NumDims>& tensor_to_block_dim_map,
      const array<StorageIndex, NumDims>& tensor_strides, const Scalar* src_data) {
    Base::Copy(*block, first_coeff_index, tensor_to_block_dim_map,
               tensor_strides, src_data, block->data());
  }
};

/**
 * \class TensorBlockWriter
 * \ingroup CXX11_Tensor_Module
 *
 * \brief Tensor block writer class.
 *
 * This class is responsible for writing a tensor block.
 *
 */
template <typename Scalar, typename StorageIndex, int NumDims, int Layout>
class TensorBlockWriter : public TensorBlockIO<Scalar, StorageIndex, NumDims,
                                               Layout, /*BlockRead=*/false> {
 public:
  typedef TensorBlock<Scalar, StorageIndex, NumDims, Layout> Block;
  typedef TensorBlockIO<Scalar, StorageIndex, NumDims, Layout, /*BlockRead=*/false> Base;

  static EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void Run(
      const Block& block, Scalar* dst_data) {
    array<StorageIndex, NumDims> tensor_to_block_dim_map;
    for (int i = 0; i < NumDims; ++i) {
      tensor_to_block_dim_map[i] = i;
    }
    Base::Copy(block, block.first_coeff_index(), tensor_to_block_dim_map,
               block.tensor_strides(), block.data(), dst_data);
  }

  static EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void Run(
      const Block& block, StorageIndex first_coeff_index,
      const array<StorageIndex, NumDims>& tensor_to_block_dim_map,
      const array<StorageIndex, NumDims>& tensor_strides, Scalar* dst_data) {
    Base::Copy(block, first_coeff_index, tensor_to_block_dim_map,
               tensor_strides, block.data(), dst_data);
  }
};

/**
 * \class TensorBlockCwiseUnaryOp
 * \ingroup CXX11_Tensor_Module
 *
 * \brief Carries out a cwise binary op on a number of coefficients.
 *
 * This class reads strided input from the argument, and writes the
 * result of the cwise unary op to the strided output array.
 *
 */
struct TensorBlockCwiseUnaryOp {
  template <typename StorageIndex, typename UnaryFunctor,
            typename OutputScalar, typename InputScalar>
  static EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void Run(
      const UnaryFunctor& functor, const StorageIndex num_coeff,
      const StorageIndex output_index, const StorageIndex output_stride,
      OutputScalar* output_data, const StorageIndex input_index,
      const StorageIndex input_stride, const InputScalar* input_data) {
    typedef const Eigen::Array<InputScalar, Dynamic, 1> Input;
    typedef Eigen::Array<OutputScalar, Dynamic, 1> Output;

    typedef Eigen::Map<Input, 0, InnerStride<> > InputMap;
    typedef Eigen::Map<Output, 0, InnerStride<> > OutputMap;

    const InputScalar* input_base = &input_data[input_index];
    OutputScalar* output_base = &output_data[output_index];

    const InputMap input(input_base, num_coeff, InnerStride<>(input_stride));
    OutputMap output(output_base, num_coeff, InnerStride<>(output_stride));

    output = Eigen::CwiseUnaryOp<UnaryFunctor, InputMap>(input, functor);
  }
};

/**
 * \class TensorBlockCwiseUnaryIO
 * \ingroup CXX11_Tensor_Module
 *
 * \brief Tensor block IO class for carrying out cwise unary ops.
 *
 * This class carries out the unary op on given blocks.
 */
template <typename UnaryFunctor, typename StorageIndex, typename OutputScalar,
          int NumDims, int Layout>
struct TensorBlockCwiseUnaryIO {
  typedef typename internal::TensorBlock<OutputScalar, StorageIndex, NumDims,
                                         Layout>::Dimensions Dimensions;

  struct BlockIteratorState {
    StorageIndex output_stride, output_span;
    StorageIndex input_stride, input_span;
    StorageIndex size, count;
  };

  template <typename InputScalar>
  static EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void Run(
      const UnaryFunctor& functor, const Dimensions& block_sizes,
      const Dimensions& block_strides, OutputScalar* output_data,
      const array<StorageIndex, NumDims>& input_strides,
      const InputScalar* input_data) {
    // Find the innermost dimension whose size is not 1. This is the effective
    // inner dim. If all dimensions are of size 1, fallback to using the actual
    // innermost dim to avoid out-of-bound access.
    int num_size_one_inner_dims = 0;
    for (int i = 0; i < NumDims; ++i) {
      const int dim = cond<Layout>()(i, NumDims - i - 1);
      if (block_sizes[dim] != 1) {
        num_size_one_inner_dims = i;
        break;
      }
    }
    // Calculate strides and dimensions.
    const int inner_dim =
        NumDims == 0 ? 1
                     : cond<Layout>()(num_size_one_inner_dims,
                                      NumDims - num_size_one_inner_dims - 1);
    StorageIndex inner_dim_size = NumDims == 0 ? 1 : block_sizes[inner_dim];
    for (int i = num_size_one_inner_dims + 1; i < NumDims; ++i) {
      const int dim = cond<Layout>()(i, NumDims - i - 1);
      // Merge multiple inner dims into one for larger inner dim size (i.e.
      // fewer calls to TensorBlockCwiseUnaryOp::Run()).
      if (inner_dim_size == block_strides[dim] &&
          block_strides[dim] == input_strides[dim]) {
        inner_dim_size *= block_sizes[dim];
        ++num_size_one_inner_dims;
      } else {
        break;
      }
    }

    StorageIndex output_index = 0, input_index = 0;

    const StorageIndex output_stride =
        NumDims == 0 ? 1 : block_strides[inner_dim];
    const StorageIndex input_stride =
        NumDims == 0 ? 1 : input_strides[inner_dim];

    const int at_least_1_dim = NumDims <= 1 ? 1 : NumDims - 1;
    array<BlockIteratorState, at_least_1_dim> block_iter_state;

    // Initialize block iterator state. Squeeze away any dimension of size 1.
    int num_squeezed_dims = 0;
    for (int i = num_size_one_inner_dims; i < NumDims - 1; ++i) {
      const int dim = cond<Layout>()(i + 1, NumDims - i - 2);
      const StorageIndex size = block_sizes[dim];
      if (size == 1) {
        continue;
      }
      BlockIteratorState& state = block_iter_state[num_squeezed_dims];
      state.output_stride = block_strides[dim];
      state.input_stride = input_strides[dim];
      state.size = size;
      state.output_span = state.output_stride * (size - 1);
      state.input_span = state.input_stride * (size - 1);
      state.count = 0;
      ++num_squeezed_dims;
    }

    // Compute cwise unary op.
    const StorageIndex block_total_size =
        NumDims == 0 ? 1 : block_sizes.TotalSize();
    for (StorageIndex i = 0; i < block_total_size; i += inner_dim_size) {
      TensorBlockCwiseUnaryOp::Run(functor, inner_dim_size, output_index,
                                   output_stride, output_data, input_index,
                                   input_stride, input_data);
      // Update index.
      for (int j = 0; j < num_squeezed_dims; ++j) {
        BlockIteratorState& state = block_iter_state[j];
        if (++state.count < state.size) {
          output_index += state.output_stride;
          input_index += state.input_stride;
          break;
        }
        state.count = 0;
        output_index -= state.output_span;
        input_index -= state.input_span;
      }
    }
  }
};

/**
 * \class TensorBlockCwiseBinaryOp
 * \ingroup CXX11_Tensor_Module
 *
 * \brief Carries out a cwise binary op on a number of coefficients.
 *
 * This class reads strided inputs from left and right operands, and writes the
 * result of the cwise binary op to the strided output array.
 *
 */
struct TensorBlockCwiseBinaryOp {
  template <typename StorageIndex, typename BinaryFunctor, typename OutputScalar,
            typename LeftScalar, typename RightScalar>
  static EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void Run(
      const BinaryFunctor& functor, const StorageIndex num_coeff,
      const StorageIndex output_index, const StorageIndex output_stride,
      OutputScalar* output_data, const StorageIndex left_index,
      const StorageIndex left_stride, const LeftScalar* left_data,
      const StorageIndex right_index, const StorageIndex right_stride,
      const RightScalar* right_data) {
    typedef const Array<LeftScalar, Dynamic, 1> Lhs;
    typedef const Array<RightScalar, Dynamic, 1> Rhs;
    typedef Array<OutputScalar, Dynamic, 1> Out;

    typedef Map<Lhs, 0, InnerStride<> > LhsMap;
    typedef Map<Rhs, 0, InnerStride<> > RhsMap;
    typedef Map<Out, 0, InnerStride<> > OutMap;

    const LeftScalar* lhs_base = &left_data[left_index];
    const RightScalar* rhs_base = &right_data[right_index];
    OutputScalar* out_base = &output_data[output_index];

    const LhsMap lhs(lhs_base, num_coeff, InnerStride<>(left_stride));
    const RhsMap rhs(rhs_base, num_coeff, InnerStride<>(right_stride));
    OutMap out(out_base, num_coeff, InnerStride<>(output_stride));

    out = CwiseBinaryOp<BinaryFunctor, LhsMap, RhsMap>(lhs, rhs, functor);
  }
};

/**
 * \class TensorBlockCwiseBinaryIO
 * \ingroup CXX11_Tensor_Module
 *
 * \brief Tensor block IO class for carrying out cwise binary ops.
 *
 * This class carries out the binary op on given blocks.
 *
 */
template <typename BinaryFunctor, typename StorageIndex, typename OutputScalar,
          int NumDims, int Layout>
struct TensorBlockCwiseBinaryIO {
  typedef typename TensorBlock<OutputScalar, StorageIndex, NumDims, Layout>::Dimensions Dimensions;

  struct BlockIteratorState {
    StorageIndex output_stride, output_span;
    StorageIndex left_stride, left_span;
    StorageIndex right_stride, right_span;
    StorageIndex size, count;
  };

  template <typename LeftScalar, typename RightScalar>
  static EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void Run(
      const BinaryFunctor& functor, const Dimensions& block_sizes,
      const Dimensions& block_strides, OutputScalar* output_data,
      const array<StorageIndex, NumDims>& left_strides,
      const LeftScalar* left_data,
      const array<StorageIndex, NumDims>& right_strides,
      const RightScalar* right_data) {
    // Find the innermost dimension whose size is not 1. This is the effective
    // inner dim. If all dimensions are of size 1, fallback to using the actual
    // innermost dim to avoid out-of-bound access.
    int num_size_one_inner_dims = 0;
    for (int i = 0; i < NumDims; ++i) {
      const int dim = cond<Layout>()(i, NumDims - i - 1);
      if (block_sizes[dim] != 1) {
        num_size_one_inner_dims = i;
        break;
      }
    }
    // Calculate strides and dimensions.
    const int inner_dim =
        NumDims == 0 ? 1
                     : cond<Layout>()(num_size_one_inner_dims,
                                      NumDims - num_size_one_inner_dims - 1);
    StorageIndex inner_dim_size = NumDims == 0 ? 1 : block_sizes[inner_dim];
    for (int i = num_size_one_inner_dims + 1; i < NumDims; ++i) {
      const int dim = cond<Layout>()(i, NumDims - i - 1);
      // Merge multiple inner dims into one for larger inner dim size (i.e.
      // fewer calls to TensorBlockCwiseBinaryOp::Run()).
      if (inner_dim_size == block_strides[dim] &&
          block_strides[dim] == left_strides[dim] &&
          block_strides[dim] == right_strides[dim]) {
        inner_dim_size *= block_sizes[dim];
        ++num_size_one_inner_dims;
      } else {
        break;
      }
    }

    StorageIndex output_index = 0, left_index = 0, right_index = 0;
    const StorageIndex output_stride =
        NumDims == 0 ? 1 : block_strides[inner_dim];
    const StorageIndex left_stride = NumDims == 0 ? 1 : left_strides[inner_dim];
    const StorageIndex right_stride =
        NumDims == 0 ? 1 : right_strides[inner_dim];

    const int at_least_1_dim = NumDims <= 1 ? 1 : NumDims - 1;
    array<BlockIteratorState, at_least_1_dim> block_iter_state;

    // Initialize block iterator state. Squeeze away any dimension of size 1.
    int num_squeezed_dims = 0;
    for (int i = num_size_one_inner_dims; i < NumDims - 1; ++i) {
      const int dim = cond<Layout>()(i + 1, NumDims - i - 2);
      const StorageIndex size = block_sizes[dim];
      if (size == 1) {
        continue;
      }
      BlockIteratorState& state = block_iter_state[num_squeezed_dims];
      state.output_stride = block_strides[dim];
      state.left_stride = left_strides[dim];
      state.right_stride = right_strides[dim];
      state.size = size;
      state.output_span = state.output_stride * (size - 1);
      state.left_span = state.left_stride * (size - 1);
      state.right_span = state.right_stride * (size - 1);
      state.count = 0;
      ++num_squeezed_dims;
    }

    // Compute cwise binary op.
    const StorageIndex block_total_size =
        NumDims == 0 ? 1 : block_sizes.TotalSize();
    for (StorageIndex i = 0; i < block_total_size; i += inner_dim_size) {
      TensorBlockCwiseBinaryOp::Run(functor, inner_dim_size, output_index,
                                    output_stride, output_data, left_index,
                                    left_stride, left_data, right_index,
                                    right_stride, right_data);
      // Update index.
      for (int j = 0; j < num_squeezed_dims; ++j) {
        BlockIteratorState& state = block_iter_state[j];
        if (++state.count < state.size) {
          output_index += state.output_stride;
          left_index += state.left_stride;
          right_index += state.right_stride;
          break;
        }
        state.count = 0;
        output_index -= state.output_span;
        left_index -= state.left_span;
        right_index -= state.right_span;
      }
    }
  }
};

/**
 * \class TensorBlockView
 * \ingroup CXX11_Tensor_Module
 *
 * \brief Read-only view into a block of data.
 *
 * This class provides read-only access to a block of data in impl. It may need
 * to allocate space for holding the intermediate result.
 *
 */
template <class ArgType, class Device>
struct TensorBlockView {
  typedef TensorEvaluator<ArgType, Device> Impl;
  typedef typename Impl::Index StorageIndex;
  typedef typename remove_const<typename Impl::Scalar>::type Scalar;
  static const int NumDims = array_size<typename Impl::Dimensions>::value;
  typedef DSizes<StorageIndex, NumDims> Dimensions;

  // Constructs a TensorBlockView for `impl`. `block` is only used for for
  // specifying the start offset, shape, and strides of the block.
  template <typename OtherTensorBlock>
  TensorBlockView(const Device& device,
                  const TensorEvaluator<ArgType, Device>& impl,
                  const OtherTensorBlock& block)
      : m_device(device),
        m_block_sizes(block.block_sizes()),
        m_data(NULL),
        m_allocated_data(NULL) {
    if (Impl::RawAccess && impl.data() != NULL) {
      m_data = impl.data() + block.first_coeff_index();
      m_block_strides = block.tensor_strides();
    } else {
      // Actually make a copy.

      // TODO(wuke): This sometimes put a lot pressure on the heap allocator.
      // Consider allowing ops to request additional temporary block memory in
      // TensorOpResourceRequirements.
      m_allocated_data = static_cast<Scalar*>(
          m_device.allocate(m_block_sizes.TotalSize() * sizeof(Scalar)));
      m_data = m_allocated_data;
      if (NumDims > 0) {
        if (static_cast<int>(Impl::Layout) == static_cast<int>(ColMajor)) {
          m_block_strides[0] = 1;
          for (int i = 1; i < NumDims; ++i) {
            m_block_strides[i] = m_block_strides[i - 1] * m_block_sizes[i - 1];
          }
        } else {
          m_block_strides[NumDims - 1] = 1;
          for (int i = NumDims - 2; i >= 0; --i) {
            m_block_strides[i] = m_block_strides[i + 1] * m_block_sizes[i + 1];
          }
        }
      }
      TensorBlock<Scalar, StorageIndex, NumDims, Impl::Layout> input_block(
          block.first_coeff_index(), m_block_sizes, m_block_strides,
          block.tensor_strides(), m_allocated_data);
      impl.block(&input_block);
    }
  }

  ~TensorBlockView() {
    if (m_allocated_data != NULL) {
      m_device.deallocate(m_allocated_data);
    }
  }

  const Dimensions& block_sizes() const { return m_block_sizes; }
  const Dimensions& block_strides() const { return m_block_strides; }
  const Scalar* data() const { return m_data; }

 private:
  const Device& m_device;
  Dimensions m_block_sizes, m_block_strides;
  const Scalar* m_data;      // Not owned.
  Scalar* m_allocated_data;  // Owned.
};

/**
 * \class TensorBlockMapper
 * \ingroup CXX11_Tensor_Module
 *
 * \brief Tensor block mapper class.
 *
 * This class is responsible for iterating over the blocks of a tensor.
 */
template <typename Scalar, typename StorageIndex, int NumDims, int Layout>
class TensorBlockMapper {
 public:
  typedef TensorBlock<Scalar, StorageIndex, NumDims, Layout> Block;
  typedef DSizes<StorageIndex, NumDims> Dimensions;

  TensorBlockMapper(const Dimensions& dims,
                    const TensorBlockShapeType block_shape,
                    Index min_target_size)
      : m_dimensions(dims),
        m_block_dim_sizes(BlockDimensions(dims, block_shape, internal::convert_index<StorageIndex>(min_target_size))) {
    // Calculate block counts by dimension and total block count.
    DSizes<StorageIndex, NumDims> block_count;
    for (Index i = 0; i < block_count.rank(); ++i) {
      block_count[i] = divup(m_dimensions[i], m_block_dim_sizes[i]);
    }
    m_total_block_count = array_prod(block_count);

    // Calculate block strides (used for enumerating blocks).
    if (NumDims > 0) {
      if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
        m_block_strides[0] = 1;
        m_tensor_strides[0] = 1;
        for (int i = 1; i < NumDims; ++i) {
          m_block_strides[i] = m_block_strides[i - 1] * block_count[i - 1];
          m_tensor_strides[i] = m_tensor_strides[i - 1] * m_dimensions[i - 1];
        }
      } else {
        m_block_strides[NumDims - 1] = 1;
        m_tensor_strides[NumDims - 1] = 1;
        for (int i = NumDims - 2; i >= 0; --i) {
          m_block_strides[i] = m_block_strides[i + 1] * block_count[i + 1];
          m_tensor_strides[i] = m_tensor_strides[i + 1] * m_dimensions[i + 1];
        }
      }
    }
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE Block
  GetBlockForIndex(StorageIndex block_index, Scalar* data) const {
    StorageIndex first_coeff_index = 0;
    DSizes<StorageIndex, NumDims> coords;
    DSizes<StorageIndex, NumDims> sizes;
    DSizes<StorageIndex, NumDims> strides;
    if (NumDims > 0) {
      if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
        for (int i = NumDims - 1; i > 0; --i) {
          const StorageIndex idx = block_index / m_block_strides[i];
          coords[i] = idx * m_block_dim_sizes[i];
          sizes[i] =
              numext::mini((m_dimensions[i] - coords[i]), m_block_dim_sizes[i]);
          block_index -= idx * m_block_strides[i];
          first_coeff_index += coords[i] * m_tensor_strides[i];
        }
        coords[0] = block_index * m_block_dim_sizes[0];
        sizes[0] =
            numext::mini((m_dimensions[0] - coords[0]), m_block_dim_sizes[0]);
        first_coeff_index += coords[0] * m_tensor_strides[0];

        strides[0] = 1;
        for (int i = 1; i < NumDims; ++i) {
          strides[i] = strides[i - 1] * sizes[i - 1];
        }
      } else {
        for (int i = 0; i < NumDims - 1; ++i) {
          const StorageIndex idx = block_index / m_block_strides[i];
          coords[i] = idx * m_block_dim_sizes[i];
          sizes[i] =
              numext::mini((m_dimensions[i] - coords[i]), m_block_dim_sizes[i]);
          block_index -= idx * m_block_strides[i];
          first_coeff_index += coords[i] * m_tensor_strides[i];
        }
        coords[NumDims - 1] = block_index * m_block_dim_sizes[NumDims - 1];
        sizes[NumDims - 1] =
            numext::mini((m_dimensions[NumDims - 1] - coords[NumDims - 1]),
                         m_block_dim_sizes[NumDims - 1]);
        first_coeff_index +=
            coords[NumDims - 1] * m_tensor_strides[NumDims - 1];

        strides[NumDims - 1] = 1;
        for (int i = NumDims - 2; i >= 0; --i) {
          strides[i] = strides[i + 1] * sizes[i + 1];
        }
      }
    }

    return Block(first_coeff_index, sizes, strides, m_tensor_strides, data);
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE StorageIndex total_block_count() const {
    return m_total_block_count;
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE StorageIndex
  block_dims_total_size() const {
    return m_block_dim_sizes.TotalSize();
  }

 private:
  static Dimensions BlockDimensions(const Dimensions& tensor_dims,
                                    const TensorBlockShapeType block_shape,
                                    StorageIndex min_target_size) {
    min_target_size = numext::maxi<StorageIndex>(1, min_target_size);

    // If tensor fully fits into the target size, we'll treat it a single block.
    Dimensions block_dim_sizes = tensor_dims;

    if (tensor_dims.TotalSize() == 0) {
      // Corner case: one of the dimensions is zero. Logic below is too complex
      // to handle this case on a general basis, just use unit block size.
      // Note: we must not yield blocks with zero dimensions (recipe for
      // overflows/underflows, divisions by zero and NaNs later).
      for (int i = 0; i < NumDims; ++i) {
        block_dim_sizes[i] = 1;
      }
    } else if (block_dim_sizes.TotalSize() > min_target_size) {
      if (block_shape == kUniformAllDims) {
        // Tensor will not fit within 'min_target_size' budget: calculate tensor
        // block dimension sizes based on "square" dimension size target.
        const StorageIndex dim_size_target = internal::convert_index<StorageIndex>(
          std::pow(static_cast<float>(min_target_size),
                   1.0f / static_cast<float>(block_dim_sizes.rank())));
        for (Index i = 0; i < block_dim_sizes.rank(); ++i) {
          // TODO(andydavis) Adjust the inner most 'block_dim_size' to make it
          // a multiple of the packet size. Note that reducing
          // 'block_dim_size' in this manner can increase the number of
          // blocks, and so will amplify any per-block overhead.
          block_dim_sizes[i] = numext::mini(dim_size_target, tensor_dims[i]);
        }
        // Add any un-allocated coefficients to inner dimension(s).
        StorageIndex total_size = block_dim_sizes.TotalSize();
        for (int i = 0; i < NumDims; ++i) {
          const int dim = cond<Layout>()(i, NumDims - i - 1);
          if (block_dim_sizes[dim] < tensor_dims[dim]) {
            const StorageIndex total_size_other_dims =
                total_size / block_dim_sizes[dim];
            const StorageIndex alloc_avail =
                divup<StorageIndex>(min_target_size, total_size_other_dims);
            if (alloc_avail == block_dim_sizes[dim]) {
              // Insufficient excess coefficients to allocate.
              break;
            }
            block_dim_sizes[dim] = numext::mini(tensor_dims[dim], alloc_avail);
            total_size = total_size_other_dims * block_dim_sizes[dim];
          }
        }
      } else if (block_shape == kSkewedInnerDims) {
        StorageIndex coeff_to_allocate = min_target_size;
        for (int i = 0; i < NumDims; ++i) {
          const int dim = cond<Layout>()(i, NumDims - i - 1);
          block_dim_sizes[dim] =
              numext::mini(coeff_to_allocate, tensor_dims[dim]);
          coeff_to_allocate = divup(
              coeff_to_allocate,
              numext::maxi(static_cast<StorageIndex>(1), block_dim_sizes[dim]));
        }
        eigen_assert(coeff_to_allocate == 1);
      } else {
        eigen_assert(false);  // someone added new block shape type
      }
    }

    eigen_assert(
        block_dim_sizes.TotalSize() >=
        numext::mini<Index>(min_target_size, tensor_dims.TotalSize()));

    return block_dim_sizes;
  }

  Dimensions m_dimensions;
  Dimensions m_block_dim_sizes;
  Dimensions m_block_strides;
  Dimensions m_tensor_strides;
  StorageIndex m_total_block_count;
};

/**
 * \class TensorSliceBlockMapper
 * \ingroup CXX11_Tensor_Module
 *
 * \brief Tensor slice block mapper class.
 *
 * This class is responsible for iterating over the blocks of
 * a slice of a tensor. Supports shuffling of the block strides
 * for callers that want to reduce strides for dimensions to be
 * processed together.
 *
 */
template <typename Scalar, typename StorageIndex, int NumDims, int Layout>
class TensorSliceBlockMapper {
 public:
  typedef TensorBlock<Scalar, StorageIndex, NumDims, Layout> Block;
  typedef DSizes<StorageIndex, NumDims> Dimensions;

  TensorSliceBlockMapper(const Dimensions& tensor_dims,
                         const Dimensions& tensor_slice_offsets,
                         const Dimensions& tensor_slice_extents,
                         const Dimensions& block_dim_sizes,
                         const Dimensions& block_stride_order)
      : m_tensor_dimensions(tensor_dims),
        m_tensor_slice_offsets(tensor_slice_offsets),
        m_tensor_slice_extents(tensor_slice_extents),
        m_block_dim_sizes(block_dim_sizes),
        m_block_stride_order(block_stride_order),
        m_total_block_count(1) {
    // Calculate block counts by dimension and total block count.
    DSizes<StorageIndex, NumDims> block_count;
    for (Index i = 0; i < block_count.rank(); ++i) {
      block_count[i] = divup(m_tensor_slice_extents[i], m_block_dim_sizes[i]);
    }
    m_total_block_count = array_prod(block_count);

    // Calculate block strides (used for enumerating blocks).
    if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
      m_block_strides[0] = 1;
      m_tensor_strides[0] = 1;
      for (int i = 1; i < NumDims; ++i) {
        m_block_strides[i] = m_block_strides[i - 1] * block_count[i - 1];
        m_tensor_strides[i] =
            m_tensor_strides[i - 1] * m_tensor_dimensions[i - 1];
      }
    } else {
      m_block_strides[NumDims - 1] = 1;
      m_tensor_strides[NumDims - 1] = 1;
      for (int i = NumDims - 2; i >= 0; --i) {
        m_block_strides[i] = m_block_strides[i + 1] * block_count[i + 1];
        m_tensor_strides[i] =
            m_tensor_strides[i + 1] * m_tensor_dimensions[i + 1];
      }
    }
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE Block
  GetBlockForIndex(StorageIndex block_index, Scalar* data) const {
    StorageIndex first_coeff_index = 0;
    DSizes<StorageIndex, NumDims> coords;
    DSizes<StorageIndex, NumDims> sizes;
    DSizes<StorageIndex, NumDims> strides;
    if (static_cast<int>(Layout) == static_cast<int>(ColMajor)) {
      for (int i = NumDims - 1; i > 0; --i) {
        const Index idx = block_index / m_block_strides[i];
        coords[i] = m_tensor_slice_offsets[i] + idx * m_block_dim_sizes[i];
        sizes[i] = numext::mini(
            m_tensor_slice_offsets[i] + m_tensor_slice_extents[i] - coords[i],
            m_block_dim_sizes[i]);
        block_index -= idx * m_block_strides[i];
        first_coeff_index += coords[i] * m_tensor_strides[i];
      }
      coords[0] =
          m_tensor_slice_offsets[0] + block_index * m_block_dim_sizes[0];
      sizes[0] = numext::mini(
          m_tensor_slice_offsets[0] + m_tensor_slice_extents[0] - coords[0],
          m_block_dim_sizes[0]);
      first_coeff_index += coords[0] * m_tensor_strides[0];

      StorageIndex prev_dim = m_block_stride_order[0];
      strides[prev_dim] = 1;
      for (int i = 1; i < NumDims; ++i) {
        const StorageIndex curr_dim = m_block_stride_order[i];
        strides[curr_dim] = strides[prev_dim] * sizes[prev_dim];
        prev_dim = curr_dim;
      }
    } else {
      for (int i = 0; i < NumDims - 1; ++i) {
        const StorageIndex idx = block_index / m_block_strides[i];
        coords[i] = m_tensor_slice_offsets[i] + idx * m_block_dim_sizes[i];
        sizes[i] = numext::mini(
            m_tensor_slice_offsets[i] + m_tensor_slice_extents[i] - coords[i],
            m_block_dim_sizes[i]);
        block_index -= idx * m_block_strides[i];
        first_coeff_index += coords[i] * m_tensor_strides[i];
      }
      coords[NumDims - 1] = m_tensor_slice_offsets[NumDims - 1] +
                            block_index * m_block_dim_sizes[NumDims - 1];
      sizes[NumDims - 1] = numext::mini(
          m_tensor_slice_offsets[NumDims - 1] +
              m_tensor_slice_extents[NumDims - 1] - coords[NumDims - 1],
          m_block_dim_sizes[NumDims - 1]);
      first_coeff_index += coords[NumDims - 1] * m_tensor_strides[NumDims - 1];

      StorageIndex prev_dim = m_block_stride_order[NumDims - 1];
      strides[prev_dim] = 1;
      for (int i = NumDims - 2; i >= 0; --i) {
        const StorageIndex curr_dim = m_block_stride_order[i];
        strides[curr_dim] = strides[prev_dim] * sizes[prev_dim];
        prev_dim = curr_dim;
      }
    }

    return Block(first_coeff_index, sizes, strides, m_tensor_strides, data);
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE StorageIndex total_block_count() const {
    return m_total_block_count;
  }

 private:
  Dimensions m_tensor_dimensions;
  Dimensions m_tensor_slice_offsets;
  Dimensions m_tensor_slice_extents;
  Dimensions m_tensor_strides;
  Dimensions m_block_dim_sizes;
  Dimensions m_block_stride_order;
  Dimensions m_block_strides;
  StorageIndex m_total_block_count;
};

}  // namespace internal

}  // namespace Eigen

#endif  // EIGEN_CXX11_TENSOR_TENSOR_BLOCK_H
