// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Mehdi Goli    Codeplay Software Ltd.
// Ralph Potter  Codeplay Software Ltd.
// Luke Iwanski  Codeplay Software Ltd.
// Contact: <eigen@codeplay.com>
// Copyright (C) 2016 Benoit Steiner <benoit.steiner.goog@gmail.com>

//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#ifndef EIGEN_CXX11_TENSOR_TENSOR_CONVOLUTION_SYCL_H
#define EIGEN_CXX11_TENSOR_TENSOR_CONVOLUTION_SYCL_H

namespace Eigen {

/** \class TensorConvolution
  * \ingroup CXX11_Tensor_Module
  *
  * \brief Tensor convolution class.
  *
  *
  */
template <typename CoeffReturnType, typename KernelType, typename HostExpr, typename FunctorExpr, typename Index,
typename InputDims, typename Kernel_accessor, typename Buffer_accessor, typename Local_accessor, typename TupleType>
struct EigenConvolutionKernel1D{
typedef  typename TensorSycl::internal::createPlaceHolderExpression<HostExpr>::Type PlaceHolderExpr;
internal::IndexMapper<Index, InputDims, 1, Eigen::internal::traits<HostExpr>::Layout> indexMapper;
Kernel_accessor kernel_filter;
const size_t kernelSize, range_x, range_y;
Buffer_accessor buffer_acc;
ptrdiff_t out_offset;
Local_accessor local_acc;
FunctorExpr functors;
TupleType tuple_of_accessors;
EigenConvolutionKernel1D(internal::IndexMapper<Index, InputDims, 1, Eigen::internal::traits<HostExpr>::Layout> indexMapper_,
  Kernel_accessor kernel_filter_,  const size_t kernelSize_, const size_t range_x_, const size_t range_y_,
  Buffer_accessor buffer_acc_, ptrdiff_t out_offset_, Local_accessor local_acc_, FunctorExpr functors_, TupleType tuple_of_accessors_)
  :indexMapper(indexMapper_), kernel_filter(kernel_filter_), kernelSize(kernelSize_), range_x(range_x_), range_y(range_y_),
  buffer_acc(buffer_acc_), out_offset(out_offset_),local_acc(local_acc_), functors(functors_), tuple_of_accessors(tuple_of_accessors_) {}

  void operator()(cl::sycl::nd_item<2> itemID) {
    typedef typename TensorSycl::internal::ConvertToDeviceExpression<HostExpr>::Type DevExpr;
    auto device_expr =TensorSycl::internal::createDeviceExpression<DevExpr, PlaceHolderExpr>(functors, tuple_of_accessors);
    auto device_evaluator = Eigen::TensorEvaluator<DevExpr, Eigen::SyclKernelDevice>(device_expr.expr, Eigen::SyclKernelDevice());

    auto buffer_ptr = ConvertToActualTypeSycl(CoeffReturnType, buffer_acc);
    auto kernel_ptr = ConvertToActualTypeSycl(KernelType, kernel_filter);

    const size_t num_x_input =  (itemID.get_local_range()[0] +kernelSize -1); //the required row to be calculated for the for each plane in shered memory
    const size_t plane_kernel_offset = itemID.get_local(1) * num_x_input;
    const size_t first_input_start = itemID.get_group(0)*itemID.get_local_range()[0];
    const size_t plane_tensor_offset =indexMapper.mapCudaInputPlaneToTensorInputOffset(itemID.get_global(1));
    /// fill the shared memory
    for (size_t i = itemID.get_local(0); i < num_x_input ; i += itemID.get_local_range()[0]) {
      const size_t local_index = i + plane_kernel_offset ;
      const size_t tensor_index  =  plane_tensor_offset + indexMapper.mapCudaInputKernelToTensorInputOffset(i + first_input_start);
      if(((i + first_input_start) < (range_x +kernelSize-1)) && itemID.get_global(1)< range_y){
        local_acc[local_index] = device_evaluator.coeff(tensor_index);
      }
      else local_acc[local_index]=0.0f;
    }

    itemID.barrier(cl::sycl::access::fence_space::local_space);

    // calculate the convolution
    const size_t first_output_start =itemID.get_group(0)*(itemID.get_local_range()[0]); // output start x
    if(itemID.get_global(0)< range_x && itemID.get_global(1)< range_y){
      CoeffReturnType result = static_cast<CoeffReturnType>(0);
      const size_t index = plane_kernel_offset+ itemID.get_local(0);
      for (size_t k = 0; k < kernelSize; ++k) {
        result += (local_acc[k + index] * kernel_ptr[k]);
      }
      const size_t tensor_index = indexMapper.mapCudaOutputPlaneToTensorOutputOffset(itemID.get_global(1))
      +indexMapper.mapCudaOutputKernelToTensorOutputOffset(itemID.get_local(0) + first_output_start);
      buffer_ptr[tensor_index+ConvertToActualSyclOffset(CoeffReturnType, out_offset)] = result;
    }
  }
};


template <typename CoeffReturnType, typename KernelType, typename HostExpr, typename FunctorExpr, typename Index,
typename InputDims, typename Kernel_accessor, typename Buffer_accessor, typename Local_accessor, typename TupleType>
struct EigenConvolutionKernel2D{
typedef  typename TensorSycl::internal::createPlaceHolderExpression<HostExpr>::Type PlaceHolderExpr;
internal::IndexMapper<Index, InputDims, 2, Eigen::internal::traits<HostExpr>::Layout> indexMapper;
Kernel_accessor kernel_filter;
const size_t kernelSize_x, kernelSize_y, range_x, range_y , range_z;
Buffer_accessor buffer_acc;
ptrdiff_t out_offset;
Local_accessor local_acc;
FunctorExpr functors;
TupleType tuple_of_accessors;
EigenConvolutionKernel2D(internal::IndexMapper<Index, InputDims, 2, Eigen::internal::traits<HostExpr>::Layout> indexMapper_,
  Kernel_accessor kernel_filter_,  const size_t kernelSize_x_, const size_t kernelSize_y_ ,const size_t range_x_, const size_t range_y_, const size_t range_z_,
  Buffer_accessor buffer_acc_, ptrdiff_t out_offset_, Local_accessor local_acc_, FunctorExpr functors_, TupleType tuple_of_accessors_)
  :indexMapper(indexMapper_), kernel_filter(kernel_filter_), kernelSize_x(kernelSize_x_), kernelSize_y(kernelSize_y_), range_x(range_x_), range_y(range_y_), range_z(range_z_),
  buffer_acc(buffer_acc_), out_offset(out_offset_), local_acc(local_acc_), functors(functors_), tuple_of_accessors(tuple_of_accessors_) {}

  void operator()(cl::sycl::nd_item<3> itemID) {
    typedef typename TensorSycl::internal::ConvertToDeviceExpression<HostExpr>::Type DevExpr;
    auto device_expr =TensorSycl::internal::createDeviceExpression<DevExpr, PlaceHolderExpr>(functors, tuple_of_accessors);
    auto device_evaluator = Eigen::TensorEvaluator<DevExpr, Eigen::SyclKernelDevice>(device_expr.expr, Eigen::SyclKernelDevice());

    auto buffer_ptr = ConvertToActualTypeSycl(CoeffReturnType, buffer_acc);
    auto kernel_ptr = ConvertToActualTypeSycl(KernelType, kernel_filter);
    const size_t num_x_input =  (itemID.get_local_range()[0] +kernelSize_x -1); //the required row to be calculated for the for each plane in shered memory
    const size_t num_y_input =  (itemID.get_local_range()[1] +kernelSize_y -1); //the required row to be calculated for the for each plane in shered memory
    const size_t plane_input_offset = indexMapper.mapCudaInputPlaneToTensorInputOffset(itemID.get_global(2));
    const size_t plane_kernel_offset = itemID.get_local(2) * num_y_input;

    /// fill the shared memory
    const size_t first_x_input_start = itemID.get_group(0)*itemID.get_local_range()[0];
    const size_t first_y_input_start = itemID.get_group(1)*itemID.get_local_range()[1];
    for (size_t j = itemID.get_local(1); j < num_y_input; j += itemID.get_local_range()[1]) {
      const size_t local_input_offset = num_x_input * (j + plane_kernel_offset);
      for (size_t i = itemID.get_local(0); i < num_x_input ; i += itemID.get_local_range()[0]) {
        const size_t local_index = i + local_input_offset;
        const size_t tensor_index  = plane_input_offset + indexMapper.mapCudaInputKernelToTensorInputOffset(i + first_x_input_start, j+ first_y_input_start );
        if(((i + first_x_input_start) < (range_x +kernelSize_x-1))  &&((j + first_y_input_start) < (range_y +kernelSize_y-1)) && itemID.get_global(2)< range_z){
          local_acc[local_index] = device_evaluator.coeff(tensor_index);
        }
        else local_acc[local_index]=0.0f;
    }
  }

    itemID.barrier(cl::sycl::access::fence_space::local_space);

    // calculate the convolution
    const size_t fitst_x_output_start =itemID.get_group(0)*(itemID.get_local_range()[0]); // output start x
    const size_t fitst_y_output_start =itemID.get_group(1)*(itemID.get_local_range()[1]); // output start y
    if(itemID.get_global(0)< range_x && itemID.get_global(1)< range_y && itemID.get_global(2)< range_z){
      CoeffReturnType result = static_cast<CoeffReturnType>(0);
      for (size_t j = 0; j < kernelSize_y; j++) {
        size_t kernel_offset =kernelSize_x * j;
        const size_t index = (num_x_input*(plane_kernel_offset + j+ itemID.get_local(1))) + itemID.get_local(0);
        for (size_t i = 0; i < kernelSize_x; i++) {
        result += (local_acc[i + index] * kernel_ptr[i+kernel_offset]);
        }
      }
      const size_t tensor_index = indexMapper.mapCudaOutputPlaneToTensorOutputOffset(itemID.get_global(2))
      +indexMapper.mapCudaOutputKernelToTensorOutputOffset(itemID.get_local(0) + fitst_x_output_start, itemID.get_local(1) + fitst_y_output_start);
      buffer_ptr[tensor_index  +ConvertToActualSyclOffset(CoeffReturnType, out_offset)] = result;
    }
  }
};



template <typename CoeffReturnType, typename KernelType, typename HostExpr, typename FunctorExpr, typename Index,
typename InputDims, typename Kernel_accessor, typename Buffer_accessor, typename Local_accessor, typename TupleType>
struct EigenConvolutionKernel3D{
typedef  typename TensorSycl::internal::createPlaceHolderExpression<HostExpr>::Type PlaceHolderExpr;
internal::IndexMapper<Index, InputDims, 3, Eigen::internal::traits<HostExpr>::Layout> indexMapper;
Kernel_accessor kernel_filter;
const size_t kernelSize_x, kernelSize_y, kernelSize_z, range_x, range_y , range_z, numP;
Buffer_accessor buffer_acc;
ptrdiff_t out_offset;
Local_accessor local_acc;
FunctorExpr functors;
TupleType tuple_of_accessors;
EigenConvolutionKernel3D(internal::IndexMapper<Index, InputDims, 3, Eigen::internal::traits<HostExpr>::Layout> indexMapper_,
  Kernel_accessor kernel_filter_,  const size_t kernelSize_x_, const size_t kernelSize_y_ , const size_t kernelSize_z_ ,
  const size_t range_x_, const size_t range_y_, const size_t range_z_, const size_t numP_,
  Buffer_accessor buffer_acc_, ptrdiff_t out_offset_, Local_accessor local_acc_, FunctorExpr functors_, TupleType tuple_of_accessors_)
  :indexMapper(indexMapper_), kernel_filter(kernel_filter_), kernelSize_x(kernelSize_x_), kernelSize_y(kernelSize_y_),
  kernelSize_z(kernelSize_z_), range_x(range_x_), range_y(range_y_), range_z(range_z_), numP(numP_),
  buffer_acc(buffer_acc_), out_offset(out_offset_), local_acc(local_acc_), functors(functors_), tuple_of_accessors(tuple_of_accessors_) {}

  void operator()(cl::sycl::nd_item<3> itemID) {
    typedef typename TensorSycl::internal::ConvertToDeviceExpression<HostExpr>::Type DevExpr;
    auto device_expr =TensorSycl::internal::createDeviceExpression<DevExpr, PlaceHolderExpr>(functors, tuple_of_accessors);
    auto device_evaluator = Eigen::TensorEvaluator<DevExpr, Eigen::SyclKernelDevice>(device_expr.expr, Eigen::SyclKernelDevice());

    auto buffer_ptr = ConvertToActualTypeSycl(CoeffReturnType, buffer_acc);
    auto kernel_ptr = ConvertToActualTypeSycl(KernelType, kernel_filter);
    const size_t num_x_input =  (itemID.get_local_range()[0] +kernelSize_x -1); //the required row to be calculated for the for each plane in shered memory
    const size_t num_y_input =  (itemID.get_local_range()[1] +kernelSize_y -1); //the required row to be calculated for the for each plane in shered memory
    const size_t num_z_input =  (itemID.get_local_range()[2] +kernelSize_z -1); //the required row to be calculated for the for each plane in shered memory
    const size_t first_x_input_start = itemID.get_group(0)*itemID.get_local_range()[0];
    const size_t first_y_input_start = itemID.get_group(1)*itemID.get_local_range()[1];
    const size_t first_z_input_start = itemID.get_group(2)*itemID.get_local_range()[2];
    for(size_t p=0; p<numP; p++){
      /// fill the shared memory
      const size_t plane_input_offset = indexMapper.mapCudaInputPlaneToTensorInputOffset(p);
      for (size_t k = itemID.get_local(2); k < num_z_input; k += itemID.get_local_range()[2]) {
        for (size_t j = itemID.get_local(1); j < num_y_input; j += itemID.get_local_range()[1]) {
          for (size_t i = itemID.get_local(0); i < num_x_input ; i += itemID.get_local_range()[0]) {
            const size_t local_index = i + (num_x_input * (j + (num_y_input * k)));
            const size_t tensor_index  = plane_input_offset + indexMapper.mapCudaInputKernelToTensorInputOffset(i + first_x_input_start, j+ first_y_input_start , k+ first_z_input_start );
            if(((i + first_x_input_start) < (range_x +kernelSize_x-1))  && ((j + first_y_input_start) < (range_y +kernelSize_y-1)) &&  ((k + first_z_input_start) < (range_z +kernelSize_z-1)) ){
              local_acc[local_index] = device_evaluator.coeff(tensor_index);
            }
            else local_acc[local_index]=0.0f;
          }
        }
      }
      itemID.barrier(cl::sycl::access::fence_space::local_space);

      // calculate the convolution
      const size_t fitst_x_output_start =itemID.get_group(0)*(itemID.get_local_range()[0]); // x
      const size_t fitst_y_output_start =itemID.get_group(1)*(itemID.get_local_range()[1]); // y
      const size_t fitst_z_output_start =itemID.get_group(2)*(itemID.get_local_range()[2]); // z

      if(itemID.get_global(0)< range_x && itemID.get_global(1)< range_y && itemID.get_global(2)< range_z){
        CoeffReturnType result = static_cast<CoeffReturnType>(0);
        for (size_t k = 0; k < kernelSize_z; k++) {
          for (size_t j = 0; j < kernelSize_y; j++) {
            for (size_t i = 0; i < kernelSize_x; i++) {
              const size_t kernel_index =i + kernelSize_x * (j + kernelSize_y * k);
              const size_t local_index = ((i+ itemID.get_local(0))+  num_x_input*((j+ itemID.get_local(1)) + num_y_input * (k+ itemID.get_local(2))));
              result += (local_acc[local_index] * kernel_ptr[kernel_index]);
            }
          }
        }
        const size_t tensor_index = indexMapper.mapCudaOutputPlaneToTensorOutputOffset(p)
        +indexMapper.mapCudaOutputKernelToTensorOutputOffset(itemID.get_local(0) + fitst_x_output_start, itemID.get_local(1) + fitst_y_output_start, itemID.get_local(2) + fitst_z_output_start );
        buffer_ptr[tensor_index+ConvertToActualSyclOffset(CoeffReturnType, out_offset)] = result;
      }

      itemID.barrier(cl::sycl::access::fence_space::local_space);
    }
  }
};


template<typename Indices, typename InputArgType, typename KernelArgType>
struct TensorEvaluator<const TensorConvolutionOp<Indices, InputArgType, KernelArgType>, const Eigen::SyclDevice>
{
  typedef TensorConvolutionOp<Indices, InputArgType, KernelArgType> XprType;

  static const int NumDims =  internal::array_size<typename TensorEvaluator<InputArgType, const Eigen::SyclDevice>::Dimensions>::value;
  static const int NumKernelDims = internal::array_size<Indices>::value;
  typedef typename XprType::Index Index;
  typedef DSizes<Index, NumDims> Dimensions;
  typedef typename TensorEvaluator<KernelArgType, const Eigen::SyclDevice>::Dimensions KernelDimensions;
  typedef const Eigen::SyclDevice Device;

  enum {
    IsAligned = TensorEvaluator<InputArgType, const Eigen::SyclDevice>::IsAligned & TensorEvaluator<KernelArgType, const Eigen::SyclDevice>::IsAligned,
    PacketAccess = false,
    BlockAccess = false,
    PreferBlockAccess = false,
    Layout = TensorEvaluator<InputArgType, const Eigen::SyclDevice>::Layout,
    CoordAccess = false,  // to be implemented
    RawAccess = false
  };

  EIGEN_DEVICE_FUNC TensorEvaluator(const XprType& op, const Eigen::SyclDevice& device)
      : m_inputImpl(op.inputExpression(), device), m_kernelArg(op.kernelExpression()), m_kernelImpl(op.kernelExpression(), device), m_indices(op.indices()), m_buf(NULL), m_kernel(NULL), m_local_kernel(false), m_device(device)
  {
    EIGEN_STATIC_ASSERT((static_cast<int>(TensorEvaluator<InputArgType, const Eigen::SyclDevice>::Layout) == static_cast<int>(TensorEvaluator<KernelArgType, const Eigen::SyclDevice>::Layout)), YOU_MADE_A_PROGRAMMING_MISTAKE);

    const typename TensorEvaluator<InputArgType, const Eigen::SyclDevice>::Dimensions& input_dims = m_inputImpl.dimensions();
    const typename TensorEvaluator<KernelArgType, const Eigen::SyclDevice>::Dimensions& kernel_dims = m_kernelImpl.dimensions();

    m_dimensions = m_inputImpl.dimensions();
    for (int i = 0; i < NumKernelDims; ++i) {
      const Index index = op.indices()[i];
      const Index input_dim = input_dims[index];
      const Index kernel_dim = kernel_dims[i];
      const Index result_dim = input_dim - kernel_dim + 1;
      m_dimensions[index] = result_dim;
    }
  }

  typedef typename XprType::CoeffReturnType CoeffReturnType;
  typedef typename PacketType<CoeffReturnType, const Eigen::SyclDevice>::type PacketReturnType;
  typedef typename InputArgType::Scalar Scalar;
  static const int PacketSize = internal::unpacket_traits<PacketReturnType>::size;

  EIGEN_DEVICE_FUNC const Dimensions& dimensions() const { return m_dimensions; }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE bool evalSubExprsIfNeeded(Scalar* data) {
    preloadKernel();
    m_inputImpl.evalSubExprsIfNeeded(NULL);
    if (data) {
      executeEval(data);
      return false;
    } else {
      m_buf = (Scalar*)m_device.allocate(dimensions().TotalSize() * sizeof(Scalar));
      executeEval(m_buf);
      return true;
    }
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void cleanup() {
    m_inputImpl.cleanup();
    if (m_buf) {
      m_device.deallocate(m_buf);
      m_buf = NULL;
    }
    if (m_local_kernel) {
      m_device.deallocate((void*)m_kernel);
      m_local_kernel = false;
    }
    m_kernel = NULL;
  }
  /// used by sycl in order to build the sycl buffer
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE const Device& device() const{return m_device;}
  /// used by sycl in order to build the sycl buffer
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE typename Eigen::internal::traits<XprType>::PointerType data() const { return m_buf; }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE void preloadKernel() {
    // Don't make a local copy of the kernel unless we have to (i.e. it's an
    // expression that needs to be evaluated)
    const Scalar* in_place = m_kernelImpl.data();
    if (in_place) {
      m_kernel = in_place;
      m_local_kernel = false;
    } else {
      ptrdiff_t kernel_sz = m_kernelImpl.dimensions().TotalSize() * sizeof(Scalar);
      Scalar* local = (Scalar*)m_device.allocate(kernel_sz);
      typedef TensorEvalToOp<const KernelArgType> EvalTo;
      EvalTo evalToTmp(local, m_kernelArg);
      const bool PacketAccess = internal::IsVectorizable<const Eigen::SyclDevice, KernelArgType>::value;
      internal::TensorExecutor<const EvalTo, const Eigen::SyclDevice, PacketAccess>::run(evalToTmp, m_device);
      m_kernel = local;
      m_local_kernel = true;
    }
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE  void executeEval(Scalar* data) const {
    typedef TensorEvaluator<InputArgType, const Eigen::SyclDevice> InputEvaluator;
    typedef typename InputEvaluator::Dimensions InputDims;

    typedef Eigen::TensorSycl::internal::FunctorExtractor<InputEvaluator> InputFunctorExpr;
    // extract input functor list
    InputFunctorExpr input_functors = Eigen::TensorSycl::internal::extractFunctors(m_inputImpl);
    ptrdiff_t out_offset = m_device.get_offset(data);


    m_device.sycl_queue().submit([&](cl::sycl::handler &cgh) {

      typedef cl::sycl::accessor<CoeffReturnType, 1, cl::sycl::access::mode::read_write, cl::sycl::access::target::local> InputLocalAcc;
      /// work-around for gcc 4.8 auto bug
      typedef decltype(Eigen::TensorSycl::internal::createTupleOfAccessors<InputEvaluator>(cgh, m_inputImpl)) InputTupleType;
      // create input tuple of accessors
      InputTupleType tuple_of_accessors = Eigen::TensorSycl::internal::createTupleOfAccessors<InputEvaluator>(cgh, m_inputImpl);

      typedef cl::sycl::accessor<uint8_t, 1, cl::sycl::access::mode::write, cl::sycl::access::target::global_buffer> OutputAccessorType;
      OutputAccessorType out_res= m_device. template get_sycl_accessor<cl::sycl::access::mode::write>(cgh, data);
      typedef cl::sycl::accessor<uint8_t, 1, cl::sycl::access::mode::read, cl::sycl::access::target::global_buffer> KernelAccessorType;
      KernelAccessorType kernel_acc= m_device. template get_sycl_accessor<cl::sycl::access::mode::read>(cgh, m_kernel);

      switch (NumKernelDims) {
        case 1: {
          const size_t numX = dimensions()[m_indices[0]];
          const size_t numP = dimensions().TotalSize() / numX;
          const size_t kernel_size = m_kernelImpl.dimensions().TotalSize();
          size_t range_x, GRange_x, tileSize_x, range_y, GRange_y, tileSize_y;
          m_device.parallel_for_setup(numX, numP, tileSize_x,tileSize_y,range_x,range_y, GRange_x, GRange_y );
          const size_t shared_mem =(tileSize_x +kernel_size -1)*(tileSize_y);
          gpu_assert(static_cast<unsigned long>(shared_mem) <= m_device.sharedMemPerBlock());
          auto global_range=cl::sycl::range<2>(GRange_x, GRange_y);  // global range
          auto local_range=cl::sycl::range<2>(tileSize_x, tileSize_y);  // local range
          InputLocalAcc local_acc(cl::sycl::range<1>(shared_mem), cgh);
          const array<Index, 1> indices{{m_indices[0]}};
          const array<Index, 1> kernel_dims{{m_kernelImpl.dimensions()[0]}};
          internal::IndexMapper<Index, InputDims, 1, Layout> indexMapper(m_inputImpl.dimensions(), kernel_dims, indices);
          cgh.parallel_for(cl::sycl::nd_range<2>(global_range, local_range),
          EigenConvolutionKernel1D<CoeffReturnType, Scalar, InputArgType, InputFunctorExpr, Index,
          InputDims, KernelAccessorType, OutputAccessorType, InputLocalAcc, InputTupleType>(
          indexMapper,kernel_acc, kernel_size, numX, numP, out_res, out_offset, local_acc, input_functors, tuple_of_accessors));
          break;
        }

        case 2: {
          const size_t idxX =static_cast<int>(Layout) == static_cast<int>(ColMajor) ? 0 : 1;
          const size_t idxY =static_cast<int>(Layout) == static_cast<int>(ColMajor) ? 1 : 0;
          const size_t kernel_size_x = m_kernelImpl.dimensions()[idxX];
          const size_t kernel_size_y = m_kernelImpl.dimensions()[idxY];
          const size_t numX = dimensions()[m_indices[idxX]];
          const size_t numY = dimensions()[m_indices[idxY]];
          const size_t numP = dimensions().TotalSize() / (numX*numY);
          size_t range_x, GRange_x, tileSize_x, range_y, GRange_y, tileSize_y, range_z, GRange_z, tileSize_z;
          m_device.parallel_for_setup(numX, numY, numP, tileSize_x, tileSize_y, tileSize_z, range_x, range_y, range_z, GRange_x, GRange_y, GRange_z );
          const size_t shared_mem =(tileSize_x +kernel_size_x -1)*(tileSize_y +kernel_size_y -1) * tileSize_z;
          gpu_assert(static_cast<unsigned long>(shared_mem) <= m_device.sharedMemPerBlock());
          auto global_range=cl::sycl::range<3>(GRange_x, GRange_y, GRange_z);  // global range
          auto local_range=cl::sycl::range<3>(tileSize_x, tileSize_y, tileSize_z);  // local range
          InputLocalAcc local_acc(cl::sycl::range<1>(shared_mem), cgh);
          const array<Index, 2> indices {{m_indices[idxX], m_indices[idxY]}};
          const array<Index, 2> kernel_dims{{m_kernelImpl.dimensions()[idxX], m_kernelImpl.dimensions()[idxY]}};
          internal::IndexMapper<Index, InputDims, 2, Layout> indexMapper(m_inputImpl.dimensions(), kernel_dims, indices);
          cgh.parallel_for(cl::sycl::nd_range<3>(global_range, local_range),
          EigenConvolutionKernel2D<CoeffReturnType, Scalar, InputArgType, InputFunctorExpr, Index,
          InputDims, KernelAccessorType, OutputAccessorType, InputLocalAcc, InputTupleType>(
          indexMapper,kernel_acc, kernel_size_x,  kernel_size_y, numX, numY, numP, out_res, out_offset, local_acc, input_functors, tuple_of_accessors));
          break;
        }

        case 3: {
          const size_t idxX =static_cast<int>(Layout) == static_cast<int>(ColMajor) ? 0 : 2;
          const size_t idxY =static_cast<int>(Layout) == static_cast<int>(ColMajor) ? 1 : 1;
          const size_t idxZ =static_cast<int>(Layout) == static_cast<int>(ColMajor) ? 2 : 0;
          const size_t kernel_size_x = m_kernelImpl.dimensions()[idxX];
          const size_t kernel_size_y = m_kernelImpl.dimensions()[idxY];
          const size_t kernel_size_z = m_kernelImpl.dimensions()[idxZ];
          const size_t numX = dimensions()[m_indices[idxX]];
          const size_t numY = dimensions()[m_indices[idxY]];
          const size_t numZ = dimensions()[m_indices[idxZ]];
          const size_t numP = dimensions().TotalSize() / (numX*numY*numZ);
          const array<Index, 3> indices{{m_indices[idxX], m_indices[idxY], m_indices[idxZ]}};
          const array<Index, 3> kernel_dims{{m_kernelImpl.dimensions()[idxX],m_kernelImpl.dimensions()[idxY], m_kernelImpl.dimensions()[idxZ]}};
          internal::IndexMapper<Index, InputDims, 3, Layout> indexMapper(m_inputImpl.dimensions(), kernel_dims, indices);
          size_t range_x, GRange_x, tileSize_x, range_y, GRange_y, tileSize_y, range_z, GRange_z, tileSize_z;
          m_device.parallel_for_setup(numX, numY, numZ, tileSize_x, tileSize_y, tileSize_z, range_x, range_y, range_z, GRange_x, GRange_y, GRange_z );
          const size_t shared_mem =(tileSize_x +kernel_size_x -1)*(tileSize_y +kernel_size_y -1) * (tileSize_z +kernel_size_y -1);
          gpu_assert(static_cast<unsigned long>(shared_mem) <= m_device.sharedMemPerBlock());
          auto global_range=cl::sycl::range<3>(GRange_x, GRange_y, GRange_z);  // global range
          auto local_range=cl::sycl::range<3>(tileSize_x, tileSize_y, tileSize_z);  // local range
          InputLocalAcc local_acc(cl::sycl::range<1>(shared_mem), cgh);
          cgh.parallel_for(cl::sycl::nd_range<3>(global_range, local_range),
          EigenConvolutionKernel3D<CoeffReturnType, Scalar, InputArgType, InputFunctorExpr, Index,
          InputDims, KernelAccessorType, OutputAccessorType, InputLocalAcc, InputTupleType>(
          indexMapper,kernel_acc, kernel_size_x,  kernel_size_y, kernel_size_z, numX, numY,
          numZ, numP, out_res, out_offset, local_acc, input_functors, tuple_of_accessors));
          break;
        }

        default: {
          EIGEN_STATIC_ASSERT((NumKernelDims >= 1 && NumKernelDims <= 3), THIS_METHOD_IS_ONLY_FOR_OBJECTS_OF_A_SPECIFIC_SIZE);
        }
      }
    });
    m_device.asynchronousExec();
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE CoeffReturnType coeff(Index index) const
  {
    eigen_assert(m_buf);
    eigen_assert(index < m_dimensions.TotalSize());
    return m_buf[index];
  }

  template<int LoadMode>
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE PacketReturnType packet(const Index index) const
  {
    eigen_assert(m_buf);
    eigen_assert(index < m_dimensions.TotalSize());
    return internal::ploadt<PacketReturnType, LoadMode>(m_buf+index);
  }

  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE TensorOpCost
  costPerCoeff(bool vectorized) const {
    // TODO(rmlarsen): FIXME: For now, this is just a copy of the CPU cost
    // model.
    const double kernel_size = m_kernelImpl.dimensions().TotalSize();
    // We ignore the use of fused multiply-add.
    const double convolve_compute_cost =
        TensorOpCost::AddCost<Scalar>() + TensorOpCost::MulCost<Scalar>();
    const double firstIndex_compute_cost =
        NumDims *
        (2 * TensorOpCost::AddCost<Index>() + 2 * TensorOpCost::MulCost<Index>() +
         TensorOpCost::DivCost<Index>());
    return TensorOpCost(0, 0, firstIndex_compute_cost, vectorized, PacketSize) +
           kernel_size * (m_inputImpl.costPerCoeff(vectorized) +
                          m_kernelImpl.costPerCoeff(vectorized) +
                          TensorOpCost(0, 0, convolve_compute_cost, vectorized,
                                       PacketSize));
  }

 private:
  // No assignment (copies are needed by the kernels)
  TensorEvaluator& operator = (const TensorEvaluator&);
  TensorEvaluator<InputArgType, const Eigen::SyclDevice> m_inputImpl;
  KernelArgType m_kernelArg;
  TensorEvaluator<KernelArgType, const Eigen::SyclDevice> m_kernelImpl;
  Indices m_indices;
  Dimensions m_dimensions;
  Scalar* m_buf;
  const Scalar* m_kernel;
  bool m_local_kernel;
  const Eigen::SyclDevice& m_device;
};

} // end namespace Eigen

#endif // EIGEN_CXX11_TENSOR_TENSOR_CONVOLUTION_H
