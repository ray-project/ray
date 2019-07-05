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
 * TensorTensorContractionsycl.h
 *
 * \brief:
 *  TensorContractionsycl
 *
*****************************************************************/

#ifndef EIGEN_CXX11_TENSOR_TENSOR_CONTRACTION_SYCL_H
#define EIGEN_CXX11_TENSOR_TENSOR_CONTRACTION_SYCL_H
namespace Eigen {

template <typename Index, typename LhsScalar, typename RhsScalar,bool lhs_inner_dim_contiguous, bool rhs_inner_dim_contiguous, bool rhs_inner_dim_reordered> struct LaunchSyclKernels;
template<typename Indices, typename LeftArgType, typename RightArgType, typename OutputKernelType>
struct TensorEvaluator<const TensorContractionOp<Indices, LeftArgType, RightArgType, OutputKernelType>, const Eigen::SyclDevice> :
    public TensorContractionEvaluatorBase<TensorEvaluator<const TensorContractionOp<Indices, LeftArgType, RightArgType, OutputKernelType>, const Eigen::SyclDevice> > {

  static_assert(std::is_same<OutputKernelType, const NoOpOutputKernel>::value,
                "SYCL tensor contraction does not support output kernels.");

  typedef const Eigen::SyclDevice Device;

  typedef TensorEvaluator<const TensorContractionOp<Indices, LeftArgType, RightArgType, OutputKernelType>, Device> Self;
  typedef TensorContractionEvaluatorBase<Self> Base;
  typedef TensorContractionOp<Indices, LeftArgType, RightArgType, OutputKernelType> XprType;
  typedef typename internal::remove_const<typename XprType::Scalar>::type Scalar;
  typedef typename XprType::Index Index;
  typedef typename XprType::CoeffReturnType CoeffReturnType;
  typedef typename PacketType<CoeffReturnType, Device>::type PacketReturnType;

  enum {
    Layout = TensorEvaluator<LeftArgType, Device>::Layout,
  };

  // Most of the code is assuming that both input tensors are ColMajor. If the
  // inputs are RowMajor, we will "cheat" by swapping the LHS and RHS:
  // If we want to compute A * B = C, where A is LHS and B is RHS, the code
  // will pretend B is LHS and A is RHS.
  typedef typename internal::conditional<
    static_cast<int>(Layout) == static_cast<int>(ColMajor), LeftArgType, RightArgType>::type EvalLeftArgType;
  typedef typename internal::conditional<
    static_cast<int>(Layout) == static_cast<int>(ColMajor), RightArgType, LeftArgType>::type EvalRightArgType;

  static const int LDims =
      internal::array_size<typename TensorEvaluator<EvalLeftArgType, Device>::Dimensions>::value;
  static const int RDims =
      internal::array_size<typename TensorEvaluator<EvalRightArgType, Device>::Dimensions>::value;
  static const int ContractDims = internal::array_size<Indices>::value;

  typedef array<Index, LDims> left_dim_mapper_t;
  typedef array<Index, RDims> right_dim_mapper_t;

  typedef array<Index, ContractDims> contract_t;
  typedef array<Index, LDims - ContractDims> left_nocontract_t;
  typedef array<Index, RDims - ContractDims> right_nocontract_t;

  static const int NumDims = LDims + RDims - 2 * ContractDims;

  typedef DSizes<Index, NumDims> Dimensions;

  // typedefs needed in evalTo
  typedef typename internal::remove_const<typename EvalLeftArgType::Scalar>::type LhsScalar;
  typedef typename internal::remove_const<typename EvalRightArgType::Scalar>::type RhsScalar;

  typedef TensorEvaluator<EvalLeftArgType, Device> LeftEvaluator;
  typedef TensorEvaluator<EvalRightArgType, Device> RightEvaluator;

  typedef typename LeftEvaluator::Dimensions LeftDimensions;
  typedef typename RightEvaluator::Dimensions RightDimensions;

  EIGEN_DEVICE_FUNC TensorEvaluator(const XprType& op, const Device& device) :
      Base(op, device) {}

  // We need to redefine this method to make nvcc happy
  EIGEN_DEVICE_FUNC EIGEN_STRONG_INLINE bool evalSubExprsIfNeeded(Scalar* data) {
    this->m_leftImpl.evalSubExprsIfNeeded(NULL);
    this->m_rightImpl.evalSubExprsIfNeeded(NULL);
   if (data) {
      evalTo(data);
      return false;
    } else {
      this->m_result = static_cast<Scalar*>(this->m_device.allocate(this->dimensions().TotalSize() * sizeof(Scalar)));
      evalTo(this->m_result);
      return true;
    }
  }
  const Eigen::SyclDevice& device() const {return this->m_device;}
  void evalTo(Scalar* buffer) const {
    // Here is the result
    if (this->m_lhs_inner_dim_contiguous) {
      if (this->m_rhs_inner_dim_contiguous) {
        if (this->m_rhs_inner_dim_reordered) {
          evalTyped<true, true, true, Unaligned>(buffer);
        }
        else {
          evalTyped<true, true, false, Unaligned>(buffer);
        }
      }
      else {
       if (this->m_rhs_inner_dim_reordered) {
          evalTyped<true, false, true, Unaligned>(buffer);
        }
        else {
          evalTyped<true, false, false, Unaligned>(buffer);
        }
      }
    }
    else {
      if (this->m_rhs_inner_dim_contiguous) {
        if (this->m_rhs_inner_dim_reordered) {
          evalTyped<false, true, true, Unaligned>(buffer);
        }
        else {
          evalTyped<false, true, false, Unaligned>(buffer);
        }
      }
      else {
       if (this->m_rhs_inner_dim_reordered) {
          evalTyped<false, false, true, Unaligned>(buffer);
        }
        else {
          evalTyped<false, false, false, Unaligned>(buffer);
        }
      }
    }
  }

  template <bool lhs_inner_dim_contiguous, bool rhs_inner_dim_contiguous, bool rhs_inner_dim_reordered, int Alignment>
  void evalTyped(Scalar* buffer) const {
    // columns in left side, rows in right side
    const Index k = this->m_k_size;
    EIGEN_UNUSED_VARIABLE(k)
    // rows in left side
    const Index m = this->m_i_size;
    // columns in right side
    const Index n = this->m_j_size;

    // zero out the result buffer (which must be of size at least m * n * sizeof(Scalar)
    this->m_device.memset(buffer, 0, m * n * sizeof(Scalar));
    LaunchSyclKernels<Index, LhsScalar, RhsScalar,lhs_inner_dim_contiguous, rhs_inner_dim_contiguous, rhs_inner_dim_reordered>::Run(*this, buffer, m, n, k,
                       this->m_k_strides, this->m_left_contracting_strides, this->m_right_contracting_strides,
                       this->m_i_strides, this->m_j_strides, this->m_left_nocontract_strides, this->m_right_nocontract_strides);
  }
  // required by sycl to construct the expr on the device. Returns original left_impl
  const TensorEvaluator<LeftArgType, Device>& left_impl() const {
    return choose(Cond<static_cast<int>(Layout) == static_cast<int>(ColMajor)>(), this->m_leftImpl, this->m_rightImpl);
  }
  // required by sycl to construct the expr on the device. Returns original right_impl
  const TensorEvaluator<RightArgType, Device>& right_impl() const {
    return choose(Cond<static_cast<int>(Layout) == static_cast<int>(ColMajor)>(), this->m_rightImpl, this->m_leftImpl);
  }
};

template <typename HostExpr, typename OutScalar, typename LhsScalar, typename RhsScalar,  typename LHSFunctorExpr,  typename RHSFunctorExpr, typename LhsLocalAcc, typename RhsLocalAcc, typename OutAccessor, typename Index, typename ContractT, typename LeftNocontractT,
typename RightNocontractT, bool lhs_inner_dim_contiguous, bool rhs_inner_dim_contiguous, bool rhs_inner_dim_reordered,
typename HostExpr::Index TileSizeDimM, typename HostExpr::Index TileSizeDimN,typename HostExpr::Index TileSizeDimK, typename HostExpr::Index WorkLoadPerThreadM,typename HostExpr::Index WorkLoadPerThreadN,
typename HostExpr::Index LocalThreadSizeM, typename HostExpr::Index LocalThreadSizeN, typename HostExpr::Index LoadPerThreadLhs, typename HostExpr::Index LoadPerThreadRhs, typename LHSTupleType, typename RHSTupleType, typename Device> struct KernelConstructor{
  typedef typename Eigen::internal::traits<HostExpr>::_LhsNested LHSHostExpr;
  typedef typename Eigen::internal::traits<HostExpr>::_RhsNested RHSHostExpr;
  typedef typename Eigen::TensorSycl::internal::createPlaceHolderExpression<LHSHostExpr>::Type LHSPlaceHolderExpr;
  typedef typename Eigen::TensorSycl::internal::createPlaceHolderExpression<RHSHostExpr>::Type RHSPlaceHolderExpr;
  LHSFunctorExpr lhs_functors;
  RHSFunctorExpr rhs_functors;
  LhsLocalAcc localLhs;
  RhsLocalAcc localRhs;
  OutAccessor out_res;
  size_t out_offset;
  Index roundUpK, M, N, K;
  ContractT m_k_strides, m_left_contracting_strides, m_right_contracting_strides;
  LeftNocontractT m_i_strides, m_left_nocontract_strides;
  RightNocontractT m_j_strides,  m_right_nocontract_strides;
  LHSTupleType left_tuple_of_accessors;
  RHSTupleType right_tuple_of_accessors;
  Device dev;


  KernelConstructor(LHSFunctorExpr lhs_functors_, RHSFunctorExpr rhs_functors_, LhsLocalAcc localLhs_, RhsLocalAcc localRhs_, OutAccessor out_res_, size_t out_offset_,
    Index roundUpK_, Index M_, Index N_, Index K_, ContractT m_k_strides_, ContractT m_left_contracting_strides_,
    ContractT m_right_contracting_strides_, LeftNocontractT m_i_strides_, RightNocontractT m_j_strides_,
    LeftNocontractT m_left_nocontract_strides_, RightNocontractT m_right_nocontract_strides_, LHSTupleType left_tuple_of_accessors_, RHSTupleType right_tuple_of_accessors_, Device dev_)
    :lhs_functors(lhs_functors_), rhs_functors(rhs_functors_), localLhs(localLhs_), localRhs(localRhs_), out_res(out_res_),
    out_offset(out_offset_), roundUpK(roundUpK_), M(M_), N(N_), K(K_),
    m_k_strides(m_k_strides_), m_left_contracting_strides(m_left_contracting_strides_),
    m_right_contracting_strides(m_right_contracting_strides_),
    m_i_strides(m_i_strides_), m_left_nocontract_strides(m_left_nocontract_strides_),
    m_j_strides(m_j_strides_),  m_right_nocontract_strides(m_right_nocontract_strides_),
    left_tuple_of_accessors(left_tuple_of_accessors_), right_tuple_of_accessors(right_tuple_of_accessors_), dev(dev_){}

    void operator()(cl::sycl::nd_item<2> itemID) {
      typedef typename Eigen::TensorSycl::internal::ConvertToDeviceExpression<HostExpr>::Type DevExpr;
      typedef typename Eigen::TensorSycl::internal::ConvertToDeviceExpression<LHSHostExpr>::Type LHSDevExpr;
      typedef typename Eigen::TensorSycl::internal::ConvertToDeviceExpression<RHSHostExpr>::Type RHSDevExpr;
      auto lhs_dev_expr = Eigen::TensorSycl::internal::createDeviceExpression<LHSDevExpr, LHSPlaceHolderExpr>(lhs_functors, left_tuple_of_accessors);
      auto rhs_dev_expr = Eigen::TensorSycl::internal::createDeviceExpression<RHSDevExpr, RHSPlaceHolderExpr>(rhs_functors, right_tuple_of_accessors);
      typedef decltype(lhs_dev_expr.expr) LeftArgType;
      typedef decltype(rhs_dev_expr.expr) RightArgType;
      typedef typename internal::conditional<static_cast<int>(Eigen::internal::traits<DevExpr>::Layout) == static_cast<int>(ColMajor), LeftArgType, RightArgType>::type EvalLeftArgType;
      typedef typename internal::conditional<static_cast<int>(Eigen::internal::traits<DevExpr>::Layout) == static_cast<int>(ColMajor), RightArgType, LeftArgType>::type EvalRightArgType;
      typedef TensorEvaluator<EvalLeftArgType, Device> LeftEvaluator;
      typedef TensorEvaluator<EvalRightArgType, Device> RightEvaluator;
      typedef internal::TensorContractionInputMapper<LhsScalar, Index, internal::Lhs,
                                                      LeftEvaluator, LeftNocontractT,
                                                     ContractT, 1,
                                                     lhs_inner_dim_contiguous,
                                                     false, Unaligned, MakeGlobalPointer> LhsMapper;

      typedef internal::TensorContractionInputMapper<RhsScalar, Index, internal::Rhs,
                                                     RightEvaluator, RightNocontractT,
                                                     ContractT, 1,
                                                     rhs_inner_dim_contiguous,
                                                     rhs_inner_dim_reordered, Unaligned, MakeGlobalPointer> RhsMapper;
      // initialize data mappers must happen inside the kernel for device eval
      LhsMapper lhs(LeftEvaluator(choose(Cond<static_cast<int>(Eigen::internal::traits<DevExpr>::Layout) == static_cast<int>(ColMajor)>(),
                    lhs_dev_expr.expr, rhs_dev_expr.expr), dev), m_left_nocontract_strides, m_i_strides, m_left_contracting_strides, m_k_strides);
      RhsMapper rhs(RightEvaluator(choose(Cond<static_cast<int>(Eigen::internal::traits<DevExpr>::Layout) == static_cast<int>(ColMajor)>(),
                    rhs_dev_expr.expr, lhs_dev_expr.expr),dev), m_right_nocontract_strides, m_j_strides, m_right_contracting_strides, m_k_strides);
      auto out_ptr = ConvertToActualTypeSycl(OutScalar, out_res);
      // Matmul Kernel
      // Thread identifiers
      const Index mLocalThreadId = itemID.get_local(0); // Local ID row
      const Index nLocalThreadId = itemID.get_local(1); // Local ID col
      const Index mGroupId = itemID.get_group(0); // Work-group ID row
      const Index nGroupId = itemID.get_group(1); // Work-group ID localCol
      const Index linearLocalThreadId = nLocalThreadId*LocalThreadSizeM + mLocalThreadId; // linear local thread ID
      // Allocate register space
      LhsScalar privateLhs;
      RhsScalar privateRhs[WorkLoadPerThreadN];
      OutScalar privateRes[WorkLoadPerThreadM][WorkLoadPerThreadN];
      // Initialise the privateResumulation registers
      for (Index wLPTM=0; wLPTM<WorkLoadPerThreadM; wLPTM++) {
          for (Index wLPTN=0; wLPTN<WorkLoadPerThreadN; wLPTN++) {
              privateRes[wLPTM][wLPTN] = static_cast<OutScalar>(0);
          }
      }

      // Tile Lhs
      for (Index lPTL=0; lPTL<LoadPerThreadLhs; lPTL++) {
          Index localLhsLinearId = lPTL*LocalThreadSizeN*LocalThreadSizeM + linearLocalThreadId;
          Index localLhsRow =  localLhsLinearId% TileSizeDimM;
          Index localLhsCol = localLhsLinearId/TileSizeDimM;
          // Load the value (wide vector load)
          Index GlobalLhsColId = TileSizeDimK*0 + localLhsCol;
          localLhs[0 + ((localLhsCol*TileSizeDimM + localLhsRow)*2)] =((GlobalLhsColId < K)&& (mGroupId*(TileSizeDimM)+ localLhsRow <M))? lhs(mGroupId*(TileSizeDimM) + localLhsRow, GlobalLhsColId):static_cast<OutScalar>(0);
      }
      // Tile Rhs
      for (Index lPTR=0; lPTR<LoadPerThreadRhs; lPTR++) {
          Index localRhsLinearId = lPTR*LocalThreadSizeN*LocalThreadSizeM + linearLocalThreadId;
          Index localRhsRow =  localRhsLinearId% TileSizeDimN;
          Index localRhsCol = localRhsLinearId/TileSizeDimN;
          // Load the value (wide vector load)
          Index GlobalRhsRowId = TileSizeDimK*0 + localRhsCol;
          localRhs[0 + ((localRhsCol*TileSizeDimN + localRhsRow) *2)] = ((GlobalRhsRowId < K)&& ((nGroupId*(TileSizeDimN) + localRhsRow)< N))? rhs(GlobalRhsRowId, nGroupId*(TileSizeDimN) + localRhsRow): static_cast<OutScalar>(0);

      }
      // Loop over all tiles
      const Index numTiles = roundUpK/TileSizeDimK;
      Index firstHalf=0;
      do {
          // Synchronise
          itemID.barrier(cl::sycl::access::fence_space::local_space);
          // Load the next tile of Lhs and Rhs into local memory
          Index nextHalf = firstHalf + 1;
          if (nextHalf < numTiles) {
              // Tile A
              for (Index lPTL=0; lPTL<LoadPerThreadLhs; lPTL++) {
                  Index localLhsLinearId = lPTL*LocalThreadSizeN*LocalThreadSizeM + linearLocalThreadId;
                  Index localLhsRow =  localLhsLinearId% TileSizeDimM;
                  Index localLhsCol = localLhsLinearId/TileSizeDimM;
                  // global K id
                  Index GlobalLhsColId = TileSizeDimK*nextHalf + localLhsCol;
                  // Store the loaded value into local memory
                  localLhs[(nextHalf%2) + ((localLhsCol*TileSizeDimM + localLhsRow) *2)] = ((GlobalLhsColId < K)&& (mGroupId*(TileSizeDimM)+ localLhsRow <M))? lhs(mGroupId*(TileSizeDimM) + localLhsRow, GlobalLhsColId): static_cast<OutScalar>(0);
              }
              // Tile B
              for (Index lPTR=0; lPTR<LoadPerThreadRhs; lPTR++) {
                  Index localRhsLinearId = lPTR*LocalThreadSizeN*LocalThreadSizeM + linearLocalThreadId;
                  Index localRhsRow =  localRhsLinearId% TileSizeDimN;
                  Index localRhsCol = localRhsLinearId/TileSizeDimN;
                  // Load the value (wide vector load)
                  Index GlobalRhsRowId = TileSizeDimK*nextHalf + localRhsCol;
                  // Store the loaded vector into local memory
                  localRhs[(nextHalf%2) +((localRhsCol*TileSizeDimN + localRhsRow)*2)] = ((GlobalRhsRowId < K)&& ((nGroupId*(TileSizeDimN) + localRhsRow)< N))? rhs(GlobalRhsRowId, nGroupId*(TileSizeDimN) + localRhsRow):static_cast<OutScalar>(0);
              }
          }
          // Loop over the values of a single tile
          for (Index k=0; k<TileSizeDimK; k++) {
              // Cache the values of localRhs in registers
              for (Index wLPTN=0; wLPTN<WorkLoadPerThreadN; wLPTN++) {
                  Index localRhsCol = nLocalThreadId + wLPTN*LocalThreadSizeN;
                  privateRhs[wLPTN] = localRhs[(firstHalf%2) +((k*TileSizeDimN + localRhsCol)*2)];
              }
              // Perform the computation
              for (Index wLPTM=0; wLPTM<WorkLoadPerThreadM; wLPTM++) {
                  Index localLhsRow = mLocalThreadId + wLPTM*LocalThreadSizeM;
                  privateLhs = localLhs[(firstHalf%2)+ ((k*TileSizeDimM + localLhsRow)*2)];
                  for (Index wLPTN=0; wLPTN<WorkLoadPerThreadN; wLPTN++) {
                      privateRes[wLPTM][wLPTN] += privateLhs * privateRhs[wLPTN];
                  }
              }
          }
          // Next tile
          firstHalf++;
      } while (firstHalf<numTiles);

      // Store the final results in C
      for (Index wLPTM=0; wLPTM<WorkLoadPerThreadM; wLPTM++) {
          Index globalRow = mGroupId*TileSizeDimM + mLocalThreadId + wLPTM*LocalThreadSizeM;
          if (globalRow< M){
            for (Index wLPTN=0; wLPTN<WorkLoadPerThreadN; wLPTN++) {
                Index globalCol = nGroupId*TileSizeDimN + nLocalThreadId + wLPTN*LocalThreadSizeN;
                if(globalCol<N)
                  out_ptr[globalCol*M + globalRow +ConvertToActualSyclOffset(OutScalar, out_offset)] = privateRes[wLPTM][wLPTN];
            }
          }
      }

    }

};
template <typename Index, typename LhsScalar, typename RhsScalar, bool lhs_inner_dim_contiguous, bool rhs_inner_dim_contiguous, bool rhs_inner_dim_reordered> struct LaunchSyclKernels {

static const Index TileSizeDimM = 32ul;                                      // Tile size for dimension M
static const Index TileSizeDimN = 32ul;                                      // Tile size for dimension N
static const Index TileSizeDimK = 16ul;                                      // Tile size for dimension K
static const Index WorkLoadPerThreadM = 4ul;                                 // Work load per thread in dimension M
static const Index WorkLoadPerThreadN = 4ul;                                 // work load per thread in dimension N
static const Index LocalThreadSizeM = (TileSizeDimM/WorkLoadPerThreadM);   // Local thread size for the first dimension (M here)
static const Index LocalThreadSizeN = (TileSizeDimN/WorkLoadPerThreadN);   // Local thread size for the second dimension (N here)
static const Index LoadPerThreadLhs = ((TileSizeDimK*WorkLoadPerThreadM*WorkLoadPerThreadN)/(TileSizeDimN));  // workload per thread for Lhs expression
static const Index LoadPerThreadRhs = ((TileSizeDimK*WorkLoadPerThreadM*WorkLoadPerThreadN)/(TileSizeDimM));  // workload per thread for Rhs expression

// RoundUp function to make sure that the global threadId is divisable by local threadId
static Index RoundUp(Index x, Index y) {
  return ((((x) + (y) - 1) / (y))*(y));
}

template< typename Self, typename OutScalar, typename ContractT, typename LeftNocontractT, typename RightNocontractT>
  static void Run(const Self& self, OutScalar* buffer,  Index M, Index N, Index K,
    ContractT m_k_strides, ContractT m_left_contracting_strides, ContractT m_right_contracting_strides,
    LeftNocontractT m_i_strides, RightNocontractT m_j_strides, LeftNocontractT m_left_nocontract_strides, RightNocontractT m_right_nocontract_strides){

    typedef typename Self::XprType HostExpr;
    typedef typename Eigen::internal::traits<HostExpr>::_LhsNested LHSHostExpr;
    typedef typename Eigen::internal::traits<HostExpr>::_RhsNested RHSHostExpr;
    typedef TensorEvaluator<LHSHostExpr, const Eigen::SyclDevice> OrigLHSExpr;
    typedef TensorEvaluator<RHSHostExpr, const Eigen::SyclDevice> OrigRHSExpr;
    typedef Eigen::TensorSycl::internal::FunctorExtractor<OrigLHSExpr> LHSFunctorExpr;
    typedef Eigen::TensorSycl::internal::FunctorExtractor<OrigRHSExpr> RHSFunctorExpr;
    // extract lhs functor list
    LHSFunctorExpr lhs_functors = Eigen::TensorSycl::internal::extractFunctors(self.left_impl());
    // extract rhs functor list
    RHSFunctorExpr rhs_functors = Eigen::TensorSycl::internal::extractFunctors(self.right_impl());

    Index roundUpK = RoundUp(K, TileSizeDimK);
    Index roundUpM = RoundUp(M, TileSizeDimM);
    Index roundUpN = RoundUp(N, TileSizeDimN);
    ptrdiff_t out_offset = self.device().get_offset(buffer);
    self.device().sycl_queue().submit([&](cl::sycl::handler &cgh) {
      /// work-around for gcc bug
      typedef decltype(Eigen::TensorSycl::internal::createTupleOfAccessors<OrigLHSExpr>(cgh, self.left_impl())) LHSTupleType;
      /// work-around for gcc bug
      typedef decltype(Eigen::TensorSycl::internal::createTupleOfAccessors<OrigRHSExpr>(cgh, self.right_impl())) RHSTupleType;
      // create lhs tuple of accessors
      LHSTupleType left_tuple_of_accessors = Eigen::TensorSycl::internal::createTupleOfAccessors<OrigLHSExpr>(cgh, self.left_impl());
      // create rhs tuple of accessors
      RHSTupleType right_tuple_of_accessors = Eigen::TensorSycl::internal::createTupleOfAccessors<OrigRHSExpr>(cgh, self.right_impl());

      // Local memory for elements of Lhs
      typedef cl::sycl::accessor<LhsScalar, 1, cl::sycl::access::mode::read_write, cl::sycl::access::target::local> LhsLocalAcc;
      LhsLocalAcc localLhs(cl::sycl::range<1>(2* TileSizeDimM * TileSizeDimK), cgh);
      // Local memory for elements of Rhs
      typedef cl::sycl::accessor<RhsScalar, 1, cl::sycl::access::mode::read_write, cl::sycl::access::target::local> RhsLocalAcc;
      RhsLocalAcc localRhs(cl::sycl::range<1>(2* TileSizeDimK * TileSizeDimN), cgh);

      typedef cl::sycl::accessor<uint8_t, 1, cl::sycl::access::mode::read_write, cl::sycl::access::target::global_buffer> OutAccessor;
      //OutScalar memory
      OutAccessor out_res= self.device(). template get_sycl_accessor<cl::sycl::access::mode::read_write>(cgh, buffer);
      // sycl parallel for
      cgh.parallel_for(cl::sycl::nd_range<2>(cl::sycl::range<2>(roundUpM/WorkLoadPerThreadM, roundUpN/WorkLoadPerThreadN),
      cl::sycl::range<2>(LocalThreadSizeM, LocalThreadSizeN)),
       KernelConstructor<HostExpr, OutScalar, LhsScalar, RhsScalar, LHSFunctorExpr, RHSFunctorExpr, LhsLocalAcc, RhsLocalAcc, OutAccessor, Index, ContractT, LeftNocontractT,
       RightNocontractT, lhs_inner_dim_contiguous, rhs_inner_dim_contiguous, rhs_inner_dim_reordered, TileSizeDimM, TileSizeDimN, TileSizeDimK,
       WorkLoadPerThreadM, WorkLoadPerThreadN, LocalThreadSizeM, LocalThreadSizeN, LoadPerThreadLhs, LoadPerThreadRhs, LHSTupleType, RHSTupleType, Eigen::SyclKernelDevice>(lhs_functors, rhs_functors,
          localLhs, localRhs, out_res, out_offset, roundUpK, M, N, K, m_k_strides, m_left_contracting_strides, m_right_contracting_strides,m_i_strides, m_j_strides,
          m_left_nocontract_strides,m_right_nocontract_strides, left_tuple_of_accessors, right_tuple_of_accessors, Eigen::SyclKernelDevice()));
    });
    self.device().asynchronousExec();
  }
};

} // end namespace Eigen
#endif // EIGEN_CXX11_TENSOR_TENSOR_CONTRACTION_SYCL_H
