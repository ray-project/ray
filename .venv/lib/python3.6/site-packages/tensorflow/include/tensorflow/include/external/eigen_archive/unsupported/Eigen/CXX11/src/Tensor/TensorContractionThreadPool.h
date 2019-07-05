// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2014 Benoit Steiner <benoit.steiner.goog@gmail.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#ifndef EIGEN_CXX11_TENSOR_TENSOR_CONTRACTION_THREAD_POOL_H
#define EIGEN_CXX11_TENSOR_TENSOR_CONTRACTION_THREAD_POOL_H

// evaluator for thread pool device
#ifdef EIGEN_USE_THREADS

namespace Eigen {

template<typename Indices, typename LeftArgType, typename RightArgType, typename OutputKernelType>
struct TensorEvaluator<const TensorContractionOp<Indices, LeftArgType, RightArgType, OutputKernelType>, ThreadPoolDevice> :
    public TensorContractionEvaluatorBase<TensorEvaluator<const TensorContractionOp<Indices, LeftArgType, RightArgType, OutputKernelType>, ThreadPoolDevice> > {

  typedef ThreadPoolDevice Device;

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
  typedef typename internal::gebp_traits<LhsScalar, RhsScalar> Traits;

  typedef TensorEvaluator<EvalLeftArgType, Device> LeftEvaluator;
  typedef TensorEvaluator<EvalRightArgType, Device> RightEvaluator;

  TensorEvaluator(const XprType& op, const Device& device) :
      Base(op, device) {}

  template <int Alignment>
  void evalProduct(Scalar* buffer) const {
    const Index m = this->m_i_size;
    const Index n = this->m_j_size;
    const Index k = this->m_k_size;
    if (m == 0 || n == 0 || k == 0) return;

#if defined(EIGEN_VECTORIZE_AVX) && defined(EIGEN_USE_LIBXSMM)
    if (this->m_can_use_xsmm) {
      bool transposeA = !this->m_lhs_inner_dim_contiguous;
      bool transposeB = !this->m_rhs_inner_dim_contiguous;
      internal::TensorXsmmContractionBlocking<LhsScalar, RhsScalar, Index>
          blocking(k, m, n, this->m_device.numThreads(), transposeA,
                   transposeB);

      if (blocking.num_threads() == 1) {
        this->evalGemmXSMM(buffer);
      } else {
        ContextXsmm<Alignment>(this, buffer, m, n, k, blocking).run();
      }
      return;
    }
#endif

    // Compute a set of algorithm parameters:
    // - kernel block sizes (bm, bn, bk)
    // - task grain sizes (number of kernels executed per task: gm, gn)
    // - number of threads
    // - sharding by row/column
    // - parallel packing or first lhs then rhs
    // and some derived parameters:
    // - number of tasks (nm, nn, nk)
    // - number of kernels (nm0, nn0)
    // Unfortunately, all these parameters are tightly interdependent.
    // So in some cases we first compute approximate values, then compute other
    // values based on these approximations and then refine the approximations.

    // There are lots of heuristics here. There is some reasoning behind them,
    // but ultimately they are just tuned on contraction benchmarks for
    // different input configurations, thread counts and instruction sets.
    // So feel free to question any of them.

    // Compute whether we want to shard by row or by column.
    // This is a first approximation, it will be refined later. Since we don't
    // know number of threads yet we use 2, because what's we are most
    // interested in at this point is whether it makes sense to use
    // parallelization at all or not.
    bool shard_by_col = shardByCol(m, n, 2);

    // First approximation of kernel blocking sizes.
    // Again, we don't know number of threads yet, so we use 2.
    Index bm, bn, bk;
    if (shard_by_col) {
      internal::TensorContractionBlocking<Scalar, LhsScalar, RhsScalar, Index,
                                          internal::ShardByCol>
          blocking(k, m, n, 2);
      bm = blocking.mc();
      bn = blocking.nc();
      bk = blocking.kc();
    } else {
      internal::TensorContractionBlocking<Scalar, LhsScalar, RhsScalar, Index,
                                          internal::ShardByRow>
          blocking(k, m, n, 2);
      bm = blocking.mc();
      bn = blocking.nc();
      bk = blocking.kc();
    }

    // Compute optimal number of threads.
    // Note: we use bk instead of k here because we are interested in amount of
    // _parallelizable_ computations, and computations are not parallelizable
    // across k dimension.
    const TensorOpCost cost =
        contractionCost(m, n, bm, bn, bk, shard_by_col, false);
    int num_threads = TensorCostModel<ThreadPoolDevice>::numThreads(
        static_cast<double>(n) * m, cost, this->m_device.numThreads());
    int num_threads_by_k = numThreadsInnerDim(m, n, k);
    if (shardByInnerDim(m, n, k, num_threads, num_threads_by_k)) {
      // We are in the scenario where it is more effective to shard by the
      // inner dimension.
      this->template evalShardedByInnerDim<Alignment>(num_threads_by_k,
                                                      buffer);
      return;
    }

    // TODO(dvyukov): this is a stop-gap to prevent regressions while the cost
    // model is not tuned. Remove this when the cost model is tuned.
    if (n == 1) num_threads = 1;

    if (num_threads == 1) {
      TENSOR_CONTRACTION_DISPATCH(this->template evalProductSequential,
                                  Unaligned, (buffer));
      return;
    }

    // Now that we know number of threads, recalculate sharding and blocking.
    shard_by_col = shardByCol(m, n, num_threads);
    if (shard_by_col) {
      internal::TensorContractionBlocking<Scalar, LhsScalar, RhsScalar, Index,
                                          internal::ShardByCol>
          blocking(k, m, n, num_threads);
      bm = blocking.mc();
      bn = blocking.nc();
      bk = blocking.kc();
    } else {
      internal::TensorContractionBlocking<Scalar, LhsScalar, RhsScalar, Index,
                                          internal::ShardByRow>
          blocking(k, m, n, num_threads);
      bm = blocking.mc();
      bn = blocking.nc();
      bk = blocking.kc();
    }

    // Number of kernels for each dimension.
    Index nm0 = divup(m, bm);
    Index nn0 = divup(n, bn);
    Index nk = divup(k, bk);

    // Calculate task grain size (number of kernels executed per task).
    // This task size coarsening serves two purposes:
    // 1. It reduces per-task overheads including synchronization overheads.
    // 2. It allows to use caches better (reuse the same packed rhs in several
    // consecutive kernels).
    Index gm = 1;
    Index gn = 1;
    // If we are sharding by column, then we prefer to reduce rows first.
    if (shard_by_col) {
      gm = coarsenM(m, n, bm, bn, bk, gn, num_threads, shard_by_col);
      gn = coarsenN(m, n, bm, bn, bk, gm, num_threads, shard_by_col);
    } else {
      gn = coarsenN(m, n, bm, bn, bk, gm, num_threads, shard_by_col);
      gm = coarsenM(m, n, bm, bn, bk, gn, num_threads, shard_by_col);
    }
    // Number of tasks in each dimension.
    Index nm = divup(nm0, gm);
    Index nn = divup(nn0, gn);

    // Last by not least, decide whether we want to issue both lhs and rhs
    // packing in parallel; or issue lhs packing first, and then issue rhs
    // packing when lhs packing completes (for !shard_by_col lhs and rhs are
    // swapped). Parallel packing allows more parallelism (for both packing and
    // kernels), while sequential packing provides better locality (once
    // a thread finishes rhs packing it proceed to kernels with that rhs).
    // First, we are interested in parallel packing if there are few tasks.
    bool parallel_pack = num_threads >= nm * nn;
    // Also do parallel packing if all data fits into L2$.
    if (m * bk * Index(sizeof(LhsScalar)) + n * bk * Index(sizeof(RhsScalar)) <=
        l2CacheSize() * num_threads)
      parallel_pack = true;
    // But don't do it if we will use each rhs only once. Locality seems to be
    // more important in this case.
    if ((shard_by_col ? nm : nn) == 1) parallel_pack = false;

    #define CONTEXT_ARGS                                                        \
  (this, num_threads, buffer, m, n, k, bm, bn, bk, nm, nn, nk, gm, gn, nm0, \
   nn0, shard_by_col, parallel_pack)                                        \
      .run()

    TENSOR_CONTRACTION_DISPATCH(Context, Alignment, CONTEXT_ARGS);

#undef CONTEXT_ARGS

  }

  // Context coordinates a single parallel gemm operation.
 template <bool lhs_inner_dim_contiguous, bool rhs_inner_dim_contiguous,
            bool rhs_inner_dim_reordered, int Alignment>
  class Context {
   public:
    typedef internal::TensorContractionInputMapper<
        LhsScalar, Index, internal::Lhs, LeftEvaluator, left_nocontract_t,
        contract_t, internal::packet_traits<LhsScalar>::size,
        lhs_inner_dim_contiguous, false, Unaligned>
        LhsMapper;
    typedef internal::TensorContractionInputMapper<
        RhsScalar, Index, internal::Rhs, RightEvaluator, right_nocontract_t,
        contract_t, internal::packet_traits<RhsScalar>::size,
        rhs_inner_dim_contiguous, rhs_inner_dim_reordered, Unaligned>
        RhsMapper;

    typedef internal::blas_data_mapper<Scalar, Index, ColMajor> OutputMapper;

    typedef internal::TensorContractionKernel<
        Scalar, LhsScalar, RhsScalar, Index, OutputMapper, LhsMapper, RhsMapper>
        TensorContractionKernel;

    Context(const Self* self, int num_threads, Scalar* buffer, Index tm, Index tn,
            Index tk, Index bm, Index bn, Index bk, Index nm, Index nn, Index nk,
            Index gm, Index gn, Index nm0, Index nn0, bool shard_by_col,
            bool parallel_pack)
        : device_(self->m_device),
          lhs_(self->m_leftImpl, self->m_left_nocontract_strides,
               self->m_i_strides, self->m_left_contracting_strides,
               self->m_k_strides),
          rhs_(self->m_rightImpl, self->m_right_nocontract_strides,
               self->m_j_strides, self->m_right_contracting_strides,
               self->m_k_strides),
          buffer_(buffer),
          output_(buffer, tm),
          output_kernel_(self->m_output_kernel),
          tensor_contraction_params_(self->m_tensor_contraction_params),
          num_threads_(num_threads),
          shard_by_col_(shard_by_col),
          parallel_pack_(parallel_pack),
          m_(tm),
          n_(tn),
          k_(tk),
          bm_(bm),
          bn_(bn),
          bk_(bk),
          nm_(nm),
          nn_(nn),
          nk_(nk),
          gm_(gm),
          gn_(gn),
          nm0_(nm0),
          nn0_(nn0)
  {
      for (Index x = 0; x < P; x++) {
        // Normal number of notifications for k slice switch is
        // nm_ + nn_ + nm_ * nn_. However, first P - 1 slices will receive only
        // nm_ + nn_ notifications, because they will not receive notifications
        // from preceding kernels.
        state_switch_[x] =
            x == 0
                ? 1
                : (parallel_pack_ ? nn_ + nm_ : (shard_by_col_ ? nn_ : nm_)) +
                      (x == P - 1 ? nm_ * nn_ : 0);
        state_packing_ready_[x] =
            parallel_pack_ ? 0 : (shard_by_col_ ? nm_ : nn_);
        state_kernel_[x] = new std::atomic<uint8_t>*[nm_];
        for (Index m = 0; m < nm_; m++) {
          state_kernel_[x][m] = new std::atomic<uint8_t>[nn_];
          // Kernels generally receive 3 notifications (previous kernel + 2
          // packing), but the first slice won't get notifications from previous
          // kernels.
          for (Index n = 0; n < nn_; n++)
            state_kernel_[x][m][n].store(
                (x == 0 ? 0 : 1) + (parallel_pack_ ? 2 : 1),
                std::memory_order_relaxed);
        }
      }

      // Allocate memory for packed rhs/lhs matrices.
      size_t align = numext::maxi(EIGEN_MAX_ALIGN_BYTES, 1);
      size_t lhs_size =
          divup<size_t>(bm_ * bk_ * sizeof(LhsScalar), align) * align;
      size_t rhs_size =
          divup<size_t>(bn_ * bk_ * sizeof(RhsScalar), align) * align;
      packed_mem_ = static_cast<char*>(device_.allocate(
          (nm0_ * lhs_size + nn0_ * rhs_size) * std::min<size_t>(nk_, P - 1)));
      char* mem = static_cast<char*>(packed_mem_);
      for (Index x = 0; x < numext::mini<Index>(nk_, P - 1); x++) {
        packed_lhs_[x].resize(nm0_);
        for (Index m = 0; m < nm0_; m++) {
          packed_lhs_[x][m] = reinterpret_cast<LhsScalar*>(mem);
          mem += lhs_size;
        }
        packed_rhs_[x].resize(nn0_);
        for (Index n = 0; n < nn0_; n++) {
          packed_rhs_[x][n] = reinterpret_cast<RhsScalar*>(mem);
          mem += rhs_size;
        }
      }
    }

    ~Context() {
      for (Index x = 0; x < P; x++) {
        for (Index m = 0; m < nm_; m++) delete[] state_kernel_[x][m];
        delete[] state_kernel_[x];
      }
      device_.deallocate(packed_mem_);
    }

    void run() {
      // Kick off packing of the first slice.
      signal_switch(0, 1);
      // Wait for overall completion.
      // TODO(dvyukov): this wait can lead to deadlock.
      // If nthreads contractions are concurrently submitted from worker
      // threads, this wait will block all worker threads and the system will
      // deadlock.
      done_.Wait();
    }

   private:
    Notification done_;
    const Device& device_;
    LhsMapper lhs_;
    RhsMapper rhs_;
    Scalar* const buffer_;
    OutputMapper output_;
    OutputKernelType output_kernel_;
    TensorContractionParams tensor_contraction_params_;
    const int num_threads_;
    const bool shard_by_col_;
    const bool parallel_pack_;
    // Matrix sizes.
    const Index m_;
    const Index n_;
    const Index k_;
    // Block sizes.
    const Index bm_;
    const Index bn_;
    const Index bk_;
    // Number of tasks.
    const Index nm_;
    const Index nn_;
    const Index nk_;
    // Task grain sizes (number of kernels executed per task).
    const Index gm_;
    const Index gn_;
    // Number of blocks (this is different from ni_/nn_ because of task size
    // coarsening).
    const Index nm0_;
    const Index nn0_;

    // Parallelization strategy.
    //
    // Blocks related to the same k block can run in parallel because they write
    // to different output blocks. So we parallelize within k slices, this
    // gives us parallelism level of m x n. Before we can start any kernels
    // related to k-th slice, we need to issue m lhs packing tasks and n rhs
    // packing tasks.
    //
    // However, there is a bottleneck when we are finishing kernels for k-th
    // slice (at the very end there is only 1 runnable kernel). To mitigate this
    // bottleneck we allow kernels from k-th and k+1-th slices to run in
    // parallel. Note that (m, n, k) and (m, n, k+1) kernels write to the same
    // output block, so they must not run in parallel.
    //
    // This gives us the following dependency graph.
    // On each k slice we have m x n kernel tasks, m lhs paking tasks and n rhs
    // packing tasks.
    // Kernel (m, n, k) can start when:
    //  - kernel (m, n, k-1) has finished
    //  - lhs packing (m, k) has finished
    //  - rhs packing (n, k) has finished
    // Lhs/rhs packing can start when:
    //  - all k-1 packing has finished (artificially imposed to limit amount of
    //  parallel packing)
    //
    // On top of that we limit runnable tasks to two consecutive k slices.
    // This is done to limit amount of memory we need for packed lhs/rhs
    // (for each k slice we need m*bk + n*bk memory in packed_lhs_/packed_rhs_).
    //
    // state_switch_ tracks when we are ready to switch to the next k slice.
    // state_kernel_[m][n] tracks when we are ready to kick off kernel (m, n).
    // These variable are rolling over 3 consecutive k slices: first two we are
    // actively executing + one to track completion of kernels in the second
    // slice.
    static const Index P = 3;
    void* packed_mem_;
    std::vector<LhsScalar*> packed_lhs_[P - 1];
    std::vector<RhsScalar*> packed_rhs_[P - 1];
    std::atomic<uint8_t>** state_kernel_[P];
    // state_switch_ is frequently modified by worker threads, while other
    // fields are read-only after constructor. Let's move it to a separate cache
    // line to reduce cache-coherency traffic.
    char pad_[128];
    std::atomic<Index> state_packing_ready_[P];
    std::atomic<Index> state_switch_[P];

    void pack_lhs(Index m, Index k) {
      const Index mend = m * gm_ + gm(m);
      for (Index m1 = m * gm_; m1 < mend; m1++)
        TensorContractionKernel::packLhs(packed_lhs_[k % (P - 1)][m1],
                                         lhs_.getSubMapper(m1 * bm_, k * bk_),
                                         bk(k), bm(m1));

      if (!parallel_pack_ && shard_by_col_) {
        signal_packing(k);
      } else {
        signal_switch(k + 1);
        for (Index n = nn_ - 1; n >= 0; n--) signal_kernel(m, n, k, n == 0);
      }
    }

    void pack_rhs(Index n, Index k) {
      const Index nend = n * gn_ + gn(n);
      for (Index n1 = n * gn_; n1 < nend; n1++) {
        if (k == 0) {
          // Zero the output memory in parallel.
          // On 10000x2x10000 mm zeroing can easily take half of time.
          // Zero (bn x m) row. Safe to do here because all kernels that will
          // write to this memory depend on completion of this task.
          // Note: don't call device_.memset() here. device_.memset() blocks on
          // thread pool worker thread, which can lead to underutilization and
          // deadlocks.
          memset(buffer_ + n1 * bn_ * m_, 0, bn(n1) * m_ * sizeof(Scalar));
        }
        TensorContractionKernel::packRhs(packed_rhs_[k % (P - 1)][n1],
                                         rhs_.getSubMapper(k * bk_, n1 * bn_),
                                         bk(k), bn(n1));
      }

      if (parallel_pack_ || shard_by_col_) {
        signal_switch(k + 1);
        for (Index m = nm_ - 1; m >= 0; m--) signal_kernel(m, n, k, m == 0);
      } else {
        signal_packing(k);
      }
    }

    void kernel(Index m, Index n, Index k) {
      // Note: order of iteration matters here. Iteration over m is innermost
      // because we want to reuse the same packed rhs in consecutive tasks
      // (rhs fits into L2$ while lhs only into L3$).
      const Index nend = n * gn_ + gn(n);
      const Index mend = m * gm_ + gm(m);
      if (shard_by_col_) {
        for (Index n1 = n * gn_; n1 < nend; n1++) {
          for (Index m1 = m * gm_; m1 < mend; m1++) {
            const auto output_mapper = output_.getSubMapper(m1 * bm_, n1 * bn_);
            TensorContractionKernel::invoke(
                output_mapper, packed_lhs_[k % (P - 1)][m1],
                packed_rhs_[k % (P - 1)][n1], bm(m1), bk(k), bn(n1), Scalar(1));

            // We are done with the last task for the [m1, n1] block.
            if (k + 1 == nk_) {
              output_kernel_(output_mapper, tensor_contraction_params_,
                             m1 * bm_, n1 * bn_, bm(m1), bn(n1));
            }
          }
        }
      } else {
        for (Index m1 = m * gm_; m1 < mend; m1++)
          for (Index n1 = n * gn_; n1 < nend; n1++) {
            const auto output_mapper = output_.getSubMapper(m1 * bm_, n1 * bn_);
            TensorContractionKernel::invoke(
                output_mapper, packed_lhs_[k % (P - 1)][m1],
                packed_rhs_[k % (P - 1)][n1], bm(m1), bk(k), bn(n1), Scalar(1));

            // We are done with the last task for the [m1, n1] block.
            if (k + 1 == nk_) {
              output_kernel_(output_mapper, tensor_contraction_params_,
                             m1 * bm_, n1 * bn_, bm(m1), bn(n1));
            }
          }
      }
      signal_kernel(m, n, k + 1, false);
      signal_switch(k + 2);
    }

    void signal_packing(Index k) {
      eigen_assert(!parallel_pack_);
      Index s = state_packing_ready_[k % P].fetch_sub(1);
      eigen_assert(s > 0);
      if (s != 1) return;
      state_packing_ready_[k % P] = shard_by_col_ ? nm_ : nn_;
      enqueue_packing(k, shard_by_col_);
    }

    void signal_kernel(Index m, Index n, Index k, bool sync) {
      std::atomic<uint8_t>* state = &state_kernel_[k % P][m][n];
      Index s = state->load();
      eigen_assert(s > 0);
      if (s != 1 && state->fetch_sub(1) != 1) return;
      state->store(parallel_pack_ ? 3 : 2, std::memory_order_relaxed);
      if (sync)
        kernel(m, n, k);
      else
        device_.enqueueNoNotification([=]() { kernel(m, n, k); });
    }

    void signal_switch(Index k, Index v = 1) {
      Index s = state_switch_[k % P].fetch_sub(v);
      eigen_assert(s >= v);
      if (s != v) return;

      // Ready to switch to the next k slice.
      // Reset counter for the next iteration.
      state_switch_[k % P] =
          (parallel_pack_ ? nm_ + nn_ : (shard_by_col_ ? nn_ : nm_)) +
          nm_ * nn_;
      if (k < nk_) {
        // Issue lhs/rhs packing. Their completion will in turn kick off
        // kernels.
        if (parallel_pack_) {
          enqueue_packing(k, !shard_by_col_);
          enqueue_packing(k, shard_by_col_);
        } else if (shard_by_col_) {
          enqueue_packing(k, false);
        } else {
          enqueue_packing(k, true);
        }

        // Termination handling.
        // Because kernel completion signals k + 2 switch, we need to finish nk
        // + 2 slices without issuing any tasks on nk + 1 slice. So here we
        // pretend that all nk + 1 packing tasks just finish instantly; so that
        // nk + 2 switch only waits for completion of nk kernels.
      } else if (k == nk_) {
        signal_switch(k + 1,
                      parallel_pack_ ? nm_ + nn_ : (shard_by_col_ ? nn_ : nm_));
      } else {
        done_.Notify();
      }
    }

    // Enqueue all rhs/lhs packing for k-th slice.
    void enqueue_packing(Index k, bool rhs) {
      enqueue_packing_helper(0, rhs ? nn_ : nm_, k, rhs);
    }

    void enqueue_packing_helper(Index start, Index end, Index k, bool rhs) {
      if (end - start == 1) {
        if (rhs)
          pack_rhs(start, k);
        else
          pack_lhs(start, k);
      } else {
        while (end - start > 1) {
          Index mid = (start + end) / 2;
          device_.enqueueNoNotification(
              [=]() { enqueue_packing_helper(mid, end, k, rhs); });
          end = mid;
        }
        enqueue_packing_helper(start, end, k, rhs);
      }
    }

    // Block sizes with accounting for potentially incomplete last block.
    Index bm(Index m) const { return m + 1 < nm0_ ? bm_ : m_ + bm_ - bm_ * nm0_; }
    Index bn(Index n) const { return n + 1 < nn0_ ? bn_ : n_ + bn_ - bn_ * nn0_; }
    Index bk(Index k) const { return k + 1 < nk_ ? bk_ : k_ + bk_ - bk_ * nk_; }
    // Task grain sizes accounting for potentially incomplete last task.
    Index gm(Index m) const { return m + 1 < nm_ ? gm_ : nm0_ + gm_ - gm_ * nm_; }
    Index gn(Index n) const { return n + 1 < nn_ ? gn_ : nn0_ + gn_ - gn_ * nn_; }

    Context(const Context&) = delete;
    void operator=(const Context&) = delete;
  };

  // Decide whether we want to shard m x n contraction by columns or by rows.
  static bool shardByCol(Index m, Index n, Index num_threads) {
    // Note: we are comparing both n and m against Traits::nr, it is not
    // a mistake. We are trying to figure out how both n and m will fit into
    // the main sharding dimension.

    // Sharding by column is the default
    // ... unless there is enough data for vectorization over rows
    if (m / num_threads >= Traits::nr &&
        // and not enough data for vectorization over columns
        (n / num_threads < Traits::nr ||
         // ... or barely enough data for vectorization over columns,
         // but it is not evenly dividable across threads
         (n / num_threads < 4 * Traits::nr &&
          (n % (num_threads * Traits::nr)) != 0 &&
          // ... and it is evenly dividable across threads for rows
          ((m % (num_threads * Traits::nr)) == 0 ||
           // .. or it is not evenly dividable for both dimensions but
           // there is much more data over rows so that corner effects are
           // mitigated.
           (m / n >= 6)))))
      return false;
    // Wait, or if matrices are just substantially prolonged over the other
    // dimension.
    if (n / num_threads < 16 * Traits::nr && m > n * 32) return false;
    return true;
  }

  Index coarsenM(Index m, Index n, Index bm, Index bn, Index bk, Index gn,
                 int num_threads, bool shard_by_col) const {
    Index gm = 1;
    Index gm1 = 1;
    Index nm0 = divup(m, bm);
    Index nm1 = nm0;
    for (;;) {
      // Find the next candidate for m grain size. It needs to result in
      // different number of blocks. E.g. if we have 10 kernels, we want to try
      // 5 and 10, but not 6, 7, 8 and 9.
      while (gm1 <= nm0 && nm1 == divup(nm0, gm1)) gm1++;
      if (gm1 > nm0) break;
      // Check the candidate.
      int res = checkGrain(m, n, bm, bn, bk, gm1, gn, gm, gn, num_threads,
                           shard_by_col);
      if (res < 0) break;
      nm1 = divup(nm0, gm1);
      if (res == 0) continue;
      // Commit new grain size.
      gm = gm1;
    }
    return gm;
  }

  Index coarsenN(Index m, Index n, Index bm, Index bn, Index bk, Index gm,
                 int num_threads, bool shard_by_col) const {
    Index gn = 1;
    Index gn1 = 1;
    Index nn0 = divup(n, bn);
    Index nn1 = nn0;
    for (;;) {
      while (gn1 <= nn0 && nn1 == divup(nn0, gn1)) gn1++;
      if (gn1 > nn0) break;
      int res = checkGrain(m, n, bm, bn, bk, gm, gn1, gm, gn, num_threads,
                           shard_by_col);
      if (res < 0) break;
      nn1 = divup(nn0, gn1);
      if (res == 0) continue;
      gn = gn1;
    }
    return gn;
  }

  // checkGrain checks whether grain (gm, gn) is suitable and is better than
  // (oldgm, oldgn).
  int checkGrain(Index m, Index n, Index bm, Index bn, Index bk, Index gm,
                 Index gn, Index oldgm, Index oldgn, int num_threads,
                 bool shard_by_col) const {
    const TensorOpCost cost =
        contractionCost(bm * gm, bn * gn, bm, bn, bk, shard_by_col, true);
    double taskSize = TensorCostModel<ThreadPoolDevice>::taskSize(
        static_cast<double>(bm) * gm * bn * gn, cost);
    // If the task is too small, then we agree on it regardless of anything
    // else. Otherwise synchronization overheads will dominate.
    if (taskSize < 1) return 1;
    // If it is too large, then we reject it and all larger tasks.
    if (taskSize > 2) return -1;
    // Now we are in presumably good task size range.
    // The main deciding factor here is parallelism. Consider that we have 12
    // kernels and 4 threads. Grains of 2, 3 and 4 all yield good task sizes.
    // But 2/4 yield 6/3 tasks, which gives us parallelism of 0.75 (at most 3/4
    // of cores will be busy). While grain size 3 gives us 4 tasks, which gives
    // us parallelism of 1 (we can load all cores).
    Index nm0 = divup(m, bm);
    Index nn0 = divup(n, bn);
    Index new_tasks = divup(nm0, gm) * divup(nn0, gn);
    double new_parallelism = static_cast<double>(new_tasks) /
                             (divup<int>(new_tasks, num_threads) * num_threads);
    Index old_tasks = divup(nm0, oldgm) * divup(nn0, oldgn);
    double old_parallelism = static_cast<double>(old_tasks) /
                             (divup<int>(old_tasks, num_threads) * num_threads);
    if (new_parallelism > old_parallelism || new_parallelism == 1) return 1;
    return 0;
  }

  TensorOpCost contractionCost(Index m, Index n, Index bm, Index bn, Index bk,
                               bool shard_by_col, bool prepacked) const {
    const int packed_size = std::min<int>(PacketType<LhsScalar, Device>::size,
                                          PacketType<RhsScalar, Device>::size);
    const int output_packet_size = internal::unpacket_traits<PacketReturnType>::size;
    const double kd = static_cast<double>(bk);
    double compute_bandwidth = computeBandwidth(false, bm, bn, bk);
    // Computations.
    TensorOpCost cost = TensorOpCost(0, 0, kd * compute_bandwidth, true, packed_size);
    // Output stores.
    cost += TensorOpCost(0, sizeof(CoeffReturnType), 0, true, output_packet_size);
    if (prepacked) {
      // Packing and kernels are executed in different tasks. When we calculate
      // task grain size we look only at kernel cost assuming that kernel
      // is more expensive than packing.
      return cost;
    }
    // Lhs/rhs loads + computations.
    TensorOpCost lhsCost = this->m_leftImpl.costPerCoeff(true) * (kd / n);
    TensorOpCost rhsCost = this->m_rightImpl.costPerCoeff(true) * (kd / m);
    // Lhs packing memory cost does not contribute considerably to overall
    // execution time because lhs is prefetched early and accessed sequentially.
    if (shard_by_col)
      lhsCost.dropMemoryCost();
    else
      rhsCost.dropMemoryCost();
    return cost + lhsCost + rhsCost;
  }

  template <int Alignment>
  EIGEN_STRONG_INLINE void addToBuffer(size_t n, const Scalar* src_buf,
                                       Scalar* tgt_buf) const {
    const int output_packet_size = internal::unpacket_traits<PacketReturnType>::size;
    size_t i = 0;
    const size_t num_packets = n / output_packet_size;
    for (; i < output_packet_size * num_packets; i += output_packet_size) {
      const PacketReturnType src_val =
          internal::pload<PacketReturnType>(src_buf + i);
      const PacketReturnType tgt_val =
          internal::ploadt<PacketReturnType, Alignment>(tgt_buf + i);
      const PacketReturnType sum = internal::padd(src_val, tgt_val);
      internal::pstoret<Scalar, PacketReturnType, Alignment>(tgt_buf + i, sum);
    }
    for (; i < n; ++i) {
      tgt_buf[i] += src_buf[i];
    }
  }

  // Decide whether we want to shard m x k x n contraction over the inner
  // (contraction) dimension (k).
  static bool shardByInnerDim(Index m, Index n, Index k, int num_threads,
                              int num_threads_by_k) {
    std::ptrdiff_t bufsize = m * n * sizeof(Scalar);
    bool shard_by_k = false;
    if (n == 1 ||                // If mat*vec or...
        num_threads_by_k < 2 ||  // running single threaded or...
        num_threads_by_k <
            num_threads ||  // sharding by k gives less parallelism or...
        bufsize > l3CacheSize() / num_threads_by_k ||  // need more buffer space
        // than L3 cache or...
        k / num_threads_by_k < 2 * Traits::nr) {  // k per thread is tiny.
      shard_by_k = false;
    } else if (numext::maxi(m, n) / num_threads <
                   Traits::nr ||  // both other dimensions are tiny or...
               // k per thread is not small and...
               (k / num_threads_by_k > 8 * Traits::nr &&
                // one of the outer dimensions is tiny or sharding by k offers
                // more parallelism.
                (numext::mini(m, n) < 2 * Traits::nr ||
                 num_threads_by_k > num_threads))) {
      shard_by_k = true;
    }
    return shard_by_k;
  }

  template <int Alignment>
  void evalShardedByInnerDim(int num_threads, Scalar* result) const {
    const Index m = this->m_i_size;
    const Index n = this->m_j_size;
    const Index k = this->m_k_size;
    const Index packet_size = internal::packet_traits<RhsScalar>::size;
    const Index kmultiple = packet_size <= 8 ? 8 : packet_size;
    // The underlying GEMM kernel assumes that k is a multiple of
    // the packet size and subtle breakage occurs if this is violated.
    Index block_size = kmultiple * divup<Index>(k, kmultiple * num_threads);
    Index num_blocks = divup<Index>(k, block_size);
    // we use 'result' for the first block's partial result.
    MaxSizeVector<Scalar*> block_buffers(num_blocks - 1);
    Barrier barrier(internal::convert_index<int>(num_blocks));
    auto process_block = [=, &barrier](Scalar* buf, Index begin, Index end) {
      ::memset(buf, 0, m * n * sizeof(Scalar));
      TENSOR_CONTRACTION_DISPATCH(
          this->template evalGemmPartialWithoutOutputKernel, Alignment,
          (buf, begin, end, this->m_device.numThreads()));
      barrier.Notify();
    };
    Index start = 0;
    for (Index blocks_left = num_blocks; blocks_left > 0; --blocks_left) {
      // The underlying GEMM kernel assumes that k is a multiple of packet size
      // (currently largest packet size is 16) and subtle breakage occurs if
      // this is violated.
      block_size = kmultiple * divup<Index>(k - start, kmultiple * blocks_left);
      Scalar* buf;
      if (start == 0) {
        buf = result;
      } else {
        buf = static_cast<Scalar*>(
            this->m_device.allocate(m * n * sizeof(Scalar)));
        block_buffers.push_back(buf);
      }
      Index end = start + block_size;
      if (end > k) {
        end = k;
      }
      this->m_device.enqueueNoNotification(
          [=, &process_block]() { process_block(buf, start, end); });
      start = end;
    }
    barrier.Wait();

    // Add other partial results into first partial result.
    for (const auto& buf : block_buffers) {
      addToBuffer<Alignment>(m * n, buf, result);
      this->m_device.deallocate(buf);
    }

    // Finally call output kernel with finalized output buffer.
    typedef internal::blas_data_mapper<Scalar, Index, ColMajor> OutputMapper;
    this->m_output_kernel(OutputMapper(result, m),
                          this->m_tensor_contraction_params,
                          static_cast<Eigen::Index>(0),
                          static_cast<Eigen::Index>(0),
                          m, n);
  }

  TensorOpCost contractionCostPerInnerDim(Index m, Index n, Index k) const {
    // Compute cost.
    const int output_packet_size = internal::unpacket_traits<PacketReturnType>::size;
    TensorOpCost cost(0, 0, (computeBandwidth(true, m, n, k) * m) * n);
    // Output stores.
    cost += TensorOpCost(0, sizeof(CoeffReturnType), 0, true, output_packet_size);
    TensorOpCost lhsCost = this->m_leftImpl.costPerCoeff(true) * m;
    TensorOpCost rhsCost = this->m_rightImpl.costPerCoeff(true) * n;
    // Since the inner gemm kernel is always sharded by column, the lhs
    // load cost is negligible.
    lhsCost.dropMemoryCost();
    return cost + lhsCost + rhsCost;
  }

  int numThreadsInnerDim(Index m, Index n, Index k) const {
    const int output_packet_size = internal::unpacket_traits<PacketReturnType>::size;
    TensorOpCost cost = contractionCostPerInnerDim(m, n, k);
    double total_parallel_cost =
        TensorCostModel<ThreadPoolDevice>::totalCost(k, cost);
    // Cost of reduction step accumulating the m*n per-thread buffers into the
    // result.
    double reduction_cost = TensorCostModel<ThreadPoolDevice>::totalCost(
        m * n, TensorOpCost(2, 1, 1, true, output_packet_size));
    int num_threads = 1;
    double min_cost = total_parallel_cost;
    double kPerThreadOverHead = 4000;
    double kFixedOverHead = 100000;
    for (int nt = 2; nt <= this->m_device.numThreads(); nt++) {
      double sequential_cost =
          kFixedOverHead + nt * (reduction_cost + kPerThreadOverHead);
      double parallel_cost = total_parallel_cost / nt + sequential_cost;
      if (parallel_cost < min_cost) {
        num_threads = nt;
        min_cost = parallel_cost;
      }
    }
    return num_threads;
  }


  double computeBandwidth(bool shard_by_col, Index bm, Index bn,
                          Index bk) const {
    // Peak VFMA bandwidth is 0.5. However if we have not enough data for
    // vectorization bandwidth drops. The 4.0 and 2.0 bandwidth is determined
    // experimentally.
    double computeBandwidth =
        bk == 1 ? 4.0
                : (shard_by_col ? bn : bm) < Traits::nr ||
                          (shard_by_col ? bm : bn) < Traits::mr
                      ? 2.0
                      : 0.5;
#ifndef EIGEN_VECTORIZE_FMA
    // Bandwidth of all of VFMA/MULPS/ADDPS is 0.5 on latest Intel processors.
    // However for MULPS/ADDPS we have dependent sequence of 2 such
    // instructions,
    // so overall bandwidth is 1.0.
    if (computeBandwidth == 0.5) computeBandwidth = 1.0;
#endif
    return computeBandwidth;
  }

#if defined(EIGEN_VECTORIZE_AVX) && defined(EIGEN_USE_LIBXSMM)
  // TODO(ezhulenev): Add support for output kernels and LIBXSMM.
  static_assert(std::is_same<OutputKernelType, const NoOpOutputKernel>::value,
                "XSMM does not support contraction output kernels.");

  template<int Alignment>
  class ContextXsmm {
   public:
    ContextXsmm(const Self* self, Scalar* buffer, Index m, Index n, Index k,
                const internal::TensorXsmmContractionBlocking<LhsScalar,
                    RhsScalar, Index>& blocking):
        device(self->m_device),
        m(m), k(k), n(n),
        stride_a(blocking.transposeA() ? k : m),
        stride_b(blocking.transposeB() ? n : k),
        stride_c(m),
        bm(blocking.mc()), bk(blocking.kc()), bn(blocking.nc()),
        blocks_m(blocking.blocks_m()), blocks_k(blocking.blocks_k()),
        blocks_n(blocking.blocks_n()),
        copyA(blocking.copyA()), copyB(blocking.copyB()),
        transposeA(blocking.transposeA()), transposeB(blocking.transposeB()),
        num_threads(blocking.num_threads()),
        buffer(buffer),
        leftData(self->m_leftImpl.data()), rightData(self->m_rightImpl.data()),
        workers_done(blocking.num_threads()),

        packingA_jobs(0), packingB_jobs(0), compute_jobs(0),
        packingA_done(blocking.blocks_m()), packingB_done(blocking.blocks_n()) {}

    void worker() {
      // Pack

      if (copyA) {
        while (true) {
          uint32_t mk = packingA_jobs++;
          Index mi = mk / blocks_k;
          Index ki = mk % blocks_k;
          if (mi >= blocks_m) break;

          LhsScalar * blockA = blocksA + (bk*bm) * (mi*blocks_k+ki);
          if (transposeA) {
            const LhsScalar * current_a = leftData + (bm*mi)*stride_a + (bk*ki);
            libxsmm_otrans(blockA, current_a, sizeof(LhsScalar), actual_bk(ki),
                           actual_bm(mi), stride_a, bm);
          } else {
            const LhsScalar * current_a = leftData + (bk*ki)*stride_a + (bm*mi);
            internal::pack_simple<LhsScalar, Index>(blockA, current_a,
                actual_bk(ki), actual_bm(mi), bm, stride_a);
          }
          packingA_done.at(mi)++;
        }
      }

      if (copyB) {
        while (true) {
          uint32_t nk = packingB_jobs++;
          Index ni = nk / blocks_k;
          Index ki = nk % blocks_k;
          if (ni >= blocks_n) break;

          RhsScalar * blockB = blocksB + (bk*bn) * (ni*blocks_k+ki);
          if (transposeB) {
            const RhsScalar * current_b = rightData + (ki*bk)*stride_b +
                                          (ni*bn);
            libxsmm_otrans(blockB, current_b, sizeof(RhsScalar), actual_bn(ni),
                           actual_bk(ki), stride_b, bk);
          } else {
            const RhsScalar * current_b = rightData + (ni*bn)*stride_b +
                                          (ki*bk);
            internal::pack_simple<RhsScalar, Index>(blockB, current_b,
                actual_bn(ni), actual_bk(ki), bk, stride_b);
          }
          packingB_done.at(ni)++;
        }
      }

      // Compute

      while (true) {
        uint32_t mn = compute_jobs++;
        Index mi = mn / blocks_n;
        Index ni = mn % blocks_n;
        if (mi >= blocks_m) break;

        // Wait for mi, ni packings to be done. This is more fine-grained than
        // waiting for all workers to finish packing.
        while ((copyA && (packingA_done.at(mi) < blocks_k)) ||
               (copyB && (packingB_done.at(ni) < blocks_k)))
        {}

        for (Index ki=0; ki < blocks_k; ++ki) {
          const LhsScalar * current_a = copyA ?
              blocksA + (bk*bm) * (mi*blocks_k+ki) :
              leftData + (bk*ki)*stride_a + (bm*mi);
          const RhsScalar * current_b = copyB ?
              blocksB + (bk*bn) * (ni*blocks_k+ki) :
              rightData + (ni*bn)*stride_b + (bk*ki);

          Index current_stride_a = copyA ? bm : stride_a;
          Index current_stride_b = copyB ? bk : stride_b;

          // Memory may not be zeroed, overwrite instead of adding in first
          // iteration.
          float beta = ki == 0 ? 0 : 1;

          Scalar * current_c = buffer + (mi*bm) + (ni*bn)*stride_c;
          internal::libxsmm_wrapper<LhsScalar, RhsScalar, Scalar>(
              0, actual_bm(mi), actual_bn(ni), actual_bk(ki),
              current_stride_a, current_stride_b, stride_c, 1, beta, 0)
          (current_a, current_b, current_c);
        }
      }

      workers_done.Notify();
    }

    void run() {
      // Parallelization strategy.
      //
      // First pack A into blocks (sharding by m, k) and B (sharding by n,k),
      // then shard by m, n.
      //
      // Do not use advanced ThreadPool queuing, just run a single long-standing
      // function in each thread.
      if (copyA) {
        blocksA = static_cast<LhsScalar*>(device.allocate(
            (blocks_m*bm)*(blocks_k*bk)*sizeof(LhsScalar)));
      }
      if (copyB) {
        blocksB = static_cast<RhsScalar*>(device.allocate(
            (blocks_n*bn)*(blocks_k*bk)*sizeof(RhsScalar)));
      }

      for (Index i = 0; i < num_threads; ++i) {
          device.enqueueNoNotification([=]() { worker(); });
      }

      workers_done.Wait();

      if (copyA) {
        device.deallocate(blocksA);
      }
      if (copyB) {
        device.deallocate(blocksB);
      }
    }

   private:
    // real block size for block index in [0, ..., blocks - 1].
    Index actual_bm(Index mi) const {
      return mi != blocks_m - 1 ? bm : m + bm - bm * blocks_m;
    }
    Index actual_bk(Index ki) const {
      return ki != blocks_k - 1 ? bk : k + bk - bk * blocks_k;
    }
    Index actual_bn(Index ni) const {
      return ni != blocks_n - 1 ? bn : n + bn - bn * blocks_n;
    }

    const Device& device;
    Index m, k, n;
    Index stride_a, stride_b, stride_c;
    Index bm, bk, bn;  // Block sizes.
    Index blocks_m, blocks_k, blocks_n;  // Number of blocks in each dimension.
    bool copyA, copyB, transposeA, transposeB;
    Index num_threads;
    Scalar *buffer;
    const LhsScalar *leftData;
    const RhsScalar *rightData;

    LhsScalar *blocksA;
    RhsScalar *blocksB;
    // barrier for joining all threads after all done.
    Barrier workers_done;
    // "queues" of (mi,ki), (ki,ni), (mi,ni) jobs packed [0,p)x[0,q) -> [0, p*q)
    std::atomic<uint32_t> packingA_jobs;
    std::atomic<uint32_t> packingB_jobs;
    std::atomic<uint32_t> compute_jobs;
    // already packed blocks for each mi-panel in A and ni-panel in B.
    std::vector<std::atomic<uint8_t>> packingA_done;
    std::vector<std::atomic<uint8_t>> packingB_done;
  };
#endif

};

} // end namespace Eigen

#endif  // EIGEN_USE_THREADS
#endif // EIGEN_CXX11_TENSOR_TENSOR_CONTRACTION_THREAD_POOL_H
