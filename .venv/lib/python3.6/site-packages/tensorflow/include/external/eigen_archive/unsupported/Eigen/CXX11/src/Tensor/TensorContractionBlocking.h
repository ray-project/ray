// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2014 Benoit Steiner <benoit.steiner.goog@gmail.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#ifndef EIGEN_CXX11_TENSOR_TENSOR_CONTRACTION_BLOCKING_H
#define EIGEN_CXX11_TENSOR_TENSOR_CONTRACTION_BLOCKING_H


namespace Eigen {
namespace internal {

enum {
  ShardByRow = 0,
  ShardByCol = 1
};


// Default Blocking Strategy
template<typename ResScalar, typename LhsScalar, typename RhsScalar, typename StorageIndex, int ShardingType = ShardByCol>
class TensorContractionBlocking {
 public:

 /*
   adding EIGEN_DEVICE_FUNC unconditionally to 'TensorContractionBlocking' constructor in `TensorContractionBlocking.h`
     requires adding EIGEN_DEVICE_FUNC to `computeProductBlockingSizes` in `GeneralBlockPanelKernel.h`
     which in turn, requires adding EIGEN_DEVICE_FUNC to `evaluateProductBlockingSizesHeuristic` in `GeneralBlockPanelKernel.h`
     which in turn, requires adding EIGEN_DEVICE_FUNC to `manage_caching_sizes` in `GeneralBlockPanelKernel.h`
     (else HIPCC will error out)

   However adding EIGEN_DEVICE_FUNC to `manage_caching_sizes` in `GeneralBlockPanelKernel.h`
   results in NVCC erroring out with the following error

   ../Eigen/src/Core/products/GeneralBlockPanelKernel.h(57): error #2901:
      dynamic initialization is not supported for function-scope static variables within a __device__/__global__ function
 */

  #if !defined(EIGEN_HIPCC)
  EIGEN_DEVICE_FUNC
  #endif
 TensorContractionBlocking(StorageIndex k, StorageIndex m, StorageIndex n, StorageIndex num_threads = 1) :
      kc_(k), mc_(m), nc_(n)
  {
    if (ShardingType == ShardByCol) {
      computeProductBlockingSizes<LhsScalar, RhsScalar, 1>(kc_, mc_, nc_, num_threads);
    }
    else {
      computeProductBlockingSizes<LhsScalar, RhsScalar, 1>(kc_, nc_, mc_, num_threads);
    }

    const int rhs_packet_size = internal::packet_traits<RhsScalar>::size;
    kc_ = (rhs_packet_size <= 8 || kc_ <= rhs_packet_size) ?
      kc_ : (kc_ / rhs_packet_size) * rhs_packet_size;
  }

  EIGEN_DEVICE_FUNC EIGEN_ALWAYS_INLINE StorageIndex kc() const { return kc_; }
  EIGEN_DEVICE_FUNC EIGEN_ALWAYS_INLINE StorageIndex mc() const { return mc_; }
  EIGEN_DEVICE_FUNC EIGEN_ALWAYS_INLINE StorageIndex nc() const { return nc_; }

 private:
  StorageIndex kc_;
  StorageIndex mc_;
  StorageIndex nc_;
};



#if defined(EIGEN_USE_LIBXSMM)
template <typename LhsScalar, typename RhsScalar, typename StorageIndex>
class TensorXsmmContractionBlocking {
 public:
  TensorXsmmContractionBlocking(StorageIndex k, StorageIndex m, StorageIndex n,
      size_t max_num_threads = 1, bool transposeA = false,
      bool transposeB = false):
    k_(k), m_(m), n_(n), transposeA_(transposeA),
    transposeB_(transposeB), num_threads_(max_num_threads) {
#ifdef EIGEN_TEST_SPECIFIC_BLOCKING_SIZES
    if (EIGEN_TEST_SPECIFIC_BLOCKING_SIZES) {
      mc_ = EIGEN_TEST_SPECIFIC_BLOCKING_SIZE_M;
      kc_ = EIGEN_TEST_SPECIFIC_BLOCKING_SIZE_K;
      nc_ = EIGEN_TEST_SPECIFIC_BLOCKING_SIZE_N;
      outer_m_ = EIGEN_TEST_SPECIFIC_OUTER_BLOCKING_SIZE_M;
      outer_k_ = EIGEN_TEST_SPECIFIC_OUTER_BLOCKING_SIZE_K;
      outer_n_ = EIGEN_TEST_SPECIFIC_OUTER_BLOCKING_SIZE_N;
      copyA_ = EIGEN_TEST_SPECIFIC_BLOCKING_COPY_A;
      copyB_ = EIGEN_TEST_SPECIFIC_BLOCKING_COPY_B;
      outer_m_ = outer_m_ != 0 ? outer_m_ : m;
      outer_k_ = outer_k_ != 0 ? outer_k_ : k;
      outer_n_ = outer_n_ != 0 ? outer_n_ : n;
    }
#else
    // Defaults, possibly overridden per-platform.
    copyA_ = true;
    copyB_ = false;

    // If the matrix is small enough, don't do blocking, just call single xsmm
    // kernel.
    if (static_cast<double>(m)*k*n <= LIBXSMM_THRESHOLD) {
      mc_ = m; kc_ = k; nc_ = n;
      outer_m_ = m; outer_k_ = k; outer_n_ = n;
      copyA_ = false; copyB_ = false;
    } else {
      int arch = libxsmm_cpuid_x86();

      if (arch == LIBXSMM_X86_AVX512_CORE) {
        // skylake
        mc_ = 64; kc_ = 64; nc_ = 24;
        outer_m_ = 512; outer_k_ = 512; outer_n_ = 24*22;
        // Hack to use this kernel architecture as the other one has performance
        // issues (no hardware prefetching).
        // TODO(nishantpatil): This should be removed if the issues are fixed,
        // or this one becomes the default.
        setenv("LIBXSMM_AVX512_CLASSIC_GEMM", "1", 1);
      } else if (arch == LIBXSMM_X86_AVX2) {
        // haswell
        mc_ = 32; kc_ = 192; nc_ = 33;
        outer_m_ = 512; outer_k_ = 3*192; outer_n_ = 33*16;
      } else if (arch == LIBXSMM_X86_AVX) {
        // ivybridge
        mc_ = 32; kc_ = 192; nc_ = 48;
        outer_m_ = 512; outer_k_ = 3*192; outer_n_ = 48*11;
      } else {
        // generic kernel size, usually performing well
        mc_ = 32; kc_ = 128; nc_ = 32;
        outer_m_ = 512; outer_k_ = 512; outer_n_ = 512;
      }

      // Only copy if it makes the stride smaller.
      copyA_ = copyA_ && (m > mc_);
      copyB_ = copyB_ && (k > kc_);
    }

    // We need to copy anyway if transposing
    copyA_ = copyA_ || transposeA;
    copyB_ = copyB_ || transposeB;

    // See libxsmm_gemm_prefetch_type definition in libxsmm_typedefs.h
    prefetch_ = LIBXSMM_PREFETCH_AL2CL2BL2_VIA_C;

#endif

    mc_ = mc_ > m ? m : mc_;
    nc_ = nc_ > n ? n : nc_;
    kc_ = kc_ > k ? k : kc_;

    size_t compute_parallelism = (m / mc_) * (n / nc_);
    size_t pack_parallelism = 0;
    if (copyA_) {
      pack_parallelism += (m / mc_) * (k / kc_);
    }
    if (copyB_) {
      pack_parallelism += (n / nc_) * (k / kc_);
    }
    size_t parallelism = numext::maxi(compute_parallelism, pack_parallelism);

    num_threads_ = numext::mini<size_t>(num_threads_,
                                    parallelism / MIN_JOBS_PER_THREAD);
    num_threads_ = numext::maxi<size_t>(num_threads_, 1);

    // For optimal performance outer block sizes should be multiplies of kernel
    // sizes, or bigger than matrix size (=no outer blocking).
    eigen_assert(outer_m_ % mc_ == 0 || outer_m_ >= m);
    eigen_assert(outer_k_ % kc_ == 0 || outer_k_ >= k);
    eigen_assert(outer_n_ % nc_ == 0 || outer_n_ >= n);
  }

  EIGEN_ALWAYS_INLINE StorageIndex kc() const { return kc_; }
  EIGEN_ALWAYS_INLINE StorageIndex mc() const { return mc_; }
  EIGEN_ALWAYS_INLINE StorageIndex nc() const { return nc_; }
  EIGEN_ALWAYS_INLINE StorageIndex outer_k() const { return outer_k_; }
  EIGEN_ALWAYS_INLINE StorageIndex outer_m() const { return outer_m_; }
  EIGEN_ALWAYS_INLINE StorageIndex outer_n() const { return outer_n_; }
  EIGEN_ALWAYS_INLINE bool copyA() const { return copyA_; }
  EIGEN_ALWAYS_INLINE bool copyB() const { return copyB_; }
  EIGEN_ALWAYS_INLINE bool transposeA() const { return transposeA_; }
  EIGEN_ALWAYS_INLINE bool transposeB() const { return transposeB_; }
  EIGEN_ALWAYS_INLINE int num_threads() const { return num_threads_; }
  EIGEN_ALWAYS_INLINE StorageIndex blocks_m() const { return divup(m_, mc_); }
  EIGEN_ALWAYS_INLINE StorageIndex blocks_k() const { return divup(k_, kc_); }
  EIGEN_ALWAYS_INLINE StorageIndex blocks_n() const { return divup(n_, nc_); }
  EIGEN_ALWAYS_INLINE libxsmm_gemm_prefetch_type prefetch() const {
    return prefetch_;
  }

 private:
  StorageIndex k_, m_, n_;
  StorageIndex kc_, mc_, nc_;
  StorageIndex outer_k_, outer_m_, outer_n_;
  bool copyA_, copyB_, transposeA_, transposeB_;
  size_t num_threads_;

  // Threshold for m*k*n to skip blocking and just call libxsmm
  const double LIBXSMM_THRESHOLD = 80*80*80;
  // For computing optimal number of threads - so that each thread gets at least
  // that many jobs.
  const double MIN_JOBS_PER_THREAD = 3;
  libxsmm_gemm_prefetch_type prefetch_;
};
#endif // EIGEN_USE_LIBXSMM

} // end namespace internal
} // end namespace Eigen

#endif // EIGEN_CXX11_TENSOR_TENSOR_CONTRACTION_BLOCKING_H
