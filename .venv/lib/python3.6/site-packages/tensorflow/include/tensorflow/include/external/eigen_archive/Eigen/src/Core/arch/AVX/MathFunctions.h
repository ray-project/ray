// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2014 Pedro Gonnet (pedro.gonnet@gmail.com)
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#ifndef EIGEN_MATH_FUNCTIONS_AVX_H
#define EIGEN_MATH_FUNCTIONS_AVX_H

/* The sin and cos functions of this file are loosely derived from
 * Julien Pommier's sse math library: http://gruntthepeon.free.fr/ssemath/
 */

namespace Eigen {

namespace internal {

template <>
EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS EIGEN_UNUSED Packet8f
psin<Packet8f>(const Packet8f& _x) {
  return psin_float(_x);
}

template <>
EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS EIGEN_UNUSED Packet8f
pcos<Packet8f>(const Packet8f& _x) {
  return pcos_float(_x);
}

template <>
EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS EIGEN_UNUSED Packet8f
plog<Packet8f>(const Packet8f& _x) {
  return plog_float(_x);
}

// Exponential function. Works by writing "x = m*log(2) + r" where
// "m = floor(x/log(2)+1/2)" and "r" is the remainder. The result is then
// "exp(x) = 2^m*exp(r)" where exp(r) is in the range [-1,1).
template <>
EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS EIGEN_UNUSED Packet8f
pexp<Packet8f>(const Packet8f& _x) {
  return pexp_float(_x);
}

// Hyperbolic Tangent function.
template <>
EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS EIGEN_UNUSED Packet8f
ptanh<Packet8f>(const Packet8f& x) {
  return internal::generic_fast_tanh_float(x);
}

template <>
EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS EIGEN_UNUSED Packet4d
pexp<Packet4d>(const Packet4d& x) {
  return pexp_double(x);
}

// Functions for sqrt.
// The EIGEN_FAST_MATH version uses the _mm_rsqrt_ps approximation and one step
// of Newton's method, at a cost of 1-2 bits of precision as opposed to the
// exact solution. It does not handle +inf, or denormalized numbers correctly.
// The main advantage of this approach is not just speed, but also the fact that
// it can be inlined and pipelined with other computations, further reducing its
// effective latency. This is similar to Quake3's fast inverse square root.
// For detail see here: http://www.beyond3d.com/content/articles/8/
#if EIGEN_FAST_MATH
template <>
EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS EIGEN_UNUSED Packet8f
psqrt<Packet8f>(const Packet8f& _x) {
  Packet8f half = pmul(_x, pset1<Packet8f>(.5f));
  Packet8f denormal_mask = _mm256_and_ps(
      _mm256_cmp_ps(_x, pset1<Packet8f>((std::numeric_limits<float>::min)()),
                    _CMP_LT_OQ),
      _mm256_cmp_ps(_x, _mm256_setzero_ps(), _CMP_GE_OQ));

  // Compute approximate reciprocal sqrt.
  Packet8f x = _mm256_rsqrt_ps(_x);
  // Do a single step of Newton's iteration.
  x = pmul(x, psub(pset1<Packet8f>(1.5f), pmul(half, pmul(x,x))));
  // Flush results for denormals to zero.
  return _mm256_andnot_ps(denormal_mask, pmul(_x,x));
}
#else
template <> EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS EIGEN_UNUSED
Packet8f psqrt<Packet8f>(const Packet8f& x) {
  return _mm256_sqrt_ps(x);
}
#endif
template <> EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS EIGEN_UNUSED
Packet4d psqrt<Packet4d>(const Packet4d& x) {
  return _mm256_sqrt_pd(x);
}
#if EIGEN_FAST_MATH

template<> EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS EIGEN_UNUSED
Packet8f prsqrt<Packet8f>(const Packet8f& _x) {
  _EIGEN_DECLARE_CONST_Packet8f_FROM_INT(inf, 0x7f800000);
  _EIGEN_DECLARE_CONST_Packet8f_FROM_INT(nan, 0x7fc00000);
  _EIGEN_DECLARE_CONST_Packet8f(one_point_five, 1.5f);
  _EIGEN_DECLARE_CONST_Packet8f(minus_half, -0.5f);
  _EIGEN_DECLARE_CONST_Packet8f_FROM_INT(flt_min, 0x00800000);

  Packet8f neg_half = pmul(_x, p8f_minus_half);

  // select only the inverse sqrt of positive normal inputs (denormals are
  // flushed to zero and cause infs as well).
  Packet8f le_zero_mask = _mm256_cmp_ps(_x, p8f_flt_min, _CMP_LT_OQ);
  Packet8f x = _mm256_andnot_ps(le_zero_mask, _mm256_rsqrt_ps(_x));

  // Fill in NaNs and Infs for the negative/zero entries.
  Packet8f neg_mask = _mm256_cmp_ps(_x, _mm256_setzero_ps(), _CMP_LT_OQ);
  Packet8f zero_mask = _mm256_andnot_ps(neg_mask, le_zero_mask);
  Packet8f infs_and_nans = _mm256_or_ps(_mm256_and_ps(neg_mask, p8f_nan),
                                        _mm256_and_ps(zero_mask, p8f_inf));

  // Do a single step of Newton's iteration.
  x = pmul(x, pmadd(neg_half, pmul(x, x), p8f_one_point_five));

  // Insert NaNs and Infs in all the right places.
  return _mm256_or_ps(x, infs_and_nans);
}

#else
template <> EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS EIGEN_UNUSED
Packet8f prsqrt<Packet8f>(const Packet8f& x) {
  _EIGEN_DECLARE_CONST_Packet8f(one, 1.0f);
  return _mm256_div_ps(p8f_one, _mm256_sqrt_ps(x));
}
#endif

template <> EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS EIGEN_UNUSED
Packet4d prsqrt<Packet4d>(const Packet4d& x) {
  _EIGEN_DECLARE_CONST_Packet4d(one, 1.0);
  return _mm256_div_pd(p4d_one, _mm256_sqrt_pd(x));
}


}  // end namespace internal

}  // end namespace Eigen

#endif  // EIGEN_MATH_FUNCTIONS_AVX_H
