// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2015 Eugene Brevdo <ebrevdo@gmail.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#ifndef EIGEN_SPECIAL_FUNCTIONS_H
#define EIGEN_SPECIAL_FUNCTIONS_H

namespace Eigen {
namespace internal {

//  Parts of this code are based on the Cephes Math Library.
//
//  Cephes Math Library Release 2.8:  June, 2000
//  Copyright 1984, 1987, 1992, 2000 by Stephen L. Moshier
//
//  Permission has been kindly provided by the original author
//  to incorporate the Cephes software into the Eigen codebase:
//
//    From: Stephen Moshier
//    To: Eugene Brevdo
//    Subject: Re: Permission to wrap several cephes functions in Eigen
//
//    Hello Eugene,
//
//    Thank you for writing.
//
//    If your licensing is similar to BSD, the formal way that has been
//    handled is simply to add a statement to the effect that you are incorporating
//    the Cephes software by permission of the author.
//
//    Good luck with your project,
//    Steve

namespace cephes {

/* polevl (modified for Eigen)
 *
 *      Evaluate polynomial
 *
 *
 *
 * SYNOPSIS:
 *
 * int N;
 * Scalar x, y, coef[N+1];
 *
 * y = polevl<decltype(x), N>( x, coef);
 *
 *
 *
 * DESCRIPTION:
 *
 * Evaluates polynomial of degree N:
 *
 *                     2          N
 * y  =  C  + C x + C x  +...+ C x
 *        0    1     2          N
 *
 * Coefficients are stored in reverse order:
 *
 * coef[0] = C  , ..., coef[N] = C  .
 *            N                   0
 *
 *  The function p1evl() assumes that coef[N] = 1.0 and is
 * omitted from the array.  Its calling arguments are
 * otherwise the same as polevl().
 *
 *
 * The Eigen implementation is templatized.  For best speed, store
 * coef as a const array (constexpr), e.g.
 *
 * const double coef[] = {1.0, 2.0, 3.0, ...};
 *
 */
template <typename Scalar, int N>
struct polevl {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(const Scalar x, const Scalar coef[]) {
    EIGEN_STATIC_ASSERT((N > 0), YOU_MADE_A_PROGRAMMING_MISTAKE);

    return polevl<Scalar, N - 1>::run(x, coef) * x + coef[N];
  }
};

template <typename Scalar>
struct polevl<Scalar, 0> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(const Scalar, const Scalar coef[]) {
    return coef[0];
  }
};

/* chbevl (modified for Eigen)
 *
 *     Evaluate Chebyshev series
 *
 *
 *
 * SYNOPSIS:
 *
 * int N;
 * Scalar x, y, coef[N], chebevl();
 *
 * y = chbevl( x, coef, N );
 *
 *
 *
 * DESCRIPTION:
 *
 * Evaluates the series
 *
 *        N-1
 *         - '
 *  y  =   >   coef[i] T (x/2)
 *         -            i
 *        i=0
 *
 * of Chebyshev polynomials Ti at argument x/2.
 *
 * Coefficients are stored in reverse order, i.e. the zero
 * order term is last in the array.  Note N is the number of
 * coefficients, not the order.
 *
 * If coefficients are for the interval a to b, x must
 * have been transformed to x -> 2(2x - b - a)/(b-a) before
 * entering the routine.  This maps x from (a, b) to (-1, 1),
 * over which the Chebyshev polynomials are defined.
 *
 * If the coefficients are for the inverted interval, in
 * which (a, b) is mapped to (1/b, 1/a), the transformation
 * required is x -> 2(2ab/x - b - a)/(b-a).  If b is infinity,
 * this becomes x -> 4a/x - 1.
 *
 *
 *
 * SPEED:
 *
 * Taking advantage of the recurrence properties of the
 * Chebyshev polynomials, the routine requires one more
 * addition per loop than evaluating a nested polynomial of
 * the same degree.
 *
 */
template <typename Scalar, int N>
struct chebevl {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(Scalar x, const Scalar coef[]) {
    Scalar b0 = coef[0];
    Scalar b1 = 0;
    Scalar b2;

    for (int i = 1; i < N; i++) {
      b2 = b1;
      b1 = b0;
      b0 = x * b1 - b2 + coef[i];
    }

    return Scalar(0.5) * (b0 - b2);
  }
};

}  // end namespace cephes

/****************************************************************************
 * Implementation of lgamma, requires C++11/C99                             *
 ****************************************************************************/

template <typename Scalar>
struct lgamma_impl {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(const Scalar) {
    EIGEN_STATIC_ASSERT((internal::is_same<Scalar, Scalar>::value == false),
                        THIS_TYPE_IS_NOT_SUPPORTED);
    return Scalar(0);
  }
};

template <typename Scalar>
struct lgamma_retval {
  typedef Scalar type;
};

#if EIGEN_HAS_C99_MATH
template <>
struct lgamma_impl<float> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE float run(float x) {
#if !defined(EIGEN_GPU_COMPILE_PHASE) && (defined(_BSD_SOURCE) || defined(_SVID_SOURCE)) && !defined(__APPLE__) 
    int dummy;
    return ::lgammaf_r(x, &dummy);
#elif defined(EIGEN_USE_SYCL) && defined(__SYCL_DEVICE_ONLY__)
    return cl::sycl::lgamma(x);
#else
    return ::lgammaf(x);
#endif
  }
};

template <>
struct lgamma_impl<double> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE double run(double x) {
#if !defined(EIGEN_GPU_COMPILE_PHASE) && (defined(_BSD_SOURCE) || defined(_SVID_SOURCE)) && !defined(__APPLE__) 
    int dummy;
    return ::lgamma_r(x, &dummy);
#elif defined(EIGEN_USE_SYCL) && defined(__SYCL_DEVICE_ONLY__)
    return cl::sycl::lgamma(x);
#else
    return ::lgamma(x);
#endif
  }
};
#endif

/****************************************************************************
 * Implementation of digamma (psi), based on Cephes                         *
 ****************************************************************************/

template <typename Scalar>
struct digamma_retval {
  typedef Scalar type;
};

/*
 *
 * Polynomial evaluation helper for the Psi (digamma) function.
 *
 * digamma_impl_maybe_poly::run(s) evaluates the asymptotic Psi expansion for
 * input Scalar s, assuming s is above 10.0.
 *
 * If s is above a certain threshold for the given Scalar type, zero
 * is returned.  Otherwise the polynomial is evaluated with enough
 * coefficients for results matching Scalar machine precision.
 *
 *
 */
template <typename Scalar>
struct digamma_impl_maybe_poly {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(const Scalar) {
    EIGEN_STATIC_ASSERT((internal::is_same<Scalar, Scalar>::value == false),
                        THIS_TYPE_IS_NOT_SUPPORTED);
    return Scalar(0);
  }
};


template <>
struct digamma_impl_maybe_poly<float> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE float run(const float s) {
    const float A[] = {
      -4.16666666666666666667E-3f,
      3.96825396825396825397E-3f,
      -8.33333333333333333333E-3f,
      8.33333333333333333333E-2f
    };

    float z;
    if (s < 1.0e8f) {
      z = 1.0f / (s * s);
      return z * cephes::polevl<float, 3>::run(z, A);
    } else return 0.0f;
  }
};

template <>
struct digamma_impl_maybe_poly<double> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE double run(const double s) {
    const double A[] = {
      8.33333333333333333333E-2,
      -2.10927960927960927961E-2,
      7.57575757575757575758E-3,
      -4.16666666666666666667E-3,
      3.96825396825396825397E-3,
      -8.33333333333333333333E-3,
      8.33333333333333333333E-2
    };

    double z;
    if (s < 1.0e17) {
      z = 1.0 / (s * s);
      return z * cephes::polevl<double, 6>::run(z, A);
    }
    else return 0.0;
  }
};

template <typename Scalar>
struct digamma_impl {
  EIGEN_DEVICE_FUNC
  static Scalar run(Scalar x) {
    /*
     *
     *     Psi (digamma) function (modified for Eigen)
     *
     *
     * SYNOPSIS:
     *
     * double x, y, psi();
     *
     * y = psi( x );
     *
     *
     * DESCRIPTION:
     *
     *              d      -
     *   psi(x)  =  -- ln | (x)
     *              dx
     *
     * is the logarithmic derivative of the gamma function.
     * For integer x,
     *                   n-1
     *                    -
     * psi(n) = -EUL  +   >  1/k.
     *                    -
     *                   k=1
     *
     * If x is negative, it is transformed to a positive argument by the
     * reflection formula  psi(1-x) = psi(x) + pi cot(pi x).
     * For general positive x, the argument is made greater than 10
     * using the recurrence  psi(x+1) = psi(x) + 1/x.
     * Then the following asymptotic expansion is applied:
     *
     *                           inf.   B
     *                            -      2k
     * psi(x) = log(x) - 1/2x -   >   -------
     *                            -        2k
     *                           k=1   2k x
     *
     * where the B2k are Bernoulli numbers.
     *
     * ACCURACY (float):
     *    Relative error (except absolute when |psi| < 1):
     * arithmetic   domain     # trials      peak         rms
     *    IEEE      0,30        30000       1.3e-15     1.4e-16
     *    IEEE      -30,0       40000       1.5e-15     2.2e-16
     *
     * ACCURACY (double):
     *    Absolute error,  relative when |psi| > 1 :
     * arithmetic   domain     # trials      peak         rms
     *    IEEE      -33,0        30000      8.2e-7      1.2e-7
     *    IEEE      0,33        100000      7.3e-7      7.7e-8
     *
     * ERROR MESSAGES:
     *     message         condition      value returned
     * psi singularity    x integer <=0      INFINITY
     */

    Scalar p, q, nz, s, w, y;
    bool negative = false;

    const Scalar maxnum = NumTraits<Scalar>::infinity();
    const Scalar m_pi = Scalar(EIGEN_PI);

    const Scalar zero = Scalar(0);
    const Scalar one = Scalar(1);
    const Scalar half = Scalar(0.5);
    nz = zero;

    if (x <= zero) {
      negative = true;
      q = x;
      p = numext::floor(q);
      if (p == q) {
        return maxnum;
      }
      /* Remove the zeros of tan(m_pi x)
       * by subtracting the nearest integer from x
       */
      nz = q - p;
      if (nz != half) {
        if (nz > half) {
          p += one;
          nz = q - p;
        }
        nz = m_pi / numext::tan(m_pi * nz);
      }
      else {
        nz = zero;
      }
      x = one - x;
    }

    /* use the recurrence psi(x+1) = psi(x) + 1/x. */
    s = x;
    w = zero;
    while (s < Scalar(10)) {
      w += one / s;
      s += one;
    }

    y = digamma_impl_maybe_poly<Scalar>::run(s);

    y = numext::log(s) - (half / s) - y - w;

    return (negative) ? y - nz : y;
  }
};

/****************************************************************************
 * Implementation of erf, requires C++11/C99                                *
 ****************************************************************************/

template <typename Scalar>
struct erf_impl {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(const Scalar) {
    EIGEN_STATIC_ASSERT((internal::is_same<Scalar, Scalar>::value == false),
                        THIS_TYPE_IS_NOT_SUPPORTED);
    return Scalar(0);
  }
};

template <typename Scalar>
struct erf_retval {
  typedef Scalar type;
};

#if EIGEN_HAS_C99_MATH
template <>
struct erf_impl<float> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE float run(float x) {
#if defined(EIGEN_USE_SYCL) && defined(__SYCL_DEVICE_ONLY__)
    return cl::sycl::erf(x);
#else
    return ::erff(x);
#endif
  }
};

template <>
struct erf_impl<double> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE double run(double x) {
#if defined(EIGEN_USE_SYCL) && defined(__SYCL_DEVICE_ONLY__)
    return cl::sycl::erf(x);
#else
    return ::erf(x);
#endif
  }
};
#endif  // EIGEN_HAS_C99_MATH

/***************************************************************************
* Implementation of erfc, requires C++11/C99                               *
****************************************************************************/

template <typename Scalar>
struct erfc_impl {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(const Scalar) {
    EIGEN_STATIC_ASSERT((internal::is_same<Scalar, Scalar>::value == false),
                        THIS_TYPE_IS_NOT_SUPPORTED);
    return Scalar(0);
  }
};

template <typename Scalar>
struct erfc_retval {
  typedef Scalar type;
};

#if EIGEN_HAS_C99_MATH
template <>
struct erfc_impl<float> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE float run(const float x) {
#if defined(EIGEN_USE_SYCL) && defined(__SYCL_DEVICE_ONLY__)
    return cl::sycl::erfc(x);
#else
    return ::erfcf(x);
#endif
  }
};

template <>
struct erfc_impl<double> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE double run(const double x) {
#if defined(EIGEN_USE_SYCL) && defined(__SYCL_DEVICE_ONLY__)
    return cl::sycl::erfc(x);
#else
    return ::erfc(x);
#endif
  }
};
#endif  // EIGEN_HAS_C99_MATH

/**************************************************************************************************************
 * Implementation of igammac (complemented incomplete gamma integral), based on Cephes but requires C++11/C99 *
 **************************************************************************************************************/

template <typename Scalar>
struct igammac_retval {
  typedef Scalar type;
};

// NOTE: cephes_helper is also used to implement zeta
template <typename Scalar>
struct cephes_helper {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar machep() { assert(false && "machep not supported for this type"); return 0.0; }
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar big() { assert(false && "big not supported for this type"); return 0.0; }
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar biginv() { assert(false && "biginv not supported for this type"); return 0.0; }
};

template <>
struct cephes_helper<float> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE float machep() {
    return NumTraits<float>::epsilon() / 2;  // 1.0 - machep == 1.0
  }
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE float big() {
    // use epsneg (1.0 - epsneg == 1.0)
    return 1.0f / (NumTraits<float>::epsilon() / 2);
  }
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE float biginv() {
    // epsneg
    return machep();
  }
};

template <>
struct cephes_helper<double> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE double machep() {
    return NumTraits<double>::epsilon() / 2;  // 1.0 - machep == 1.0
  }
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE double big() {
    return 1.0 / NumTraits<double>::epsilon();
  }
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE double biginv() {
    // inverse of eps
    return NumTraits<double>::epsilon();
  }
};

enum IgammaComputationMode { VALUE, DERIVATIVE, SAMPLE_DERIVATIVE };

template <typename Scalar, IgammaComputationMode mode>
EIGEN_DEVICE_FUNC
int igamma_num_iterations() {
  /* Returns the maximum number of internal iterations for igamma computation.
   */
  if (mode == VALUE) {
    return 2000;
  }

  if (internal::is_same<Scalar, float>::value) {
    return 200;
  } else if (internal::is_same<Scalar, double>::value) {
    return 500;
  } else {
    return 2000;
  }
}

template <typename Scalar, IgammaComputationMode mode>
struct igammac_cf_impl {
  /* Computes igamc(a, x) or derivative (depending on the mode)
   * using the continued fraction expansion of the complementary
   * incomplete Gamma function.
   *
   * Preconditions:
   *   a > 0
   *   x >= 1
   *   x >= a
   */
  EIGEN_DEVICE_FUNC
  static Scalar run(Scalar a, Scalar x) {
    const Scalar zero = 0;
    const Scalar one = 1;
    const Scalar two = 2;
    const Scalar machep = cephes_helper<Scalar>::machep();
    const Scalar big = cephes_helper<Scalar>::big();
    const Scalar biginv = cephes_helper<Scalar>::biginv();

    if ((numext::isinf)(x)) {
      return zero;
    }

    // continued fraction
    Scalar y = one - a;
    Scalar z = x + y + one;
    Scalar c = zero;
    Scalar pkm2 = one;
    Scalar qkm2 = x;
    Scalar pkm1 = x + one;
    Scalar qkm1 = z * x;
    Scalar ans = pkm1 / qkm1;

    Scalar dpkm2_da = zero;
    Scalar dqkm2_da = zero;
    Scalar dpkm1_da = zero;
    Scalar dqkm1_da = -x;
    Scalar dans_da = (dpkm1_da - ans * dqkm1_da) / qkm1;

    for (int i = 0; i < igamma_num_iterations<Scalar, mode>(); i++) {
      c += one;
      y += one;
      z += two;

      Scalar yc = y * c;
      Scalar pk = pkm1 * z - pkm2 * yc;
      Scalar qk = qkm1 * z - qkm2 * yc;

      Scalar dpk_da = dpkm1_da * z - pkm1 - dpkm2_da * yc + pkm2 * c;
      Scalar dqk_da = dqkm1_da * z - qkm1 - dqkm2_da * yc + qkm2 * c;

      if (qk != zero) {
        Scalar ans_prev = ans;
        ans = pk / qk;

        Scalar dans_da_prev = dans_da;
        dans_da = (dpk_da - ans * dqk_da) / qk;

        if (mode == VALUE) {
          if (numext::abs(ans_prev - ans) <= machep * numext::abs(ans)) {
            break;
          }
        } else {
          if (numext::abs(dans_da - dans_da_prev) <= machep) {
            break;
          }
        }
      }

      pkm2 = pkm1;
      pkm1 = pk;
      qkm2 = qkm1;
      qkm1 = qk;

      dpkm2_da = dpkm1_da;
      dpkm1_da = dpk_da;
      dqkm2_da = dqkm1_da;
      dqkm1_da = dqk_da;

      if (numext::abs(pk) > big) {
        pkm2 *= biginv;
        pkm1 *= biginv;
        qkm2 *= biginv;
        qkm1 *= biginv;

        dpkm2_da *= biginv;
        dpkm1_da *= biginv;
        dqkm2_da *= biginv;
        dqkm1_da *= biginv;
      }
    }

    /* Compute  x**a * exp(-x) / gamma(a)  */
    Scalar logax = a * numext::log(x) - x - lgamma_impl<Scalar>::run(a);
    Scalar dlogax_da = numext::log(x) - digamma_impl<Scalar>::run(a);
    Scalar ax = numext::exp(logax);
    Scalar dax_da = ax * dlogax_da;

    switch (mode) {
      case VALUE:
        return ans * ax;
      case DERIVATIVE:
        return ans * dax_da + dans_da * ax;
      case SAMPLE_DERIVATIVE:
      default: // this is needed to suppress clang warning
        return -(dans_da + ans * dlogax_da) * x;
    }
  }
};

template <typename Scalar, IgammaComputationMode mode>
struct igamma_series_impl {
  /* Computes igam(a, x) or its derivative (depending on the mode)
   * using the series expansion of the incomplete Gamma function.
   *
   * Preconditions:
   *   x > 0
   *   a > 0
   *   !(x > 1 && x > a)
   */
  EIGEN_DEVICE_FUNC
  static Scalar run(Scalar a, Scalar x) {
    const Scalar zero = 0;
    const Scalar one = 1;
    const Scalar machep = cephes_helper<Scalar>::machep();

    /* power series */
    Scalar r = a;
    Scalar c = one;
    Scalar ans = one;

    Scalar dc_da = zero;
    Scalar dans_da = zero;

    for (int i = 0; i < igamma_num_iterations<Scalar, mode>(); i++) {
      r += one;
      Scalar term = x / r;
      Scalar dterm_da = -x / (r * r);
      dc_da = term * dc_da + dterm_da * c;
      dans_da += dc_da;
      c *= term;
      ans += c;

      if (mode == VALUE) {
        if (c <= machep * ans) {
          break;
        }
      } else {
        if (numext::abs(dc_da) <= machep * numext::abs(dans_da)) {
          break;
        }
      }
    }

    /* Compute  x**a * exp(-x) / gamma(a + 1)  */
    Scalar logax = a * numext::log(x) - x - lgamma_impl<Scalar>::run(a + one);
    Scalar dlogax_da = numext::log(x) - digamma_impl<Scalar>::run(a + one);
    Scalar ax = numext::exp(logax);
    Scalar dax_da = ax * dlogax_da;

    switch (mode) {
      case VALUE:
        return ans * ax;
      case DERIVATIVE:
        return ans * dax_da + dans_da * ax;
      case SAMPLE_DERIVATIVE:
      default: // this is needed to suppress clang warning
        return -(dans_da + ans * dlogax_da) * x / a;
    }
  }
};

#if !EIGEN_HAS_C99_MATH

template <typename Scalar>
struct igammac_impl {
  EIGEN_DEVICE_FUNC
  static Scalar run(Scalar a, Scalar x) {
    EIGEN_STATIC_ASSERT((internal::is_same<Scalar, Scalar>::value == false),
                        THIS_TYPE_IS_NOT_SUPPORTED);
    return Scalar(0);
  }
};

#else

template <typename Scalar>
struct igammac_impl {
  EIGEN_DEVICE_FUNC
  static Scalar run(Scalar a, Scalar x) {
    /*  igamc()
     *
     *	Incomplete gamma integral (modified for Eigen)
     *
     *
     *
     * SYNOPSIS:
     *
     * double a, x, y, igamc();
     *
     * y = igamc( a, x );
     *
     * DESCRIPTION:
     *
     * The function is defined by
     *
     *
     *  igamc(a,x)   =   1 - igam(a,x)
     *
     *                            inf.
     *                              -
     *                     1       | |  -t  a-1
     *               =   -----     |   e   t   dt.
     *                    -      | |
     *                   | (a)    -
     *                             x
     *
     *
     * In this implementation both arguments must be positive.
     * The integral is evaluated by either a power series or
     * continued fraction expansion, depending on the relative
     * values of a and x.
     *
     * ACCURACY (float):
     *
     *                      Relative error:
     * arithmetic   domain     # trials      peak         rms
     *    IEEE      0,30        30000       7.8e-6      5.9e-7
     *
     *
     * ACCURACY (double):
     *
     * Tested at random a, x.
     *                a         x                      Relative error:
     * arithmetic   domain   domain     # trials      peak         rms
     *    IEEE     0.5,100   0,100      200000       1.9e-14     1.7e-15
     *    IEEE     0.01,0.5  0,100      200000       1.4e-13     1.6e-15
     *
     */
    /*
      Cephes Math Library Release 2.2: June, 1992
      Copyright 1985, 1987, 1992 by Stephen L. Moshier
      Direct inquiries to 30 Frost Street, Cambridge, MA 02140
    */
    const Scalar zero = 0;
    const Scalar one = 1;
    const Scalar nan = NumTraits<Scalar>::quiet_NaN();

    if ((x < zero) || (a <= zero)) {
      // domain error
      return nan;
    }

    if ((numext::isnan)(a) || (numext::isnan)(x)) {  // propagate nans
      return nan;
    }

    if ((x < one) || (x < a)) {
      return (one - igamma_series_impl<Scalar, VALUE>::run(a, x));
    }

    return igammac_cf_impl<Scalar, VALUE>::run(a, x);
  }
};

#endif  // EIGEN_HAS_C99_MATH

/************************************************************************************************
 * Implementation of igamma (incomplete gamma integral), based on Cephes but requires C++11/C99 *
 ************************************************************************************************/

#if !EIGEN_HAS_C99_MATH

template <typename Scalar, IgammaComputationMode mode>
struct igamma_generic_impl {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(Scalar a, Scalar x) {
    EIGEN_STATIC_ASSERT((internal::is_same<Scalar, Scalar>::value == false),
                        THIS_TYPE_IS_NOT_SUPPORTED);
    return Scalar(0);
  }
};

#else

template <typename Scalar, IgammaComputationMode mode>
struct igamma_generic_impl {
  EIGEN_DEVICE_FUNC
  static Scalar run(Scalar a, Scalar x) {
    /* Depending on the mode, returns
     * - VALUE: incomplete Gamma function igamma(a, x)
     * - DERIVATIVE: derivative of incomplete Gamma function d/da igamma(a, x)
     * - SAMPLE_DERIVATIVE: implicit derivative of a Gamma random variable
     * x ~ Gamma(x | a, 1), dx/da = -1 / Gamma(x | a, 1) * d igamma(a, x) / dx
     *
     * Derivatives are implemented by forward-mode differentiation.
     */
    const Scalar zero = 0;
    const Scalar one = 1;
    const Scalar nan = NumTraits<Scalar>::quiet_NaN();

    if (x == zero) return zero;

    if ((x < zero) || (a <= zero)) {  // domain error
      return nan;
    }

    if ((numext::isnan)(a) || (numext::isnan)(x)) {  // propagate nans
      return nan;
    }

    if ((x > one) && (x > a)) {
      Scalar ret = igammac_cf_impl<Scalar, mode>::run(a, x);
      if (mode == VALUE) {
        return one - ret;
      } else {
        return -ret;
      }
    }

    return igamma_series_impl<Scalar, mode>::run(a, x);
  }
};

#endif  // EIGEN_HAS_C99_MATH

template <typename Scalar>
struct igamma_retval {
  typedef Scalar type;
};

template <typename Scalar>
struct igamma_impl : igamma_generic_impl<Scalar, VALUE> {
  /* igam()
   * Incomplete gamma integral.
   *
   * The CDF of Gamma(a, 1) random variable at the point x.
   *
   * Accuracy estimation. For each a in [10^-2, 10^-1...10^3] we sample
   * 50 Gamma random variables x ~ Gamma(x | a, 1), a total of 300 points.
   * The ground truth is computed by mpmath. Mean absolute error:
   * float: 1.26713e-05
   * double: 2.33606e-12
   *
   * Cephes documentation below.
   *
   * SYNOPSIS:
   *
   * double a, x, y, igam();
   *
   * y = igam( a, x );
   *
   * DESCRIPTION:
   *
   * The function is defined by
   *
   *                           x
   *                            -
   *                   1       | |  -t  a-1
   *  igam(a,x)  =   -----     |   e   t   dt.
   *                  -      | |
   *                 | (a)    -
   *                           0
   *
   *
   * In this implementation both arguments must be positive.
   * The integral is evaluated by either a power series or
   * continued fraction expansion, depending on the relative
   * values of a and x.
   *
   * ACCURACY (double):
   *
   *                      Relative error:
   * arithmetic   domain     # trials      peak         rms
   *    IEEE      0,30       200000       3.6e-14     2.9e-15
   *    IEEE      0,100      300000       9.9e-14     1.5e-14
   *
   *
   * ACCURACY (float):
   *
   *                      Relative error:
   * arithmetic   domain     # trials      peak         rms
   *    IEEE      0,30        20000       7.8e-6      5.9e-7
   *
   */
  /*
    Cephes Math Library Release 2.2: June, 1992
    Copyright 1985, 1987, 1992 by Stephen L. Moshier
    Direct inquiries to 30 Frost Street, Cambridge, MA 02140
  */

  /* left tail of incomplete gamma function:
   *
   *          inf.      k
   *   a  -x   -       x
   *  x  e     >   ----------
   *           -     -
   *          k=0   | (a+k+1)
   *
   */
};

template <typename Scalar>
struct igamma_der_a_retval : igamma_retval<Scalar> {};

template <typename Scalar>
struct igamma_der_a_impl : igamma_generic_impl<Scalar, DERIVATIVE> {
  /* Derivative of the incomplete Gamma function with respect to a.
   *
   * Computes d/da igamma(a, x) by forward differentiation of the igamma code.
   *
   * Accuracy estimation. For each a in [10^-2, 10^-1...10^3] we sample
   * 50 Gamma random variables x ~ Gamma(x | a, 1), a total of 300 points.
   * The ground truth is computed by mpmath. Mean absolute error:
   * float: 6.17992e-07
   * double: 4.60453e-12
   *
   * Reference:
   * R. Moore. "Algorithm AS 187: Derivatives of the incomplete gamma
   * integral". Journal of the Royal Statistical Society. 1982
   */
};

template <typename Scalar>
struct gamma_sample_der_alpha_retval : igamma_retval<Scalar> {};

template <typename Scalar>
struct gamma_sample_der_alpha_impl
    : igamma_generic_impl<Scalar, SAMPLE_DERIVATIVE> {
  /* Derivative of a Gamma random variable sample with respect to alpha.
   *
   * Consider a sample of a Gamma random variable with the concentration
   * parameter alpha: sample ~ Gamma(alpha, 1). The reparameterization
   * derivative that we want to compute is dsample / dalpha =
   * d igammainv(alpha, u) / dalpha, where u = igamma(alpha, sample).
   * However, this formula is numerically unstable and expensive, so instead
   * we use implicit differentiation:
   *
   * igamma(alpha, sample) = u, where u ~ Uniform(0, 1).
   * Apply d / dalpha to both sides:
   * d igamma(alpha, sample) / dalpha
   *     + d igamma(alpha, sample) / dsample * dsample/dalpha  = 0
   * d igamma(alpha, sample) / dalpha
   *     + Gamma(sample | alpha, 1) dsample / dalpha = 0
   * dsample/dalpha = - (d igamma(alpha, sample) / dalpha)
   *                   / Gamma(sample | alpha, 1)
   *
   * Here Gamma(sample | alpha, 1) is the PDF of the Gamma distribution
   * (note that the derivative of the CDF w.r.t. sample is the PDF).
   * See the reference below for more details.
   *
   * The derivative of igamma(alpha, sample) is computed by forward
   * differentiation of the igamma code. Division by the Gamma PDF is performed
   * in the same code, increasing the accuracy and speed due to cancellation
   * of some terms.
   *
   * Accuracy estimation. For each alpha in [10^-2, 10^-1...10^3] we sample
   * 50 Gamma random variables sample ~ Gamma(sample | alpha, 1), a total of 300
   * points. The ground truth is computed by mpmath. Mean absolute error:
   * float: 2.1686e-06
   * double: 1.4774e-12
   *
   * Reference:
   * M. Figurnov, S. Mohamed, A. Mnih "Implicit Reparameterization Gradients".
   * 2018
   */
};

/*****************************************************************************
 * Implementation of Riemann zeta function of two arguments, based on Cephes *
 *****************************************************************************/

template <typename Scalar>
struct zeta_retval {
    typedef Scalar type;
};

template <typename Scalar>
struct zeta_impl_series {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(const Scalar) {
    EIGEN_STATIC_ASSERT((internal::is_same<Scalar, Scalar>::value == false),
                        THIS_TYPE_IS_NOT_SUPPORTED);
    return Scalar(0);
  }
};

template <>
struct zeta_impl_series<float> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE bool run(float& a, float& b, float& s, const float x, const float machep) {
    int i = 0;
    while(i < 9)
    {
        i += 1;
        a += 1.0f;
        b = numext::pow( a, -x );
        s += b;
        if( numext::abs(b/s) < machep )
            return true;
    }

    //Return whether we are done
    return false;
  }
};

template <>
struct zeta_impl_series<double> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE bool run(double& a, double& b, double& s, const double x, const double machep) {
    int i = 0;
    while( (i < 9) || (a <= 9.0) )
    {
        i += 1;
        a += 1.0;
        b = numext::pow( a, -x );
        s += b;
        if( numext::abs(b/s) < machep )
            return true;
    }

    //Return whether we are done
    return false;
  }
};

template <typename Scalar>
struct zeta_impl {
    EIGEN_DEVICE_FUNC
    static Scalar run(Scalar x, Scalar q) {
        /*							zeta.c
         *
         *	Riemann zeta function of two arguments
         *
         *
         *
         * SYNOPSIS:
         *
         * double x, q, y, zeta();
         *
         * y = zeta( x, q );
         *
         *
         *
         * DESCRIPTION:
         *
         *
         *
         *                 inf.
         *                  -        -x
         *   zeta(x,q)  =   >   (k+q)
         *                  -
         *                 k=0
         *
         * where x > 1 and q is not a negative integer or zero.
         * The Euler-Maclaurin summation formula is used to obtain
         * the expansion
         *
         *                n
         *                -       -x
         * zeta(x,q)  =   >  (k+q)
         *                -
         *               k=1
         *
         *           1-x                 inf.  B   x(x+1)...(x+2j)
         *      (n+q)           1         -     2j
         *  +  ---------  -  -------  +   >    --------------------
         *        x-1              x      -                   x+2j+1
         *                   2(n+q)      j=1       (2j)! (n+q)
         *
         * where the B2j are Bernoulli numbers.  Note that (see zetac.c)
         * zeta(x,1) = zetac(x) + 1.
         *
         *
         *
         * ACCURACY:
         *
         * Relative error for single precision:
         * arithmetic   domain     # trials      peak         rms
         *    IEEE      0,25        10000       6.9e-7      1.0e-7
         *
         * Large arguments may produce underflow in powf(), in which
         * case the results are inaccurate.
         *
         * REFERENCE:
         *
         * Gradshteyn, I. S., and I. M. Ryzhik, Tables of Integrals,
         * Series, and Products, p. 1073; Academic Press, 1980.
         *
         */

        int i;
        Scalar p, r, a, b, k, s, t, w;

        const Scalar A[] = {
            Scalar(12.0),
            Scalar(-720.0),
            Scalar(30240.0),
            Scalar(-1209600.0),
            Scalar(47900160.0),
            Scalar(-1.8924375803183791606e9), /*1.307674368e12/691*/
            Scalar(7.47242496e10),
            Scalar(-2.950130727918164224e12), /*1.067062284288e16/3617*/
            Scalar(1.1646782814350067249e14), /*5.109094217170944e18/43867*/
            Scalar(-4.5979787224074726105e15), /*8.028576626982912e20/174611*/
            Scalar(1.8152105401943546773e17), /*1.5511210043330985984e23/854513*/
            Scalar(-7.1661652561756670113e18) /*1.6938241367317436694528e27/236364091*/
            };

        const Scalar maxnum = NumTraits<Scalar>::infinity();
        const Scalar zero = 0.0, half = 0.5, one = 1.0;
        const Scalar machep = cephes_helper<Scalar>::machep();
        const Scalar nan = NumTraits<Scalar>::quiet_NaN();

        if( x == one )
            return maxnum;

        if( x < one )
        {
            return nan;
        }

        if( q <= zero )
        {
            if(q == numext::floor(q))
            {
                return maxnum;
            }
            p = x;
            r = numext::floor(p);
            if (p != r)
                return nan;
        }

        /* Permit negative q but continue sum until n+q > +9 .
         * This case should be handled by a reflection formula.
         * If q<0 and x is an integer, there is a relation to
         * the polygamma function.
         */
        s = numext::pow( q, -x );
        a = q;
        b = zero;
        // Run the summation in a helper function that is specific to the floating precision
        if (zeta_impl_series<Scalar>::run(a, b, s, x, machep)) {
            return s;
        }

        w = a;
        s += b*w/(x-one);
        s -= half * b;
        a = one;
        k = zero;
        for( i=0; i<12; i++ )
        {
            a *= x + k;
            b /= w;
            t = a*b/A[i];
            s = s + t;
            t = numext::abs(t/s);
            if( t < machep ) {
              break;
            }
            k += one;
            a *= x + k;
            b /= w;
            k += one;
        }
        return s;
  }
};

/****************************************************************************
 * Implementation of polygamma function, requires C++11/C99                 *
 ****************************************************************************/

template <typename Scalar>
struct polygamma_retval {
    typedef Scalar type;
};

#if !EIGEN_HAS_C99_MATH

template <typename Scalar>
struct polygamma_impl {
    EIGEN_DEVICE_FUNC
    static EIGEN_STRONG_INLINE Scalar run(Scalar n, Scalar x) {
        EIGEN_STATIC_ASSERT((internal::is_same<Scalar, Scalar>::value == false),
                            THIS_TYPE_IS_NOT_SUPPORTED);
        return Scalar(0);
    }
};

#else

template <typename Scalar>
struct polygamma_impl {
    EIGEN_DEVICE_FUNC
    static Scalar run(Scalar n, Scalar x) {
        Scalar zero = 0.0, one = 1.0;
        Scalar nplus = n + one;
        const Scalar nan = NumTraits<Scalar>::quiet_NaN();

        // Check that n is an integer
        if (numext::floor(n) != n) {
            return nan;
        }
        // Just return the digamma function for n = 1
        else if (n == zero) {
            return digamma_impl<Scalar>::run(x);
        }
        // Use the same implementation as scipy
        else {
            Scalar factorial = numext::exp(lgamma_impl<Scalar>::run(nplus));
            return numext::pow(-one, nplus) * factorial * zeta_impl<Scalar>::run(nplus, x);
        }
  }
};

#endif  // EIGEN_HAS_C99_MATH

/************************************************************************************************
 * Implementation of betainc (incomplete beta integral), based on Cephes but requires C++11/C99 *
 ************************************************************************************************/

template <typename Scalar>
struct betainc_retval {
  typedef Scalar type;
};

#if !EIGEN_HAS_C99_MATH

template <typename Scalar>
struct betainc_impl {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(Scalar a, Scalar b, Scalar x) {
    EIGEN_STATIC_ASSERT((internal::is_same<Scalar, Scalar>::value == false),
                        THIS_TYPE_IS_NOT_SUPPORTED);
    return Scalar(0);
  }
};

#else

template <typename Scalar>
struct betainc_impl {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(Scalar, Scalar, Scalar) {
    /*	betaincf.c
     *
     *	Incomplete beta integral
     *
     *
     * SYNOPSIS:
     *
     * float a, b, x, y, betaincf();
     *
     * y = betaincf( a, b, x );
     *
     *
     * DESCRIPTION:
     *
     * Returns incomplete beta integral of the arguments, evaluated
     * from zero to x.  The function is defined as
     *
     *                  x
     *     -            -
     *    | (a+b)      | |  a-1     b-1
     *  -----------    |   t   (1-t)   dt.
     *   -     -     | |
     *  | (a) | (b)   -
     *                 0
     *
     * The domain of definition is 0 <= x <= 1.  In this
     * implementation a and b are restricted to positive values.
     * The integral from x to 1 may be obtained by the symmetry
     * relation
     *
     *    1 - betainc( a, b, x )  =  betainc( b, a, 1-x ).
     *
     * The integral is evaluated by a continued fraction expansion.
     * If a < 1, the function calls itself recursively after a
     * transformation to increase a to a+1.
     *
     * ACCURACY (float):
     *
     * Tested at random points (a,b,x) with a and b in the indicated
     * interval and x between 0 and 1.
     *
     * arithmetic   domain     # trials      peak         rms
     * Relative error:
     *    IEEE       0,30       10000       3.7e-5      5.1e-6
     *    IEEE       0,100      10000       1.7e-4      2.5e-5
     * The useful domain for relative error is limited by underflow
     * of the single precision exponential function.
     * Absolute error:
     *    IEEE       0,30      100000       2.2e-5      9.6e-7
     *    IEEE       0,100      10000       6.5e-5      3.7e-6
     *
     * Larger errors may occur for extreme ratios of a and b.
     *
     * ACCURACY (double):
     * arithmetic   domain     # trials      peak         rms
     *    IEEE      0,5         10000       6.9e-15     4.5e-16
     *    IEEE      0,85       250000       2.2e-13     1.7e-14
     *    IEEE      0,1000      30000       5.3e-12     6.3e-13
     *    IEEE      0,10000    250000       9.3e-11     7.1e-12
     *    IEEE      0,100000    10000       8.7e-10     4.8e-11
     * Outputs smaller than the IEEE gradual underflow threshold
     * were excluded from these statistics.
     *
     * ERROR MESSAGES:
     *   message         condition      value returned
     * incbet domain      x<0, x>1          nan
     * incbet underflow                     nan
     */

    EIGEN_STATIC_ASSERT((internal::is_same<Scalar, Scalar>::value == false),
                        THIS_TYPE_IS_NOT_SUPPORTED);
    return Scalar(0);
  }
};

/* Continued fraction expansion #1 for incomplete beta integral (small_branch = True)
 * Continued fraction expansion #2 for incomplete beta integral (small_branch = False)
 */
template <typename Scalar>
struct incbeta_cfe {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(Scalar a, Scalar b, Scalar x, bool small_branch) {
    EIGEN_STATIC_ASSERT((internal::is_same<Scalar, float>::value ||
                         internal::is_same<Scalar, double>::value),
                        THIS_TYPE_IS_NOT_SUPPORTED);
    const Scalar big = cephes_helper<Scalar>::big();
    const Scalar machep = cephes_helper<Scalar>::machep();
    const Scalar biginv = cephes_helper<Scalar>::biginv();

    const Scalar zero = 0;
    const Scalar one = 1;
    const Scalar two = 2;

    Scalar xk, pk, pkm1, pkm2, qk, qkm1, qkm2;
    Scalar k1, k2, k3, k4, k5, k6, k7, k8, k26update;
    Scalar ans;
    int n;

    const int num_iters = (internal::is_same<Scalar, float>::value) ? 100 : 300;
    const Scalar thresh =
        (internal::is_same<Scalar, float>::value) ? machep : Scalar(3) * machep;
    Scalar r = (internal::is_same<Scalar, float>::value) ? zero : one;

    if (small_branch) {
      k1 = a;
      k2 = a + b;
      k3 = a;
      k4 = a + one;
      k5 = one;
      k6 = b - one;
      k7 = k4;
      k8 = a + two;
      k26update = one;
    } else {
      k1 = a;
      k2 = b - one;
      k3 = a;
      k4 = a + one;
      k5 = one;
      k6 = a + b;
      k7 = a + one;
      k8 = a + two;
      k26update = -one;
      x = x / (one - x);
    }

    pkm2 = zero;
    qkm2 = one;
    pkm1 = one;
    qkm1 = one;
    ans = one;
    n = 0;

    do {
      xk = -(x * k1 * k2) / (k3 * k4);
      pk = pkm1 + pkm2 * xk;
      qk = qkm1 + qkm2 * xk;
      pkm2 = pkm1;
      pkm1 = pk;
      qkm2 = qkm1;
      qkm1 = qk;

      xk = (x * k5 * k6) / (k7 * k8);
      pk = pkm1 + pkm2 * xk;
      qk = qkm1 + qkm2 * xk;
      pkm2 = pkm1;
      pkm1 = pk;
      qkm2 = qkm1;
      qkm1 = qk;

      if (qk != zero) {
        r = pk / qk;
        if (numext::abs(ans - r) < numext::abs(r) * thresh) {
          return r;
        }
        ans = r;
      }

      k1 += one;
      k2 += k26update;
      k3 += two;
      k4 += two;
      k5 += one;
      k6 -= k26update;
      k7 += two;
      k8 += two;

      if ((numext::abs(qk) + numext::abs(pk)) > big) {
        pkm2 *= biginv;
        pkm1 *= biginv;
        qkm2 *= biginv;
        qkm1 *= biginv;
      }
      if ((numext::abs(qk) < biginv) || (numext::abs(pk) < biginv)) {
        pkm2 *= big;
        pkm1 *= big;
        qkm2 *= big;
        qkm1 *= big;
      }
    } while (++n < num_iters);

    return ans;
  }
};

/* Helper functions depending on the Scalar type */
template <typename Scalar>
struct betainc_helper {};

template <>
struct betainc_helper<float> {
  /* Core implementation, assumes a large (> 1.0) */
  EIGEN_DEVICE_FUNC static EIGEN_STRONG_INLINE float incbsa(float aa, float bb,
                                                            float xx) {
    float ans, a, b, t, x, onemx;
    bool reversed_a_b = false;

    onemx = 1.0f - xx;

    /* see if x is greater than the mean */
    if (xx > (aa / (aa + bb))) {
      reversed_a_b = true;
      a = bb;
      b = aa;
      t = xx;
      x = onemx;
    } else {
      a = aa;
      b = bb;
      t = onemx;
      x = xx;
    }

    /* Choose expansion for optimal convergence */
    if (b > 10.0f) {
      if (numext::abs(b * x / a) < 0.3f) {
        t = betainc_helper<float>::incbps(a, b, x);
        if (reversed_a_b) t = 1.0f - t;
        return t;
      }
    }

    ans = x * (a + b - 2.0f) / (a - 1.0f);
    if (ans < 1.0f) {
      ans = incbeta_cfe<float>::run(a, b, x, true /* small_branch */);
      t = b * numext::log(t);
    } else {
      ans = incbeta_cfe<float>::run(a, b, x, false /* small_branch */);
      t = (b - 1.0f) * numext::log(t);
    }

    t += a * numext::log(x) + lgamma_impl<float>::run(a + b) -
         lgamma_impl<float>::run(a) - lgamma_impl<float>::run(b);
    t += numext::log(ans / a);
    t = numext::exp(t);

    if (reversed_a_b) t = 1.0f - t;
    return t;
  }

  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE float incbps(float a, float b, float x) {
    float t, u, y, s;
    const float machep = cephes_helper<float>::machep();

    y = a * numext::log(x) + (b - 1.0f) * numext::log1p(-x) - numext::log(a);
    y -= lgamma_impl<float>::run(a) + lgamma_impl<float>::run(b);
    y += lgamma_impl<float>::run(a + b);

    t = x / (1.0f - x);
    s = 0.0f;
    u = 1.0f;
    do {
      b -= 1.0f;
      if (b == 0.0f) {
        break;
      }
      a += 1.0f;
      u *= t * b / a;
      s += u;
    } while (numext::abs(u) > machep);

    return numext::exp(y) * (1.0f + s);
  }
};

template <>
struct betainc_impl<float> {
  EIGEN_DEVICE_FUNC
  static float run(float a, float b, float x) {
    const float nan = NumTraits<float>::quiet_NaN();
    float ans, t;

    if (a <= 0.0f) return nan;
    if (b <= 0.0f) return nan;
    if ((x <= 0.0f) || (x >= 1.0f)) {
      if (x == 0.0f) return 0.0f;
      if (x == 1.0f) return 1.0f;
      // mtherr("betaincf", DOMAIN);
      return nan;
    }

    /* transformation for small aa */
    if (a <= 1.0f) {
      ans = betainc_helper<float>::incbsa(a + 1.0f, b, x);
      t = a * numext::log(x) + b * numext::log1p(-x) +
          lgamma_impl<float>::run(a + b) - lgamma_impl<float>::run(a + 1.0f) -
          lgamma_impl<float>::run(b);
      return (ans + numext::exp(t));
    } else {
      return betainc_helper<float>::incbsa(a, b, x);
    }
  }
};

template <>
struct betainc_helper<double> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE double incbps(double a, double b, double x) {
    const double machep = cephes_helper<double>::machep();

    double s, t, u, v, n, t1, z, ai;

    ai = 1.0 / a;
    u = (1.0 - b) * x;
    v = u / (a + 1.0);
    t1 = v;
    t = u;
    n = 2.0;
    s = 0.0;
    z = machep * ai;
    while (numext::abs(v) > z) {
      u = (n - b) * x / n;
      t *= u;
      v = t / (a + n);
      s += v;
      n += 1.0;
    }
    s += t1;
    s += ai;

    u = a * numext::log(x);
    // TODO: gamma() is not directly implemented in Eigen.
    /*
    if ((a + b) < maxgam && numext::abs(u) < maxlog) {
      t = gamma(a + b) / (gamma(a) * gamma(b));
      s = s * t * pow(x, a);
    } else {
    */
    t = lgamma_impl<double>::run(a + b) - lgamma_impl<double>::run(a) -
        lgamma_impl<double>::run(b) + u + numext::log(s);
    return s = numext::exp(t);
  }
};

template <>
struct betainc_impl<double> {
  EIGEN_DEVICE_FUNC
  static double run(double aa, double bb, double xx) {
    const double nan = NumTraits<double>::quiet_NaN();
    const double machep = cephes_helper<double>::machep();
    // const double maxgam = 171.624376956302725;

    double a, b, t, x, xc, w, y;
    bool reversed_a_b = false;

    if (aa <= 0.0 || bb <= 0.0) {
      return nan;  // goto domerr;
    }

    if ((xx <= 0.0) || (xx >= 1.0)) {
      if (xx == 0.0) return (0.0);
      if (xx == 1.0) return (1.0);
      // mtherr("incbet", DOMAIN);
      return nan;
    }

    if ((bb * xx) <= 1.0 && xx <= 0.95) {
      return betainc_helper<double>::incbps(aa, bb, xx);
    }

    w = 1.0 - xx;

    /* Reverse a and b if x is greater than the mean. */
    if (xx > (aa / (aa + bb))) {
      reversed_a_b = true;
      a = bb;
      b = aa;
      xc = xx;
      x = w;
    } else {
      a = aa;
      b = bb;
      xc = w;
      x = xx;
    }

    if (reversed_a_b && (b * x) <= 1.0 && x <= 0.95) {
      t = betainc_helper<double>::incbps(a, b, x);
      if (t <= machep) {
        t = 1.0 - machep;
      } else {
        t = 1.0 - t;
      }
      return t;
    }

    /* Choose expansion for better convergence. */
    y = x * (a + b - 2.0) - (a - 1.0);
    if (y < 0.0) {
      w = incbeta_cfe<double>::run(a, b, x, true /* small_branch */);
    } else {
      w = incbeta_cfe<double>::run(a, b, x, false /* small_branch */) / xc;
    }

    /* Multiply w by the factor
         a      b   _             _     _
        x  (1-x)   | (a+b) / ( a | (a) | (b) ) .   */

    y = a * numext::log(x);
    t = b * numext::log(xc);
    // TODO: gamma is not directly implemented in Eigen.
    /*
    if ((a + b) < maxgam && numext::abs(y) < maxlog && numext::abs(t) < maxlog)
    {
      t = pow(xc, b);
      t *= pow(x, a);
      t /= a;
      t *= w;
      t *= gamma(a + b) / (gamma(a) * gamma(b));
    } else {
    */
    /* Resort to logarithms.  */
    y += t + lgamma_impl<double>::run(a + b) - lgamma_impl<double>::run(a) -
         lgamma_impl<double>::run(b);
    y += numext::log(w / a);
    t = numext::exp(y);

    /* } */
    // done:

    if (reversed_a_b) {
      if (t <= machep) {
        t = 1.0 - machep;
      } else {
        t = 1.0 - t;
      }
    }
    return t;
  }
};

#endif  // EIGEN_HAS_C99_MATH

/****************************************************************************
 * Implementation of Bessel function, based on Cephes                       *
 ****************************************************************************/

template <typename Scalar>
struct i0e_retval {
  typedef Scalar type;
};

template <typename Scalar>
struct i0e_impl {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(const Scalar) {
    EIGEN_STATIC_ASSERT((internal::is_same<Scalar, Scalar>::value == false),
                        THIS_TYPE_IS_NOT_SUPPORTED);
    return Scalar(0);
  }
};

template <>
struct i0e_impl<float> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE float run(float x) {
    /*  i0ef.c
     *
     *  Modified Bessel function of order zero,
     *  exponentially scaled
     *
     *
     *
     * SYNOPSIS:
     *
     * float x, y, i0ef();
     *
     * y = i0ef( x );
     *
     *
     *
     * DESCRIPTION:
     *
     * Returns exponentially scaled modified Bessel function
     * of order zero of the argument.
     *
     * The function is defined as i0e(x) = exp(-|x|) j0( ix ).
     *
     *
     *
     * ACCURACY:
     *
     *                      Relative error:
     * arithmetic   domain     # trials      peak         rms
     *    IEEE      0,30        100000      3.7e-7      7.0e-8
     * See i0f().
     *
     */
    const float A[] = {-1.30002500998624804212E-8f, 6.04699502254191894932E-8f,
                       -2.67079385394061173391E-7f, 1.11738753912010371815E-6f,
                       -4.41673835845875056359E-6f, 1.64484480707288970893E-5f,
                       -5.75419501008210370398E-5f, 1.88502885095841655729E-4f,
                       -5.76375574538582365885E-4f, 1.63947561694133579842E-3f,
                       -4.32430999505057594430E-3f, 1.05464603945949983183E-2f,
                       -2.37374148058994688156E-2f, 4.93052842396707084878E-2f,
                       -9.49010970480476444210E-2f, 1.71620901522208775349E-1f,
                       -3.04682672343198398683E-1f, 6.76795274409476084995E-1f};

    const float B[] = {3.39623202570838634515E-9f, 2.26666899049817806459E-8f,
                       2.04891858946906374183E-7f, 2.89137052083475648297E-6f,
                       6.88975834691682398426E-5f, 3.36911647825569408990E-3f,
                       8.04490411014108831608E-1f};
    if (x < 0.0f) {
      x = -x;
    }

    if (x <= 8.0f) {
      float y = 0.5f * x - 2.0f;
      return cephes::chebevl<float, 18>::run(y, A);
    }

    return cephes::chebevl<float, 7>::run(32.0f / x - 2.0f, B) / numext::sqrt(x);
  }
};

template <>
struct i0e_impl<double> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE double run(double x) {
    /*  i0e.c
     *
     *  Modified Bessel function of order zero,
     *  exponentially scaled
     *
     *
     *
     * SYNOPSIS:
     *
     * double x, y, i0e();
     *
     * y = i0e( x );
     *
     *
     *
     * DESCRIPTION:
     *
     * Returns exponentially scaled modified Bessel function
     * of order zero of the argument.
     *
     * The function is defined as i0e(x) = exp(-|x|) j0( ix ).
     *
     *
     *
     * ACCURACY:
     *
     *                      Relative error:
     * arithmetic   domain     # trials      peak         rms
     *    IEEE      0,30        30000       5.4e-16     1.2e-16
     * See i0().
     *
     */
    const double A[] = {-4.41534164647933937950E-18, 3.33079451882223809783E-17,
                        -2.43127984654795469359E-16, 1.71539128555513303061E-15,
                        -1.16853328779934516808E-14, 7.67618549860493561688E-14,
                        -4.85644678311192946090E-13, 2.95505266312963983461E-12,
                        -1.72682629144155570723E-11, 9.67580903537323691224E-11,
                        -5.18979560163526290666E-10, 2.65982372468238665035E-9,
                        -1.30002500998624804212E-8,  6.04699502254191894932E-8,
                        -2.67079385394061173391E-7,  1.11738753912010371815E-6,
                        -4.41673835845875056359E-6,  1.64484480707288970893E-5,
                        -5.75419501008210370398E-5,  1.88502885095841655729E-4,
                        -5.76375574538582365885E-4,  1.63947561694133579842E-3,
                        -4.32430999505057594430E-3,  1.05464603945949983183E-2,
                        -2.37374148058994688156E-2,  4.93052842396707084878E-2,
                        -9.49010970480476444210E-2,  1.71620901522208775349E-1,
                        -3.04682672343198398683E-1,  6.76795274409476084995E-1};
    const double B[] = {
        -7.23318048787475395456E-18, -4.83050448594418207126E-18,
        4.46562142029675999901E-17,  3.46122286769746109310E-17,
        -2.82762398051658348494E-16, -3.42548561967721913462E-16,
        1.77256013305652638360E-15,  3.81168066935262242075E-15,
        -9.55484669882830764870E-15, -4.15056934728722208663E-14,
        1.54008621752140982691E-14,  3.85277838274214270114E-13,
        7.18012445138366623367E-13,  -1.79417853150680611778E-12,
        -1.32158118404477131188E-11, -3.14991652796324136454E-11,
        1.18891471078464383424E-11,  4.94060238822496958910E-10,
        3.39623202570838634515E-9,   2.26666899049817806459E-8,
        2.04891858946906374183E-7,   2.89137052083475648297E-6,
        6.88975834691682398426E-5,   3.36911647825569408990E-3,
        8.04490411014108831608E-1};

    if (x < 0.0) {
      x = -x;
    }

    if (x <= 8.0) {
      double y = (x / 2.0) - 2.0;
      return cephes::chebevl<double, 30>::run(y, A);
    }

    return cephes::chebevl<double, 25>::run(32.0 / x - 2.0, B) /
           numext::sqrt(x);
  }
};

template <typename Scalar>
struct i1e_retval {
  typedef Scalar type;
};

template <typename Scalar>
struct i1e_impl {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE Scalar run(const Scalar) {
    EIGEN_STATIC_ASSERT((internal::is_same<Scalar, Scalar>::value == false),
                        THIS_TYPE_IS_NOT_SUPPORTED);
    return Scalar(0);
  }
};

template <>
struct i1e_impl<float> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE float run(float x) {
    /* i1ef.c
     *
     *  Modified Bessel function of order one,
     *  exponentially scaled
     *
     *
     *
     * SYNOPSIS:
     *
     * float x, y, i1ef();
     *
     * y = i1ef( x );
     *
     *
     *
     * DESCRIPTION:
     *
     * Returns exponentially scaled modified Bessel function
     * of order one of the argument.
     *
     * The function is defined as i1(x) = -i exp(-|x|) j1( ix ).
     *
     *
     *
     * ACCURACY:
     *
     *                      Relative error:
     * arithmetic   domain     # trials      peak         rms
     *    IEEE      0, 30       30000       1.5e-6      1.5e-7
     * See i1().
     *
     */
    const float A[] = {9.38153738649577178388E-9f, -4.44505912879632808065E-8f,
                       2.00329475355213526229E-7f, -8.56872026469545474066E-7f,
                       3.47025130813767847674E-6f, -1.32731636560394358279E-5f,
                       4.78156510755005422638E-5f, -1.61760815825896745588E-4f,
                       5.12285956168575772895E-4f, -1.51357245063125314899E-3f,
                       4.15642294431288815669E-3f, -1.05640848946261981558E-2f,
                       2.47264490306265168283E-2f, -5.29459812080949914269E-2f,
                       1.02643658689847095384E-1f, -1.76416518357834055153E-1f,
                       2.52587186443633654823E-1f};

    const float B[] = {-3.83538038596423702205E-9f, -2.63146884688951950684E-8f,
                       -2.51223623787020892529E-7f, -3.88256480887769039346E-6f,
                       -1.10588938762623716291E-4f, -9.76109749136146840777E-3f,
                       7.78576235018280120474E-1f};

    float z = numext::abs(x);

    if (z <= 8.0f) {
      float y = 0.5f * z - 2.0f;
      z = cephes::chebevl<float, 17>::run(y, A) * z;
    } else {
      z = cephes::chebevl<float, 7>::run(32.0f / z - 2.0f, B) / numext::sqrt(z);
    }

    if (x < 0.0f) {
      z = -z;
    }

    return z;
  }
};

template <>
struct i1e_impl<double> {
  EIGEN_DEVICE_FUNC
  static EIGEN_STRONG_INLINE double run(double x) {
    /*  i1e.c
     *
     *  Modified Bessel function of order one,
     *  exponentially scaled
     *
     *
     *
     * SYNOPSIS:
     *
     * double x, y, i1e();
     *
     * y = i1e( x );
     *
     *
     *
     * DESCRIPTION:
     *
     * Returns exponentially scaled modified Bessel function
     * of order one of the argument.
     *
     * The function is defined as i1(x) = -i exp(-|x|) j1( ix ).
     *
     *
     *
     * ACCURACY:
     *
     *                      Relative error:
     * arithmetic   domain     # trials      peak         rms
     *    IEEE      0, 30       30000       2.0e-15     2.0e-16
     * See i1().
     *
     */
    const double A[] = {2.77791411276104639959E-18, -2.11142121435816608115E-17,
                        1.55363195773620046921E-16, -1.10559694773538630805E-15,
                        7.60068429473540693410E-15, -5.04218550472791168711E-14,
                        3.22379336594557470981E-13, -1.98397439776494371520E-12,
                        1.17361862988909016308E-11, -6.66348972350202774223E-11,
                        3.62559028155211703701E-10, -1.88724975172282928790E-9,
                        9.38153738649577178388E-9,  -4.44505912879632808065E-8,
                        2.00329475355213526229E-7,  -8.56872026469545474066E-7,
                        3.47025130813767847674E-6,  -1.32731636560394358279E-5,
                        4.78156510755005422638E-5,  -1.61760815825896745588E-4,
                        5.12285956168575772895E-4,  -1.51357245063125314899E-3,
                        4.15642294431288815669E-3,  -1.05640848946261981558E-2,
                        2.47264490306265168283E-2,  -5.29459812080949914269E-2,
                        1.02643658689847095384E-1,  -1.76416518357834055153E-1,
                        2.52587186443633654823E-1};
    const double B[] = {
        7.51729631084210481353E-18,  4.41434832307170791151E-18,
        -4.65030536848935832153E-17, -3.20952592199342395980E-17,
        2.96262899764595013876E-16,  3.30820231092092828324E-16,
        -1.88035477551078244854E-15, -3.81440307243700780478E-15,
        1.04202769841288027642E-14,  4.27244001671195135429E-14,
        -2.10154184277266431302E-14, -4.08355111109219731823E-13,
        -7.19855177624590851209E-13, 2.03562854414708950722E-12,
        1.41258074366137813316E-11,  3.25260358301548823856E-11,
        -1.89749581235054123450E-11, -5.58974346219658380687E-10,
        -3.83538038596423702205E-9,  -2.63146884688951950684E-8,
        -2.51223623787020892529E-7,  -3.88256480887769039346E-6,
        -1.10588938762623716291E-4,  -9.76109749136146840777E-3,
        7.78576235018280120474E-1};

    double z = numext::abs(x);

    if (z <= 8.0) {
      double y = (z / 2.0) - 2.0;
      z = cephes::chebevl<double, 29>::run(y, A) * z;
    } else {
      z = cephes::chebevl<double, 25>::run(32.0 / z - 2.0, B) / numext::sqrt(z);
    }

    if (x < 0.0) {
      z = -z;
    }

    return z;
  }
};

}  // end namespace internal

namespace numext {

template <typename Scalar>
EIGEN_DEVICE_FUNC inline EIGEN_MATHFUNC_RETVAL(lgamma, Scalar)
    lgamma(const Scalar& x) {
  return EIGEN_MATHFUNC_IMPL(lgamma, Scalar)::run(x);
}

template <typename Scalar>
EIGEN_DEVICE_FUNC inline EIGEN_MATHFUNC_RETVAL(digamma, Scalar)
    digamma(const Scalar& x) {
  return EIGEN_MATHFUNC_IMPL(digamma, Scalar)::run(x);
}

template <typename Scalar>
EIGEN_DEVICE_FUNC inline EIGEN_MATHFUNC_RETVAL(zeta, Scalar)
zeta(const Scalar& x, const Scalar& q) {
    return EIGEN_MATHFUNC_IMPL(zeta, Scalar)::run(x, q);
}

template <typename Scalar>
EIGEN_DEVICE_FUNC inline EIGEN_MATHFUNC_RETVAL(polygamma, Scalar)
polygamma(const Scalar& n, const Scalar& x) {
    return EIGEN_MATHFUNC_IMPL(polygamma, Scalar)::run(n, x);
}

template <typename Scalar>
EIGEN_DEVICE_FUNC inline EIGEN_MATHFUNC_RETVAL(erf, Scalar)
    erf(const Scalar& x) {
  return EIGEN_MATHFUNC_IMPL(erf, Scalar)::run(x);
}

template <typename Scalar>
EIGEN_DEVICE_FUNC inline EIGEN_MATHFUNC_RETVAL(erfc, Scalar)
    erfc(const Scalar& x) {
  return EIGEN_MATHFUNC_IMPL(erfc, Scalar)::run(x);
}

template <typename Scalar>
EIGEN_DEVICE_FUNC inline EIGEN_MATHFUNC_RETVAL(igamma, Scalar)
    igamma(const Scalar& a, const Scalar& x) {
  return EIGEN_MATHFUNC_IMPL(igamma, Scalar)::run(a, x);
}

template <typename Scalar>
EIGEN_DEVICE_FUNC inline EIGEN_MATHFUNC_RETVAL(igamma_der_a, Scalar)
    igamma_der_a(const Scalar& a, const Scalar& x) {
  return EIGEN_MATHFUNC_IMPL(igamma_der_a, Scalar)::run(a, x);
}

template <typename Scalar>
EIGEN_DEVICE_FUNC inline EIGEN_MATHFUNC_RETVAL(gamma_sample_der_alpha, Scalar)
    gamma_sample_der_alpha(const Scalar& a, const Scalar& x) {
  return EIGEN_MATHFUNC_IMPL(gamma_sample_der_alpha, Scalar)::run(a, x);
}

template <typename Scalar>
EIGEN_DEVICE_FUNC inline EIGEN_MATHFUNC_RETVAL(igammac, Scalar)
    igammac(const Scalar& a, const Scalar& x) {
  return EIGEN_MATHFUNC_IMPL(igammac, Scalar)::run(a, x);
}

template <typename Scalar>
EIGEN_DEVICE_FUNC inline EIGEN_MATHFUNC_RETVAL(betainc, Scalar)
    betainc(const Scalar& a, const Scalar& b, const Scalar& x) {
  return EIGEN_MATHFUNC_IMPL(betainc, Scalar)::run(a, b, x);
}

template <typename Scalar>
EIGEN_DEVICE_FUNC inline EIGEN_MATHFUNC_RETVAL(i0e, Scalar)
    i0e(const Scalar& x) {
  return EIGEN_MATHFUNC_IMPL(i0e, Scalar)::run(x);
}

template <typename Scalar>
EIGEN_DEVICE_FUNC inline EIGEN_MATHFUNC_RETVAL(i1e, Scalar)
    i1e(const Scalar& x) {
  return EIGEN_MATHFUNC_IMPL(i1e, Scalar)::run(x);
}

}  // end namespace numext


}  // end namespace Eigen

#endif  // EIGEN_SPECIAL_FUNCTIONS_H
