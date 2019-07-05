// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2007 Julien Pommier
// Copyright (C) 2014 Pedro Gonnet (pedro.gonnet@gmail.com)
// Copyright (C) 2009-2018 Gael Guennebaud <gael.guennebaud@inria.fr>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

/* The exp and log functions of this file initially come from
 * Julien Pommier's sse math library: http://gruntthepeon.free.fr/ssemath/
 */

namespace Eigen {
namespace internal {

template<typename Packet> EIGEN_STRONG_INLINE Packet
pfrexp_float(const Packet& a, Packet& exponent) {
  typedef typename unpacket_traits<Packet>::integer_packet PacketI;
  const Packet cst_126f = pset1<Packet>(126.0f);
  const Packet cst_half = pset1<Packet>(0.5f);
  const Packet cst_inv_mant_mask  = pset1frombits<Packet>(~0x7f800000u);
  exponent = psub(pcast<PacketI,Packet>(pshiftright<23>(preinterpret<PacketI>(a))), cst_126f);
  return por(pand(a, cst_inv_mant_mask), cst_half);
}

template<typename Packet> EIGEN_STRONG_INLINE Packet
pldexp_float(Packet a, Packet exponent)
{
  typedef typename unpacket_traits<Packet>::integer_packet PacketI;
  const Packet cst_127 = pset1<Packet>(127.f);
  // return a * 2^exponent
  PacketI ei = pcast<Packet,PacketI>(padd(exponent, cst_127));
  return pmul(a, preinterpret<Packet>(pshiftleft<23>(ei)));
}

// Natural logarithm
// Computes log(x) as log(2^e * m) = C*e + log(m), where the constant C =log(2)
// and m is in the range [sqrt(1/2),sqrt(2)). In this range, the logarithm can
// be easily approximated by a polynomial centered on m=1 for stability.
// TODO(gonnet): Further reduce the interval allowing for lower-degree
//               polynomial interpolants -> ... -> profit!
template <typename Packet>
EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS
EIGEN_UNUSED
Packet plog_float(const Packet _x)
{
  Packet x = _x;

  const Packet cst_1              = pset1<Packet>(1.0f);
  const Packet cst_half           = pset1<Packet>(0.5f);
  // The smallest non denormalized float number.
  const Packet cst_min_norm_pos   = pset1frombits<Packet>( 0x00800000u);
  const Packet cst_minus_inf      = pset1frombits<Packet>( 0xff800000u);

  // Polynomial coefficients.
  const Packet cst_cephes_SQRTHF = pset1<Packet>(0.707106781186547524f);
  const Packet cst_cephes_log_p0 = pset1<Packet>(7.0376836292E-2f);
  const Packet cst_cephes_log_p1 = pset1<Packet>(-1.1514610310E-1f);
  const Packet cst_cephes_log_p2 = pset1<Packet>(1.1676998740E-1f);
  const Packet cst_cephes_log_p3 = pset1<Packet>(-1.2420140846E-1f);
  const Packet cst_cephes_log_p4 = pset1<Packet>(+1.4249322787E-1f);
  const Packet cst_cephes_log_p5 = pset1<Packet>(-1.6668057665E-1f);
  const Packet cst_cephes_log_p6 = pset1<Packet>(+2.0000714765E-1f);
  const Packet cst_cephes_log_p7 = pset1<Packet>(-2.4999993993E-1f);
  const Packet cst_cephes_log_p8 = pset1<Packet>(+3.3333331174E-1f);
  const Packet cst_cephes_log_q1 = pset1<Packet>(-2.12194440e-4f);
  const Packet cst_cephes_log_q2 = pset1<Packet>(0.693359375f);

  Packet invalid_mask = pcmp_lt_or_nan(x, pzero(x));
  Packet iszero_mask  = pcmp_eq(x,pzero(x));

  // Truncate input values to the minimum positive normal.
  x = pmax(x, cst_min_norm_pos);

  Packet e;
  // extract significant in the range [0.5,1) and exponent
  x = pfrexp(x,e);

  // part2: Shift the inputs from the range [0.5,1) to [sqrt(1/2),sqrt(2))
  // and shift by -1. The values are then centered around 0, which improves
  // the stability of the polynomial evaluation.
  //   if( x < SQRTHF ) {
  //     e -= 1;
  //     x = x + x - 1.0;
  //   } else { x = x - 1.0; }
  Packet mask = pcmp_lt(x, cst_cephes_SQRTHF);
  Packet tmp = pand(x, mask);
  x = psub(x, cst_1);
  e = psub(e, pand(cst_1, mask));
  x = padd(x, tmp);

  Packet x2 = pmul(x, x);
  Packet x3 = pmul(x2, x);

  // Evaluate the polynomial approximant of degree 8 in three parts, probably
  // to improve instruction-level parallelism.
  Packet y, y1, y2;
  y  = pmadd(cst_cephes_log_p0, x, cst_cephes_log_p1);
  y1 = pmadd(cst_cephes_log_p3, x, cst_cephes_log_p4);
  y2 = pmadd(cst_cephes_log_p6, x, cst_cephes_log_p7);
  y  = pmadd(y, x, cst_cephes_log_p2);
  y1 = pmadd(y1, x, cst_cephes_log_p5);
  y2 = pmadd(y2, x, cst_cephes_log_p8);
  y  = pmadd(y, x3, y1);
  y  = pmadd(y, x3, y2);
  y  = pmul(y, x3);

  // Add the logarithm of the exponent back to the result of the interpolation.
  y1  = pmul(e, cst_cephes_log_q1);
  tmp = pmul(x2, cst_half);
  y   = padd(y, y1);
  x   = psub(x, tmp);
  y2  = pmul(e, cst_cephes_log_q2);
  x   = padd(x, y);
  x   = padd(x, y2);

  // Filter out invalid inputs, i.e. negative arg will be NAN, 0 will be -INF.
  return pselect(iszero_mask, cst_minus_inf, por(x, invalid_mask));
}

// Exponential function. Works by writing "x = m*log(2) + r" where
// "m = floor(x/log(2)+1/2)" and "r" is the remainder. The result is then
// "exp(x) = 2^m*exp(r)" where exp(r) is in the range [-1,1).
template <typename Packet>
EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS
EIGEN_UNUSED
Packet pexp_float(const Packet _x)
{
  const Packet cst_1      = pset1<Packet>(1.0f);
  const Packet cst_half   = pset1<Packet>(0.5f);
  const Packet cst_exp_hi = pset1<Packet>( 88.3762626647950f);
  const Packet cst_exp_lo = pset1<Packet>(-88.3762626647949f);

  const Packet cst_cephes_LOG2EF = pset1<Packet>(1.44269504088896341f);
  const Packet cst_cephes_exp_p0 = pset1<Packet>(1.9875691500E-4f);
  const Packet cst_cephes_exp_p1 = pset1<Packet>(1.3981999507E-3f);
  const Packet cst_cephes_exp_p2 = pset1<Packet>(8.3334519073E-3f);
  const Packet cst_cephes_exp_p3 = pset1<Packet>(4.1665795894E-2f);
  const Packet cst_cephes_exp_p4 = pset1<Packet>(1.6666665459E-1f);
  const Packet cst_cephes_exp_p5 = pset1<Packet>(5.0000001201E-1f);

  // Clamp x.
  Packet x = pmax(pmin(_x, cst_exp_hi), cst_exp_lo);

  // Express exp(x) as exp(m*ln(2) + r), start by extracting
  // m = floor(x/ln(2) + 0.5).
  Packet m = pfloor(pmadd(x, cst_cephes_LOG2EF, cst_half));

  // Get r = x - m*ln(2). If no FMA instructions are available, m*ln(2) is
  // subtracted out in two parts, m*C1+m*C2 = m*ln(2), to avoid accumulating
  // truncation errors.
  Packet r;
#ifdef EIGEN_HAS_SINGLE_INSTRUCTION_MADD
  const Packet cst_nln2 = pset1<Packet>(-0.6931471805599453f);
  r = pmadd(m, cst_nln2, x);
#else
  const Packet cst_cephes_exp_C1 = pset1<Packet>(0.693359375f);
  const Packet cst_cephes_exp_C2 = pset1<Packet>(-2.12194440e-4f);
  r = psub(x, pmul(m, cst_cephes_exp_C1));
  r = psub(r, pmul(m, cst_cephes_exp_C2));
#endif

  Packet r2 = pmul(r, r);

  // TODO(gonnet): Split into odd/even polynomials and try to exploit
  //               instruction-level parallelism.
  Packet y = cst_cephes_exp_p0;
  y = pmadd(y, r, cst_cephes_exp_p1);
  y = pmadd(y, r, cst_cephes_exp_p2);
  y = pmadd(y, r, cst_cephes_exp_p3);
  y = pmadd(y, r, cst_cephes_exp_p4);
  y = pmadd(y, r, cst_cephes_exp_p5);
  y = pmadd(y, r2, r);
  y = padd(y, cst_1);

  // Return 2^m * exp(r).
  return pmax(pldexp(y,m), _x);
}

template <typename Packet>
EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS
EIGEN_UNUSED
Packet pexp_double(const Packet _x)
{
  Packet x = _x;

  const Packet cst_1 = pset1<Packet>(1.0);
  const Packet cst_2 = pset1<Packet>(2.0);
  const Packet cst_half = pset1<Packet>(0.5);

  const Packet cst_exp_hi = pset1<Packet>(709.437);
  const Packet cst_exp_lo = pset1<Packet>(-709.436139303);

  const Packet cst_cephes_LOG2EF = pset1<Packet>(1.4426950408889634073599);
  const Packet cst_cephes_exp_p0 = pset1<Packet>(1.26177193074810590878e-4);
  const Packet cst_cephes_exp_p1 = pset1<Packet>(3.02994407707441961300e-2);
  const Packet cst_cephes_exp_p2 = pset1<Packet>(9.99999999999999999910e-1);
  const Packet cst_cephes_exp_q0 = pset1<Packet>(3.00198505138664455042e-6);
  const Packet cst_cephes_exp_q1 = pset1<Packet>(2.52448340349684104192e-3);
  const Packet cst_cephes_exp_q2 = pset1<Packet>(2.27265548208155028766e-1);
  const Packet cst_cephes_exp_q3 = pset1<Packet>(2.00000000000000000009e0);
  const Packet cst_cephes_exp_C1 = pset1<Packet>(0.693145751953125);
  const Packet cst_cephes_exp_C2 = pset1<Packet>(1.42860682030941723212e-6);

  Packet tmp, fx;

  // clamp x
  x = pmax(pmin(x, cst_exp_hi), cst_exp_lo);
  // Express exp(x) as exp(g + n*log(2)).
  fx = pmadd(cst_cephes_LOG2EF, x, cst_half);

  // Get the integer modulus of log(2), i.e. the "n" described above.
  fx = pfloor(fx);

  // Get the remainder modulo log(2), i.e. the "g" described above. Subtract
  // n*log(2) out in two steps, i.e. n*C1 + n*C2, C1+C2=log2 to get the last
  // digits right.
  tmp = pmul(fx, cst_cephes_exp_C1);
  Packet z = pmul(fx, cst_cephes_exp_C2);
  x = psub(x, tmp);
  x = psub(x, z);

  Packet x2 = pmul(x, x);

  // Evaluate the numerator polynomial of the rational interpolant.
  Packet px = cst_cephes_exp_p0;
  px = pmadd(px, x2, cst_cephes_exp_p1);
  px = pmadd(px, x2, cst_cephes_exp_p2);
  px = pmul(px, x);

  // Evaluate the denominator polynomial of the rational interpolant.
  Packet qx = cst_cephes_exp_q0;
  qx = pmadd(qx, x2, cst_cephes_exp_q1);
  qx = pmadd(qx, x2, cst_cephes_exp_q2);
  qx = pmadd(qx, x2, cst_cephes_exp_q3);

  // I don't really get this bit, copied from the SSE2 routines, so...
  // TODO(gonnet): Figure out what is going on here, perhaps find a better
  // rational interpolant?
  x = pdiv(px, psub(qx, px));
  x = pmadd(cst_2, x, cst_1);

  // Construct the result 2^n * exp(g) = e * x. The max is used to catch
  // non-finite values in the input.
  return pmax(pldexp(x,fx), _x);
}

/* The code is the rewriting of the cephes sinf/cosf functions.
   Precision is excellent as long as x < 8192 (I did not bother to
   take into account the special handling they have for greater values
   -- it does not return garbage for arguments over 8192, though, but
   the extra precision is missing).

   Note that it is such that sinf((float)M_PI) = 8.74e-8, which is the
   surprising but correct result.
*/

template<bool ComputeSine,typename Packet>
EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS
EIGEN_UNUSED
Packet psincos_float(const Packet& _x)
{
  typedef typename unpacket_traits<Packet>::integer_packet PacketI;
  const Packet cst_1          = pset1<Packet>(1.0f);
  const Packet cst_half       = pset1<Packet>(0.5f);

  const PacketI csti_1        = pset1<PacketI>(1);
  const PacketI csti_not1     = pset1<PacketI>(~1);
  const PacketI csti_2        = pset1<PacketI>(2);
  const PacketI csti_3        = pset1<PacketI>(3);

  const Packet cst_sign_mask  = pset1frombits<Packet>(0x80000000u);

  const Packet cst_minus_cephes_DP1 = pset1<Packet>(-0.78515625f);
  const Packet cst_minus_cephes_DP2 = pset1<Packet>(-2.4187564849853515625e-4f);
  const Packet cst_minus_cephes_DP3 = pset1<Packet>(-3.77489497744594108e-8f);
  const Packet cst_sincof_p0        = pset1<Packet>(-1.9515295891E-4f);
  const Packet cst_sincof_p1        = pset1<Packet>( 8.3321608736E-3f);
  const Packet cst_sincof_p2        = pset1<Packet>(-1.6666654611E-1f);
  const Packet cst_coscof_p0        = pset1<Packet>( 2.443315711809948E-005f);
  const Packet cst_coscof_p1        = pset1<Packet>(-1.388731625493765E-003f);
  const Packet cst_coscof_p2        = pset1<Packet>( 4.166664568298827E-002f);
  const Packet cst_cephes_FOPI      = pset1<Packet>( 1.27323954473516f); // 4 / M_PI

  Packet x = pabs(_x);

  // Scale x by 4/Pi to find x's octant.
  Packet y = pmul(x, cst_cephes_FOPI);

  // Get the octant. We'll reduce x by this number of octants or by one more than it.
  PacketI y_int = pcast<Packet,PacketI>(y);
  // x's from even-numbered octants will translate to octant 0: [0, +Pi/4].
  // x's from odd-numbered octants will translate to octant -1: [-Pi/4, 0].
  // Adjustment for odd-numbered octants: octant = (octant + 1) & (~1).
  PacketI y_int1 = pand(padd(y_int, csti_1), csti_not1); // could be pbitclear<0>(...)
  y = pcast<PacketI,Packet>(y_int1);

  // Compute the sign to apply to the polynomial.
  // sign = third_bit(y_int1) xor signbit(_x)
  Packet sign_bit = ComputeSine ? pxor(_x, preinterpret<Packet>(pshiftleft<29>(y_int1)))
                                : preinterpret<Packet>(pshiftleft<29>(padd(y_int1,csti_3)));
  sign_bit = pand(sign_bit, cst_sign_mask); // clear all but left most bit

  // Get the polynomial selection mask from the second bit of y_int1
  // We'll calculate both (sin and cos) polynomials and then select from the two.
  Packet poly_mask = preinterpret<Packet>(pcmp_eq(pand(y_int1, csti_2), pzero(y_int1)));

  // Reduce x by y octants to get: -Pi/4 <= x <= +Pi/4.
  // The magic pass: "Extended precision modular arithmetic"
  // x = ((x - y * DP1) - y * DP2) - y * DP3
  x = pmadd(y, cst_minus_cephes_DP1, x);
  x = pmadd(y, cst_minus_cephes_DP2, x);
  x = pmadd(y, cst_minus_cephes_DP3, x);

  Packet x2 = pmul(x,x);

  // Evaluate the cos(x) polynomial. (0 <= x <= Pi/4)
  Packet y1 = cst_coscof_p0;
  y1 = pmadd(y1, x2, cst_coscof_p1);
  y1 = pmadd(y1, x2, cst_coscof_p2);
  y1 = pmul(y1, x2);
  y1 = pmul(y1, x2);
  y1 = psub(y1, pmul(x2, cst_half));
  y1 = padd(y1, cst_1);

  // Evaluate the sin(x) polynomial. (Pi/4 <= x <= 0) 
  Packet y2 = cst_sincof_p0;
  y2 = pmadd(y2, x2, cst_sincof_p1);
  y2 = pmadd(y2, x2, cst_sincof_p2);
  y2 = pmul(y2, x2);
  y2 = pmadd(y2, x, x);

  // Select the correct result from the two polynoms.
  y = ComputeSine ? pselect(poly_mask,y2,y1)
                  : pselect(poly_mask,y1,y2);

  // Update the sign
  return pxor(y, sign_bit);
}

template<typename Packet>
EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS
EIGEN_UNUSED
Packet psin_float(const Packet& x)
{
  return psincos_float<true>(x);
}

template<typename Packet>
EIGEN_DEFINE_FUNCTION_ALLOWING_MULTIPLE_DEFINITIONS
EIGEN_UNUSED
Packet pcos_float(const Packet& x)
{
  return psincos_float<false>(x);
}

} // end namespace internal
} // end namespace Eigen
