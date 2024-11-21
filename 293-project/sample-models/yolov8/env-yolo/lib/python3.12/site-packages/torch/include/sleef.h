//   Copyright Naoki Shibata and contributors 2010 - 2023.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#ifndef __SLEEF_H__
#define __SLEEF_H__

#define SLEEF_VERSION_MAJOR 3
#define SLEEF_VERSION_MINOR 6
#define SLEEF_VERSION_PATCHLEVEL 0

#include <stddef.h>
#include <stdint.h>

#if defined (__GNUC__) || defined (__clang__) || defined(__INTEL_COMPILER)
#define SLEEF_CONST __attribute__((const))
#define SLEEF_INLINE __attribute__((always_inline))
#elif defined(_MSC_VER)
#define SLEEF_CONST
#define SLEEF_INLINE __forceinline
#endif

#if defined(__AVX2__) || defined(__aarch64__) || defined(__arm__) || defined(__powerpc64__) || defined(__zarch__)
#ifndef FP_FAST_FMA
#define FP_FAST_FMA
#endif
#ifndef FP_FAST_FMAF
#define FP_FAST_FMAF
#endif
#endif

#if defined(_MSC_VER) && !defined(__STDC__)
#define __STDC__ 1
#endif

#if (defined(__MINGW32__) || defined(__MINGW64__) || defined(__CYGWIN__) || defined(_MSC_VER)) && !defined(SLEEF_STATIC_LIBS)
#ifdef SLEEF_IMPORT_IS_EXPORT
#define SLEEF_IMPORT __declspec(dllexport)
#else // #ifdef SLEEF_IMPORT_IS_EXPORT
#define SLEEF_IMPORT __declspec(dllimport)
#if (defined(_MSC_VER))
#pragma comment(lib,"sleef.lib")
#endif // #if (defined(_MSC_VER))
#endif // #ifdef SLEEF_IMPORT_IS_EXPORT
#else // #if (defined(__MINGW32__) || defined(__MINGW64__) || defined(__CYGWIN__) || defined(_MSC_VER)) && !defined(SLEEF_STATIC_LIBS)
#define SLEEF_IMPORT
#endif // #if (defined(__MINGW32__) || defined(__MINGW64__) || defined(__CYGWIN__) || defined(_MSC_VER)) && !defined(SLEEF_STATIC_LIBS)

#if (defined(__GNUC__) || defined(__CLANG__)) && (defined(__i386__) || defined(__x86_64__))
#include <x86intrin.h>
#endif

#if (defined(_MSC_VER))
#include <intrin.h>
#endif

#if defined(__ARM_NEON__) || defined(__ARM_NEON)
#include <arm_neon.h>
#endif

#if defined(__ARM_FEATURE_SVE)
#include <arm_sve.h>
#endif

#if defined(__VSX__) && defined(__PPC64__) && defined(__LITTLE_ENDIAN__)
#include <altivec.h>
typedef __vector double       SLEEF_VECTOR_DOUBLE;
typedef __vector float        SLEEF_VECTOR_FLOAT;
typedef __vector int          SLEEF_VECTOR_INT;
typedef __vector unsigned int SLEEF_VECTOR_UINT;
typedef __vector long long SLEEF_VECTOR_LONGLONG;
typedef __vector unsigned long long SLEEF_VECTOR_ULONGLONG;
#endif

#if defined(__VX__) && defined(__VEC__)
#ifndef SLEEF_VECINTRIN_H_INCLUDED
#include <vecintrin.h>
#define SLEEF_VECINTRIN_H_INCLUDED
#endif
typedef __vector double       SLEEF_VECTOR_DOUBLE;
typedef __vector float        SLEEF_VECTOR_FLOAT;
typedef __vector int          SLEEF_VECTOR_INT;
typedef __vector unsigned int SLEEF_VECTOR_UINT;
typedef __vector long long SLEEF_VECTOR_LONGLONG;
typedef __vector unsigned long long SLEEF_VECTOR_ULONGLONG;
#endif

//

#if defined(SLEEF_ENABLE_OMP_SIMD) && (defined(__GNUC__) || defined(__CLANG__)) && !defined(__INTEL_COMPILER)
#if defined(__aarch64__)
//#define SLEEF_PRAGMA_OMP_SIMD_DP _Pragma ("omp declare simd simdlen(2) notinbranch")
//#define SLEEF_PRAGMA_OMP_SIMD_SP _Pragma ("omp declare simd simdlen(4) notinbranch")
//#elif defined(__x86_64__) && defined(__AVX512F__)
//#define SLEEF_PRAGMA_OMP_SIMD_DP _Pragma ("omp declare simd simdlen(8) notinbranch")
//#define SLEEF_PRAGMA_OMP_SIMD_SP _Pragma ("omp declare simd simdlen(16) notinbranch")
#elif defined(__x86_64__) && defined(__AVX__)
#define SLEEF_PRAGMA_OMP_SIMD_DP _Pragma ("omp declare simd simdlen(4) notinbranch")
#define SLEEF_PRAGMA_OMP_SIMD_SP _Pragma ("omp declare simd simdlen(8) notinbranch")
#elif defined(__x86_64__) && defined(__SSE2__)
#define SLEEF_PRAGMA_OMP_SIMD_DP _Pragma ("omp declare simd simdlen(2) notinbranch")
#define SLEEF_PRAGMA_OMP_SIMD_SP _Pragma ("omp declare simd simdlen(4) notinbranch")
#endif
#endif

#ifndef SLEEF_PRAGMA_OMP_SIMD_DP
#define SLEEF_PRAGMA_OMP_SIMD_DP
#define SLEEF_PRAGMA_OMP_SIMD_SP
#endif

//

#ifndef SLEEF_FP_ILOGB0
#define SLEEF_FP_ILOGB0 ((int)0x80000000)
#endif

#ifndef SLEEF_FP_ILOGBNAN
#define SLEEF_FP_ILOGBNAN ((int)2147483647)
#endif

//

SLEEF_IMPORT void *Sleef_malloc(size_t z);
SLEEF_IMPORT void Sleef_free(void *ptr);
SLEEF_IMPORT uint64_t Sleef_currentTimeMicros();

#if defined(__i386__) || defined(__x86_64__) || defined(_MSC_VER)
SLEEF_IMPORT void Sleef_x86CpuID(int32_t out[4], uint32_t eax, uint32_t ecx);
#endif

//

#if defined(__riscv_v)
#include <riscv_vector.h>
typedef vfloat64m2_t Sleef_vfloat64m1_t_2;
typedef vfloat32m2_t Sleef_vfloat32m1_t_2;
typedef vfloat64m4_t Sleef_vfloat64m2_t_2;
typedef vfloat32m4_t Sleef_vfloat32m2_t_2;
#define Sleef_vfloat64m1_t_2_DEFINED
#define Sleef_vfloat32m1_t_2_DEFINED
#define Sleef_vfloat64m2_t_2_DEFINED
#define Sleef_vfloat32m2_t_2_DEFINED
#endif

#ifndef Sleef_double2_DEFINED
#define Sleef_double2_DEFINED
typedef struct {
  double x, y;
} Sleef_double2;
#endif

#ifndef Sleef_float2_DEFINED
#define Sleef_float2_DEFINED
typedef struct {
  float x, y;
} Sleef_float2;
#endif

#ifdef __cplusplus
extern "C"
{
#endif

SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sin_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_cos_u35(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double2 Sleef_sincos_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_tan_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_asin_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_acos_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_atan_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_atan2_u35(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_log_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_cbrt_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sin_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_cos_u10(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double2 Sleef_sincos_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_tan_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_asin_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_acos_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_atan_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_atan2_u10(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_log_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_cbrt_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_exp_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_pow_u10(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sinh_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_cosh_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_tanh_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sinh_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_cosh_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_tanh_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_asinh_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_acosh_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_atanh_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_exp2_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_exp10_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_exp2_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_exp10_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_expm1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_log10_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_log2_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_log2_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_log1p_u10(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double2 Sleef_sincospi_u05(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double2 Sleef_sincospi_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sinpi_u05(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_cospi_u05(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_ldexp(double, int);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST int Sleef_ilogb(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fma(double, double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sqrt(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sqrt_u05(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sqrt_u35(double);

SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_hypot_u05(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_hypot_u35(double, double);

SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fabs(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_copysign(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fmax(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fmin(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fdim(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_trunc(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_floor(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_ceil(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_round(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_rint(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_nextafter(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_frfrexp(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST int Sleef_expfrexp(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fmod(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_remainder(double, double);
SLEEF_IMPORT SLEEF_CONST Sleef_double2 Sleef_modf(double);

SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_lgamma_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_tgamma_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_erf_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_erfc_u15(double);

SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sinf_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_cosf_u35(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float2 Sleef_sincosf_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_tanf_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_asinf_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_acosf_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_atanf_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_atan2f_u35(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_logf_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_cbrtf_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sinf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_cosf_u10(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float2 Sleef_sincosf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fastsinf_u3500(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fastcosf_u3500(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_tanf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_asinf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_acosf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_atanf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_atan2f_u10(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_logf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_cbrtf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_expf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_powf_u10(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fastpowf_u3500(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sinhf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_coshf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_tanhf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sinhf_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_coshf_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_tanhf_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_asinhf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_acoshf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_atanhf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_exp2f_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_exp10f_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_exp2f_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_exp10f_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_expm1f_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_log10f_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_log2f_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_log2f_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_log1pf_u10(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float2 Sleef_sincospif_u05(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float2 Sleef_sincospif_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sinpif_u05(float d);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_cospif_u05(float d);
SLEEF_IMPORT SLEEF_CONST float Sleef_ldexpf(float, int);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST int Sleef_ilogbf(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fmaf(float, float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sqrtf(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sqrtf_u05(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sqrtf_u35(float);

SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_hypotf_u05(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_hypotf_u35(float, float);

SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fabsf(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_copysignf(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fmaxf(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fminf(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fdimf(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_truncf(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_floorf(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_ceilf(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_roundf(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_rintf(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_nextafterf(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_frfrexpf(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST int Sleef_expfrexpf(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fmodf(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_remainderf(float, float);
SLEEF_IMPORT SLEEF_CONST Sleef_float2 Sleef_modff(float);

SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_lgammaf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_tgammaf_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_erff_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_erfcf_u15(float);
#ifdef __ARM_NEON

#ifndef Sleef_float64x2_t_2_DEFINED
typedef struct {
  float64x2_t x, y;
} Sleef_float64x2_t_2;
#define Sleef_float64x2_t_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sind2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cosd2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_sincosd2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tand2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_asind2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_acosd2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atand2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atan2d2_u35(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_logd2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cbrtd2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sind2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cosd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_sincosd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tand2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_asind2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_acosd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atand2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atan2d2_u10(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_logd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cbrtd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_expd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_powd2_u10(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sinhd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_coshd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tanhd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sinhd2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_coshd2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tanhd2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fastsind2_u3500(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fastcosd2_u3500(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fastpowd2_u3500(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_asinhd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_acoshd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atanhd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_exp2d2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_exp2d2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_exp10d2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_exp10d2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_expm1d2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_log10d2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_log2d2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_log2d2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_log1pd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_sincospid2_u05(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_sincospid2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sinpid2_u05(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cospid2_u05(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_ldexpd2(float64x2_t, int32x2_t);
SLEEF_IMPORT SLEEF_CONST int32x2_t Sleef_ilogbd2(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fmad2(float64x2_t, float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sqrtd2(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sqrtd2_u05(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sqrtd2_u35(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_hypotd2_u05(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_hypotd2_u35(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fabsd2(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_copysignd2(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fmaxd2(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fmind2(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fdimd2(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_truncd2(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_floord2(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_ceild2(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_roundd2(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_rintd2(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_nextafterd2(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_frfrexpd2(float64x2_t);
SLEEF_IMPORT SLEEF_CONST int32x2_t Sleef_expfrexpd2(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fmodd2(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_remainderd2(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_modfd2(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_lgammad2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tgammad2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_erfd2_u10(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_erfcd2_u15(float64x2_t);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntd2(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrd2(int);

#ifndef Sleef_float32x4_t_2_DEFINED
typedef struct {
  float32x4_t x, y;
} Sleef_float32x4_t_2;
#define Sleef_float32x4_t_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinf4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cosf4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_sincosf4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tanf4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_asinf4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_acosf4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atanf4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atan2f4_u35(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_logf4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cbrtf4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cosf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_sincosf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tanf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_asinf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_acosf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atanf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atan2f4_u10(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_logf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cbrtf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_expf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_powf4_u10(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinhf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_coshf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tanhf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinhf4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_coshf4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tanhf4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fastsinf4_u3500(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fastcosf4_u3500(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fastpowf4_u3500(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_asinhf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_acoshf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atanhf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_exp2f4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_exp2f4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_exp10f4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_exp10f4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_expm1f4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_log10f4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_log2f4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_log2f4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_log1pf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_sincospif4_u05(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_sincospif4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinpif4_u05(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cospif4_u05(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fmaf4(float32x4_t, float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sqrtf4(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sqrtf4_u05(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sqrtf4_u35(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_hypotf4_u05(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_hypotf4_u35(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fabsf4(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_copysignf4(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fmaxf4(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fminf4(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fdimf4(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_truncf4(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_floorf4(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_ceilf4(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_roundf4(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_rintf4(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_nextafterf4(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_frfrexpf4(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fmodf4(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_remainderf4(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_modff4(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_lgammaf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tgammaf4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_erff4_u10(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_erfcf4_u15(float32x4_t);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntf4(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrf4(int);
#endif
#ifdef __ARM_NEON

#ifndef Sleef_float64x2_t_2_DEFINED
typedef struct {
  float64x2_t x, y;
} Sleef_float64x2_t_2;
#define Sleef_float64x2_t_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sind2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_sind2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cosd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_cosd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_sincosd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_finz_sincosd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tand2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_tand2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_asind2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_asind2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_acosd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_acosd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atand2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_atand2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atan2d2_u35advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_atan2d2_u35advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_logd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_logd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cbrtd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_cbrtd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sind2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_sind2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cosd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_cosd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_sincosd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_finz_sincosd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tand2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_tand2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_asind2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_asind2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_acosd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_acosd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atand2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_atand2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atan2d2_u10advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_atan2d2_u10advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_logd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_logd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cbrtd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_cbrtd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_expd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_expd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_powd2_u10advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_powd2_u10advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sinhd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_sinhd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_coshd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_coshd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tanhd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_tanhd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sinhd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_sinhd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_coshd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_coshd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tanhd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_tanhd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fastsind2_u3500advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_fastsind2_u3500advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fastcosd2_u3500advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_fastcosd2_u3500advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fastpowd2_u3500advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_fastpowd2_u3500advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_asinhd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_asinhd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_acoshd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_acoshd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atanhd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_atanhd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_exp2d2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_exp2d2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_exp2d2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_exp2d2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_exp10d2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_exp10d2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_exp10d2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_exp10d2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_expm1d2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_expm1d2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_log10d2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_log10d2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_log2d2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_log2d2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_log2d2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_log2d2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_log1pd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_log1pd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_sincospid2_u05advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_finz_sincospid2_u05advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_sincospid2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_finz_sincospid2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sinpid2_u05advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_sinpid2_u05advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cospid2_u05advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_cospid2_u05advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_ldexpd2_advsimd(float64x2_t, int32x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_ldexpd2_advsimd(float64x2_t, int32x2_t);
SLEEF_IMPORT SLEEF_CONST int32x2_t Sleef_ilogbd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST int32x2_t Sleef_finz_ilogbd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fmad2_advsimd(float64x2_t, float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_fmad2_advsimd(float64x2_t, float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sqrtd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_sqrtd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sqrtd2_u05advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_sqrtd2_u05advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sqrtd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_sqrtd2_u35advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_hypotd2_u05advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_hypotd2_u05advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_hypotd2_u35advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_hypotd2_u35advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fabsd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_fabsd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_copysignd2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_copysignd2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fmaxd2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_fmaxd2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fmind2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_fmind2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fdimd2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_fdimd2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_truncd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_truncd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_floord2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_floord2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_ceild2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_ceild2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_roundd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_roundd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_rintd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_rintd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_nextafterd2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_nextafterd2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_frfrexpd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_frfrexpd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST int32x2_t Sleef_expfrexpd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST int32x2_t Sleef_finz_expfrexpd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fmodd2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_fmodd2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_remainderd2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_remainderd2_advsimd(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_modfd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_finz_modfd2_advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_lgammad2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_lgammad2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tgammad2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_tgammad2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_erfd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_erfd2_u10advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_erfcd2_u15advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_finz_erfcd2_u15advsimd(float64x2_t);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntd2_advsimd(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrd2_advsimd(int);

#ifndef Sleef_float32x4_t_2_DEFINED
typedef struct {
  float32x4_t x, y;
} Sleef_float32x4_t_2;
#define Sleef_float32x4_t_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_sinf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cosf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_cosf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_sincosf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_finz_sincosf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tanf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_tanf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_asinf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_asinf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_acosf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_acosf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atanf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_atanf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atan2f4_u35advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_atan2f4_u35advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_logf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_logf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cbrtf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_cbrtf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_sinf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cosf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_cosf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_sincosf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_finz_sincosf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tanf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_tanf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_asinf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_asinf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_acosf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_acosf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atanf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_atanf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atan2f4_u10advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_atan2f4_u10advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_logf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_logf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cbrtf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_cbrtf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_expf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_expf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_powf4_u10advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_powf4_u10advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinhf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_sinhf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_coshf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_coshf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tanhf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_tanhf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinhf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_sinhf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_coshf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_coshf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tanhf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_tanhf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fastsinf4_u3500advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_fastsinf4_u3500advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fastcosf4_u3500advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_fastcosf4_u3500advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fastpowf4_u3500advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_fastpowf4_u3500advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_asinhf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_asinhf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_acoshf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_acoshf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atanhf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_atanhf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_exp2f4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_exp2f4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_exp2f4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_exp2f4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_exp10f4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_exp10f4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_exp10f4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_exp10f4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_expm1f4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_expm1f4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_log10f4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_log10f4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_log2f4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_log2f4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_log2f4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_log2f4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_log1pf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_log1pf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_sincospif4_u05advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_finz_sincospif4_u05advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_sincospif4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_finz_sincospif4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinpif4_u05advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_sinpif4_u05advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cospif4_u05advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_cospif4_u05advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fmaf4_advsimd(float32x4_t, float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_fmaf4_advsimd(float32x4_t, float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sqrtf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_sqrtf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sqrtf4_u05advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_sqrtf4_u05advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sqrtf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_sqrtf4_u35advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_hypotf4_u05advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_hypotf4_u05advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_hypotf4_u35advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_hypotf4_u35advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fabsf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_fabsf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_copysignf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_copysignf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fmaxf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_fmaxf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fminf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_fminf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fdimf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_fdimf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_truncf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_truncf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_floorf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_floorf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_ceilf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_ceilf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_roundf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_roundf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_rintf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_rintf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_nextafterf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_nextafterf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_frfrexpf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_frfrexpf4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fmodf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_fmodf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_remainderf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_remainderf4_advsimd(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_modff4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_finz_modff4_advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_lgammaf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_lgammaf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tgammaf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_tgammaf4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_erff4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_erff4_u10advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_erfcf4_u15advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_finz_erfcf4_u15advsimd(float32x4_t);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntf4_advsimd(int);
SLEEF_IMPORT SLEEF_CONST int Sleef_finz_getIntf4_advsimd(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrf4_advsimd(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_finz_getPtrf4_advsimd(int);
#endif
#ifdef __ARM_NEON

#ifndef Sleef_float64x2_t_2_DEFINED
typedef struct {
  float64x2_t x, y;
} Sleef_float64x2_t_2;
#define Sleef_float64x2_t_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sind2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_sind2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cosd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_cosd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_sincosd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_cinz_sincosd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tand2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_tand2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_asind2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_asind2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_acosd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_acosd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atand2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_atand2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atan2d2_u35advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_atan2d2_u35advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_logd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_logd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cbrtd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_cbrtd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sind2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_sind2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cosd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_cosd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_sincosd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_cinz_sincosd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tand2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_tand2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_asind2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_asind2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_acosd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_acosd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atand2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_atand2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atan2d2_u10advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_atan2d2_u10advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_logd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_logd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cbrtd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_cbrtd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_expd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_expd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_powd2_u10advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_powd2_u10advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sinhd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_sinhd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_coshd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_coshd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tanhd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_tanhd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sinhd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_sinhd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_coshd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_coshd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tanhd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_tanhd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fastsind2_u3500advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_fastsind2_u3500advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fastcosd2_u3500advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_fastcosd2_u3500advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fastpowd2_u3500advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_fastpowd2_u3500advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_asinhd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_asinhd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_acoshd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_acoshd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_atanhd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_atanhd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_exp2d2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_exp2d2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_exp2d2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_exp2d2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_exp10d2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_exp10d2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_exp10d2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_exp10d2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_expm1d2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_expm1d2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_log10d2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_log10d2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_log2d2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_log2d2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_log2d2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_log2d2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_log1pd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_log1pd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_sincospid2_u05advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_cinz_sincospid2_u05advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_sincospid2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_cinz_sincospid2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sinpid2_u05advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_sinpid2_u05advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cospid2_u05advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_cospid2_u05advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_ldexpd2_advsimdnofma(float64x2_t, int32x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_ldexpd2_advsimdnofma(float64x2_t, int32x2_t);
SLEEF_IMPORT SLEEF_CONST int32x2_t Sleef_ilogbd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST int32x2_t Sleef_cinz_ilogbd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fmad2_advsimdnofma(float64x2_t, float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_fmad2_advsimdnofma(float64x2_t, float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sqrtd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_sqrtd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sqrtd2_u05advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_sqrtd2_u05advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_sqrtd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_sqrtd2_u35advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_hypotd2_u05advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_hypotd2_u05advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_hypotd2_u35advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_hypotd2_u35advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fabsd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_fabsd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_copysignd2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_copysignd2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fmaxd2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_fmaxd2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fmind2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_fmind2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fdimd2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_fdimd2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_truncd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_truncd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_floord2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_floord2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_ceild2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_ceild2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_roundd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_roundd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_rintd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_rintd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_nextafterd2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_nextafterd2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_frfrexpd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_frfrexpd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST int32x2_t Sleef_expfrexpd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST int32x2_t Sleef_cinz_expfrexpd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_fmodd2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_fmodd2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_remainderd2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_remainderd2_advsimdnofma(float64x2_t, float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_modfd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float64x2_t_2 Sleef_cinz_modfd2_advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_lgammad2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_lgammad2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_tgammad2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_tgammad2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_erfd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_erfd2_u10advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_erfcd2_u15advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST float64x2_t Sleef_cinz_erfcd2_u15advsimdnofma(float64x2_t);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntd2_advsimdnofma(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrd2_advsimdnofma(int);

#ifndef Sleef_float32x4_t_2_DEFINED
typedef struct {
  float32x4_t x, y;
} Sleef_float32x4_t_2;
#define Sleef_float32x4_t_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_sinf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cosf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_cosf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_sincosf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_cinz_sincosf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tanf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_tanf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_asinf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_asinf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_acosf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_acosf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atanf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_atanf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atan2f4_u35advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_atan2f4_u35advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_logf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_logf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cbrtf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_cbrtf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_sinf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cosf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_cosf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_sincosf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_cinz_sincosf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tanf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_tanf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_asinf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_asinf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_acosf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_acosf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atanf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_atanf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atan2f4_u10advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_atan2f4_u10advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_logf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_logf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cbrtf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_cbrtf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_expf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_expf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_powf4_u10advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_powf4_u10advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinhf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_sinhf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_coshf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_coshf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tanhf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_tanhf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinhf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_sinhf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_coshf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_coshf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tanhf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_tanhf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fastsinf4_u3500advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_fastsinf4_u3500advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fastcosf4_u3500advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_fastcosf4_u3500advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fastpowf4_u3500advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_fastpowf4_u3500advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_asinhf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_asinhf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_acoshf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_acoshf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_atanhf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_atanhf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_exp2f4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_exp2f4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_exp2f4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_exp2f4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_exp10f4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_exp10f4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_exp10f4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_exp10f4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_expm1f4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_expm1f4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_log10f4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_log10f4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_log2f4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_log2f4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_log2f4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_log2f4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_log1pf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_log1pf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_sincospif4_u05advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_cinz_sincospif4_u05advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_sincospif4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_cinz_sincospif4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sinpif4_u05advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_sinpif4_u05advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cospif4_u05advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_cospif4_u05advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fmaf4_advsimdnofma(float32x4_t, float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_fmaf4_advsimdnofma(float32x4_t, float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sqrtf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_sqrtf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sqrtf4_u05advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_sqrtf4_u05advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_sqrtf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_sqrtf4_u35advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_hypotf4_u05advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_hypotf4_u05advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_hypotf4_u35advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_hypotf4_u35advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fabsf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_fabsf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_copysignf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_copysignf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fmaxf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_fmaxf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fminf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_fminf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fdimf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_fdimf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_truncf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_truncf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_floorf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_floorf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_ceilf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_ceilf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_roundf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_roundf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_rintf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_rintf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_nextafterf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_nextafterf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_frfrexpf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_frfrexpf4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_fmodf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_fmodf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_remainderf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_remainderf4_advsimdnofma(float32x4_t, float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_modff4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST Sleef_float32x4_t_2 Sleef_cinz_modff4_advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_lgammaf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_lgammaf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_tgammaf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_tgammaf4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_erff4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_erff4_u10advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_erfcf4_u15advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST float32x4_t Sleef_cinz_erfcf4_u15advsimdnofma(float32x4_t);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntf4_advsimdnofma(int);
SLEEF_IMPORT SLEEF_CONST int Sleef_cinz_getIntf4_advsimdnofma(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrf4_advsimdnofma(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_cinz_getPtrf4_advsimdnofma(int);
#endif
#ifdef __ARM_FEATURE_SVE

#ifndef Sleef_svfloat64_t_2_DEFINED
typedef svfloat64x2_t Sleef_svfloat64_t_2;
#define Sleef_svfloat64_t_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sindx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_sindx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cosdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_cosdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_sincosdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_finz_sincosdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_tandx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_tandx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_asindx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_asindx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_acosdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_acosdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_atandx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_atandx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_atan2dx_u35sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_atan2dx_u35sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_logdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_logdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cbrtdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_cbrtdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sindx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_sindx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cosdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_cosdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_sincosdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_finz_sincosdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_tandx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_tandx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_asindx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_asindx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_acosdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_acosdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_atandx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_atandx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_atan2dx_u10sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_atan2dx_u10sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_logdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_logdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cbrtdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_cbrtdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_expdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_expdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_powdx_u10sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_powdx_u10sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sinhdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_sinhdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_coshdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_coshdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_tanhdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_tanhdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sinhdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_sinhdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_coshdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_coshdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_tanhdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_tanhdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fastsindx_u3500sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_fastsindx_u3500sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fastcosdx_u3500sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_fastcosdx_u3500sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fastpowdx_u3500sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_fastpowdx_u3500sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_asinhdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_asinhdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_acoshdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_acoshdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_atanhdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_atanhdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_exp2dx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_exp2dx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_exp2dx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_exp2dx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_exp10dx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_exp10dx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_exp10dx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_exp10dx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_expm1dx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_expm1dx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_log10dx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_log10dx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_log2dx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_log2dx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_log2dx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_log2dx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_log1pdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_log1pdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_sincospidx_u05sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_finz_sincospidx_u05sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_sincospidx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_finz_sincospidx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sinpidx_u05sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_sinpidx_u05sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cospidx_u05sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_cospidx_u05sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_ldexpdx_sve(svfloat64_t, svint32_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_ldexpdx_sve(svfloat64_t, svint32_t);
SLEEF_IMPORT SLEEF_CONST svint32_t Sleef_ilogbdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svint32_t Sleef_finz_ilogbdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fmadx_sve(svfloat64_t, svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_fmadx_sve(svfloat64_t, svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sqrtdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_sqrtdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sqrtdx_u05sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_sqrtdx_u05sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sqrtdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_sqrtdx_u35sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_hypotdx_u05sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_hypotdx_u05sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_hypotdx_u35sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_hypotdx_u35sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fabsdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_fabsdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_copysigndx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_copysigndx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fmaxdx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_fmaxdx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fmindx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_fmindx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fdimdx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_fdimdx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_truncdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_truncdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_floordx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_floordx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_ceildx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_ceildx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_rounddx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_rounddx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_rintdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_rintdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_nextafterdx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_nextafterdx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_frfrexpdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_frfrexpdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svint32_t Sleef_expfrexpdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svint32_t Sleef_finz_expfrexpdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fmoddx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_fmoddx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_remainderdx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_remainderdx_sve(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_modfdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_finz_modfdx_sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_lgammadx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_lgammadx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_tgammadx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_tgammadx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_erfdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_erfdx_u10sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_erfcdx_u15sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_finz_erfcdx_u15sve(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntdx_sve(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrdx_sve(int);

#ifndef Sleef_svfloat32_t_2_DEFINED
typedef svfloat32x2_t Sleef_svfloat32_t_2;
#define Sleef_svfloat32_t_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sinfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_sinfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cosfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_cosfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_sincosfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_finz_sincosfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_tanfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_tanfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_asinfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_asinfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_acosfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_acosfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_atanfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_atanfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_atan2fx_u35sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_atan2fx_u35sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_logfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_logfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cbrtfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_cbrtfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sinfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_sinfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cosfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_cosfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_sincosfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_finz_sincosfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_tanfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_tanfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_asinfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_asinfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_acosfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_acosfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_atanfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_atanfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_atan2fx_u10sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_atan2fx_u10sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_logfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_logfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cbrtfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_cbrtfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_expfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_expfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_powfx_u10sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_powfx_u10sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sinhfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_sinhfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_coshfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_coshfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_tanhfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_tanhfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sinhfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_sinhfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_coshfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_coshfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_tanhfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_tanhfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fastsinfx_u3500sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_fastsinfx_u3500sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fastcosfx_u3500sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_fastcosfx_u3500sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fastpowfx_u3500sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_fastpowfx_u3500sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_asinhfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_asinhfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_acoshfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_acoshfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_atanhfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_atanhfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_exp2fx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_exp2fx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_exp2fx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_exp2fx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_exp10fx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_exp10fx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_exp10fx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_exp10fx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_expm1fx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_expm1fx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_log10fx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_log10fx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_log2fx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_log2fx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_log2fx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_log2fx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_log1pfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_log1pfx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_sincospifx_u05sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_finz_sincospifx_u05sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_sincospifx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_finz_sincospifx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sinpifx_u05sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_sinpifx_u05sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cospifx_u05sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_cospifx_u05sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fmafx_sve(svfloat32_t, svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_fmafx_sve(svfloat32_t, svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sqrtfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_sqrtfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sqrtfx_u05sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_sqrtfx_u05sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sqrtfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_sqrtfx_u35sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_hypotfx_u05sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_hypotfx_u05sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_hypotfx_u35sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_hypotfx_u35sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fabsfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_fabsfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_copysignfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_copysignfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fmaxfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_fmaxfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fminfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_fminfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fdimfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_fdimfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_truncfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_truncfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_floorfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_floorfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_ceilfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_ceilfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_roundfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_roundfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_rintfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_rintfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_nextafterfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_nextafterfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_frfrexpfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_frfrexpfx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fmodfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_fmodfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_remainderfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_remainderfx_sve(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_modffx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_finz_modffx_sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_lgammafx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_lgammafx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_tgammafx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_tgammafx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_erffx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_erffx_u10sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_erfcfx_u15sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_finz_erfcfx_u15sve(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntfx_sve(int);
SLEEF_IMPORT SLEEF_CONST int Sleef_finz_getIntfx_sve(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrfx_sve(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_finz_getPtrfx_sve(int);
#endif
#ifdef __ARM_FEATURE_SVE

#ifndef Sleef_svfloat64_t_2_DEFINED
typedef svfloat64x2_t Sleef_svfloat64_t_2;
#define Sleef_svfloat64_t_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sindx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_sindx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cosdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_cosdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_sincosdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_cinz_sincosdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_tandx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_tandx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_asindx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_asindx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_acosdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_acosdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_atandx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_atandx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_atan2dx_u35svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_atan2dx_u35svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_logdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_logdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cbrtdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_cbrtdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sindx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_sindx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cosdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_cosdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_sincosdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_cinz_sincosdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_tandx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_tandx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_asindx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_asindx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_acosdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_acosdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_atandx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_atandx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_atan2dx_u10svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_atan2dx_u10svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_logdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_logdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cbrtdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_cbrtdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_expdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_expdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_powdx_u10svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_powdx_u10svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sinhdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_sinhdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_coshdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_coshdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_tanhdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_tanhdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sinhdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_sinhdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_coshdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_coshdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_tanhdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_tanhdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fastsindx_u3500svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_fastsindx_u3500svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fastcosdx_u3500svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_fastcosdx_u3500svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fastpowdx_u3500svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_fastpowdx_u3500svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_asinhdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_asinhdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_acoshdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_acoshdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_atanhdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_atanhdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_exp2dx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_exp2dx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_exp2dx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_exp2dx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_exp10dx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_exp10dx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_exp10dx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_exp10dx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_expm1dx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_expm1dx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_log10dx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_log10dx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_log2dx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_log2dx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_log2dx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_log2dx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_log1pdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_log1pdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_sincospidx_u05svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_cinz_sincospidx_u05svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_sincospidx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_cinz_sincospidx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sinpidx_u05svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_sinpidx_u05svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cospidx_u05svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_cospidx_u05svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_ldexpdx_svenofma(svfloat64_t, svint32_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_ldexpdx_svenofma(svfloat64_t, svint32_t);
SLEEF_IMPORT SLEEF_CONST svint32_t Sleef_ilogbdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svint32_t Sleef_cinz_ilogbdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fmadx_svenofma(svfloat64_t, svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_fmadx_svenofma(svfloat64_t, svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sqrtdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_sqrtdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sqrtdx_u05svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_sqrtdx_u05svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_sqrtdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_sqrtdx_u35svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_hypotdx_u05svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_hypotdx_u05svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_hypotdx_u35svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_hypotdx_u35svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fabsdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_fabsdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_copysigndx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_copysigndx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fmaxdx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_fmaxdx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fmindx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_fmindx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fdimdx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_fdimdx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_truncdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_truncdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_floordx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_floordx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_ceildx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_ceildx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_rounddx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_rounddx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_rintdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_rintdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_nextafterdx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_nextafterdx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_frfrexpdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_frfrexpdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svint32_t Sleef_expfrexpdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svint32_t Sleef_cinz_expfrexpdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_fmoddx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_fmoddx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_remainderdx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_remainderdx_svenofma(svfloat64_t, svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_modfdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat64_t_2 Sleef_cinz_modfdx_svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_lgammadx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_lgammadx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_tgammadx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_tgammadx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_erfdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_erfdx_u10svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_erfcdx_u15svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST svfloat64_t Sleef_cinz_erfcdx_u15svenofma(svfloat64_t);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntdx_svenofma(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrdx_svenofma(int);

#ifndef Sleef_svfloat32_t_2_DEFINED
typedef svfloat32x2_t Sleef_svfloat32_t_2;
#define Sleef_svfloat32_t_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sinfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_sinfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cosfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_cosfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_sincosfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_cinz_sincosfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_tanfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_tanfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_asinfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_asinfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_acosfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_acosfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_atanfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_atanfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_atan2fx_u35svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_atan2fx_u35svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_logfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_logfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cbrtfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_cbrtfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sinfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_sinfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cosfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_cosfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_sincosfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_cinz_sincosfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_tanfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_tanfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_asinfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_asinfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_acosfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_acosfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_atanfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_atanfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_atan2fx_u10svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_atan2fx_u10svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_logfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_logfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cbrtfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_cbrtfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_expfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_expfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_powfx_u10svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_powfx_u10svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sinhfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_sinhfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_coshfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_coshfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_tanhfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_tanhfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sinhfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_sinhfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_coshfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_coshfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_tanhfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_tanhfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fastsinfx_u3500svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_fastsinfx_u3500svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fastcosfx_u3500svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_fastcosfx_u3500svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fastpowfx_u3500svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_fastpowfx_u3500svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_asinhfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_asinhfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_acoshfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_acoshfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_atanhfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_atanhfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_exp2fx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_exp2fx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_exp2fx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_exp2fx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_exp10fx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_exp10fx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_exp10fx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_exp10fx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_expm1fx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_expm1fx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_log10fx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_log10fx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_log2fx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_log2fx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_log2fx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_log2fx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_log1pfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_log1pfx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_sincospifx_u05svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_cinz_sincospifx_u05svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_sincospifx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_cinz_sincospifx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sinpifx_u05svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_sinpifx_u05svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cospifx_u05svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_cospifx_u05svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fmafx_svenofma(svfloat32_t, svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_fmafx_svenofma(svfloat32_t, svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sqrtfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_sqrtfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sqrtfx_u05svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_sqrtfx_u05svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_sqrtfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_sqrtfx_u35svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_hypotfx_u05svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_hypotfx_u05svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_hypotfx_u35svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_hypotfx_u35svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fabsfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_fabsfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_copysignfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_copysignfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fmaxfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_fmaxfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fminfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_fminfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fdimfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_fdimfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_truncfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_truncfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_floorfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_floorfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_ceilfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_ceilfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_roundfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_roundfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_rintfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_rintfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_nextafterfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_nextafterfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_frfrexpfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_frfrexpfx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_fmodfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_fmodfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_remainderfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_remainderfx_svenofma(svfloat32_t, svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_modffx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST Sleef_svfloat32_t_2 Sleef_cinz_modffx_svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_lgammafx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_lgammafx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_tgammafx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_tgammafx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_erffx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_erffx_u10svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_erfcfx_u15svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST svfloat32_t Sleef_cinz_erfcfx_u15svenofma(svfloat32_t);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntfx_svenofma(int);
SLEEF_IMPORT SLEEF_CONST int Sleef_cinz_getIntfx_svenofma(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrfx_svenofma(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_cinz_getPtrfx_svenofma(int);
#endif
#ifdef __STDC__

#ifndef Sleef_double_2_DEFINED
typedef Sleef_double2 Sleef_double_2;
#define Sleef_double_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST double Sleef_sind1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_sind1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cosd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_cosd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_sincosd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_cinz_sincosd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_tand1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_tand1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_asind1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_asind1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_acosd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_acosd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_atand1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_atand1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_atan2d1_u35purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_atan2d1_u35purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_logd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_logd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cbrtd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_cbrtd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sind1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_sind1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cosd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_cosd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_sincosd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_cinz_sincosd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_tand1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_tand1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_asind1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_asind1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_acosd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_acosd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_atand1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_atand1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_atan2d1_u10purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_atan2d1_u10purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_logd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_logd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cbrtd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_cbrtd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_expd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_expd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_powd1_u10purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_powd1_u10purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sinhd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_sinhd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_coshd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_coshd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_tanhd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_tanhd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sinhd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_sinhd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_coshd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_coshd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_tanhd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_tanhd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fastsind1_u3500purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_fastsind1_u3500purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fastcosd1_u3500purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_fastcosd1_u3500purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fastpowd1_u3500purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_fastpowd1_u3500purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_asinhd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_asinhd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_acoshd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_acoshd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_atanhd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_atanhd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_exp2d1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_exp2d1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_exp2d1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_exp2d1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_exp10d1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_exp10d1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_exp10d1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_exp10d1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_expm1d1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_expm1d1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_log10d1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_log10d1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_log2d1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_log2d1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_log2d1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_log2d1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_log1pd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_log1pd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_sincospid1_u05purec(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_cinz_sincospid1_u05purec(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_sincospid1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_cinz_sincospid1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sinpid1_u05purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_sinpid1_u05purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cospid1_u05purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_cospid1_u05purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_ldexpd1_purec(double, int32_t);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_ldexpd1_purec(double, int32_t);
SLEEF_IMPORT SLEEF_CONST int32_t Sleef_ilogbd1_purec(double);
SLEEF_IMPORT SLEEF_CONST int32_t Sleef_cinz_ilogbd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fmad1_purec(double, double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_fmad1_purec(double, double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sqrtd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_sqrtd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sqrtd1_u05purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_sqrtd1_u05purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sqrtd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_sqrtd1_u35purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_hypotd1_u05purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_hypotd1_u05purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_hypotd1_u35purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_hypotd1_u35purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fabsd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_fabsd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_copysignd1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_copysignd1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fmaxd1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_fmaxd1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fmind1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_fmind1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fdimd1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_fdimd1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_truncd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_truncd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_floord1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_floord1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_ceild1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_ceild1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_roundd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_roundd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_rintd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_rintd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_nextafterd1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_nextafterd1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_frfrexpd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_frfrexpd1_purec(double);
SLEEF_IMPORT SLEEF_CONST int32_t Sleef_expfrexpd1_purec(double);
SLEEF_IMPORT SLEEF_CONST int32_t Sleef_cinz_expfrexpd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fmodd1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_fmodd1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_remainderd1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_remainderd1_purec(double, double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_modfd1_purec(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_cinz_modfd1_purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_lgammad1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_lgammad1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_tgammad1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_tgammad1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_erfd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_erfd1_u10purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_erfcd1_u15purec(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cinz_erfcd1_u15purec(double);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntd1_purec(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrd1_purec(int);

#ifndef Sleef_float_2_DEFINED
typedef Sleef_float2 Sleef_float_2;
#define Sleef_float_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST float Sleef_sinf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_sinf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cosf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_cosf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_sincosf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_cinz_sincosf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_tanf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_tanf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_asinf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_asinf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_acosf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_acosf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_atanf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_atanf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_atan2f1_u35purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_atan2f1_u35purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_logf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_logf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cbrtf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_cbrtf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sinf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_sinf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cosf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_cosf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_sincosf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_cinz_sincosf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_tanf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_tanf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_asinf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_asinf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_acosf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_acosf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_atanf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_atanf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_atan2f1_u10purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_atan2f1_u10purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_logf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_logf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cbrtf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_cbrtf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_expf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_expf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_powf1_u10purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_powf1_u10purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sinhf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_sinhf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_coshf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_coshf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_tanhf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_tanhf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sinhf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_sinhf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_coshf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_coshf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_tanhf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_tanhf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fastsinf1_u3500purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_fastsinf1_u3500purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fastcosf1_u3500purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_fastcosf1_u3500purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fastpowf1_u3500purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_fastpowf1_u3500purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_asinhf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_asinhf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_acoshf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_acoshf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_atanhf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_atanhf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_exp2f1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_exp2f1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_exp2f1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_exp2f1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_exp10f1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_exp10f1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_exp10f1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_exp10f1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_expm1f1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_expm1f1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_log10f1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_log10f1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_log2f1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_log2f1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_log2f1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_log2f1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_log1pf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_log1pf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_sincospif1_u05purec(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_cinz_sincospif1_u05purec(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_sincospif1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_cinz_sincospif1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sinpif1_u05purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_sinpif1_u05purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cospif1_u05purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_cospif1_u05purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fmaf1_purec(float, float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_fmaf1_purec(float, float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sqrtf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_sqrtf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sqrtf1_u05purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_sqrtf1_u05purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sqrtf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_sqrtf1_u35purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_hypotf1_u05purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_hypotf1_u05purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_hypotf1_u35purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_hypotf1_u35purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fabsf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_fabsf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_copysignf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_copysignf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fmaxf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_fmaxf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fminf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_fminf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fdimf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_fdimf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_truncf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_truncf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_floorf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_floorf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_ceilf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_ceilf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_roundf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_roundf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_rintf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_rintf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_nextafterf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_nextafterf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_frfrexpf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_frfrexpf1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fmodf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_fmodf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_remainderf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_remainderf1_purec(float, float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_modff1_purec(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_cinz_modff1_purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_lgammaf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_lgammaf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_tgammaf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_tgammaf1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_erff1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_erff1_u10purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_erfcf1_u15purec(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cinz_erfcf1_u15purec(float);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntf1_purec(int);
SLEEF_IMPORT SLEEF_CONST int Sleef_cinz_getIntf1_purec(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrf1_purec(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_cinz_getPtrf1_purec(int);
#endif
#ifdef __STDC__

#ifndef Sleef_double_2_DEFINED
typedef Sleef_double2 Sleef_double_2;
#define Sleef_double_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST double Sleef_sind1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_sind1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cosd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_cosd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_sincosd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_finz_sincosd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_tand1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_tand1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_asind1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_asind1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_acosd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_acosd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_atand1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_atand1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_atan2d1_u35purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_atan2d1_u35purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_logd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_logd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cbrtd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_cbrtd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sind1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_sind1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cosd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_cosd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_sincosd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_finz_sincosd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_tand1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_tand1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_asind1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_asind1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_acosd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_acosd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_atand1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_atand1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_atan2d1_u10purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_atan2d1_u10purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_logd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_logd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cbrtd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_cbrtd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_expd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_expd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_powd1_u10purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_powd1_u10purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sinhd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_sinhd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_coshd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_coshd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_tanhd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_tanhd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sinhd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_sinhd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_coshd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_coshd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_tanhd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_tanhd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fastsind1_u3500purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_fastsind1_u3500purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fastcosd1_u3500purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_fastcosd1_u3500purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fastpowd1_u3500purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_fastpowd1_u3500purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_asinhd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_asinhd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_acoshd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_acoshd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_atanhd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_atanhd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_exp2d1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_exp2d1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_exp2d1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_exp2d1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_exp10d1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_exp10d1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_exp10d1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_exp10d1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_expm1d1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_expm1d1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_log10d1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_log10d1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_log2d1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_log2d1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_log2d1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_log2d1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_log1pd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_log1pd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_sincospid1_u05purecfma(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_finz_sincospid1_u05purecfma(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_sincospid1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_finz_sincospid1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sinpid1_u05purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_sinpid1_u05purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_cospid1_u05purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_cospid1_u05purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_ldexpd1_purecfma(double, int32_t);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_ldexpd1_purecfma(double, int32_t);
SLEEF_IMPORT SLEEF_CONST int32_t Sleef_ilogbd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST int32_t Sleef_finz_ilogbd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fmad1_purecfma(double, double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_fmad1_purecfma(double, double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sqrtd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_sqrtd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sqrtd1_u05purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_sqrtd1_u05purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_sqrtd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_sqrtd1_u35purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_hypotd1_u05purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_hypotd1_u05purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_hypotd1_u35purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_hypotd1_u35purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fabsd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_fabsd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_copysignd1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_copysignd1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fmaxd1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_fmaxd1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fmind1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_fmind1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fdimd1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_fdimd1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_truncd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_truncd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_floord1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_floord1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_ceild1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_ceild1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_roundd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_roundd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_rintd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_rintd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_nextafterd1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_nextafterd1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_frfrexpd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_frfrexpd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST int32_t Sleef_expfrexpd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST int32_t Sleef_finz_expfrexpd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fmodd1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_fmodd1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_remainderd1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_remainderd1_purecfma(double, double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_modfd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_finz_modfd1_purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_lgammad1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_lgammad1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_tgammad1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_tgammad1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_erfd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_erfd1_u10purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_erfcd1_u15purecfma(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_finz_erfcd1_u15purecfma(double);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntd1_purecfma(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrd1_purecfma(int);

#ifndef Sleef_float_2_DEFINED
typedef Sleef_float2 Sleef_float_2;
#define Sleef_float_2_DEFINED
#endif

SLEEF_IMPORT SLEEF_CONST float Sleef_sinf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_sinf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cosf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_cosf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_sincosf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_finz_sincosf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_tanf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_tanf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_asinf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_asinf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_acosf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_acosf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_atanf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_atanf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_atan2f1_u35purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_atan2f1_u35purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_logf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_logf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cbrtf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_cbrtf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sinf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_sinf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cosf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_cosf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_sincosf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_finz_sincosf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_tanf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_tanf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_asinf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_asinf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_acosf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_acosf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_atanf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_atanf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_atan2f1_u10purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_atan2f1_u10purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_logf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_logf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cbrtf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_cbrtf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_expf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_expf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_powf1_u10purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_powf1_u10purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sinhf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_sinhf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_coshf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_coshf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_tanhf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_tanhf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sinhf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_sinhf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_coshf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_coshf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_tanhf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_tanhf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fastsinf1_u3500purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_fastsinf1_u3500purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fastcosf1_u3500purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_fastcosf1_u3500purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fastpowf1_u3500purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_fastpowf1_u3500purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_asinhf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_asinhf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_acoshf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_acoshf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_atanhf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_atanhf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_exp2f1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_exp2f1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_exp2f1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_exp2f1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_exp10f1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_exp10f1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_exp10f1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_exp10f1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_expm1f1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_expm1f1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_log10f1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_log10f1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_log2f1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_log2f1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_log2f1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_log2f1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_log1pf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_log1pf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_sincospif1_u05purecfma(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_finz_sincospif1_u05purecfma(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_sincospif1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_finz_sincospif1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sinpif1_u05purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_sinpif1_u05purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_cospif1_u05purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_cospif1_u05purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fmaf1_purecfma(float, float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_fmaf1_purecfma(float, float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sqrtf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_sqrtf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sqrtf1_u05purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_sqrtf1_u05purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_sqrtf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_sqrtf1_u35purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_hypotf1_u05purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_hypotf1_u05purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_hypotf1_u35purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_hypotf1_u35purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fabsf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_fabsf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_copysignf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_copysignf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fmaxf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_fmaxf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fminf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_fminf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fdimf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_fdimf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_truncf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_truncf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_floorf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_floorf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_ceilf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_ceilf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_roundf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_roundf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_rintf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_rintf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_nextafterf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_nextafterf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_frfrexpf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_frfrexpf1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_fmodf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_fmodf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_remainderf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_remainderf1_purecfma(float, float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_modff1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_finz_modff1_purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_lgammaf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_lgammaf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_tgammaf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_tgammaf1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_erff1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_erff1_u10purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_erfcf1_u15purecfma(float);
SLEEF_IMPORT SLEEF_CONST float Sleef_finz_erfcf1_u15purecfma(float);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntf1_purecfma(int);
SLEEF_IMPORT SLEEF_CONST int Sleef_finz_getIntf1_purecfma(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrf1_purecfma(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_finz_getPtrf1_purecfma(int);
#endif
#ifdef __STDC__

#ifndef Sleef_double_2_DEFINED
typedef Sleef_double2 Sleef_double_2;
#define Sleef_double_2_DEFINED
#endif

SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sind1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_cosd1_u35(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_sincosd1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_tand1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_asind1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_acosd1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_atand1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_atan2d1_u35(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_logd1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_cbrtd1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sind1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_cosd1_u10(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_sincosd1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_tand1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_asind1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_acosd1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_atand1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_atan2d1_u10(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_logd1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_cbrtd1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_expd1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_powd1_u10(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sinhd1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_coshd1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_tanhd1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sinhd1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_coshd1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_tanhd1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fastsind1_u3500(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fastcosd1_u3500(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fastpowd1_u3500(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_asinhd1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_acoshd1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_atanhd1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_exp2d1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_exp2d1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_exp10d1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_exp10d1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_expm1d1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_log10d1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_log2d1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_log2d1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_log1pd1_u10(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_sincospid1_u05(double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_sincospid1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sinpid1_u05(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_cospid1_u05(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_ldexpd1(double, int32_t);
SLEEF_IMPORT SLEEF_CONST int32_t Sleef_ilogbd1(double);
SLEEF_IMPORT SLEEF_CONST double Sleef_fmad1(double, double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sqrtd1(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sqrtd1_u05(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_sqrtd1_u35(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_hypotd1_u05(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_hypotd1_u35(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fabsd1(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_copysignd1(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fmaxd1(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fmind1(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fdimd1(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_truncd1(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_floord1(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_ceild1(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_roundd1(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_rintd1(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_nextafterd1(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_frfrexpd1(double);
SLEEF_IMPORT SLEEF_CONST int32_t Sleef_expfrexpd1(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_fmodd1(double, double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_remainderd1(double, double);
SLEEF_IMPORT SLEEF_CONST Sleef_double_2 Sleef_modfd1(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_lgammad1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_tgammad1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_erfd1_u10(double);
SLEEF_PRAGMA_OMP_SIMD_DP SLEEF_IMPORT SLEEF_CONST double Sleef_erfcd1_u15(double);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntd1(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrd1(int);

#ifndef Sleef_float_2_DEFINED
typedef Sleef_float2 Sleef_float_2;
#define Sleef_float_2_DEFINED
#endif

SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sinf1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_cosf1_u35(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_sincosf1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_tanf1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_asinf1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_acosf1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_atanf1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_atan2f1_u35(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_logf1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_cbrtf1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sinf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_cosf1_u10(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_sincosf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_tanf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_asinf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_acosf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_atanf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_atan2f1_u10(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_logf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_cbrtf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_expf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_powf1_u10(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sinhf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_coshf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_tanhf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sinhf1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_coshf1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_tanhf1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fastsinf1_u3500(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fastcosf1_u3500(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fastpowf1_u3500(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_asinhf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_acoshf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_atanhf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_exp2f1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_exp2f1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_exp10f1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_exp10f1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_expm1f1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_log10f1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_log2f1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_log2f1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_log1pf1_u10(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_sincospif1_u05(float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_sincospif1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sinpif1_u05(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_cospif1_u05(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fmaf1(float, float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sqrtf1(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sqrtf1_u05(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_sqrtf1_u35(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_hypotf1_u05(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_hypotf1_u35(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fabsf1(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_copysignf1(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fmaxf1(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fminf1(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fdimf1(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_truncf1(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_floorf1(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_ceilf1(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_roundf1(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_rintf1(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_nextafterf1(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_frfrexpf1(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_fmodf1(float, float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_remainderf1(float, float);
SLEEF_IMPORT SLEEF_CONST Sleef_float_2 Sleef_modff1(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_lgammaf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_tgammaf1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_erff1_u10(float);
SLEEF_PRAGMA_OMP_SIMD_SP SLEEF_IMPORT SLEEF_CONST float Sleef_erfcf1_u15(float);
SLEEF_IMPORT SLEEF_CONST int Sleef_getIntf1(int);
SLEEF_IMPORT SLEEF_CONST void *Sleef_getPtrf1(int);
#endif

//

#ifdef __cplusplus
} // extern "C"
#endif

#endif // #ifndef __SLEEF_H__
