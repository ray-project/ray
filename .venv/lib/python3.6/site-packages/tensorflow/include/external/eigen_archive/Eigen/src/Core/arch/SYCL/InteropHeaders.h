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
 * InteropHeaders.h
 *
 * \brief:
 *  InteropHeaders
 *
*****************************************************************/

#ifndef EIGEN_INTEROP_HEADERS_SYCL_H
#define EIGEN_INTEROP_HEADERS_SYCL_H
#if defined EIGEN_USE_SYCL
namespace Eigen {

namespace internal {
#define SYCL_PACKET_TRAITS(packet_type, val, unpacket_type, lengths)\
  template<> struct packet_traits<unpacket_type> : default_packet_traits\
  {\
    typedef packet_type type;\
    typedef packet_type half;\
    enum {\
      Vectorizable = 1,\
      AlignedOnScalar = 1,\
      size=lengths,\
      HasHalfPacket = 0,\
      HasDiv  = 1,\
      HasLog  = 1,\
      HasExp  = 1,\
      HasSqrt = 1,\
      HasRsqrt = 1,\
      HasSin    = 1,\
      HasCos    = 1,\
      HasTan    = 1,\
      HasASin   = 1,\
      HasACos   = 1,\
      HasATan   = 1,\
      HasSinh   = 1,\
      HasCosh   = 1,\
      HasTanh   = 1,\
      HasLGamma = 0,\
      HasDiGamma = 0,\
      HasZeta = 0,\
      HasPolygamma = 0,\
      HasErf = 0,\
      HasErfc = 0,\
      HasIGamma = 0,\
      HasIGammac = 0,\
      HasBetaInc = 0,\
      HasBlend = val,\
      HasMax=1,\
      HasMin=1,\
      HasMul=1,\
      HasAdd=1,\
      HasFloor=1,\
      HasRound=1,\
      HasLog1p=1,\
      HasExpm1=1,\
      HasCeil=1,\
    };\
  };

SYCL_PACKET_TRAITS(cl::sycl::cl_float4, 1, float, 4)
SYCL_PACKET_TRAITS(cl::sycl::cl_float4, 1, const float, 4)
SYCL_PACKET_TRAITS(cl::sycl::cl_double2, 0, double, 2)
SYCL_PACKET_TRAITS(cl::sycl::cl_double2, 0, const double, 2)
#undef SYCL_PACKET_TRAITS


// Make sure this is only available when targeting a GPU: we don't want to
// introduce conflicts between these packet_traits definitions and the ones
// we'll use on the host side (SSE, AVX, ...)
#define SYCL_ARITHMETIC(packet_type) template<> struct is_arithmetic<packet_type>  { enum { value = true }; };
SYCL_ARITHMETIC(cl::sycl::cl_float4)
SYCL_ARITHMETIC(cl::sycl::cl_double2)
#undef SYCL_ARITHMETIC

#define SYCL_UNPACKET_TRAITS(packet_type, unpacket_type, lengths)\
template<> struct unpacket_traits<packet_type>  {\
  typedef unpacket_type  type;\
  enum {size=lengths, alignment=Aligned16};\
  typedef packet_type half;\
};
SYCL_UNPACKET_TRAITS(cl::sycl::cl_float4, float, 4)
SYCL_UNPACKET_TRAITS(cl::sycl::cl_double2, double, 2)

#undef SYCL_UNPACKET_TRAITS

} // end namespace internal

} // end namespace Eigen

#endif // EIGEN_USE_SYCL
#endif // EIGEN_INTEROP_HEADERS_SYCL_H
