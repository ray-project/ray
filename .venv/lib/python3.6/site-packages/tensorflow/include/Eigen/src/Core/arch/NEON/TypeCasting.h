// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2018 Rasmus Munk Larsen <rmlarsen@google.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#ifndef EIGEN_TYPE_CASTING_NEON_H
#define EIGEN_TYPE_CASTING_NEON_H

namespace Eigen {

namespace internal {

template <>
struct type_casting_traits<float, int> {
  enum {
    VectorizedCast = 1,
    SrcCoeffRatio = 1,
    TgtCoeffRatio = 1
  };
};

template <>
struct type_casting_traits<int, float> {
  enum {
    VectorizedCast = 1,
    SrcCoeffRatio = 1,
    TgtCoeffRatio = 1
  };
};


template<> EIGEN_STRONG_INLINE Packet4i pcast<Packet4f, Packet4i>(const Packet4f& a) {
  return vcvtq_s32_f32(a);
}

template<> EIGEN_STRONG_INLINE Packet4f pcast<Packet4i, Packet4f>(const Packet4i& a) {
  return vcvtq_f32_s32(a);
}

template<> EIGEN_STRONG_INLINE Packet4i preinterpret<Packet4i,Packet4f>(const Packet4f& a) {
  return vreinterpretq_s32_f32(a);
}

template<> EIGEN_STRONG_INLINE Packet4f preinterpret<Packet4f,Packet4i>(const Packet4i& a) {
  return vreinterpretq_f32_s32(a);
}

} // end namespace internal

} // end namespace Eigen

#endif // EIGEN_TYPE_CASTING_NEON_H
