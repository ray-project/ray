// This file is part of Eigen, a lightweight C++ template library
// for linear algebra.
//
// Copyright (C) 2016 Benoit Steiner <benoit.steiner.goog@gmail.com>
//
// This Source Code Form is subject to the terms of the Mozilla
// Public License v. 2.0. If a copy of the MPL was not distributed
// with this file, You can obtain one at http://mozilla.org/MPL/2.0/.

#ifndef EIGEN_CXX11_THREADPOOL_THREAD_LOCAL_H
#define EIGEN_CXX11_THREADPOOL_THREAD_LOCAL_H

#if EIGEN_MAX_CPP_VER >= 11 &&                         \
    ((EIGEN_COMP_GNUC && EIGEN_GNUC_AT_LEAST(4, 8)) || \
     __has_feature(cxx_thread_local)                || \
     (EIGEN_COMP_MSVC >= 1900) )
#define EIGEN_THREAD_LOCAL static thread_local
#endif

// Disable TLS for Apple and Android builds with older toolchains.
#if defined(__APPLE__)
// Included for TARGET_OS_IPHONE, __IPHONE_OS_VERSION_MIN_REQUIRED,
// __IPHONE_8_0.
#include <Availability.h>
#include <TargetConditionals.h>
#endif
// Checks whether C++11's `thread_local` storage duration specifier is
// supported.
#if defined(__apple_build_version__) &&     \
    ((__apple_build_version__ < 8000042) || \
     (TARGET_OS_IPHONE && __IPHONE_OS_VERSION_MIN_REQUIRED < __IPHONE_9_0))
// Notes: Xcode's clang did not support `thread_local` until version
// 8, and even then not for all iOS < 9.0.
#undef EIGEN_THREAD_LOCAL

#elif defined(__ANDROID__) && EIGEN_COMP_CLANG
// There are platforms for which TLS should not be used even though the compiler
// makes it seem like it's supported (Android NDK < r12b for example).
// This is primarily because of linker problems and toolchain misconfiguration:
// TLS isn't supported until NDK r12b per
// https://developer.android.com/ndk/downloads/revision_history.html
// Since NDK r16, `__NDK_MAJOR__` and `__NDK_MINOR__` are defined in
// <android/ndk-version.h>. For NDK < r16, users should define these macros,
// e.g. `-D__NDK_MAJOR__=11 -D__NKD_MINOR__=0` for NDK r11.
#if __has_include(<android/ndk-version.h>)
#include <android/ndk-version.h>
#endif  // __has_include(<android/ndk-version.h>)
#if defined(__ANDROID__) && defined(__clang__) && defined(__NDK_MAJOR__) && \
    defined(__NDK_MINOR__) &&                                               \
    ((__NDK_MAJOR__ < 12) || ((__NDK_MAJOR__ == 12) && (__NDK_MINOR__ < 1)))
#undef EIGEN_THREAD_LOCAL
#endif
#endif  // defined(__ANDROID__) && defined(__clang__)

#endif  // EIGEN_CXX11_THREADPOOL_THREAD_LOCAL_H
