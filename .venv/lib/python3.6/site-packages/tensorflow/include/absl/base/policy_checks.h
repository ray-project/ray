// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// File: policy_checks.h
// -----------------------------------------------------------------------------
//
// This header enforces a minimum set of policies at build time, such as the
// supported compiler and library versions. Unsupported configurations are
// reported with `#error`. This enforcement is best effort, so successfully
// compiling this header does not guarantee a supported configuration.

#ifndef ABSL_BASE_POLICY_CHECKS_H_
#define ABSL_BASE_POLICY_CHECKS_H_

// Included for the __GLIBC_PREREQ macro used below.
#include <limits.h>

// Included for the _STLPORT_VERSION macro used below.
#if defined(__cplusplus)
#include <cstddef>
#endif

// -----------------------------------------------------------------------------
// Operating System Check
// -----------------------------------------------------------------------------

#if defined(__CYGWIN__)
#error "Cygwin is not supported."
#endif

// -----------------------------------------------------------------------------
// Compiler Check
// -----------------------------------------------------------------------------

// We support MSVC++ 14.0 update 2 and later.
// This minimum will go up.
#if defined(_MSC_FULL_VER) && _MSC_FULL_VER < 190023918 && !defined(__clang__)
#error "This package requires Visual Studio 2015 Update 2 or higher."
#endif

// We support gcc 4.7 and later.
// This minimum will go up.
#if defined(__GNUC__) && !defined(__clang__)
#if __GNUC__ < 4 || (__GNUC__ == 4 && __GNUC_MINOR__ < 7)
#error "This package requires gcc 4.7 or higher."
#endif
#endif

// We support Apple Xcode clang 4.2.1 (version 421.11.65) and later.
// This corresponds to Apple Xcode version 4.5.
// This minimum will go up.
#if defined(__apple_build_version__) && __apple_build_version__ < 4211165
#error "This package requires __apple_build_version__ of 4211165 or higher."
#endif

// -----------------------------------------------------------------------------
// C++ Version Check
// -----------------------------------------------------------------------------

// Enforce C++11 as the minimum.  Note that Visual Studio has not
// advanced __cplusplus despite being good enough for our purposes, so
// so we exempt it from the check.
#if defined(__cplusplus) && !defined(_MSC_VER)
#if __cplusplus < 201103L
#error "C++ versions less than C++11 are not supported."
#endif
#endif

// -----------------------------------------------------------------------------
// Standard Library Check
// -----------------------------------------------------------------------------

// We have chosen glibc 2.12 as the minimum as it was tagged for release
// in May, 2010 and includes some functionality used in Google software
// (for instance pthread_setname_np):
// https://sourceware.org/ml/libc-alpha/2010-05/msg00000.html
#if defined(__GLIBC__) && defined(__GLIBC_PREREQ)
#if !__GLIBC_PREREQ(2, 12)
#error "Minimum required version of glibc is 2.12."
#endif
#endif

#if defined(_STLPORT_VERSION)
#error "STLPort is not supported."
#endif

// -----------------------------------------------------------------------------
// `char` Size Check
// -----------------------------------------------------------------------------

// Abseil currently assumes CHAR_BIT == 8. If you would like to use Abseil on a
// platform where this is not the case, please provide us with the details about
// your platform so we can consider relaxing this requirement.
#if CHAR_BIT != 8
#error "Abseil assumes CHAR_BIT == 8."
#endif

// -----------------------------------------------------------------------------
// `int` Size Check
// -----------------------------------------------------------------------------

// Abseil currently assumes that an int is 4 bytes. If you would like to use
// Abseil on a platform where this is not the case, please provide us with the
// details about your platform so we can consider relaxing this requirement.
#if INT_MAX < 2147483647
#error "Abseil assumes that int is at least 4 bytes. "
#endif

#endif  // ABSL_BASE_POLICY_CHECKS_H_
