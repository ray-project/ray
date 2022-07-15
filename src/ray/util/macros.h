// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

// From Google gutil
#ifndef RAY_DISALLOW_COPY_AND_ASSIGN
#define RAY_DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName &) = delete;         \
  void operator=(const TypeName &) = delete
#endif

#define RAY_UNUSED(x) (void)x

//
// GCC can be told that a certain branch is not likely to be taken (for
// instance, a CHECK failure), and use that information in static analysis.
// Giving it this information can help it optimize for the common case in
// the absence of better information (ie. -fprofile-arcs).
//
#if defined(__GNUC__)
#define RAY_PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define RAY_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#define RAY_NORETURN __attribute__((noreturn))
#define RAY_PREFETCH(addr) __builtin_prefetch(addr)
#elif defined(_MSC_VER)
#define RAY_NORETURN __declspec(noreturn)
#define RAY_PREDICT_FALSE(x) x
#define RAY_PREDICT_TRUE(x) x
#define RAY_PREFETCH(addr)
#else
#define RAY_NORETURN
#define RAY_PREDICT_FALSE(x) x
#define RAY_PREDICT_TRUE(x) x
#define RAY_PREFETCH(addr)
#endif

#if (defined(__GNUC__) || defined(__APPLE__))
#define RAY_MUST_USE_RESULT __attribute__((warn_unused_result))
#elif defined(_MSC_VER)
#define RAY_MUST_USE_RESULT
#else
#define RAY_MUST_USE_RESULT
#endif

// Suppress Undefined Behavior Sanitizer (recoverable only). Usage:
// - __suppress_ubsan__("undefined")
// - __suppress_ubsan__("signed-integer-overflow")
// adaped from
// https://github.com/google/flatbuffers/blob/master/include/flatbuffers/base.h
#if defined(__clang__)
#define __suppress_ubsan__(type) __attribute__((no_sanitize(type)))
#elif defined(__GNUC__) && (__GNUC__ * 100 + __GNUC_MINOR__ >= 409)
#define __suppress_ubsan__(type) __attribute__((no_sanitize_undefined))
#else
#define __suppress_ubsan__(type)
#endif
