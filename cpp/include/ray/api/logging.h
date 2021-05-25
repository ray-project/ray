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

#include <iostream>
#include <memory>
#include <string>

#if defined(_WIN32)
#ifndef _WINDOWS_
#ifndef WIN32_LEAN_AND_MEAN  // Sorry for the inconvenience. Please include any related
                             // headers you need manually.
                             // (https://stackoverflow.com/a/8294669)
#define WIN32_LEAN_AND_MEAN  // Prevent inclusion of WinSock2.h
#endif
#include <Windows.h>  // Force inclusion of WinGDI here to resolve name conflict
#endif
#ifdef ERROR  // Should be true unless someone else undef'd it already
#undef ERROR  // Windows GDI defines this macro; make it a global enum so it doesn't
              // conflict with our code
enum { ERROR = 0 };
#endif
#endif

#if defined(DEBUG) && DEBUG == 1
// Bazel defines the DEBUG macro for historical reasons:
// https://github.com/bazelbuild/bazel/issues/3513#issuecomment-323829248
// Undefine the DEBUG macro to prevent conflicts with our usage below
#undef DEBUG
// Redefine DEBUG as itself to allow any '#ifdef DEBUG' to keep working regardless
#define DEBUG DEBUG
#endif

namespace ray {
namespace api {

enum class CppRayLogLevel {
  TRACE = -2,
  DEBUG = -1,
  INFO = 0,
  WARNING = 1,
  ERROR = 2,
  FATAL = 3
};

#ifdef RAY_LOG
#undef RAY_LOG
#undef RAY_LOG_INTERNAL
#define RAY_LOG_INTERNAL(level) *CreateCppLog(__FILE__, __LINE__, level)
#define RAY_LOG(level) \
  if (IsLevelEnabled(CppRayLogLevel::level)) RAY_LOG_INTERNAL(CppRayLogLevel::level)
#endif

// To make the logging lib plugable with other logging libs and make
// the implementation unawared by the user, CppRayLog is only a declaration
// which hide the implementation into logging.cc file.
// In logging.cc, we can choose different log libs using different macros.

// This is also a null log which does not output anything.
class CppLogBase {
 public:
  virtual ~CppLogBase(){};

  // By default, this class is a null log because it return false here.
  virtual bool IsEnabled() const = 0;

  // virtual bool IsLevelEnabled(CppRayLogLevel log_level) = 0;

  template <typename T>
  CppLogBase &operator<<(const T &t) {
    if (IsEnabled()) {
      Stream() << t;
    }
    return *this;
  }

 protected:
  virtual std::ostream &Stream() { return std::cerr; };
};

std::unique_ptr<CppLogBase> CreateCppLog(const char *file_name, int line_number,
                                         CppRayLogLevel severity);
bool IsLevelEnabled(CppRayLogLevel log_level);

}  // namespace api
}  // namespace ray
