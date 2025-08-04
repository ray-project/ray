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

enum class RayLoggerLevel { DEBUG = -1, INFO = 0, WARNING = 1, ERROR = 2, FATAL = 3 };

#define RAYLOG_INTERNAL(level) *CreateRayLogger(__FILE__, __LINE__, level)
#define RAYLOG(level)                             \
  if (IsLevelEnabled(ray::RayLoggerLevel::level)) \
  RAYLOG_INTERNAL(ray::RayLoggerLevel::level)

// To make the logging lib pluggable with other logging libs and make
// the implementation unaware by the user, RayLog is only a declaration
// which hides the implementation into logging.cc file.
// In logging.cc, we can choose different log libs using different macros.

// This is a log interface which does not output anything.
class RayLogger {
 public:
  virtual ~RayLogger(){};

  virtual bool IsEnabled() const = 0;

  template <typename T>
  RayLogger &operator<<(const T &t) {
    if (IsEnabled()) {
      Stream() << t;
    }
    return *this;
  }

 protected:
  virtual std::ostream &Stream() = 0;
};

std::unique_ptr<RayLogger> CreateRayLogger(const char *file_name,
                                           int line_number,
                                           RayLoggerLevel severity);
bool IsLevelEnabled(RayLoggerLevel log_level);

}  // namespace ray
