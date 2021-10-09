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
//
// --------------------------------------------------------------
//
// RAY_LOG_EVERY_N and RAY_LOG_EVERY_MS are adapted from
// https://github.com/google/glog/blob/master/src/glog/logging.h.in
//
// Copyright (c) 2008, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#pragma once

#include <gtest/gtest_prod.h>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include "fixed_string.h"
#include "short_alloc.h"

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

namespace ray_test {
/// This function returns the current call stack information.
std::string GetCallTrace();

enum class RayLogLevelNew {
  TRACE = -2,
  DEBUG = -1,
  INFO = 0,
  WARNING = 1,
  ERROR = 2,
  FATAL = 3
};

#define STR(x) #x
#define TOSTRING(x) STR(x)

#define RAY_LOG_INTERNAL_NEW(level)                                \
  [] {                                                             \
    constexpr auto path = ray::make_fixed_string(__FILE__);        \
    constexpr size_t pos = path.rfind('/');                        \
    constexpr auto name = path.substr<pos + 1>();                  \
    constexpr auto prefix = name + ":" + TOSTRING(__LINE__) + ":"; \
    return ::ray_test::RayLog(prefix.data(), level);               \
  }()

#define RAY_LOG_ENABLED_NEW(level) \
  ray_test::RayLog::IsLevelEnabled(ray_test::RayLogLevelNew::level)

#define RAY_LOG_NEW(level)                                               \
  if (ray_test::RayLog::IsLevelEnabled(ray_test::RayLogLevelNew::level)) \
  RAY_LOG_INTERNAL_NEW(ray_test::RayLogLevelNew::level)

#define RAY_IGNORE_EXPR_NEW(expr) ((void)(expr))

#define RAY_CHECK_NEW(condition)                                                    \
  (condition)                                                                       \
      ? RAY_IGNORE_EXPR_NEW(0)                                                      \
      : ::ray_test::Voidify() &                                                     \
            ::ray_test::RayLog(__FILE__, __LINE__, ray_test::RayLogLevelNew::FATAL) \
                << " Check failed: " #condition " "

#ifdef NDEBUG

#define RAY_DCHECK_NEW(condition)                                                   \
  (condition)                                                                       \
      ? RAY_IGNORE_EXPR_NEW(0)                                                      \
      : ::ray_test::Voidify() &                                                     \
            ::ray_test::RayLog(__FILE__, __LINE__, ray_test::RayLogLevelNew::ERROR) \
                << " Debug check failed: " #condition " "
#else

#define RAY_DCHECK_NEW(condition) RAY_CHECK_NEW(condition)

#endif  // NDEBUG

using short_string =
    std::basic_string<char, std::char_traits<char>, short_alloc<char, 1024>>;

// To make the logging lib plugable with other logging libs and make
// the implementation unawared by the user, RayLog is only a declaration
// which hide the implementation into logging.cc file.
// In logging.cc, we can choose different log libs using different macros.

/// Callback function which will be triggered to expose fatal log.
/// The first argument: a string representing log type or label.
/// The second argument: log content.
using FatalLogCallback = std::function<void(const std::string &, const std::string &)>;

class RayLog {
 public:
  RayLog(const char *file_name, int line_number, RayLogLevelNew severity);
  RayLog(const char *prefix, RayLogLevelNew severity);

  virtual ~RayLog();

  /// Return whether or not current logging instance is enabled.
  ///
  /// \return True if logging is enabled and false otherwise.
  virtual bool IsEnabled() const;

  virtual bool IsFatal() const;

  /// The init function of ray log for a program which should be called only once.
  ///
  /// \parem appName The app name which starts the log.
  /// \param severity_threshold Logging threshold for the program.
  /// \param logDir Logging output file name. If empty, the log won't output to file.
  static void StartRayLog(const std::string &appName,
                          RayLogLevelNew severity_threshold = RayLogLevelNew::INFO,
                          const std::string &logDir = "");

  /// The shutdown function of ray log which should be used with StartRayLog as a pair.
  static void ShutDownRayLog();

  /// Uninstall the signal actions installed by InstallFailureSignalHandler.
  static void UninstallSignalAction();

  /// Return whether or not the log level is enabled in current setting.
  ///
  /// \param log_level The input log level to test.
  /// \return True if input log level is not lower than the threshold.
  static bool IsLevelEnabled(RayLogLevelNew log_level);

  /// Install the failure signal handler to output call stack when crash.
  static void InstallFailureSignalHandler();

  /// To check failure signal handler enabled or not.
  static bool IsFailureSignalHandlerEnabled();

  /// Get the log level from environment variable.
  static RayLogLevelNew GetLogLevelFromEnv();

  static std::string GetLogFormatPattern();

  static std::string GetLoggerName();

  /// Add callback functions that will be triggered to expose fatal log.
  static void AddFatalLogCallbacks(
      const std::vector<FatalLogCallback> &expose_log_callbacks);

  static void EnableAlwaysFlush(bool enable) { always_flush_ = enable; }

  template <typename T>
  RayLog &operator<<(const T &t) {
    if (IsEnabled()) {
      if constexpr (std::is_integral_v<T>) {
        ToChars(t);
      } else if constexpr (std::is_enum_v<T>) {
        ToChars((int)t);
      } else if constexpr (std::is_same_v<int64_t, T> || std::is_same_v<uint64_t, T>) {
        ToChars(t);
      } else if constexpr (std::is_floating_point_v<T>) {
        str_.append(std::to_string(t));
      } else {
        str_.append(t);
      }
    }
    // if (IsFatal()) {
    //   ExposeStream() << t;
    // }
    return *this;
  }

 private:
  template <typename T>
  void ToChars(const T &t) {
    std::array<char, 20> str;
    if (auto [ptr, ec] = std::to_chars(str.data(), str.data() + str.size(), t);
        ec == std::errc()) {
      str_.append(str.data(), ptr - str.data());
    }
  }
  FRIEND_TEST(PrintLogTest, TestRayLogEveryNOrDebug);
  FRIEND_TEST(PrintLogTest, TestRayLogEveryN);

  /// True if log messages should be logged and false if they should be ignored.
  bool is_enabled_;
  /// log level.
  RayLogLevelNew severity_;
  /// Whether current log is fatal or not.
  bool is_fatal_ = false;
  // /// String stream of exposed log content.
  // std::shared_ptr<std::ostringstream> expose_osstream_ = nullptr;
  /// Callback functions which will be triggered to expose fatal log.
  static std::vector<FatalLogCallback> fatal_log_callbacks_;
  static RayLogLevelNew severity_threshold_;
  // In InitGoogleLogging, it simply keeps the pointer.
  // We need to make sure the app name passed to InitGoogleLogging exist.
  static std::string app_name_;
  /// The directory where the log files are stored.
  /// If this is empty, logs are printed to stdout.
  static std::string log_dir_;
  /// This flag is used to avoid calling UninstallSignalAction in ShutDownRayLog if
  /// InstallFailureSignalHandler was not called.
  static bool is_failure_signal_handler_installed_;
  // Log format content.
  static std::string log_format_pattern_;
  // Log rotation file size limitation.
  static long log_rotation_max_size_;
  // Log rotation file number.
  static long log_rotation_file_num_;
  // Ray default logger name.
  static std::string logger_name_;

  inline static bool always_flush_ = true;

 protected:
  short_string::allocator_type::arena_type arena_;
  short_string str_;
  int loglevel_;
};

// This class make RAY_CHECK compilation pass to change the << operator to void.
class Voidify {
 public:
  Voidify() {}
  // This has to be an operator with a precedence lower than << but
  // higher than ?:
  void operator&(RayLog &) {}
};

}  // namespace ray_test
