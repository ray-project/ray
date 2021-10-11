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

namespace {
template <typename T>
auto constexpr is_atomic_v = false;
template <typename T>
auto constexpr is_atomic_v<std::atomic<T>> = true;
template <typename T>
auto constexpr is_int64_v = std::is_same_v<int64_t, T> || std::is_same_v<uint64_t, T>;

template <class, class = void>
struct has_value : std::false_type {};
template <class T>
struct has_value<T, std::void_t<decltype(std::declval<T>().value())>> : std::true_type {};
template <typename T>
auto constexpr has_value_v = has_value<T>::value;

template <typename T>
struct is_shared_ptr : std::false_type {};
template <typename T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type {};
template <typename T>
auto constexpr is_shared_ptr_v = is_shared_ptr<T>::value;

template <class, class = void>
struct has_hex : std::false_type {};
template <class T>
struct has_hex<T, std::void_t<decltype(std::declval<T>().Hex())>> : std::true_type {};
template <typename T>
auto constexpr has_hex_v = has_hex<T>::value;

template <class, class = void>
struct has_to_string : std::false_type {};
template <class T>
struct has_to_string<T, std::void_t<decltype(std::declval<T>().ToString())>>
    : std::true_type {};
template <typename T>
auto constexpr has_to_string_v = has_to_string<T>::value;

template <class, class = void>
struct has_native_handle : std::false_type {};
template <class T>
struct has_native_handle<T, std::void_t<decltype(std::declval<T>()->GetObjectIDs())>>
    : std::true_type {};
template <typename T>
auto constexpr has_native_handle_v = has_native_handle<T>::value;
}  // namespace

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
/// This function returns the current call stack information.
std::string GetCallTrace();

enum class RayLogLevel {
  TRACE = -2,
  DEBUG = -1,
  INFO = 0,
  WARNING = 1,
  ERROR = 2,
  FATAL = 3
};

#define STR(x) #x
#define TOSTRING(x) STR(x)

#define RAY_LOG_INTERNAL(level)                                    \
  [] {                                                             \
    constexpr auto path = ray::make_fixed_string(__FILE__);        \
    constexpr size_t pos = path.rfind('/');                        \
    constexpr auto name = path.substr<pos + 1>();                  \
    constexpr auto prefix = name + ":" + TOSTRING(__LINE__) + ":"; \
    return ::ray::RayLog(prefix.data(), level);                    \
  }()

#define RAY_LOG_ENABLED(level) ray::RayLog::IsLevelEnabled(ray::RayLogLevel::level)

#define RAY_LOG(level)                                      \
  if (ray::RayLog::IsLevelEnabled(ray::RayLogLevel::level)) \
  RAY_LOG_INTERNAL(ray::RayLogLevel::level)

#define RAY_IGNORE_EXPR(expr) ((void)(expr))

#define RAY_CHECK(condition)                                                 \
  (condition) ? RAY_IGNORE_EXPR(0)                                           \
              : ::ray::Voidify() & RAY_LOG_INTERNAL(ray::RayLogLevel::FATAL) \
                                       << " Check failed: " #condition " "

#ifdef NDEBUG

#define RAY_DCHECK(condition)                                                \
  (condition) ? RAY_IGNORE_EXPR(0)                                           \
              : ::ray::Voidify() & RAY_LOG_INTERNAL(ray::RayLogLevel::ERROR) \
                                       << " Debug check failed: " #condition " "
#else

#define RAY_DCHECK(condition) RAY_CHECK(condition)

#endif  // NDEBUG

// RAY_LOG_EVERY_N/RAY_LOG_EVERY_MS, adaped from
// https://github.com/google/glog/blob/master/src/glog/logging.h.in
#define RAY_LOG_EVERY_N_VARNAME(base, line) RAY_LOG_EVERY_N_VARNAME_CONCAT(base, line)
#define RAY_LOG_EVERY_N_VARNAME_CONCAT(base, line) base##line

#define RAY_LOG_OCCURRENCES RAY_LOG_EVERY_N_VARNAME(occurrences_, __LINE__)

// Occasional logging, log every n'th occurrence of an event.
#define RAY_LOG_EVERY_N(level, n)                             \
  static std::atomic<uint64_t> RAY_LOG_OCCURRENCES(0);        \
  if (ray::RayLog::IsLevelEnabled(ray::RayLogLevel::level) && \
      RAY_LOG_OCCURRENCES.fetch_add(1) % n == 0)              \
  RAY_LOG_INTERNAL(ray::RayLogLevel::level) << "[" << RAY_LOG_OCCURRENCES << "] "

// Occasional logging with DEBUG fallback:
// If DEBUG is not enabled, log every n'th occurrence of an event.
// Otherwise, if DEBUG is enabled, always log as DEBUG events.
#define RAY_LOG_EVERY_N_OR_DEBUG(level, n)                              \
  static std::atomic<uint64_t> RAY_LOG_OCCURRENCES(0);                  \
  if (ray::RayLog::IsLevelEnabled(ray::RayLogLevel::DEBUG) ||           \
      (ray::RayLog::IsLevelEnabled(ray::RayLogLevel::level) &&          \
       RAY_LOG_OCCURRENCES.fetch_add(1) % n == 0))                      \
  RAY_LOG_INTERNAL(ray::RayLog::IsLevelEnabled(ray::RayLogLevel::level) \
                       ? ray::RayLogLevel::level                        \
                       : ray::RayLogLevel::DEBUG)                       \
      << "[" << RAY_LOG_OCCURRENCES << "] "

/// Macros for RAY_LOG_EVERY_MS
#define RAY_LOG_TIME_PERIOD RAY_LOG_EVERY_N_VARNAME(timePeriod_, __LINE__)
#define RAY_LOG_PREVIOUS_TIME_RAW RAY_LOG_EVERY_N_VARNAME(previousTimeRaw_, __LINE__)
#define RAY_LOG_TIME_DELTA RAY_LOG_EVERY_N_VARNAME(deltaTime_, __LINE__)
#define RAY_LOG_CURRENT_TIME RAY_LOG_EVERY_N_VARNAME(currentTime_, __LINE__)
#define RAY_LOG_PREVIOUS_TIME RAY_LOG_EVERY_N_VARNAME(previousTime_, __LINE__)

#define RAY_LOG_EVERY_MS(level, ms)                                                      \
  constexpr std::chrono::milliseconds RAY_LOG_TIME_PERIOD(ms);                           \
  static std::atomic<int64_t> RAY_LOG_PREVIOUS_TIME_RAW;                                 \
  const auto RAY_LOG_CURRENT_TIME = std::chrono::steady_clock::now().time_since_epoch(); \
  const decltype(RAY_LOG_CURRENT_TIME) RAY_LOG_PREVIOUS_TIME(                            \
      RAY_LOG_PREVIOUS_TIME_RAW.load(std::memory_order_relaxed));                        \
  const auto RAY_LOG_TIME_DELTA = RAY_LOG_CURRENT_TIME - RAY_LOG_PREVIOUS_TIME;          \
  if (RAY_LOG_TIME_DELTA > RAY_LOG_TIME_PERIOD)                                          \
    RAY_LOG_PREVIOUS_TIME_RAW.store(RAY_LOG_CURRENT_TIME.count(),                        \
                                    std::memory_order_relaxed);                          \
  if (ray::RayLog::IsLevelEnabled(ray::RayLogLevel::level) &&                            \
      RAY_LOG_TIME_DELTA > RAY_LOG_TIME_PERIOD)                                          \
  RAY_LOG_INTERNAL(ray::RayLogLevel::level)

/// Callback function which will be triggered to expose fatal log.
/// The first argument: a string representing log type or label.
/// The second argument: log content.
using FatalLogCallback = std::function<void(const std::string &, const std::string &)>;

using short_string =
    std::basic_string<char, std::char_traits<char>, short_alloc<char, 1024>>;

class RayLog {
 public:
  RayLog(RayLogLevel severity);
  RayLog(const char *file, int line, RayLogLevel severity);
  RayLog(const char *prefix, RayLogLevel severity);

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
                          RayLogLevel severity_threshold = RayLogLevel::INFO,
                          const std::string &logDir = "");

  /// The shutdown function of ray log which should be used with StartRayLog as a pair.
  static void ShutDownRayLog();

  /// Uninstall the signal actions installed by InstallFailureSignalHandler.
  static void UninstallSignalAction();

  /// Return whether or not the log level is enabled in current setting.
  ///
  /// \param log_level The input log level to test.
  /// \return True if input log level is not lower than the threshold.
  static bool IsLevelEnabled(RayLogLevel log_level);

  /// Install the failure signal handler to output call stack when crash.
  static void InstallFailureSignalHandler();

  /// To check failure signal handler enabled or not.
  static bool IsFailureSignalHandlerEnabled();

  /// Get the log level from environment variable.
  static RayLogLevel GetLogLevelFromEnv();

  static std::string GetLogFormatPattern();

  static std::string GetLoggerName();

  /// Add callback functions that will be triggered to expose fatal log.
  static void AddFatalLogCallbacks(
      const std::vector<FatalLogCallback> &expose_log_callbacks);

  static void EnableAlwaysFlush(bool enable) { always_flush_ = enable; }
  static bool IsAlwaysFlush() { return always_flush_; }

  template <typename T>
  RayLog &operator<<(const T &t) {
    if (IsEnabled()) {
      if constexpr (std::is_integral_v<T> || is_int64_v<T>) {
        // Make gcc8/9/10 happy, before gcc11 std::to_chars only support integer.
        if constexpr (std::is_same_v<bool, T>) {
          ToChars(int(t));
        } else {
          ToChars(t);
        }
      } else if constexpr (std::is_enum_v<T> || std::is_same_v<pid_t, T>) {
        ToChars((int)t);
      } else if constexpr (has_value_v<T>) {
        ToChars(t.value());
      } else if constexpr (is_atomic_v<T>) {
        ToChars(t.load());
      } else if constexpr (std::is_floating_point_v<T>) {
        str_.append(std::to_string(t));
      } else if constexpr (std::is_same_v<char *const *, T> || is_shared_ptr_v<T>) {
        std::stringstream oss;
        oss << t;
        str_.append(oss.str());
      } else if constexpr (has_hex_v<T>) {
        if (t.IsNil()) {
          str_.append("NIL_ID");
        } else {
          str_.append(t.Hex());
        }
      } else if constexpr (has_to_string_v<T>) {
        str_.append(t.ToString());
      } else if constexpr (has_native_handle_v<T>) {
        ToChars(t->GetNativeHandle());
      } else {
        str_.append(t);
      }
    }

    if (IsFatal()) {
      *expose_osstream_ << t;
    }
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
  RayLogLevel severity_;
  /// Whether current log is fatal or not.
  bool is_fatal_ = false;
  /// String stream of exposed log content.
  std::shared_ptr<std::ostringstream> expose_osstream_ = nullptr;
  /// Callback functions which will be triggered to expose fatal log.
  static std::vector<FatalLogCallback> fatal_log_callbacks_;
  static RayLogLevel severity_threshold_;
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

 private:
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

}  // namespace ray
