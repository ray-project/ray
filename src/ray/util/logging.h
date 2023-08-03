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

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

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
class StackTrace {
  /// This dumps the current stack trace information.
  friend std::ostream &operator<<(std::ostream &os, const StackTrace &stack_trace);
};

enum class RayLogLevel {
  TRACE = -2,
  DEBUG = -1,
  INFO = 0,
  WARNING = 1,
  ERROR = 2,
  FATAL = 3
};

#define RAY_LOG_INTERNAL(level) ::ray::RayLog(__FILE__, __LINE__, level)

#define RAY_LOG_ENABLED(level) ray::RayLog::IsLevelEnabled(ray::RayLogLevel::level)

#define RAY_LOG(level)                                      \
  if (ray::RayLog::IsLevelEnabled(ray::RayLogLevel::level)) \
  RAY_LOG_INTERNAL(ray::RayLogLevel::level)

#define RAY_IGNORE_EXPR(expr) ((void)(expr))

#define RAY_CHECK(condition)                                                          \
  (condition)                                                                         \
      ? RAY_IGNORE_EXPR(0)                                                            \
      : ::ray::Voidify() & ::ray::RayLog(__FILE__, __LINE__, ray::RayLogLevel::FATAL) \
                               << " Check failed: " #condition " "

#ifdef NDEBUG

#define RAY_DCHECK(condition)                                                         \
  (condition)                                                                         \
      ? RAY_IGNORE_EXPR(0)                                                            \
      : ::ray::Voidify() & ::ray::RayLog(__FILE__, __LINE__, ray::RayLogLevel::ERROR) \
                               << " Debug check failed: " #condition " "
#else

#define RAY_DCHECK(condition) RAY_CHECK(condition)

#endif  // NDEBUG

#define RAY_CHECK_OP(left, op, right)        \
  if (const auto &_left_ = (left); true)     \
    if (const auto &_right_ = (right); true) \
  RAY_CHECK((_left_ op _right_)) << " " << _left_ << " vs " << _right_

#define RAY_CHECK_EQ(left, right) RAY_CHECK_OP(left, ==, right)
#define RAY_CHECK_NE(left, right) RAY_CHECK_OP(left, !=, right)
#define RAY_CHECK_LE(left, right) RAY_CHECK_OP(left, <=, right)
#define RAY_CHECK_LT(left, right) RAY_CHECK_OP(left, <, right)
#define RAY_CHECK_GE(left, right) RAY_CHECK_OP(left, >=, right)
#define RAY_CHECK_GT(left, right) RAY_CHECK_OP(left, >, right)

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

#define RAY_LOG_EVERY_MS_OR(level, ms, otherLevel)                                       \
  constexpr std::chrono::milliseconds RAY_LOG_TIME_PERIOD(ms);                           \
  static std::atomic<int64_t> RAY_LOG_PREVIOUS_TIME_RAW;                                 \
  const auto RAY_LOG_CURRENT_TIME = std::chrono::steady_clock::now().time_since_epoch(); \
  const decltype(RAY_LOG_CURRENT_TIME) RAY_LOG_PREVIOUS_TIME(                            \
      RAY_LOG_PREVIOUS_TIME_RAW.load(std::memory_order_relaxed));                        \
  const auto RAY_LOG_TIME_DELTA = RAY_LOG_CURRENT_TIME - RAY_LOG_PREVIOUS_TIME;          \
  if (RAY_LOG_TIME_DELTA > RAY_LOG_TIME_PERIOD)                                          \
    RAY_LOG_PREVIOUS_TIME_RAW.store(RAY_LOG_CURRENT_TIME.count(),                        \
                                    std::memory_order_relaxed);                          \
  RAY_LOG_INTERNAL(ray::RayLog::IsLevelEnabled(ray::RayLogLevel::level) &&               \
                           RAY_LOG_TIME_DELTA > RAY_LOG_TIME_PERIOD                      \
                       ? ray::RayLogLevel::level                                         \
                       : ray::RayLogLevel::otherLevel)

// To make the logging lib plugable with other logging libs and make
// the implementation unawared by the user, RayLog is only a declaration
// which hide the implementation into logging.cc file.
// In logging.cc, we can choose different log libs using different macros.

// This is also a null log which does not output anything.
class RayLogBase {
 public:
  virtual ~RayLogBase(){};

  // By default, this class is a null log because it return false here.
  virtual bool IsEnabled() const { return false; };

  // This function to judge whether current log is fatal or not.
  virtual bool IsFatal() const { return false; };

  template <typename T>
  RayLogBase &operator<<(const T &t) {
    if (IsEnabled()) {
      Stream() << t;
    }
    if (IsFatal()) {
      ExposeStream() << t;
    }
    return *this;
  }

 protected:
  virtual std::ostream &Stream() { return std::cerr; };
  virtual std::ostream &ExposeStream() { return std::cerr; };
};

/// Callback function which will be triggered to expose fatal log.
/// The first argument: a string representing log type or label.
/// The second argument: log content.
using FatalLogCallback = std::function<void(const std::string &, const std::string &)>;

class RayLog : public RayLogBase {
 public:
  RayLog(const char *file_name, int line_number, RayLogLevel severity);

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
  /// If `StartRayLog` wasn't called before, it will be no-op.
  static void ShutDownRayLog();

  /// Uninstall the signal actions installed by InstallFailureSignalHandler.
  static void UninstallSignalAction();

  /// Return whether or not the log level is enabled in current setting.
  ///
  /// \param log_level The input log level to test.
  /// \return True if input log level is not lower than the threshold.
  static bool IsLevelEnabled(RayLogLevel log_level);

  /// Install the failure signal handler to output call stack when crash.
  ///
  /// \param argv0 This is the argv[0] supplied to main(). It enables an alternative way
  /// to locate the object file containing debug symbols for ELF format executables. If
  /// this is left as nullptr, symbolization can fail in some cases. More details in:
  /// https://github.com/abseil/abseil-cpp/blob/master/absl/debugging/symbolize_elf.inc
  /// \parem call_previous_handler Whether to call the previous signal handler. See
  /// important caveats:
  /// https://github.com/abseil/abseil-cpp/blob/7e446075d4aff4601c1e7627c7c0be2c4833a53a/absl/debugging/failure_signal_handler.h#L76-L88
  /// This is currently used to enable signal handler from both Python and C++ in Python
  /// worker.
  static void InstallFailureSignalHandler(const char *argv0,
                                          bool call_previous_handler = false);

  /// Install the terminate handler to output call stack when std::terminate() is called
  /// (e.g. unhandled exception).
  static void InstallTerminateHandler();

  /// To check failure signal handler enabled or not.
  static bool IsFailureSignalHandlerEnabled();

  /// Get the log level from environment variable.
  static RayLogLevel GetLogLevelFromEnv();

  static std::string GetLogFormatPattern();

  static std::string GetLoggerName();

  /// Add callback functions that will be triggered to expose fatal log.
  static void AddFatalLogCallbacks(
      const std::vector<FatalLogCallback> &expose_log_callbacks);

 private:
  FRIEND_TEST(PrintLogTest, TestRayLogEveryNOrDebug);
  FRIEND_TEST(PrintLogTest, TestRayLogEveryN);
  // Hide the implementation of log provider by void *.
  // Otherwise, lib user may define the same macro to use the correct header file.
  void *logging_provider_;
  /// True if log messages should be logged and false if they should be ignored.
  bool is_enabled_;
  /// log level.
  RayLogLevel severity_;
  /// Whether current log is fatal or not.
  bool is_fatal_ = false;
  /// String stream of exposed log content.
  std::shared_ptr<std::ostringstream> expose_osstream_ = nullptr;
  /// Whether or not the log is initialized.
  static std::atomic<bool> initialized_;
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

 protected:
  virtual std::ostream &Stream();
  virtual std::ostream &ExposeStream();
};

// This class make RAY_CHECK compilation pass to change the << operator to void.
class Voidify {
 public:
  Voidify() {}
  // This has to be an operator with a precedence lower than << but
  // higher than ?:
  void operator&(RayLogBase &) {}
};

}  // namespace ray
