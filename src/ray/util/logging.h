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
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "ray/util/macros.h"
#include "ray/util/string_utils.h"

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
/// Sync with ray._private.ray_logging.constants.LogKey
inline constexpr std::string_view kLogKeyAsctime = "asctime";
inline constexpr std::string_view kLogKeyLevelname = "levelname";
inline constexpr std::string_view kLogKeyMessage = "message";
inline constexpr std::string_view kLogKeyFilename = "filename";
inline constexpr std::string_view kLogKeyLineno = "lineno";
inline constexpr std::string_view kLogKeyComponent = "component";
inline constexpr std::string_view kLogKeyJobID = "job_id";
inline constexpr std::string_view kLogKeyWorkerID = "worker_id";
inline constexpr std::string_view kLogKeyNodeID = "node_id";
inline constexpr std::string_view kLogKeyActorID = "actor_id";
inline constexpr std::string_view kLogKeyTaskID = "task_id";
inline constexpr std::string_view kLogKeyObjectID = "object_id";
inline constexpr std::string_view kLogKeyPlacementGroupID = "placement_group_id";

// Define your specialization DefaultLogKey<your_type>::key to get .WithField(t)
// See src/ray/common/id.h
template <typename T>
struct DefaultLogKey {};

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

// `cond` is a `Status` class, could be `ray::Status`, or from third-party like
// `grpc::Status`.
#define RAY_LOG_IF_ERROR(level, cond) \
  if (RAY_PREDICT_FALSE(!(cond).ok())) RAY_LOG(level)

#define RAY_IGNORE_EXPR(expr) ((void)(expr))

#define RAY_CHECK_WITH_DISPLAY(condition, display)                                \
  RAY_PREDICT_TRUE((condition))                                                   \
  ? RAY_IGNORE_EXPR(0)                                                            \
  : ::ray::Voidify() & ::ray::RayLog(__FILE__, __LINE__, ray::RayLogLevel::FATAL) \
                           << " Check failed: " display " "

#define RAY_CHECK(condition) RAY_CHECK_WITH_DISPLAY(condition, #condition)

#ifdef NDEBUG

#define RAY_DCHECK(condition)                                                     \
  RAY_PREDICT_TRUE((condition))                                                   \
  ? RAY_IGNORE_EXPR(0)                                                            \
  : ::ray::Voidify() & ::ray::RayLog(__FILE__, __LINE__, ray::RayLogLevel::ERROR) \
                           << " Debug check failed: " #condition " "
#else

#define RAY_DCHECK(condition) RAY_CHECK(condition)

#endif  // NDEBUG

#define RAY_CHECK_OP(left, op, right)        \
  if (const auto &_left_ = (left); true)     \
    if (const auto &_right_ = (right); true) \
  RAY_CHECK(RAY_PREDICT_TRUE(_left_ op _right_)) << " " << _left_ << " vs " << _right_

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

/// Callback function which will be triggered to expose fatal log.
/// The first argument: a string representing log type or label.
/// The second argument: log content.
using FatalLogCallback = std::function<void(const std::string &, const std::string &)>;

class RayLog {
 public:
  RayLog(const char *file_name, int line_number, RayLogLevel severity);

  ~RayLog();

  /// Return whether or not current logging instance is enabled.
  ///
  /// \return True if logging is enabled and false otherwise.
  bool IsEnabled() const;

  /// This function to judge whether current log is fatal or not.
  bool IsFatal() const;

  /// Get filepath to dump log from [log_dir] and [app_name].
  /// If [log_dir] empty, return empty filepath.
  static std::string GetLogFilepathFromDirectory(const std::string &log_dir,
                                                 const std::string &app_name);

  static std::string GetErrLogFilepathFromDirectory(const std::string &log_dir,
                                                    const std::string &app_name);

  /// The init function of ray log for a program which should be called only once.
  ///
  /// \parem appName The app name which starts the log.
  /// \param severity_threshold Logging threshold for the program.
  /// \param log_filepath Logging output filepath. If empty, the log won't output to file,
  /// but to stdout.
  /// \param err_log_filepath Logging error filepath. If empty, the log won't output to
  /// file, but to stderr.
  /// Because of log rotations, the logs be saved to log file names with `.<number>`
  /// suffixes.
  /// Example: if log_filepath is /my/path/raylet.out, the output can be
  /// /my/path/raylet.out, /my/path/raylet.out.1 and /my/path/raylet.out.2
  ///
  /// \param log_rotation_max_size max bytes for of log rotation. 0 means no rotation.
  /// \param log_rotation_file_num max number of rotating log files.
  static void StartRayLog(const std::string &app_name,
                          RayLogLevel severity_threshold = RayLogLevel::INFO,
                          const std::string &log_filepath = "",
                          const std::string &err_log_filepath = "",
                          size_t log_rotation_max_size = 0,
                          size_t log_rotation_file_num = 1);

  /// The shutdown function of ray log which should be used with StartRayLog as a pair.
  /// If `StartRayLog` wasn't called before, it will be no-op.
  static void ShutDownRayLog();

  /// Get max bytes value from env variable.
  /// Return default value, which indicates no rotation, if env not set, parse failure or
  /// return value 0.
  ///
  /// Log rotation is disable on windows platform.
  static size_t GetRayLogRotationMaxBytesOrDefault();

  /// Get log rotation backup count.
  /// Return default value, which indicates no rotation, if env not set, parse failure or
  /// return value 1.
  ///
  /// Log rotation is disabled on windows platform.
  static size_t GetRayLogRotationBackupCountOrDefault();

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

  static std::string GetLogFormatPattern();

  static std::string GetLoggerName();

  /// Add callback functions that will be triggered to expose fatal log.
  static void AddFatalLogCallbacks(
      const std::vector<FatalLogCallback> &expose_log_callbacks);

  template <typename T>
  RayLog &operator<<(const T &t) {
    if (IsEnabled()) {
      msg_osstream_ << t;
    }
    if (IsFatal()) {
      expose_fatal_osstream_ << t;
    }
    return *this;
  }

  /// Add log context to the log.
  /// Caller should make sure key is not duplicated
  /// and doesn't conflict with system keys like levelname.
  template <typename T>
  RayLog &WithField(std::string_view key, const T &value) {
    if (log_format_json_) {
      return WithFieldJsonFormat<T>(key, value);
    } else {
      return WithFieldTextFormat<T>(key, value);
    }
  }

  template <typename T>
  RayLog &WithField(const T &t) {
    return WithField(DefaultLogKey<T>::key, t);
  }

 private:
  FRIEND_TEST(PrintLogTest, TestRayLogEveryNOrDebug);
  FRIEND_TEST(PrintLogTest, TestRayLogEveryN);

  template <typename T>
  RayLog &WithFieldTextFormat(std::string_view key, const T &value) {
    context_osstream_ << " " << key << "=" << value;
    return *this;
  }

  template <typename T>
  RayLog &WithFieldJsonFormat(std::string_view key, const T &value) {
    std::stringstream ss;
    ss << value;
    return WithFieldJsonFormat<std::string>(key, ss.str());
  }

  static void InitSeverityThreshold(RayLogLevel severity_threshold);
  static void InitLogFormat();

  /// True if log messages should be logged and false if they should be ignored.
  bool is_enabled_;
  /// log level.
  RayLogLevel severity_;
  /// Whether current log is fatal or not.
  bool is_fatal_ = false;
  /// String stream of the log message
  std::ostringstream msg_osstream_;
  /// String stream of the log context: a list of key-value pairs.
  std::ostringstream context_osstream_;
  /// String stream of exposed fatal log content.
  std::ostringstream expose_fatal_osstream_;

  /// Whether or not the log is initialized.
  static std::atomic<bool> initialized_;
  /// Callback functions which will be triggered to expose fatal log.
  static std::vector<FatalLogCallback> fatal_log_callbacks_;
  static RayLogLevel severity_threshold_;
  static std::string app_name_;
  /// This is used when we log to stderr
  /// to indicate which component generates the log.
  /// This is empty if we log to file.
  static std::string component_name_;
  /// This flag is used to avoid calling UninstallSignalAction in ShutDownRayLog if
  /// InstallFailureSignalHandler was not called.
  static bool is_failure_signal_handler_installed_;
  /// Whether emit json logs.
  static bool log_format_json_;
  // Log format pattern.
  static std::string log_format_pattern_;
  // Log rotation file size limitation.
  inline static size_t log_rotation_max_size_ = 0;
  // Log rotation file number.
  inline static size_t log_rotation_file_num_ = 1;
  // Ray default logger name.
  static std::string logger_name_;

 protected:
  virtual std::ostream &Stream() { return msg_osstream_; }
};

template <>
RayLog &RayLog::WithFieldJsonFormat<std::string>(std::string_view key,
                                                 const std::string &value);
template <>
RayLog &RayLog::WithFieldJsonFormat<int>(std::string_view key, const int &value);

// This class make RAY_CHECK compilation pass to change the << operator to void.
class Voidify {
 public:
  Voidify() {}
  // This has to be an operator with a precedence lower than << but
  // higher than ?:
  void operator&(RayLog &) {}
};

}  // namespace ray
