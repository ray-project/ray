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

#ifndef RAY_UTIL_LOGGING_H
#define RAY_UTIL_LOGGING_H

#include <iostream>
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
namespace ray {

enum class RayLogLevel { DEBUG = -1, INFO = 0, WARNING = 1, ERROR = 2, FATAL = 3 };

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

  template <typename T>
  RayLogBase &operator<<(const T &t) {
    if (IsEnabled()) {
      Stream() << t;
    }
    return *this;
  }

 protected:
  virtual std::ostream &Stream() { return std::cerr; };
};

class RayLog : public RayLogBase {
 public:
  RayLog(const char *file_name, int line_number, RayLogLevel severity);

  virtual ~RayLog();

  /// Return whether or not current logging instance is enabled.
  ///
  /// \return True if logging is enabled and false otherwise.
  virtual bool IsEnabled() const;

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
  /// If glog is not installed, this function won't do anything.
  static void InstallFailureSignalHandler();
  // Get the log level from environment variable.
  static RayLogLevel GetLogLevelFromEnv();

 private:
  // Hide the implementation of log provider by void *.
  // Otherwise, lib user may define the same macro to use the correct header file.
  void *logging_provider_;
  /// True if log messages should be logged and false if they should be ignored.
  bool is_enabled_;
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

 protected:
  virtual std::ostream &Stream();
};

// This class make RAY_CHECK compilation pass to change the << operator to void.
// This class is copied from glog.
class Voidify {
 public:
  Voidify() {}
  // This has to be an operator with a precedence lower than << but
  // higher than ?:
  void operator&(RayLogBase &) {}
};

}  // namespace ray

#endif  // RAY_UTIL_LOGGING_H
