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

#include "ray/util/logging.h"

#ifdef _WIN32
#include <process.h>
#else
#include <execinfo.h>
#endif

#include <signal.h>
#include <stdlib.h>
#ifndef _WIN32
#include <unistd.h>
#endif

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>

#ifdef RAY_USE_GLOG
#include <sys/stat.h>
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4722)  // Ignore non-returning destructor warning in GLOG
#endif
#include "glog/logging.h"
#ifdef _MSC_VER
#pragma warning(pop)
#endif
#endif

#include "ray/util/filesystem.h"

namespace ray {

#ifdef RAY_USE_GLOG
struct StreamLogger : public google::base::Logger {
  std::ofstream out_;

  std::ostream &out() { return out_.is_open() ? out_ : std::cout; }

  virtual void Write(bool /* should flush */, time_t /* timestamp */, const char *message,
                     int length) {
    // note: always flush otherwise it never shows up in raylet.out
    out().write(message, length) << std::flush;
  }

  virtual void Flush() { out().flush(); }

  virtual google::uint32 LogSize() { return 0; }
};

static StreamLogger stream_logger_singleton;
#endif

// This is the default implementation of ray log,
// which is independent of any libs.
class CerrLog {
 public:
  CerrLog(RayLogLevel severity) : severity_(severity), has_logged_(false) {}

  virtual ~CerrLog() {
    if (has_logged_) {
      std::cerr << std::endl;
    }
    if (severity_ == RayLogLevel::FATAL) {
      PrintBackTrace();
      std::abort();
    }
  }

  std::ostream &Stream() {
    has_logged_ = true;
    return std::cerr;
  }

  template <class T>
  CerrLog &operator<<(const T &t) {
    if (severity_ != RayLogLevel::DEBUG) {
      has_logged_ = true;
      std::cerr << t;
    }
    return *this;
  }

 protected:
  const RayLogLevel severity_;
  bool has_logged_;

  void PrintBackTrace() {
#if defined(_EXECINFO_H) || !defined(_WIN32)
    void *buffer[255];
    const int calls = backtrace(buffer, sizeof(buffer) / sizeof(void *));
    backtrace_symbols_fd(buffer, calls, 1);
#endif
  }
};

#ifdef RAY_USE_GLOG
typedef google::LogMessage LoggingProvider;
#else
typedef ray::CerrLog LoggingProvider;
#endif

RayLogLevel RayLog::severity_threshold_ = RayLogLevel::INFO;
std::string RayLog::app_name_ = "";
std::string RayLog::log_dir_ = "";
bool RayLog::is_failure_signal_handler_installed_ = false;

#ifdef RAY_USE_GLOG
using namespace google;

// Glog's severity map.
static int GetMappedSeverity(RayLogLevel severity) {
  switch (severity) {
  case RayLogLevel::DEBUG:
    return GLOG_INFO;
  case RayLogLevel::INFO:
    return GLOG_INFO;
  case RayLogLevel::WARNING:
    return GLOG_WARNING;
  case RayLogLevel::ERROR:
    return GLOG_ERROR;
  case RayLogLevel::FATAL:
    return GLOG_FATAL;
  default:
    RAY_LOG(FATAL) << "Unsupported logging level: " << static_cast<int>(severity);
    // This return won't be hit but compiler needs it.
    return GLOG_FATAL;
  }
}

#endif

void RayLog::StartRayLog(const std::string &app_name, RayLogLevel severity_threshold,
                         const std::string &log_dir) {
  const char *var_value = getenv("RAY_BACKEND_LOG_LEVEL");
  if (var_value != nullptr) {
    std::string data = var_value;
    std::transform(data.begin(), data.end(), data.begin(), ::tolower);
    if (data == "debug") {
      severity_threshold = RayLogLevel::DEBUG;
    } else if (data == "info") {
      severity_threshold = RayLogLevel::INFO;
    } else if (data == "warning") {
      severity_threshold = RayLogLevel::WARNING;
    } else if (data == "error") {
      severity_threshold = RayLogLevel::ERROR;
    } else if (data == "fatal") {
      severity_threshold = RayLogLevel::FATAL;
    } else {
      RAY_LOG(WARNING) << "Unrecognized setting of RAY_BACKEND_LOG_LEVEL=" << var_value;
    }
    RAY_LOG(INFO) << "Set ray log level from environment variable RAY_BACKEND_LOG_LEVEL"
                  << " to " << static_cast<int>(severity_threshold);
  }
  severity_threshold_ = severity_threshold;
  app_name_ = app_name;
  log_dir_ = log_dir;
#ifdef RAY_USE_GLOG
  google::InitGoogleLogging(app_name_.c_str());
  // Enable log file if log_dir_ is not empty.
  std::string dir_ends_with_slash = log_dir_;
  if (!ray::IsDirSep(log_dir_[log_dir_.length() - 1])) {
    dir_ends_with_slash += ray::GetDirSep();
  }
  if (!log_dir_.empty()) {
    std::string app_name_without_path = app_name;
    if (app_name.empty()) {
      app_name_without_path = "DefaultApp";
    } else {
      // Find the app name without the path.
      std::string app_file_name = ray::GetFileName(app_name);
      if (!app_file_name.empty()) {
        app_name_without_path = app_file_name;
      }
    }
    char buffer[80];
    time_t rawtime;
    time(&rawtime);
#ifdef _WIN32
    int pid = _getpid();
#else
    pid_t pid = getpid();
#endif
    strftime(buffer, sizeof(buffer), "%Y%m%d-%H%M%S", localtime(&rawtime));
    std::string path = dir_ends_with_slash + app_name_without_path + "." + buffer + "." +
                       std::to_string(pid) + ".log";
    stream_logger_singleton.out_.rdbuf()->pubsetbuf(0, 0);
    stream_logger_singleton.out_.open(path.c_str(),
                                      std::ios_base::app | std::ios_base::binary);
  }
  for (int lvl = 0; lvl < NUM_SEVERITIES; ++lvl) {
    google::base::SetLogger(lvl, &stream_logger_singleton);
  }
  google::SetStderrLogging(GetMappedSeverity(RayLogLevel::ERROR));
#endif
}

void RayLog::UninstallSignalAction() {
#ifdef RAY_USE_GLOG
  if (!is_failure_signal_handler_installed_) {
    return;
  }
  RAY_LOG(DEBUG) << "Uninstall signal handlers.";
  // This signal list comes from glog's signalhandler.cc.
  // https://github.com/google/glog/blob/master/src/signalhandler.cc#L58-L70
  std::vector<int> installed_signals({SIGSEGV, SIGILL, SIGFPE, SIGABRT, SIGTERM});
#ifdef _WIN32  // Do NOT use WIN32 (without the underscore); we want _WIN32 here
  for (int signal_num : installed_signals) {
    RAY_CHECK(signal(signal_num, SIG_DFL) != SIG_ERR);
  }
#else
  struct sigaction sig_action;
  memset(&sig_action, 0, sizeof(sig_action));
  sigemptyset(&sig_action.sa_mask);
  sig_action.sa_handler = SIG_DFL;
  for (int signal_num : installed_signals) {
    RAY_CHECK(sigaction(signal_num, &sig_action, NULL) == 0);
  }
#endif
  is_failure_signal_handler_installed_ = false;
#endif
}

void RayLog::ShutDownRayLog() {
  stream_logger_singleton.out_.close();
#ifdef RAY_USE_GLOG
  UninstallSignalAction();
  google::ShutdownGoogleLogging();
#endif
}

void RayLog::InstallFailureSignalHandler() {
#ifdef _WIN32
  // If process fails to initialize, don't display an error window.
  SetErrorMode(GetErrorMode() | SEM_FAILCRITICALERRORS);
  // If process crashes, don't display an error window.
  SetErrorMode(GetErrorMode() | SEM_NOGPFAULTERRORBOX);
#endif
#ifdef RAY_USE_GLOG
  if (is_failure_signal_handler_installed_) {
    return;
  }
  google::InstallFailureSignalHandler();
  is_failure_signal_handler_installed_ = true;
#endif
}

bool RayLog::IsLevelEnabled(RayLogLevel log_level) {
  return log_level >= severity_threshold_;
}

RayLog::RayLog(const char *file_name, int line_number, RayLogLevel severity)
    // glog does not have DEBUG level, we can handle it using is_enabled_.
    : logging_provider_(nullptr), is_enabled_(severity >= severity_threshold_) {
#ifdef RAY_USE_GLOG
  if (is_enabled_) {
    logging_provider_ =
        new google::LogMessage(file_name, line_number, GetMappedSeverity(severity));
  }
#else
  auto logging_provider = new CerrLog(severity);
  *logging_provider << file_name << ":" << line_number << ": ";
  logging_provider_ = logging_provider;
#endif
}

std::ostream &RayLog::Stream() {
  auto logging_provider = reinterpret_cast<LoggingProvider *>(logging_provider_);
#ifdef RAY_USE_GLOG
  // Before calling this function, user should check IsEnabled.
  // When IsEnabled == false, logging_provider_ will be empty.
  return logging_provider->stream();
#else
  return logging_provider->Stream();
#endif
}

bool RayLog::IsEnabled() const { return is_enabled_; }

RayLog::~RayLog() {
  if (logging_provider_ != nullptr) {
    delete reinterpret_cast<LoggingProvider *>(logging_provider_);
    logging_provider_ = nullptr;
  }
}

}  // namespace ray
