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

#include <ray/api/logging.h>

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
#include <sstream>

#ifdef RAY_USE_SPDLOG
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
#endif

#include "absl/debugging/stacktrace.h"
#include "absl/debugging/symbolize.h"
#include "boost/filesystem.hpp"
// #include "ray/util/filesystem.h"

namespace ray {
namespace api {
CppRayLogLevel CppRayLog::severity_threshold_ = CppRayLogLevel::INFO;
std::string CppRayLog::app_name_ = "";
std::string CppRayLog::log_dir_ = "";
// Format pattern is 2020-08-21 17:00:00,000 I 100 1001 msg.
// %L is loglevel, %P is process id, %t for thread id.
std::string CppRayLog::log_format_pattern_ = "[%Y-%m-%d %H:%M:%S,%e %L %P %t] %v";
std::string CppRayLog::logger_name_ = "ray_log_sink";
long CppRayLog::log_rotation_max_size_ = 1 << 29;
long CppRayLog::log_rotation_file_num_ = 10;
bool CppRayLog::is_failure_signal_handler_installed_ = false;

std::string GetCallTrace() {
  std::vector<void *> local_stack;
  local_stack.resize(50);
  absl::GetStackTrace(local_stack.data(), 50, 1);
  static constexpr size_t buf_size = 1 << 14;
  std::unique_ptr<char[]> buf(new char[buf_size]);
  std::string output;
  for (auto &stack : local_stack) {
    if (absl::Symbolize(stack, buf.get(), buf_size)) {
      output.append("    ").append(buf.get()).append("\n");
    }
  }
  return output;
}

/// NOTE(lingxuan.zlx): we reuse glog const_basename function from its utils.
inline const char *ConstBasename(const char *filepath) {
  const char *base = strrchr(filepath, '/');
#ifdef OS_WINDOWS  // Look for either path separator in Windows
  if (!base) base = strrchr(filepath, '\\');
#endif
  return base ? (base + 1) : filepath;
}

/// A logger that prints logs to stderr.
/// This is the default logger if logging is not initialized.
/// NOTE(lingxuan.zlx): Default stderr logger must be singleton and global
/// variable so core worker process can invoke `CPP_LOG` in its whole lifecyle.
class DefaultStdErrLogger final {
 public:
  std::shared_ptr<spdlog::logger> GetDefaultLogger() { return default_stderr_logger_; }

  static DefaultStdErrLogger &Instance() {
    static DefaultStdErrLogger instance;
    return instance;
  }

 private:
  DefaultStdErrLogger() {
    default_stderr_logger_ = spdlog::stderr_color_mt("stderr");
    default_stderr_logger_->set_pattern(CppRayLog::GetLogFormatPattern());
  }
  ~DefaultStdErrLogger() = default;
  DefaultStdErrLogger(DefaultStdErrLogger const &) = delete;
  DefaultStdErrLogger(DefaultStdErrLogger &&) = delete;
  std::shared_ptr<spdlog::logger> default_stderr_logger_;
};

class SpdLogMessage final {
 public:
  explicit SpdLogMessage(const char *file, int line, int loglevel) : loglevel_(loglevel) {
    stream() << ConstBasename(file) << ":" << line << ": ";
  }

  inline void Flush() {
    auto logger = spdlog::get(CppRayLog::GetLoggerName());
    if (!logger) {
      logger = DefaultStdErrLogger::Instance().GetDefaultLogger();
    }
    // To avoid dump duplicated stacktrace with installed failure signal
    // handler, we have to check whether glog failure signal handler is enabled.
    if (!CppRayLog::IsFailureSignalHandlerEnabled() &&
        loglevel_ == static_cast<int>(spdlog::level::critical)) {
      stream() << "\n*** StackTrace Information ***\n" << GetCallTrace();
    }
    // NOTE(lingxuan.zlx): See more fmt by visiting https://github.com/fmtlib/fmt.
    logger->log(static_cast<spdlog::level::level_enum>(loglevel_), /*fmt*/ "{}",
                str_.str());
    logger->flush();
    if (loglevel_ == static_cast<int>(spdlog::level::critical)) {
      // For keeping same action with glog, process will be abort if it's fatal log.
      std::abort();
    }
  }

  ~SpdLogMessage() { Flush(); }
  inline std::ostream &stream() { return str_; }

 private:
  SpdLogMessage(const SpdLogMessage &) = delete;
  SpdLogMessage &operator=(const SpdLogMessage &) = delete;

 private:
  std::ostringstream str_;
  int loglevel_;
};

// This is the default implementation of ray log,
// which is independent of any libs.
class CerrLog {
 public:
  CerrLog(CppRayLogLevel severity) : severity_(severity), has_logged_(false) {}

  virtual ~CerrLog() {
    if (has_logged_) {
      std::cerr << std::endl;
    }
    if (severity_ == CppRayLogLevel::FATAL) {
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
    if (severity_ != CppRayLogLevel::DEBUG) {
      has_logged_ = true;
      std::cerr << t;
    }
    return *this;
  }

 protected:
  const CppRayLogLevel severity_;
  bool has_logged_;

  void PrintBackTrace() {
#if defined(_EXECINFO_H) || !defined(_WIN32)
    void *buffer[255];
    const int calls = backtrace(buffer, sizeof(buffer) / sizeof(void *));
    backtrace_symbols_fd(buffer, calls, 1);
#endif
  }
};

#ifdef RAY_USE_SPDLOG
typedef SpdLogMessage LoggingProvider;
#else
typedef CerrLog LoggingProvider;
#endif

#ifdef RAY_USE_SPDLOG
// Spdlog's severity map.
static int GetMappedSeverity(CppRayLogLevel severity) {
  switch (severity) {
  case CppRayLogLevel::TRACE:
    return spdlog::level::trace;
  case CppRayLogLevel::DEBUG:
    return spdlog::level::debug;
  case CppRayLogLevel::INFO:
    return spdlog::level::info;
  case CppRayLogLevel::WARNING:
    return spdlog::level::warn;
  case CppRayLogLevel::ERROR:
    return spdlog::level::err;
  case CppRayLogLevel::FATAL:
    return spdlog::level::critical;
  default:
    CPP_LOG(FATAL) << "Unsupported logging level: " << static_cast<int>(severity);
    // This return won't be hit but compiler needs it.
    return spdlog::level::off;
  }
}
#endif

void CppRayLog::StartRayLog(const std::string &app_name,
                            CppRayLogLevel severity_threshold,
                            const std::string &log_dir) {
  const char *var_value = getenv("RAY_BACKEND_LOG_LEVEL");
  if (var_value != nullptr) {
    std::string data = var_value;
    std::transform(data.begin(), data.end(), data.begin(), ::tolower);
    if (data == "trace") {
      severity_threshold = CppRayLogLevel::TRACE;
    } else if (data == "debug") {
      severity_threshold = CppRayLogLevel::DEBUG;
    } else if (data == "info") {
      severity_threshold = CppRayLogLevel::INFO;
    } else if (data == "warning") {
      severity_threshold = CppRayLogLevel::WARNING;
    } else if (data == "error") {
      severity_threshold = CppRayLogLevel::ERROR;
    } else if (data == "fatal") {
      severity_threshold = CppRayLogLevel::FATAL;
    } else {
      CPP_LOG(WARNING) << "Unrecognized setting of RAY_BACKEND_LOG_LEVEL=" << var_value;
    }
    CPP_LOG(INFO) << "Set ray log level from environment variable RAY_BACKEND_LOG_LEVEL"
                  << " to " << static_cast<int>(severity_threshold);
  }
  severity_threshold_ = severity_threshold;
  app_name_ = app_name;
  log_dir_ = log_dir;
#if defined(RAY_USE_SPDLOG)
  if (!log_dir_.empty()) {
    // Enable log file if log_dir_ is not empty.
    std::string dir_ends_with_slash = log_dir_;
    if (log_dir_[log_dir_.length() - 1] != boost::filesystem::path::separator) {
      dir_ends_with_slash += boost::filesystem::path::separator;
    }

    std::string app_name_without_path = app_name;
    if (app_name.empty()) {
      app_name_without_path = "DefaultApp";
    } else {
      // Find the app name without the path.
      std::string app_file_name = boost::filesystem::path(app_name).filename().string();
      if (!app_file_name.empty()) {
        app_name_without_path = app_file_name;
      }
    }

#ifdef _WIN32
    int pid = _getpid();
#else
    pid_t pid = getpid();
#endif
    // Reset log pattern and level and we assume a log file can be rotated with
    // 10 files in max size 512M by default.
    if (getenv("RAY_ROTATION_MAX_BYTES")) {
      long max_size = std::atol(getenv("RAY_ROTATION_MAX_BYTES"));
      // 0 means no log rotation in python, but not in spdlog. We just use the default
      // value here.
      if (max_size != 0) {
        log_rotation_max_size_ = max_size;
      }
    }
    if (getenv("RAY_ROTATION_BACKUP_COUNT")) {
      long file_num = std::atol(getenv("RAY_ROTATION_BACKUP_COUNT"));
      if (file_num != 0) {
        log_rotation_file_num_ = file_num;
      }
    }
    spdlog::set_pattern(log_format_pattern_);
    spdlog::set_level(static_cast<spdlog::level::level_enum>(severity_threshold_));
    // Sink all log stuff to default file logger we defined here. We may need
    // multiple sinks for different files or loglevel.
    auto file_logger = spdlog::get(CppRayLog::GetLoggerName());
    if (file_logger) {
      // Drop this old logger first if we need reset filename or reconfig
      // logger.
      spdlog::drop(CppRayLog::GetLoggerName());
    }
    file_logger = spdlog::rotating_logger_mt(
        CppRayLog::GetLoggerName(),
        dir_ends_with_slash + app_name_without_path + "_" + std::to_string(pid) + ".log",
        log_rotation_max_size_, log_rotation_file_num_);
    spdlog::set_default_logger(file_logger);

  } else {
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_pattern(log_format_pattern_);
    auto level = static_cast<spdlog::level::level_enum>(severity_threshold_);
    console_sink->set_level(level);

    auto err_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    err_sink->set_pattern(log_format_pattern_);
    err_sink->set_level(spdlog::level::err);

    auto logger = std::shared_ptr<spdlog::logger>(
        new spdlog::logger(CppRayLog::GetLoggerName(), {console_sink, err_sink}));
    logger->set_level(level);
    spdlog::set_default_logger(logger);
  }
#endif
}

void CppRayLog::UninstallSignalAction() {
#if defined(RAY_USE_GLOG) || defined(RAY_USE_SPDLOG)
  if (!is_failure_signal_handler_installed_) {
    return;
  }
  CPP_LOG(DEBUG) << "Uninstall signal handlers.";
  // This signal list comes from glog's signalhandler.cc.
  // https://github.com/google/glog/blob/master/src/signalhandler.cc#L58-L70
  std::vector<int> installed_signals({SIGSEGV, SIGILL, SIGFPE, SIGABRT, SIGTERM});
#ifdef _WIN32  // Do NOT use WIN32 (without the underscore); we want _WIN32 here
  for (int signal_num : installed_signals) {
    CPP_CHECK(signal(signal_num, SIG_DFL) != SIG_ERR);
  }
#else
  struct sigaction sig_action;
  memset(&sig_action, 0, sizeof(sig_action));
  sigemptyset(&sig_action.sa_mask);
  sig_action.sa_handler = SIG_DFL;
  for (int signal_num : installed_signals) {
    CPP_CHECK(sigaction(signal_num, &sig_action, NULL) == 0);
  }
#endif
  is_failure_signal_handler_installed_ = false;
#endif
}

void CppRayLog::ShutDownRayLog() {
#if defined(RAY_USE_GLOG) || defined(RAY_USE_SPDLOG)
  UninstallSignalAction();
#endif
#if defined(RAY_USE_SPDLOG)
  if (spdlog::default_logger()) {
    spdlog::default_logger()->flush();
  }
  // NOTE(lingxuan.zlx) All loggers will be closed in shutdown but we don't need drop
  // console logger out because of some console logging might be used after shutdown ray
  // log. spdlog::shutdown();
#endif
}

void WriteFailureMessage(const char *data, int size) {
  // The data & size represent one line failure message.
  // The second parameter `size-1` means we should strip last char `\n`
  // for pretty printing.
  if (nullptr != data && size > 0) {
    CPP_LOG(ERROR) << std::string(data, size - 1);
  }
#ifdef RAY_USE_SPDLOG
  // If logger writes logs to files, logs are fully-buffered, which is different from
  // stdout (line-buffered) and stderr (unbuffered). So always flush here in case logs are
  // lost when logger writes logs to files.
  if (spdlog::default_logger()) {
    spdlog::default_logger()->flush();
  }
#endif
}

bool CppRayLog::IsFailureSignalHandlerEnabled() {
  return is_failure_signal_handler_installed_;
}

void CppRayLog::InstallFailureSignalHandler() {
#ifdef _WIN32
  // If process fails to initialize, don't display an error window.
  SetErrorMode(GetErrorMode() | SEM_FAILCRITICALERRORS);
  // If process crashes, don't display an error window.
  SetErrorMode(GetErrorMode() | SEM_NOGPFAULTERRORBOX);
#endif
}

bool CppRayLog::IsLevelEnabled(CppRayLogLevel log_level) {
  return log_level >= severity_threshold_;
}

std::string CppRayLog::GetLogFormatPattern() { return log_format_pattern_; }

std::string CppRayLog::GetLoggerName() { return logger_name_; }

CppRayLog::CppRayLog(const char *file_name, int line_number, CppRayLogLevel severity)
    // glog does not have DEBUG level, we can handle it using is_enabled_.
    : logging_provider_(nullptr), is_enabled_(severity >= severity_threshold_) {
#if defined(RAY_USE_GLOG) || defined(RAY_USE_SPDLOG)
  if (is_enabled_) {
    logging_provider_ =
        new LoggingProvider(file_name, line_number, GetMappedSeverity(severity));
  }
#else
  auto logging_provider = new CerrLog(severity);
  *logging_provider << file_name << ":" << line_number << ": ";
  logging_provider_ = logging_provider;
#endif
}

std::ostream &CppRayLog::Stream() {
  auto logging_provider = reinterpret_cast<LoggingProvider *>(logging_provider_);
  // Before calling this function, user should check IsEnabled.
  // When IsEnabled == false, logging_provider_ will be empty.
  return logging_provider->stream();
}

bool CppRayLog::IsEnabled() const { return is_enabled_; }

CppRayLog::~CppRayLog() {
  if (logging_provider_ != nullptr) {
    delete reinterpret_cast<LoggingProvider *>(logging_provider_);
    logging_provider_ = nullptr;
  }
}

}  // namespace api
}  // namespace ray
