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
#include <sstream>

#if defined(RAY_USE_GLOG) || defined(RAY_USE_SPDLOG)
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

#ifdef RAY_USE_SPDLOG
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
#endif

#include "ray/util/filesystem.h"

namespace ray {

std::string GetCallTrace() {
  std::string return_message = "Cannot get callstack information.";
#if defined(RAY_USE_GLOG) || defined(RAY_USE_SPDLOG)
  return google::GetStackTraceToString();
#endif
  return return_message;
}

#ifdef RAY_USE_GLOG
struct StdoutLogger : public google::base::Logger {
  std::ostream &out() { return std::cout; }

  virtual void Write(bool /* should flush */, time_t /* timestamp */, const char *message,
                     int length) {
    // note: always flush otherwise it never shows up in raylet.out
    out().write(message, length) << std::flush;
  }

  virtual void Flush() { out().flush(); }

  virtual google::uint32 LogSize() { return 0; }
};

static StdoutLogger stdout_logger_singleton;
#endif

#ifdef RAY_USE_SPDLOG
/// NOTE(lingxuan.zlx): we reuse glog const_basename function from its utils.
inline const char *ConstBasename(const char *filepath) {
  const char *base = strrchr(filepath, '/');
#ifdef OS_WINDOWS  // Look for either path separator in Windows
  if (!base) base = strrchr(filepath, '\\');
#endif
  return base ? (base + 1) : filepath;
}

class SpdLogMessage final {
 public:
  explicit SpdLogMessage(const char *file, int line, int loglevel) : loglevel_(loglevel) {
    stream() << ConstBasename(file) << ":" << line << ": ";
  }
  inline std::shared_ptr<spdlog::logger> GetDefaultLogger() {
    // We just emit all log informations to stderr when no default logger has been created
    // before starting ray log, which is for glog compatible.
    static auto logger = spdlog::stderr_color_mt("stderr");
    logger->set_pattern(RayLog::GetLogFormatPattern());
    return logger;
  }

  inline void Flush() {
    auto logger = spdlog::get(RayLog::GetLoggerName());
    if (!logger) {
      logger = GetDefaultLogger();
    }
    // To avoid dump duplicated stacktrace with installed failure signal
    // handler, we have to check whether glog failure signal handler is enabled.
    if (!RayLog::IsFailureSignalHandlerEnabled() &&
        loglevel_ == static_cast<int>(spdlog::level::critical)) {
      stream() << "\n*** StackTrace Information ***\n" << ray::GetCallTrace();
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
  std::ostringstream str_;
  int loglevel_;

  SpdLogMessage(const SpdLogMessage &) = delete;
  SpdLogMessage &operator=(const SpdLogMessage &) = delete;
};
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
#elif defined(RAY_USE_SPDLOG)
typedef ray::SpdLogMessage LoggingProvider;
#else
typedef ray::CerrLog LoggingProvider;
#endif

RayLogLevel RayLog::severity_threshold_ = RayLogLevel::INFO;
std::string RayLog::app_name_ = "";
std::string RayLog::log_dir_ = "";
// Format pattern is 2020-08-21 17:00:00,000 I 100 1001 msg.
// %L is loglevel, %P is process id, %t for thread id.
std::string RayLog::log_format_pattern_ = "[%Y-%m-%d %H:%M:%S,%e %L %P %t] %v";
std::string RayLog::logger_name_ = "ray_log_sink";
long RayLog::log_rotation_max_size_ = 1 << 29;
long RayLog::log_rotation_file_num_ = 10;
bool RayLog::is_failure_signal_handler_installed_ = false;

#ifdef RAY_USE_GLOG
using namespace google;

// Glog's severity map.
static int GetMappedSeverity(RayLogLevel severity) {
  switch (severity) {
  case RayLogLevel::TRACE:
  case RayLogLevel::DEBUG:
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

#elif defined(RAY_USE_SPDLOG)
// Spdlog's severity map.
static int GetMappedSeverity(RayLogLevel severity) {
  switch (severity) {
  case RayLogLevel::TRACE:
    return spdlog::level::trace;
  case RayLogLevel::DEBUG:
    return spdlog::level::debug;
  case RayLogLevel::INFO:
    return spdlog::level::info;
  case RayLogLevel::WARNING:
    return spdlog::level::warn;
  case RayLogLevel::ERROR:
    return spdlog::level::err;
  case RayLogLevel::FATAL:
    return spdlog::level::critical;
  default:
    RAY_LOG(FATAL) << "Unsupported logging level: " << static_cast<int>(severity);
    // This return won't be hit but compiler needs it.
    return spdlog::level::off;
  }
}
#endif

void RayLog::StartRayLog(const std::string &app_name, RayLogLevel severity_threshold,
                         const std::string &log_dir) {
  const char *var_value = getenv("RAY_BACKEND_LOG_LEVEL");
  if (var_value != nullptr) {
    std::string data = var_value;
    std::transform(data.begin(), data.end(), data.begin(), ::tolower);
    if (data == "trace") {
      severity_threshold = RayLogLevel::TRACE;
    } else if (data == "debug") {
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
#if defined(RAY_USE_GLOG) || defined(RAY_USE_SPDLOG)
#ifdef RAY_USE_GLOG
  google::InitGoogleLogging(app_name_.c_str());
  int level = GetMappedSeverity(static_cast<RayLogLevel>(severity_threshold_));
#endif
  if (!log_dir_.empty()) {
    // Enable log file if log_dir_ is not empty.
    std::string dir_ends_with_slash = log_dir_;
    if (!ray::IsDirSep(log_dir_[log_dir_.length() - 1])) {
      dir_ends_with_slash += ray::GetDirSep();
    }
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
#ifdef RAY_USE_GLOG
    app_name_without_path += ".";
    google::SetLogFilenameExtension(app_name_without_path.c_str());
    google::SetLogDestination(level, dir_ends_with_slash.c_str());
    FLAGS_stop_logging_if_full_disk = true;
#else
#ifdef _WIN32
    int pid = _getpid();
#else
    pid_t pid = getpid();
#endif
    // Reset log pattern and level and we assume a log file can be rotated with
    // 10 files in max size 512M by default.
    if (getenv("RAY_ROTATION_MAX_SIZE")) {
      log_rotation_max_size_ = std::atol(getenv("RAY_RAOTATION_MAX_SIZE"));
    }
    if (getenv("RAY_ROTATION_FILE_NUM")) {
      log_rotation_file_num_ = std::atol(getenv("RAY_ROTATION_FILE_NUM"));
    }
    spdlog::set_pattern(log_format_pattern_);
    spdlog::set_level(static_cast<spdlog::level::level_enum>(severity_threshold_));
    // Sink all log stuff to default file logger we defined here. We may need
    // multiple sinks for different files or loglevel.
    auto file_logger = spdlog::get(RayLog::GetLoggerName());
    if (file_logger) {
      // Drop this old logger first if we need reset filename or reconfig
      // logger.
      spdlog::drop(RayLog::GetLoggerName());
    }
    file_logger = spdlog::rotating_logger_mt(
        RayLog::GetLoggerName(),
        dir_ends_with_slash + app_name_without_path + "_" + std::to_string(pid) + ".log",
        log_rotation_max_size_, log_rotation_file_num_);
    spdlog::set_default_logger(file_logger);
#endif
  } else {
#ifdef RAY_USE_GLOG
    // NOTE(lingxuan.zlx): If no specific log dir or empty directory string,
    // we use stdout by default.
    google::base::SetLogger(level, &stdout_logger_singleton);
#else
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_pattern(log_format_pattern_);
    auto level = static_cast<spdlog::level::level_enum>(severity_threshold_);
    console_sink->set_level(level);

    auto err_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    err_sink->set_pattern(log_format_pattern_);
    err_sink->set_level(spdlog::level::err);

    auto logger = std::shared_ptr<spdlog::logger>(
        new spdlog::logger(RayLog::GetLoggerName(), {console_sink, err_sink}));
    logger->set_level(level);
    spdlog::set_default_logger(logger);
#endif
  }
#ifdef RAY_USE_GLOG
  for (int i = GLOG_INFO; i <= GLOG_FATAL; ++i) {
    if (i != level) {
      // NOTE(lingxuan.zlx): It means nothing can be printed or sinked to pass
      // an empty destination.
      // Reference from glog:
      // https://github.com/google/glog/blob/0a2e5931bd5ff22fd3bf8999eb8ce776f159cda6/src/logging.cc#L1110
      google::SetLogDestination(i, "");
    }
  }
  google::SetStderrLogging(GetMappedSeverity(RayLogLevel::ERROR));
#endif
#endif
}

void RayLog::UninstallSignalAction() {
#if defined(RAY_USE_GLOG) || defined(RAY_USE_SPDLOG)
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
#if defined(RAY_USE_GLOG) || defined(RAY_USE_SPDLOG)
  UninstallSignalAction();
#endif
#if defined(RAY_USE_GLOG)
  google::ShutdownGoogleLogging();
#elif defined(RAY_USE_SPDLOG)
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
    RAY_LOG(ERROR) << std::string(data, size - 1);
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

bool RayLog::IsFailureSignalHandlerEnabled() {
  return is_failure_signal_handler_installed_;
}

void RayLog::InstallFailureSignalHandler() {
#ifdef _WIN32
  // If process fails to initialize, don't display an error window.
  SetErrorMode(GetErrorMode() | SEM_FAILCRITICALERRORS);
  // If process crashes, don't display an error window.
  SetErrorMode(GetErrorMode() | SEM_NOGPFAULTERRORBOX);
#endif
#if defined(RAY_USE_GLOG) || defined(RAY_USE_SPDLOG)
  if (is_failure_signal_handler_installed_) {
    return;
  }
  google::InstallFailureSignalHandler();
  google::InstallFailureWriter(&WriteFailureMessage);
  is_failure_signal_handler_installed_ = true;
#endif
}

bool RayLog::IsLevelEnabled(RayLogLevel log_level) {
  return log_level >= severity_threshold_;
}

std::string RayLog::GetLogFormatPattern() { return log_format_pattern_; }

std::string RayLog::GetLoggerName() { return logger_name_; }

RayLog::RayLog(const char *file_name, int line_number, RayLogLevel severity)
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

std::ostream &RayLog::Stream() {
  auto logging_provider = reinterpret_cast<LoggingProvider *>(logging_provider_);
  // Before calling this function, user should check IsEnabled.
  // When IsEnabled == false, logging_provider_ will be empty.
  return logging_provider->stream();
}

bool RayLog::IsEnabled() const { return is_enabled_; }

RayLog::~RayLog() {
  if (logging_provider_ != nullptr) {
    delete reinterpret_cast<LoggingProvider *>(logging_provider_);
    logging_provider_ = nullptr;
  }
}

}  // namespace ray
