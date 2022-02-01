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

#include <cstdlib>
#ifdef _WIN32
#include <process.h>
#else
#include <execinfo.h>
#endif
#include <signal.h>
#ifndef _WIN32
#include <unistd.h>
#endif

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>

#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/stacktrace.h"
#include "absl/debugging/symbolize.h"
#include "ray/util/event_label.h"
#include "ray/util/filesystem.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

namespace ray {

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
std::atomic<bool> RayLog::initialized_ = false;

std::string GetCallTrace() {
  std::vector<void *> local_stack;
  local_stack.resize(50);
  absl::GetStackTrace(local_stack.data(), 50, 0);
  static constexpr size_t buf_size = 16 * 1024;
  char buf[buf_size];
  std::string output;
  for (auto &stack : local_stack) {
    if (absl::Symbolize(stack, buf, buf_size)) {
      output.append("    ").append(buf).append("\n");
    }
  }
  return output;
}

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
/// variable so core worker process can invoke `RAY_LOG` in its whole lifecyle.
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
    default_stderr_logger_->set_pattern(RayLog::GetLogFormatPattern());
  }
  ~DefaultStdErrLogger() = default;
  DefaultStdErrLogger(DefaultStdErrLogger const &) = delete;
  DefaultStdErrLogger(DefaultStdErrLogger &&) = delete;
  std::shared_ptr<spdlog::logger> default_stderr_logger_;
};

class SpdLogMessage final {
 public:
  explicit SpdLogMessage(const char *file, int line, int loglevel,
                         std::shared_ptr<std::ostringstream> expose_osstream)
      : loglevel_(loglevel), expose_osstream_(expose_osstream) {
    stream() << ConstBasename(file) << ":" << line << ": ";
  }

  inline void Flush() {
    auto logger = spdlog::get(RayLog::GetLoggerName());
    if (!logger) {
      logger = DefaultStdErrLogger::Instance().GetDefaultLogger();
    }

    if (loglevel_ == static_cast<int>(spdlog::level::critical)) {
      stream() << "\n*** StackTrace Information ***\n" << ray::GetCallTrace();
    }
    if (expose_osstream_) {
      *expose_osstream_ << "\n*** StackTrace Information ***\n" << ray::GetCallTrace();
    }
    // NOTE(lingxuan.zlx): See more fmt by visiting https://github.com/fmtlib/fmt.
    logger->log(static_cast<spdlog::level::level_enum>(loglevel_), /*fmt*/ "{}",
                str_.str());
    logger->flush();
  }

  ~SpdLogMessage() { Flush(); }
  inline std::ostream &stream() { return str_; }

 private:
  SpdLogMessage(const SpdLogMessage &) = delete;
  SpdLogMessage &operator=(const SpdLogMessage &) = delete;

 private:
  std::ostringstream str_;
  int loglevel_;
  std::shared_ptr<std::ostringstream> expose_osstream_;
};

typedef ray::SpdLogMessage LoggingProvider;

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

std::vector<FatalLogCallback> RayLog::fatal_log_callbacks_;

void RayLog::StartRayLog(const std::string &app_name, RayLogLevel severity_threshold,
                         const std::string &log_dir) {
  const char *var_value = std::getenv("RAY_BACKEND_LOG_LEVEL");
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

  // All the logging sinks to add.
  std::vector<spdlog::sink_ptr> sinks;
  auto level = static_cast<spdlog::level::level_enum>(severity_threshold_);
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

  if (!log_dir_.empty()) {
    // Enable log file if log_dir_ is not empty.
#ifdef _WIN32
    int pid = _getpid();
#else
    pid_t pid = getpid();
#endif
    // Reset log pattern and level and we assume a log file can be rotated with
    // 10 files in max size 512M by default.
    if (std::getenv("RAY_ROTATION_MAX_BYTES")) {
      long max_size = std::atol(std::getenv("RAY_ROTATION_MAX_BYTES"));
      // 0 means no log rotation in python, but not in spdlog. We just use the default
      // value here.
      if (max_size != 0) {
        log_rotation_max_size_ = max_size;
      }
    }
    if (std::getenv("RAY_ROTATION_BACKUP_COUNT")) {
      long file_num = std::atol(std::getenv("RAY_ROTATION_BACKUP_COUNT"));
      if (file_num != 0) {
        log_rotation_file_num_ = file_num;
      }
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
    auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        JoinPaths(log_dir_, app_name_without_path + "_" + std::to_string(pid) + ".log"),
        log_rotation_max_size_, log_rotation_file_num_);
    sinks.push_back(file_sink);
  } else {
    // Format pattern is 2020-08-21 17:00:00,000 I 100 1001 msg.
    // %L is loglevel, %P is process id, %t for thread id.
    log_format_pattern_ =
        "[%Y-%m-%d %H:%M:%S,%e %L %P %t] (" + app_name_without_path + ") %v";
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_pattern(log_format_pattern_);
    console_sink->set_level(level);
    sinks.push_back(console_sink);
  }

  // In all cases, log errors to the console log so they are in driver logs.
  // https://github.com/ray-project/ray/issues/12893
  auto err_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
  err_sink->set_pattern(log_format_pattern_);
  err_sink->set_level(spdlog::level::err);
  sinks.push_back(err_sink);

  // Set the combined logger.
  auto logger = std::make_shared<spdlog::logger>(RayLog::GetLoggerName(), sinks.begin(),
                                                 sinks.end());
  logger->set_level(level);
  logger->set_pattern(log_format_pattern_);
  spdlog::set_level(static_cast<spdlog::level::level_enum>(severity_threshold_));
  spdlog::set_pattern(log_format_pattern_);
  spdlog::set_default_logger(logger);

  initialized_ = true;
}

void RayLog::UninstallSignalAction() {
  if (!is_failure_signal_handler_installed_) {
    return;
  }
  RAY_LOG(DEBUG) << "Uninstall signal handlers.";
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
}

void RayLog::ShutDownRayLog() {
  if (!initialized_) {
    // If the log wasn't initialized, make it no-op.
    RAY_LOG(INFO) << "The log wasn't initialized. ShutdownRayLog requests are ignored";
    return;
  }
  UninstallSignalAction();
  if (spdlog::default_logger()) {
    spdlog::default_logger()->flush();
  }
  // NOTE(lingxuan.zlx) All loggers will be closed in shutdown but we don't need drop
  // console logger out because of some console logging might be used after shutdown ray
  // log. spdlog::shutdown();
}

void WriteFailureMessage(const char *data) {
  // The data & size represent one line failure message.
  // The second parameter `size-1` means we should strip last char `\n`
  // for pretty printing.
  if (nullptr != data) {
    RAY_LOG(ERROR) << std::string(data, strlen(data) - 1);
  }

  // If logger writes logs to files, logs are fully-buffered, which is different from
  // stdout (line-buffered) and stderr (unbuffered). So always flush here in case logs are
  // lost when logger writes logs to files.
  if (spdlog::default_logger()) {
    spdlog::default_logger()->flush();
  }
}

bool RayLog::IsFailureSignalHandlerEnabled() {
  return is_failure_signal_handler_installed_;
}

void RayLog::InstallFailureSignalHandler(const char *argv0, bool call_previous_handler) {
#ifdef _WIN32
  // If process fails to initialize, don't display an error window.
  SetErrorMode(GetErrorMode() | SEM_FAILCRITICALERRORS);
  // If process crashes, don't display an error window.
  SetErrorMode(GetErrorMode() | SEM_NOGPFAULTERRORBOX);
#endif
  if (is_failure_signal_handler_installed_) {
    return;
  }
  absl::InitializeSymbolizer(argv0);
  absl::FailureSignalHandlerOptions options;
  options.call_previous_handler = call_previous_handler;
  options.writerfn = WriteFailureMessage;
  absl::InstallFailureSignalHandler(options);
  is_failure_signal_handler_installed_ = true;
}

bool RayLog::IsLevelEnabled(RayLogLevel log_level) {
  return log_level >= severity_threshold_;
}

std::string RayLog::GetLogFormatPattern() { return log_format_pattern_; }

std::string RayLog::GetLoggerName() { return logger_name_; }

void RayLog::AddFatalLogCallbacks(
    const std::vector<FatalLogCallback> &expose_log_callbacks) {
  fatal_log_callbacks_.insert(fatal_log_callbacks_.end(), expose_log_callbacks.begin(),
                              expose_log_callbacks.end());
}

RayLog::RayLog(const char *file_name, int line_number, RayLogLevel severity)
    : logging_provider_(nullptr),
      is_enabled_(severity >= severity_threshold_),
      severity_(severity),
      is_fatal_(severity == RayLogLevel::FATAL) {
  if (is_fatal_) {
    expose_osstream_ = std::make_shared<std::ostringstream>();
    *expose_osstream_ << file_name << ":" << line_number << ":";
  }
  if (is_enabled_) {
    logging_provider_ = new LoggingProvider(
        file_name, line_number, GetMappedSeverity(severity), expose_osstream_);
  }
}

std::ostream &RayLog::Stream() {
  auto logging_provider = reinterpret_cast<LoggingProvider *>(logging_provider_);
  // Before calling this function, user should check IsEnabled.
  // When IsEnabled == false, logging_provider_ will be empty.
  return logging_provider->stream();
}

bool RayLog::IsEnabled() const { return is_enabled_; }

bool RayLog::IsFatal() const { return is_fatal_; }

std::ostream &RayLog::ExposeStream() { return *expose_osstream_; }

RayLog::~RayLog() {
  if (logging_provider_ != nullptr) {
    delete reinterpret_cast<LoggingProvider *>(logging_provider_);
    logging_provider_ = nullptr;
  }
  if (expose_osstream_ != nullptr) {
    for (const auto &callback : fatal_log_callbacks_) {
      callback(EL_RAY_FATAL_CHECK_FAILED, expose_osstream_->str());
    }
  }
  if (severity_ == RayLogLevel::FATAL) {
    std::_Exit(EXIT_FAILURE);
  }
}

}  // namespace ray
