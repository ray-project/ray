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

#include <string.h>

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
#include <array>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/debugging/failure_signal_handler.h"
#include "absl/debugging/stacktrace.h"
#include "absl/debugging/symbolize.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "nlohmann/json.hpp"
#include "ray/util/event_label.h"
#include "ray/util/string_utils.h"
#include "ray/util/thread_utils.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

namespace ray {

// Format pattern is 2020-08-21 17:00:00,000 I 100 1001 msg.
// %L is loglevel, %P is process id, %t for thread id.
constexpr char kLogFormatTextPattern[] = "[%Y-%m-%d %H:%M:%S,%e %L %P %t] %v";
constexpr char kLogFormatJsonPattern[] =
    "{\"asctime\":\"%Y-%m-%d %H:%M:%S,%e\",\"levelname\":\"%L\"%v}";

RayLogLevel RayLog::severity_threshold_ = RayLogLevel::INFO;
std::string RayLog::app_name_ = "";        // NOLINT
std::string RayLog::component_name_ = "";  // NOLINT
bool RayLog::log_format_json_ = false;
std::string RayLog::log_format_pattern_ = kLogFormatTextPattern;  // NOLINT

std::string RayLog::logger_name_ = "ray_log_sink";  // NOLINT
bool RayLog::is_failure_signal_handler_installed_ = false;
std::atomic<bool> RayLog::initialized_ = false;

std::ostream &operator<<(std::ostream &os, const StackTrace &stack_trace) {
  static constexpr int MAX_NUM_FRAMES = 64;
  char buf[16 * 1024];
  void *frames[MAX_NUM_FRAMES];

#ifndef _WIN32
  const int num_frames = backtrace(frames, MAX_NUM_FRAMES);
  char **frame_symbols = backtrace_symbols(frames, num_frames);
  for (int i = 0; i < num_frames; ++i) {
    os << frame_symbols[i];

    if (absl::Symbolize(frames[i], buf, sizeof(buf))) {
      os << " " << buf;
    }

    os << "\n";
  }
  free(frame_symbols);
#else
  const int num_frames = absl::GetStackTrace(frames, MAX_NUM_FRAMES, 0);
  for (int i = 0; i < num_frames; ++i) {
    if (absl::Symbolize(frames[i], buf, sizeof(buf))) {
      os << buf;
    } else {
      os << "unknown";
    }
    os << "\n";
  }
#endif

  return os;
}

void TerminateHandler() {
  // Print the exception info, if any.
  if (auto e_ptr = std::current_exception()) {
    try {
      std::rethrow_exception(e_ptr);
    } catch (std::exception &e) {
      RAY_LOG(ERROR) << "Unhandled exception: " << typeid(e).name()
                     << ". what(): " << e.what();
    } catch (...) {
      RAY_LOG(ERROR) << "Unhandled unknown exception.";
    }
  }

  RAY_LOG(ERROR) << "Stack trace: \n " << ray::StackTrace();

  std::abort();
}

inline const char *ConstBasename(const char *filepath) {
  const char *base = strrchr(filepath, '/');
#ifdef OS_WINDOWS  // Look for either path separator in Windows
  if (!base) base = strrchr(filepath, '\\');
#endif
  return base ? (base + 1) : filepath;
}

// Adapted from nlohmann/json
std::size_t json_extra_space(const std::string &s) {
  std::size_t result = 0;

  for (const auto &c : s) {
    switch (c) {
    case '"':
    case '\\':
    case '\b':
    case '\f':
    case '\n':
    case '\r':
    case '\t': {
      // from c (1 byte) to \x (2 bytes)
      result += 1;
      break;
    }

    default:
      break;
    }
  }

  return result;
}
std::string json_escape_string(const std::string &s) noexcept {
  const auto space = json_extra_space(s);
  if (space == 0) {
    return s;
  }

  // create a result string of necessary size
  std::string result(s.size() + space, '\\');
  std::size_t pos = 0;

  for (const auto &c : s) {
    switch (c) {
    // quotation mark (0x22)
    case '"': {
      result[pos + 1] = '"';
      pos += 2;
      break;
    }

    // reverse solidus (0x5c)
    case '\\': {
      // nothing to change
      pos += 2;
      break;
    }

    // backspace (0x08)
    case '\b': {
      result[pos + 1] = 'b';
      pos += 2;
      break;
    }

    // formfeed (0x0c)
    case '\f': {
      result[pos + 1] = 'f';
      pos += 2;
      break;
    }

    // newline (0x0a)
    case '\n': {
      result[pos + 1] = 'n';
      pos += 2;
      break;
    }

    // carriage return (0x0d)
    case '\r': {
      result[pos + 1] = 'r';
      pos += 2;
      break;
    }

    // horizontal tab (0x09)
    case '\t': {
      result[pos + 1] = 't';
      pos += 2;
      break;
    }

    default: {
      // all other characters are added as-is
      result[pos++] = c;
      break;
    }
    }
  }

  return result;
}

/// A logger that prints logs to stderr.
/// This is the default logger if logging is not initialized.
/// NOTE(lingxuan.zlx): Default stderr logger must be singleton and global
/// variable so core worker process can invoke `RAY_LOG` in its whole lifecycle.
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

// Spdlog's severity map.
static spdlog::level::level_enum GetMappedSeverity(RayLogLevel severity) {
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

void RayLog::InitSeverityThreshold(RayLogLevel severity_threshold) {
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
}

void RayLog::InitLogFormat() {
  // Default is plain text
  log_format_json_ = false;
  log_format_pattern_ = kLogFormatTextPattern;

  if (const char *var_value = std::getenv("RAY_BACKEND_LOG_JSON"); var_value != nullptr) {
    if (std::string_view{var_value} == std::string_view{"1"}) {
      log_format_json_ = true;
      log_format_pattern_ = kLogFormatJsonPattern;
    }
  }
}

/*static*/ size_t RayLog::GetRayLogRotationMaxBytesOrDefault() {
#if defined(__APPLE__) || defined(__linux__)
  if (const char *ray_rotation_max_bytes = std::getenv("RAY_ROTATION_MAX_BYTES");
      ray_rotation_max_bytes != nullptr) {
    size_t max_size = 0;
    if (absl::SimpleAtoi(ray_rotation_max_bytes, &max_size)) {
      return max_size;
    }
  }
#endif
  return 0;
}

/*static*/ size_t RayLog::GetRayLogRotationBackupCountOrDefault() {
#if defined(__APPLE__) || defined(__linux__)
  if (const char *ray_rotation_backup_count = std::getenv("RAY_ROTATION_BACKUP_COUNT");
      ray_rotation_backup_count != nullptr) {
    size_t file_num = 0;
    if (absl::SimpleAtoi(ray_rotation_backup_count, &file_num) && file_num > 0) {
      return file_num;
    }
  }
#endif
  return 1;
}

/*static*/ std::string RayLog::GetLogFilepathFromDirectory(const std::string &log_dir,
                                                           const std::string &app_name) {
  if (log_dir.empty()) {
    return "";
  }

#ifdef _WIN32
  int pid = _getpid();
#else
  pid_t pid = getpid();
#endif
  return JoinPaths(log_dir, absl::StrFormat("%s_%d.log", app_name, pid));
}

/*static*/ std::string RayLog::GetErrLogFilepathFromDirectory(
    const std::string &log_dir, const std::string &app_name) {
  if (log_dir.empty()) {
    return "";
  }

#ifdef _WIN32
  int pid = _getpid();
#else
  pid_t pid = getpid();
#endif
  return JoinPaths(log_dir, absl::StrFormat("%s_%d.err", app_name, pid));
}

/*static*/ void RayLog::StartRayLog(const std::string &app_name,
                                    RayLogLevel severity_threshold,
                                    const std::string &log_filepath,
                                    const std::string &err_log_filepath,
                                    size_t log_rotation_max_size,
                                    size_t log_rotation_file_num) {
  InitSeverityThreshold(severity_threshold);
  InitLogFormat();

  app_name_ = app_name;
  log_rotation_max_size_ = log_rotation_max_size;
  log_rotation_file_num_ = log_rotation_file_num;

  // All the logging sinks to add.
  std::array<spdlog::sink_ptr, 2> sinks;  // Intentionally no initialization.

  auto level = GetMappedSeverity(severity_threshold_);
  std::string app_name_without_path = app_name;
  if (app_name.empty()) {
    app_name_without_path = "DefaultApp";
  } else {
    // Find the app name without the path.
    std::string app_file_name = std::filesystem::path(app_name).filename().string();
    if (!app_file_name.empty()) {
      app_name_without_path = app_file_name;
    }
  }

  // Set sink for logs above the user defined level.
  if (!log_filepath.empty()) {
    // Sink all log stuff to default file logger we defined here. We may need
    // multiple sinks for different files or loglevel.
    auto file_logger = spdlog::get(RayLog::GetLoggerName());
    if (file_logger) {
      // Drop this old logger first if we need reset filename or reconfig
      // logger.
      spdlog::drop(RayLog::GetLoggerName());
    }

    spdlog::sink_ptr file_sink;
    if (log_rotation_max_size_ == 0) {
      file_sink = std::make_shared<spdlog::sinks::basic_file_sink_st>(log_filepath);
    } else {
      file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
          log_filepath, log_rotation_max_size_, log_rotation_file_num_);
    }
    file_sink->set_level(level);
    sinks[0] = std::move(file_sink);
  } else {
    component_name_ = app_name_without_path;
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_level(level);
    sinks[0] = std::move(console_sink);
  }

  // Set sink for error logs.
  if (!err_log_filepath.empty()) {
    spdlog::sink_ptr err_sink;
    if (log_rotation_max_size_ == 0) {
      err_sink = std::make_shared<spdlog::sinks::basic_file_sink_st>(err_log_filepath);
    } else {
      err_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
          err_log_filepath, log_rotation_max_size_, log_rotation_file_num_);
    }
    err_sink->set_level(spdlog::level::err);
    sinks[1] = std::move(err_sink);
  } else {
    auto err_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    err_sink->set_level(spdlog::level::err);
    sinks[1] = std::move(err_sink);
  }

  // Set the combined logger.
  auto logger = std::make_shared<spdlog::logger>(RayLog::GetLoggerName(),
                                                 std::make_move_iterator(sinks.begin()),
                                                 std::make_move_iterator(sinks.end()));
  logger->set_level(level);
  // Set the pattern of all sinks.
  logger->set_pattern(log_format_pattern_);
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
#ifndef _WIN32
  // InitializeSymbolizer cannot be called twice on windows, (causes a
  // crash)and is called in other libraries like pytorchaudio. It does not seem
  // there is a API to determine if it has already been called, and is only
  // needed to provide better stack traces on crashes. So do not call it here.
  absl::InitializeSymbolizer(argv0);
#endif
  absl::FailureSignalHandlerOptions options;
  options.call_previous_handler = call_previous_handler;
  options.writerfn = WriteFailureMessage;
  absl::InstallFailureSignalHandler(options);
  is_failure_signal_handler_installed_ = true;
}

void RayLog::InstallTerminateHandler() { std::set_terminate(TerminateHandler); }

bool RayLog::IsLevelEnabled(RayLogLevel log_level) {
  return log_level >= severity_threshold_;
}

std::string RayLog::GetLogFormatPattern() { return log_format_pattern_; }

std::string RayLog::GetLoggerName() { return logger_name_; }

void RayLog::AddFatalLogCallbacks(
    const std::vector<FatalLogCallback> &expose_log_callbacks) {
  fatal_log_callbacks_.insert(fatal_log_callbacks_.end(),
                              expose_log_callbacks.begin(),
                              expose_log_callbacks.end());
}

RayLog::RayLog(const char *file_name, int line_number, RayLogLevel severity)
    : is_enabled_(severity >= severity_threshold_),
      severity_(severity),
      is_fatal_(severity == RayLogLevel::FATAL) {
  if (is_fatal_) {
#ifdef _WIN32
    int pid = _getpid();
#else
    pid_t pid = getpid();
#endif
    expose_fatal_osstream_ << absl::StrFormat("%s:%d (PID: %d, TID: %s, errno: %d (%s)):",
                                              file_name,
                                              line_number,
                                              pid,
                                              std::to_string(GetTid()),
                                              errno,
                                              strerror(errno));
  }
  if (is_enabled_) {
    if (log_format_json_) {
      if (!component_name_.empty()) {
        WithField(kLogKeyComponent, component_name_);
      }
      WithField(kLogKeyFilename, ConstBasename(file_name));
      WithField(kLogKeyLineno, line_number);
    } else {
      if (!component_name_.empty()) {
        msg_osstream_ << "(" << component_name_ << ") ";
      }
      msg_osstream_ << ConstBasename(file_name) << ":" << line_number << ": ";
    }
  }
}

bool RayLog::IsEnabled() const { return is_enabled_; }

bool RayLog::IsFatal() const { return is_fatal_; }

RayLog::~RayLog() {
  if (IsFatal()) {
    msg_osstream_ << "\n*** StackTrace Information ***\n" << ray::StackTrace();
    expose_fatal_osstream_ << "\n*** StackTrace Information ***\n" << ray::StackTrace();
    for (const auto &callback : fatal_log_callbacks_) {
      callback(EL_RAY_FATAL_CHECK_FAILED, expose_fatal_osstream_.str());
    }
  }

  auto logger = spdlog::get(RayLog::GetLoggerName());
  if (!logger) {
    logger = DefaultStdErrLogger::Instance().GetDefaultLogger();
  }
  // NOTE(lingxuan.zlx): See more fmt by visiting https://github.com/fmtlib/fmt.
  if (log_format_json_) {
    logger->log(GetMappedSeverity(severity_),
                /*fmt*/ ",\"{}\":\"{}\"{}",
                kLogKeyMessage,
                json_escape_string(msg_osstream_.str()),
                context_osstream_.str());
  } else {
    logger->log(GetMappedSeverity(severity_),
                /*fmt*/ "{}{}",
                msg_osstream_.str(),
                context_osstream_.str());
  }
  logger->flush();

  if (severity_ == RayLogLevel::FATAL) {
    std::_Exit(EXIT_FAILURE);
  }
}

template <>
RayLog &RayLog::WithFieldJsonFormat<std::string>(std::string_view key,
                                                 const std::string &value) {
  context_osstream_ << ",\"" << key << "\":\"" << json_escape_string(value) << "\"";
  return *this;
}

template <>
RayLog &RayLog::WithFieldJsonFormat<int>(std::string_view key, const int &value) {
  context_osstream_ << ",\"" << key << "\":" << value;
  return *this;
}

}  // namespace ray
