#include "ray/util/logging.h"
#include <signal.h>
#include <algorithm>
#include <cstdlib>

namespace ray {

RayLogLevel RayLog::severity_threshold_ = RayLogLevel::INFO;
std::string RayLog::app_name_ = "";
std::string RayLog::log_dir_ = "";

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
  int mapped_severity_threshold = GetMappedSeverity(severity_threshold_);
  google::SetStderrLogging(mapped_severity_threshold);
  // Enable log file if log_dir_ is not empty.
  if (!log_dir_.empty()) {
    auto dir_ends_with_slash = log_dir_;
    if (log_dir_[log_dir_.length() - 1] != '/') {
      dir_ends_with_slash += "/";
    }
    auto app_name_without_path = app_name;
    if (app_name.empty()) {
      app_name_without_path = "DefaultApp";
    } else {
      // Find the app name without the path.
      size_t pos = app_name.rfind('/');
      if (pos != app_name.npos && pos + 1 < app_name.length()) {
        app_name_without_path = app_name.substr(pos + 1);
      }
    }
    google::InitGoogleLogging(app_name_.c_str());
    google::SetLogFilenameExtension(app_name_without_path.c_str());
    for (int i = static_cast<int>(severity_threshold_);
         i <= static_cast<int>(RayLogLevel::FATAL); ++i) {
      int level = GetMappedSeverity(static_cast<RayLogLevel>(i));
      google::SetLogDestination(level, dir_ends_with_slash.c_str());
    }
  }
#endif
}

void RayLog::UninstallSignalAction() {
#ifdef RAY_USE_GLOG
  RAY_LOG(DEBUG) << "Uninstall signal handlers.";
  // This signal list comes from glog's signalhandler.cc.
  // https://github.com/google/glog/blob/master/src/signalhandler.cc#L58-L70
  static std::vector<int> installed_signals({SIGSEGV, SIGILL, SIGFPE, SIGABRT, SIGTERM});
  struct sigaction sig_action;
  memset(&sig_action, 0, sizeof(sig_action));
  sigemptyset(&sig_action.sa_mask);
  sig_action.sa_handler = SIG_DFL;
  for (int signal_num : installed_signals) {
    sigaction(signal_num, &sig_action, NULL);
  }
#endif
}

void RayLog::ShutDownRayLog() {
#ifdef RAY_USE_GLOG
  UninstallSignalAction();
  if (!log_dir_.empty()) {
    google::ShutdownGoogleLogging();
  }
#endif
}

void RayLog::InstallFailureSignalHandler() {
#ifdef RAY_USE_GLOG
  google::InstallFailureSignalHandler();
#endif
}

bool RayLog::IsLevelEnabled(RayLogLevel log_level) {
  return log_level >= severity_threshold_;
}

RayLog::RayLog(const char *file_name, int line_number, RayLogLevel severity)
    // glog does not have DEBUG level, we can handle it using is_enabled_.
    : is_enabled_(severity >= severity_threshold_) {
#ifdef RAY_USE_GLOG
  if (is_enabled_) {
    logging_provider_.reset(
        new google::LogMessage(file_name, line_number, GetMappedSeverity(severity)));
  }
#else
  logging_provider_.reset(new CerrLog(severity));
  *logging_provider_ << file_name << ":" << line_number << ": ";
#endif
}

std::ostream &RayLog::Stream() {
#ifdef RAY_USE_GLOG
  // Before calling this function, user should check IsEnabled.
  // When IsEnabled == false, logging_provider_ will be empty.
  return logging_provider_->stream();
#else
  return logging_provider_->Stream();
#endif
}

bool RayLog::IsEnabled() const { return is_enabled_; }

}  // namespace ray
