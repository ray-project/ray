#include <cstdlib>
#include <iostream>

#include "ray/util/logging.h"

#ifdef RAY_USE_GLOG
#include "glog/logging.h"
#endif

namespace ray {

// This is the default implementation of ray log,
// which is independent of any libs.
class CerrLog {
 public:
  CerrLog(int severity) : severity_(severity), has_logged_(false) {}

  virtual ~CerrLog() {
    if (has_logged_) {
      std::cerr << std::endl;
    }
    if (severity_ == RAY_FATAL) {
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
    if (severity_ != RAY_DEBUG) {
      has_logged_ = true;
      std::cerr << t;
    }
    return *this;
  }

 protected:
  const int severity_;
  bool has_logged_;

  void PrintBackTrace() {
#if defined(_EXECINFO_H) || !defined(_WIN32)
    void *buffer[255];
    const int calls = backtrace(buffer, sizeof(buffer) / sizeof(void *));
    backtrace_symbols_fd(buffer, calls, 1);
#endif
  }
};

int RayLog::severity_threshold_ = RAY_INFO;

#ifdef RAY_USE_GLOG
using namespace google;

// Glog's severity map.
static int GetMappedSeverity(int severity) {
  switch (severity) {
  case RAY_DEBUG:
    return GLOG_INFO;
  case RAY_INFO:
    return GLOG_INFO;
  case RAY_WARNING:
    return GLOG_WARNING;
  case RAY_ERROR:
    return GLOG_ERROR;
  case RAY_FATAL:
    return GLOG_FATAL;
  default:
    RAY_LOG(FATAL) << "Unsupported logging level: " << severity;
    // This return won't be hit but compiler needs it.
    return GLOG_FATAL;
  }
}

#endif

void RayLog::StartRayLog(const std::string &app_name, int severity_threshold,
                         const std::string &log_dir) {
#ifdef RAY_USE_GLOG
  severity_threshold_ = severity_threshold;
  int mapped_severity_threshold = GetMappedSeverity(severity_threshold_);
  google::InitGoogleLogging(app_name.c_str());
  google::SetStderrLogging(mapped_severity_threshold);
  // Enble log file if log_dir is not empty.
  if (!log_dir.empty()) {
    auto dir_ends_with_slash = log_dir;
    if (log_dir[log_dir.length() - 1] != '/') {
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
    google::SetLogFilenameExtension(app_name_without_path.c_str());
    google::SetLogDestination(mapped_severity_threshold, log_dir.c_str());
  }
#endif
}

void RayLog::ShutDownRayLog() {
#ifdef RAY_USE_GLOG
  google::ShutdownGoogleLogging();
#endif
}

bool RayLog::IsLevelEnabled(int log_level) { return log_level >= severity_threshold_; }

RayLog::RayLog(const char *file_name, int line_number, int severity)
    // glog does not have DEBUG level, we can handle it here.
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

RayLog::~RayLog() { logging_provider_.reset(); }

}  // namespace ray
