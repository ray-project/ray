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
  static int severity_map[] = {
      // glog has no DEBUG level. It has verbose level but hard to adapt here.
      GLOG_INFO,     // RAY_DEBUG
      GLOG_INFO,     // RAY_INFO
      GLOG_WARNING,  // RAY_WARNING
      GLOG_ERROR,    // RAY_ERROR
      GLOG_FATAL     // RAY_FATAL
  };
  // Ray log level starts from -1 (RAY_DEBUG);
  return severity_map[severity + 1];
};

#endif

void RayLog::StartRayLog(const std::string &app_name, int severity_threshold,
                         const std::string &log_dir) {
#ifdef RAY_USE_GLOG
  std::string dir_ends_with_slash = std::string(log_dir);
  if (!dir_ends_with_slash.empty()) {
    if (dir_ends_with_slash[dir_ends_with_slash.length() - 1] != '/') {
      dir_ends_with_slash += "/";
    }
  }
  std::string app_name_str = app_name;
  if (app_name_str.empty()) {
    app_name_str = "DefaultApp";
  } else {
    // Find the app name without the path.
    std::string full_name = app_name;
    size_t pos = full_name.rfind('/');
    if (pos != full_name.npos && pos + 1 < full_name.length()) {
      app_name_str = full_name.substr(pos + 1);
    }
  }
  severity_threshold_ = severity_threshold;
  int mapped_severity_threshold_ = GetMappedSeverity(severity_threshold_);
  google::InitGoogleLogging(app_name_str.c_str());
  if (!dir_ends_with_slash.empty()) {
    google::SetLogFilenameExtension(app_name_str.c_str());
    google::SetLogDestination(mapped_severity_threshold_, dir_ends_with_slash.c_str());
  }
  google::SetStderrLogging(mapped_severity_threshold_);
#endif
}

void RayLog::ShutDownRayLog() {
#ifdef RAY_USE_GLOG
  google::ShutdownGoogleLogging();
#endif
}

RayLog::RayLog(const char *file_name, int line_number, int severity)
    : file_name_(file_name),
      line_number_(line_number),
      severity_(severity),
      implement(nullptr) {
#ifdef RAY_USE_GLOG
  // glog does not have DEBUG level, we can handle it here.
  if (severity_ >= severity_threshold_) {
    implement =
        new google::LogMessage(file_name_, line_number_, GetMappedSeverity(severity_));
  }
#else
  implement = new CerrLog(severity_);
  *reinterpret_cast<CerrLog *>(implement) << file_name_ << ":" << line_number_ << ": ";
#endif
}

std::ostream &RayLog::Stream() {
#ifdef RAY_USE_GLOG
  return reinterpret_cast<google::LogMessage *>(implement)->stream();
#else
  return reinterpret_cast<CerrLog *>(implement)->Stream();
#endif
}

RayLog::~RayLog() {
#ifdef RAY_USE_GLOG
  delete reinterpret_cast<google::LogMessage *>(implement);
#else
  delete reinterpret_cast<CerrLog *>(implement);
#endif
  implement = nullptr;
}

}  // namespace ray
