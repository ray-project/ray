#include <cstdlib>
#include <iostream>

#include "ray/util/logging.h"

#ifdef RAY_USE_GLOG
#include "glog/logging.h"
#elif RAY_USE_LOG4CPLUS
#include <log4cplus/consoleappender.h>
#include <log4cplus/fileappender.h>
#include <log4cplus/layout.h>
#include <log4cplus/logger.h>
#include <log4cplus/loggingmacros.h>
#endif

namespace ray {

// This is the default implementation of ray log,
// which is independent of any libs.
class CerrLog {
 public:
  CerrLog(int severity)  // NOLINT(runtime/explicit)
      : severity_(severity),
        has_logged_(false) {}

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

// This is a adapter class to adapt the log levels for different log libs.
class SeverityMapper {
 public:
  inline static int GetMappedSeverity(int severity) {
    // Ray log level starts from -1 (RAY_DEBUG);
    return severity_map[severity + 1];
  }

 private:
  static int severity_map[];
};

void *RayLog::static_impl = nullptr;
int RayLog::severity_threshold_ = RAY_ERROR;

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

#elif RAY_USE_LOG4CPLUS
using namespace log4cplus;
using namespace log4cplus::helpers;

// Log4cplus's severity map.
static int GetMappedSeverity(int severity) {
  static int severity_map[] = {
      DEBUG_LOG_LEVEL,  // RAY_DEBUG
      INFO_LOG_LEVEL,   // RAY_INFO
      WARN_LOG_LEVEL,   // RAY_WARNING
      ERROR_LOG_LEVEL,  // RAY_ERROR
      FATAL_LOG_LEVEL   // RAY_FATAL
  };
  // Ray log level starts from -1 (RAY_DEBUG);
  return severity_map[severity + 1];
}

// This is a helper class for log4cplus.
// Log4cplus needs initialized, so the ctor will do the default initialization.
class Log4cplusHelper {
 public:
  Log4cplusHelper() {
    // This is the function to setup a default log4cplus log.
    // `static Log4cplusHelper log4cplus_initializer;` is used to trigger this function.
    AddConsoleAppender(static_logger, "default console appender");
    static_logger.setLogLevel(ALL_LOG_LEVEL);
    RayLog::SetStaticImplement(&static_logger);
  }

  static std::string GetDafaultPatternString() {
    // Default log format.
    return "%d{%Y-%m-%d_%H-%M-%S} [%l]: %m%n";
  }

  static void AddConsoleAppender(log4cplus::Logger &logger,
                                 const std::string &appender_name) {
    SharedObjectPtr<Appender> appender(new ConsoleAppender());
    appender->setName(appender_name);
    std::unique_ptr<Layout> layout(new PatternLayout(GetDafaultPatternString()));
    appender->setLayout(std::move(layout));
    logger.addAppender(appender);
  }

  static void AddFileAppender(log4cplus::Logger &logger, const std::string &app_name,
                              const std::string &log_dir) {
    SharedObjectPtr<Appender> appender(
        new DailyRollingFileAppender(log_dir + app_name, DAILY, true, 50));
    std::unique_ptr<Layout> layout(new PatternLayout(GetDafaultPatternString()));
    appender->setName(app_name);
    appender->setLayout(std::move(layout));
    logger.addAppender(appender);
  }

  static void InitLog4cplus(const std::string &app_name, int severity_threshold,
                            const std::string &log_dir) {
    static_logger = Logger::getInstance(app_name);
    AddConsoleAppender(static_logger,
                       (std::string(app_name) + "console appender").c_str());
    AddFileAppender(static_logger, app_name, log_dir);
    static_logger.setLogLevel(GetMappedSeverity(severity_threshold));
    RayLog::SetStaticImplement(&static_logger);
  }

 private:
  static log4cplus::Logger static_logger;
  static int severity_map[];
};

log4cplus::Logger Log4cplusHelper::static_logger = Logger::getInstance("default");
static Log4cplusHelper log4cplus_initializer;

#endif

void RayLog::StartRayLog(const std::string &app_name, int severity_threshold,
                         const std::string &log_dir) {
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
#ifdef RAY_USE_GLOG
  int mapped_severity_threshold_ = GetMappedSeverity(severity_threshold_);
  google::InitGoogleLogging(app_name_str.c_str());
  if (!dir_ends_with_slash.empty()) {
    google::SetLogFilenameExtension(app_name_str.c_str());
    google::SetLogDestination(mapped_severity_threshold_, dir_ends_with_slash.c_str());
  }
  google::SetStderrLogging(mapped_severity_threshold_);
#elif RAY_USE_LOG4CPLUS
  Log4cplusHelper::InitLog4cplus(app_name_str, severity_threshold_, dir_ends_with_slash);
#endif
}

void RayLog::ShutDownRayLog() {
#ifdef RAY_USE_GLOG
  google::ShutdownGoogleLogging();
#elif RAY_USE_LOG4CPLUS
// Do not need to do anything.
#endif
}

RayLog::RayLog(const char *file_name, int line_number, int severity)
    : file_name_(file_name),
      line_number_(line_number),
      severity_(severity),
      implement(nullptr) {
#ifdef RAY_USE_GLOG
  if (severity_ >= severity_threshold_) {
    implement =
        new google::LogMessage(file_name_, line_number_, GetMappedSeverity(severity_));
  }
#elif RAY_USE_LOG4CPLUS
  implement = new std::stringstream();
#else
  implement = new CerrLog(severity_);
  *reinterpret_cast<CerrLog *>(implement) << file_name_ << ":" << line_number_ << ": ";
#endif
}

std::ostream &RayLog::Stream() {
#ifdef RAY_USE_GLOG
  return reinterpret_cast<google::LogMessage *>(implement)->stream();
#elif RAY_USE_LOG4CPLUS
  return *reinterpret_cast<std::stringstream *>(implement);
#else
  return reinterpret_cast<CerrLog *>(implement)->Stream();
#endif
}

RayLog::~RayLog() {
#ifdef RAY_USE_GLOG
  delete reinterpret_cast<google::LogMessage *>(implement);
#elif RAY_USE_LOG4CPLUS
  log4cplus::Logger &logger = *reinterpret_cast<log4cplus::Logger *>(static_impl);
  int mapped_severity = GetMappedSeverity(severity_);
  if (severity_ >= severity_threshold_) {
    log4cplus::detail::macro_forced_log(
        logger, mapped_severity, reinterpret_cast<std::stringstream *>(implement)->str(),
        file_name_, line_number_, nullptr);
  }
  delete reinterpret_cast<std::stringstream *>(implement);
  // Log4cplus won't exit at fatal level.
  if (severity_ == RAY_FATAL) {
    std::abort();
  }
#else
  delete reinterpret_cast<CerrLog *>(implement);
#endif
  implement = nullptr;
}

}  // namespace ray
