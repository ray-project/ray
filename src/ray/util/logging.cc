#include <cstdlib>
#include <iostream>

#include "ray/util/logging.h"

#ifdef RAY_USE_GLOG
#include "glog/logging.h"
static_assert(RAY_INFO == google::GLOG_INFO, "Log enum mismatch");
static_assert(RAY_WARNING == google::GLOG_WARNING, "Log enum mismatch");
static_assert(RAY_ERROR == google::GLOG_ERROR, "Log enum mismatch");
static_assert(RAY_FATAL == google::GLOG_FATAL, "Log enum mismatch");
#elif RAY_USE_LOG4CPLUS
#include <log4cplus/logger.h>
#include <log4cplus/consoleappender.h>
#include <log4cplus/fileappender.h>
#include <log4cplus/layout.h>
#include <log4cplus/loggingmacros.h>

#endif

namespace ray {

namespace internal {

void *RayLog::static_impl = nullptr;
int RayLog::severity_threshold_ = RAY_DEBUG;

#ifdef RAY_USE_LOG4CPLUS

using namespace log4cplus;
using namespace log4cplus::helpers;

static log4cplus::Logger static_logger = Logger::getInstance("default");

class DefaultLogInitializer {
 public:
  static std::string GetDefualtPatternString() {
    return "%d{%m/%d/%y %H:%M:%S}  - %m [%l]%n";
  }

  static void AddConsoleAppender(log4cplus::Logger &logger, const std::string& appender_name) {
    SharedObjectPtr<Appender> appender(new ConsoleAppender());
    appender->setName(appender_name);
    std::unique_ptr<Layout> layout(new PatternLayout(GetDefualtPatternString()));
    appender->setLayout(std::move(layout));
    logger.addAppender(appender);
  }

  static void AddFileAppender(log4cplus::Logger &logger, const std::string& app_name, const std::string& log_dir) {
    SharedObjectPtr<Appender> appender(new DailyRollingFileAppender("/tmp/Log4CPlus.log", DAILY, false, 50));
    std::unique_ptr<Layout> layout(new PatternLayout(GetDefualtPatternString()));
    appender->setName(app_name);
    appender->setLayout(std::move(layout));
    logger.addAppender(appender);
  }
  DefaultLogInitializer() {
    //log4cplus::Logger logger = Logger::getInstance("default");
    AddConsoleAppender(static_logger, "default console appender");
    /* step 6: Set a priority for the logger  */
    static_logger.setLogLevel(ALL_LOG_LEVEL);
    RayLog::SetImplPointer(&static_logger);
  }
};

static DefaultLogInitializer log4cplus_initializaer;

#endif

void RayLog::StartRayLog(const char *app_name, int severity_threshold, const char* log_dir){
  std::string dir_ends_with_slash = std::string(log_dir);
  if (dir_ends_with_slash.length() > 0) {
    if (dir_ends_with_slash[dir_ends_with_slash.length() - 1] != '/') {
      dir_ends_with_slash += "/";
    }
  }
  severity_threshold_ = std::max(severity_threshold, google::GLOG_INFO);
#ifdef RAY_USE_GLOG
  if (app_name != nullptr && strlen(app_name) > 0) {
    google::InitGoogleLogging(app_name);
    google::SetLogFilenameExtension(app_name);
  } else {
    return;
  }
  if (dir_ends_with_slash.length() > 0) {
    google::SetLogDestination(severity_threshold_, dir_ends_with_slash.c_str());
  }
  google::SetStderrLogging(severity_threshold_);
#elif RAY_USE_LOG4CPLUS

#endif
}

void RayLog::ShutDownRayLog() {
#ifdef RAY_USE_GLOG
  google::ShutdownGoogleLogging();
#elif RAY_USE_LOG4CPLUS

#endif
}

void *RayLog::GetImplPointer() {
  return RayLog::static_impl;
}

void RayLog::SetImplPointer(void *pointer) {
  RayLog::static_impl = pointer;
}

RayLog::RayLog(const char *file_name, int line_number, int severity) 
  : file_name_(file_name), line_number_(line_number),
    severity_(severity), implement(nullptr) {
#ifdef RAY_USE_GLOG
  severity_ = std::max(severity_, google::GLOG_INFO);
  if (severity_ >= severity_threshold_) {
    implement = new google::LogMessage(file_name_, line_number_, std::max(severity_, google::GLOG_INFO));
  }
#elif RAY_USE_LOG4CPLUS
  severity_ = (severity_ + 1) * 10000;
  implement = new std::stringstream();
#else
  implement = new CerrLog(severity_);
  *reinterpret_cast<CerrLog*>(implement) << file_name_ << ":" << line_number_ << ": ";
#endif
}

std::ostream &RayLog::stream() {
#ifdef RAY_USE_GLOG
  return reinterpret_cast<google::LogMessage*>(implement)->stream();
#elif RAY_USE_LOG4CPLUS
    return *reinterpret_cast<std::stringstream*>(implement);
#else
  return reinterpret_cast<CerrLog*>(implement)->stream();
#endif
}

RayLog::~RayLog() {
#ifdef RAY_USE_GLOG
  delete reinterpret_cast<google::LogMessage*>(implement);
#elif RAY_USE_LOG4CPLUS
  log4cplus::Logger& logger = *reinterpret_cast<log4cplus::Logger*>(static_impl);
  if (logger.isEnabledFor(severity_)) {
    log4cplus::detail::macro_forced_log(logger, severity_, reinterpret_cast<std::stringstream*>(implement)->str(), file_name_, line_number_, nullptr);
  }
  delete reinterpret_cast<std::stringstream*>(implement);
#else
  delete reinterpret_cast<CerrLog*>(implement);
#endif
  implement = nullptr;
}

}  // namespace internal

}  // namespace ray

