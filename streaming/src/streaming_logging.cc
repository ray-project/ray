#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "glog/log_severity.h"
#include "glog/logging.h"

#include "streaming_logging.h"

namespace ray {
namespace streaming {

// Glog's severity map.
static int GetMappedSeverity(StreamingLogLevel severity) {
  switch (severity) {
  case StreamingLogLevel::DEBUG:
    return GLOG_INFO;
  case StreamingLogLevel::INFO:
    return GLOG_INFO;
  case StreamingLogLevel::WARNING:
    return GLOG_WARNING;
  case StreamingLogLevel::ERROR:
    return GLOG_ERROR;
  case StreamingLogLevel::FATAL:
    return GLOG_FATAL;
  default:
    STREAMING_LOG(FATAL) << "Unsupported logging level: " << static_cast<int>(severity);
    // This return won't be hit but compiler needs it.
    return GLOG_FATAL;
  }
}

StreamingLogLevel StreamingLog::severity_threshold_ = StreamingLogLevel::INFO;

StreamingLog::StreamingLog(const char *file_name, int line_number, const char *func_name,
                           StreamingLogLevel severity)
    : logging_provider_(nullptr), is_enabled_(severity >= severity_threshold_) {
  if (is_enabled_) {
    logging_provider_ =
        new google::LogMessage(file_name, line_number, GetMappedSeverity(severity));
  }
}

void StreamingLog::StartStreamingLog(const std::string &app_name,
                                     StreamingLogLevel severity_threshold,
                                     int log_buffer_flush_in_secs,
                                     const std::string &log_dir) {
  severity_threshold_ = severity_threshold;
  if (!log_dir.empty()) {
    auto dir_ends_with_slash = log_dir;
    if (log_dir[log_dir.length() - 1] != '/') {
      dir_ends_with_slash += "/";
    }
    auto dir_path_cstr = dir_ends_with_slash.c_str();
    if (access(dir_path_cstr, 0) == -1) {
      mkdir(dir_path_cstr, S_IRWXU | S_IRGRP | S_IROTH);
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
    google::InitGoogleLogging(app_name.c_str());
    google::SetLogFilenameExtension(app_name_without_path.c_str());
    int level = GetMappedSeverity(static_cast<StreamingLogLevel>(severity_threshold_));
    google::SetLogDestination(level, dir_ends_with_slash.c_str());
    for (int i = static_cast<int>(StreamingLogLevel::INFO);
         i <= static_cast<int>(StreamingLogLevel::FATAL); ++i) {
      if (i != level) {
        google::SetLogDestination(i, "");
      }
    }
    FLAGS_logbufsecs = log_buffer_flush_in_secs;
    FLAGS_max_log_size = 1000;
    FLAGS_log_rotate_max_size = 10;
    FLAGS_stop_logging_if_full_disk = true;
  }
}

bool StreamingLog::IsLevelEnabled(StreamingLogLevel level) {
  return level >= severity_threshold_;
}

void StreamingLog::ShutDownStreamingLog() { google::ShutdownGoogleLogging(); }

void StreamingLog::FlushStreamingLog(int severity) { google::FlushLogFiles(severity); }

void StreamingLog::InstallFailureSignalHandler() {
  google::InstallFailureSignalHandler();
}

std::ostream &StreamingLog::Stream() {
  auto logging_provider = reinterpret_cast<google::LogMessage *>(logging_provider_);
  return logging_provider->stream();
}

bool StreamingLog::IsEnabled() const { return is_enabled_; }

StreamingLog::~StreamingLog() {
  if (logging_provider_ != nullptr) {
    auto logging_provider = reinterpret_cast<google::LogMessage *>(logging_provider_);
    delete logging_provider;
    logging_provider_ = nullptr;
  }
}
}  // namespace streaming
}  // namespace ray
