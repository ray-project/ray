#ifndef RAY_STREAMING_LOGGING_H
#define RAY_STREAMING_LOGGING_H
#include "ray/util/logging.h"

namespace ray {
namespace streaming {

class StreamingLog;

enum class StreamingLogLevel { DEBUG = -1, INFO = 0, WARNING = 1, ERROR = 2, FATAL = 3 };

#define STREAMING_LOG_INTERNAL(level) \
  ::ray::streaming::StreamingLog(__FILE__, __LINE__, __FUNCTION__, level)

#define STREAMING_LOG(level)                           \
  if (::ray::streaming::StreamingLog::IsLevelEnabled(  \
          ::ray::streaming::StreamingLogLevel::level)) \
  STREAMING_LOG_INTERNAL(::ray::streaming::StreamingLogLevel::level)

#define STREAMING_IGNORE_EXPR(expr) ((void)(expr))

#define STREAMING_CHECK(condition)                                                 \
  (condition) ? STREAMING_IGNORE_EXPR(0)                                           \
              : ::ray::Voidify() & ::ray::streaming::StreamingLog(                 \
                                       __FILE__, __LINE__, __FUNCTION__,           \
                                       ::ray::streaming::StreamingLogLevel::FATAL) \
                                       << " Check failed: " #condition " "

#ifdef NDEBUG

#define STREAMING_DCHECK(condition) \
  STREAMING_IGNORE_EXPR(condition); \
  while (false) ::ray::RayLogBase()

#else
#define RAY_DCHECK(condition) RAY_CHECK(condition)
#endif

class StreamingLog : public RayLogBase {
 public:
  StreamingLog(const char *file_name, int line_number, const char *func_name,
               StreamingLogLevel severity);

  ~StreamingLog();

  static void StartStreamingLog(const std::string &app_name,
                                StreamingLogLevel severity_threshold,
                                int log_buffer_flush_in_secs, const std::string &log_dir);

  static bool IsLevelEnabled(StreamingLogLevel level);

  static void ShutDownStreamingLog();

  static void FlushStreamingLog(int severity);

  static void InstallFailureSignalHandler();

  bool IsEnabled() const;

 private:
  void *logging_provider_;
  bool is_enabled_;
  static StreamingLogLevel severity_threshold_;

 protected:
  virtual std::ostream &Stream();
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_LOGGING_H
