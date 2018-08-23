#ifndef RAY_UTIL_LOGGING_H
#define RAY_UTIL_LOGGING_H

#ifndef _WIN32
#include <execinfo.h>
#endif

#include <cstdlib>
#include <iostream>
#include <memory>

#include "ray/util/macros.h"

// Forward declaration for the log provider.
#ifdef RAY_USE_GLOG
namespace google {
class LogMessage;
}  // namespace google
typedef google::LogMessage LoggingProvider;
#else
namespace ray {
class CerrLog;
}  // namespace ray
typedef ray::CerrLog LoggingProvider;
#endif

namespace ray {
// Log levels. LOG ignores them, so their values are abitrary.

#define RAY_DEBUG (-1)
#define RAY_INFO 0
#define RAY_WARNING 1
#define RAY_ERROR 2
#define RAY_FATAL 3

#define RAY_LOG_INTERNAL(level) ::ray::RayLog(__FILE__, __LINE__, level)

#define RAY_LOG(level) RAY_LOG_INTERNAL(RAY_##level)
#define RAY_IGNORE_EXPR(expr) ((void)(expr))

#define RAY_CHECK(condition)                                                          \
  (condition) ? RAY_IGNORE_EXPR(0) : ::ray::Voidify() &                               \
                                         ::ray::RayLog(__FILE__, __LINE__, RAY_FATAL) \
                                             << " Check failed: " #condition " "

#ifdef NDEBUG

#define RAY_DCHECK(condition) \
  RAY_IGNORE_EXPR(condition); \
  while (false) ::ray::RayLogBase()

#else

#define RAY_DCHECK(condition) RAY_CHECK(condition)

#endif  // NDEBUG

// To make the logging lib plugable with other logging libs and make
// the implementation unawared by the user, RayLog is only a declaration
// which hide the implementation into logging.cc file.
// In logging.cc, we can choose different log libs using different macros.

// This is also a null log which does not output anything.
class RayLogBase {
 public:
  virtual ~RayLogBase(){};

  template <typename T>
  RayLogBase &operator<<(const T &t) {
    RAY_IGNORE_EXPR(t);
    return *this;
  }
};

class RayLog : public RayLogBase {
 public:
  RayLog(const char *file_name, int line_number, int severity);
  virtual ~RayLog();

  template <typename T>
  RayLogBase &operator<<(const T &t) {
    if (logging_provider_ == nullptr) {
      // This means the logging level is lower than the threshold.
      RAY_IGNORE_EXPR(t);
    } else {
      this->Stream() << t;
    }
    return *this;
  }

  // The init function of ray log for a program which should be called only once.
  // If logDir is empty, the log won't output to file.
  static void StartRayLog(const std::string &appName, int severity_threshold = RAY_ERROR,
                          const std::string &logDir = "");
  // The shutdown function of ray log which should be used with StartRayLog as a pair.
  static void ShutDownRayLog();

 private:
  std::ostream &Stream();
  std::unique_ptr<LoggingProvider> logging_provider_;
  static int severity_threshold_;
};

// This class make RAY_CHECK compilation pass to change the << operator to void.
// This class is copied from glog.
class Voidify {
 public:
  Voidify() {}
  // This has to be an operator with a precedence lower than << but
  // higher than ?:
  void operator&(RayLogBase &) {}
};

}  // namespace ray

#endif  // RAY_UTIL_LOGGING_H
