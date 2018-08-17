#ifndef RAY_UTIL_LOGGING_H
#define RAY_UTIL_LOGGING_H

#ifndef _WIN32
#include <execinfo.h>
#endif

#include <cstdlib>
#include <iostream>

#include "ray/util/macros.h"

namespace ray {

// Stubbed versions of macros defined in glog/logging.h, intended for
// environments where glog headers aren't available.
//
// Add more as needed.

// Log levels. LOG ignores them, so their values are abitrary.

#define RAY_DEBUG (-1)
#define RAY_INFO 0
#define RAY_WARNING 1
#define RAY_ERROR 2
#define RAY_FATAL 3

#define RAY_LOG_INTERNAL(level) \
  ::ray::internal::RayLog(__FILE__, __LINE__, level)

#define RAY_LOG(level) RAY_LOG_INTERNAL(RAY_##level)
#define RAY_IGNORE_EXPR(expr) ((void) (expr));

#define RAY_CHECK(condition)                      \
  (condition) ? ::ray::internal::NullLog() : ::ray::internal::RayLog( \
      __FILE__, __LINE__, RAY_FATAL)     \
      << " Check failed: " #condition " "
/*#define RAY_CHECK(condition)                             \
  (condition) ? 0 : ::ray::internal::FatalLog(RAY_FATAL) \
                        << __FILE__ << ":" << __LINE__   \
                        << " Check failed: " #condition " "*/

#ifdef NDEBUG

#define RAY_DCHECK(condition) \
  RAY_IGNORE_EXPR(condition)  \
  while (false) ::ray::internal::RayLogBase()

#else

#define RAY_DCHECK(condition) RAY_CHECK(condition)

#endif  // NDEBUG

namespace internal {

// To make the logging lib plugable with other logging lib and make 
// the implementation unawared by the user, RayLog is only a declaration
// which hide the implementation into logging.cc file.
// In logging.cc, we can choose different log lib using different macroes.
class RayLogBase {
 public:
  virtual ~RayLogBase() {};
  template<typename T>
  RayLogBase &operator<<(const T &t) {
    RAY_IGNORE_EXPR(t);
    return *this;
  }
};

class RayLog : public RayLogBase {
 public:
  RayLog(const char *file_name, int line_number, int severity);
  virtual ~RayLog();
  template<typename T>
  RayLogBase &operator<<(const T &t) {
    if (implement == nullptr) {
      RAY_IGNORE_EXPR(t);
    } else {
      this->stream() << t;
    }
    return *this;
  }
  static void StartRayLog(const char *appName, int severity_threshold = RAY_ERROR, const char* logDir = "/tmp/");
  static void ShutDownRayLog();
  static void *GetImplPointer();
  static void SetImplPointer(void *pointer);
 private:
  std::ostream& stream();
  const char *file_name_;
  int line_number_;
  int severity_;
  void *implement;
  static void *static_impl;
  static int severity_threshold_;
};

// Clang-tidy isn't smart enough to determine that DCHECK using CerrLog doesn't
// return so we create a new class to give it a hint.
class RayFatalLog : public RayLog {
 public:
  explicit RayFatalLog(const char *file_name, int line_number)  // NOLINT
      : RayLog(file_name, line_number, RAY_FATAL) {}            // NOLINT
  RAY_NORETURN ~RayFatalLog() {
    std::abort();
  }
};

class NullLog : public RayLogBase {
 public:
  NullLog() : RayLogBase() {}
  template <class T>
  RayLogBase &operator<<(const T &t) {
    RAY_IGNORE_EXPR(t);
    return *this;
  }
};

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

  std::ostream &stream() {
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

// Clang-tidy isn't smart enough to determine that DCHECK using CerrLog doesn't
// return so we create a new class to give it a hint.
class FatalLog : public CerrLog {
 public:
  explicit FatalLog(int /* severity */)  // NOLINT
      : CerrLog(RAY_FATAL) {}            // NOLINT

  RAY_NORETURN ~FatalLog() {
    if (has_logged_) {
      std::cerr << std::endl;
      PrintBackTrace();
    }
    std::abort();
  }
};

}  // namespace internal

}  // namespace ray

#endif  // RAY_UTIL_LOGGING_H
