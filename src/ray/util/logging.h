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

#define RAY_LOG_INTERNAL(level) ::ray::internal::CerrLog(level)
#define RAY_LOG(level) RAY_LOG_INTERNAL(RAY_##level)
#define RAY_IGNORE_EXPR(expr) ((void) (expr));

#define RAY_CHECK(condition)                             \
  (condition) ? 0 : ::ray::internal::FatalLog(RAY_FATAL) \
                        << __FILE__ << __LINE__          \
                        << " Check failed: " #condition " "

#ifdef NDEBUG

#define RAY_DCHECK(condition) \
  RAY_IGNORE_EXPR(condition)  \
  while (false)               \
  ::ray::internal::NullLog()

#else

#define RAY_DCHECK(condition) RAY_CHECK(condition)

#endif  // NDEBUG

namespace internal {

class NullLog {
 public:
  template <class T>
  NullLog &operator<<(const T &t) {
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
      std::exit(1);
    }
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
#if defined(_EXECINFO_H) || !defined(_WIN32)
      void *buffer[255];
      const int calls = backtrace(buffer, sizeof(buffer) / sizeof(void *));
      backtrace_symbols_fd(buffer, calls, 1);
#endif
    }
    std::abort();
  }
};

}  // namespace internal

}  // namespace ray

#endif  // RAY_UTIL_LOGGING_H
