#ifndef RAY_UTIL_LOGGING_H
#define RAY_UTIL_LOGGING_H

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

// TODO(pcm): Give a backtrace here.
#define RAY_CHECK(condition)                             \
  (condition) ? 0 : ::ray::internal::FatalLog(RAY_FATAL) \
                        << __FILE__ << __LINE__          \
                        << " Check failed: " #condition " "

#ifdef NDEBUG
#define RAY_DFATAL RAY_WARNING

#define DCHECK(condition)    \
  RAY_IGNORE_EXPR(condition) \
  while (false)              \
  ::ray::internal::NullLog()
#define DCHECK_EQ(val1, val2) \
  RAY_IGNORE_EXPR(val1)       \
  while (false)               \
  ::ray::internal::NullLog()
#define DCHECK_NE(val1, val2) \
  RAY_IGNORE_EXPR(val1)       \
  while (false)               \
  ::ray::internal::NullLog()
#define DCHECK_LE(val1, val2) \
  RAY_IGNORE_EXPR(val1)       \
  while (false)               \
  ::ray::internal::NullLog()
#define DCHECK_LT(val1, val2) \
  RAY_IGNORE_EXPR(val1)       \
  while (false)               \
  ::ray::internal::NullLog()
#define DCHECK_GE(val1, val2) \
  RAY_IGNORE_EXPR(val1)       \
  while (false)               \
  ::ray::internal::NullLog()
#define DCHECK_GT(val1, val2) \
  RAY_IGNORE_EXPR(val1)       \
  while (false)               \
  ::ray::internal::NullLog()

#else
#define RAY_DFATAL RAY_FATAL

#define DCHECK(condition) RAY_CHECK(condition)
#define DCHECK_EQ(val1, val2) RAY_CHECK((val1) == (val2))
#define DCHECK_NE(val1, val2) RAY_CHECK((val1) != (val2))
#define DCHECK_LE(val1, val2) RAY_CHECK((val1) <= (val2))
#define DCHECK_LT(val1, val2) RAY_CHECK((val1) < (val2))
#define DCHECK_GE(val1, val2) RAY_CHECK((val1) >= (val2))
#define DCHECK_GT(val1, val2) RAY_CHECK((val1) > (val2))

#endif  // NDEBUG

namespace internal {

class NullLog {
 public:
  template <class T>
  NullLog &operator<<(const T &t) {
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
    }
    std::exit(1);
  }
};

}  // namespace internal

}  // namespace ray

#endif  // RAY_UTIL_LOGGING_H
