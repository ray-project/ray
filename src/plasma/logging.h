// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef ARROW_UTIL_LOGGING_H
#define ARROW_UTIL_LOGGING_H

#include <cstdlib>
#include <iostream>

namespace arrow {

// Stubbed versions of macros defined in glog/logging.h, intended for
// environments where glog headers aren't available.
//
// Add more as needed.

// Log levels. LOG ignores them, so their values are abitrary.

#define ARROW_INFO 0
#define ARROW_WARNING 1
#define ARROW_ERROR 2
#define ARROW_FATAL 3

#define ARROW_LOG_INTERNAL(level) ::arrow::internal::CerrLog(level)
#define ARROW_LOG(level) ARROW_LOG_INTERNAL(ARROW_##level)

#define ARROW_CHECK(condition)                           \
  (condition) ? 0                                        \
              : ::arrow::internal::FatalLog(ARROW_FATAL) \
                    << __FILE__ << __LINE__ << " Check failed: " #condition " "

#ifdef NDEBUG
#define ARROW_DFATAL ARROW_WARNING

#define DCHECK(condition) \
  while (false)           \
  ::arrow::internal::NullLog()
#define DCHECK_EQ(val1, val2) \
  while (false)               \
  ::arrow::internal::NullLog()
#define DCHECK_NE(val1, val2) \
  while (false)               \
  ::arrow::internal::NullLog()
#define DCHECK_LE(val1, val2) \
  while (false)               \
  ::arrow::internal::NullLog()
#define DCHECK_LT(val1, val2) \
  while (false)               \
  ::arrow::internal::NullLog()
#define DCHECK_GE(val1, val2) \
  while (false)               \
  ::arrow::internal::NullLog()
#define DCHECK_GT(val1, val2) \
  while (false)               \
  ::arrow::internal::NullLog()

#else
#define ARROW_DFATAL ARROW_FATAL

#define DCHECK(condition) ARROW_CHECK(condition)
#define DCHECK_EQ(val1, val2) ARROW_CHECK((val1) == (val2))
#define DCHECK_NE(val1, val2) ARROW_CHECK((val1) != (val2))
#define DCHECK_LE(val1, val2) ARROW_CHECK((val1) <= (val2))
#define DCHECK_LT(val1, val2) ARROW_CHECK((val1) < (val2))
#define DCHECK_GE(val1, val2) ARROW_CHECK((val1) >= (val2))
#define DCHECK_GT(val1, val2) ARROW_CHECK((val1) > (val2))

#endif  // NDEBUG

namespace internal {

class NullLog {
 public:
  template <class T>
  NullLog& operator<<(const T& t) {
    return *this;
  }
};

class CerrLog {
 public:
  CerrLog(int severity)  // NOLINT(runtime/explicit)
      : severity_(severity),
        has_logged_(false) {}

  virtual ~CerrLog() {
    if (has_logged_) { std::cerr << std::endl; }
    if (severity_ == ARROW_FATAL) { std::exit(1); }
  }

  template <class T>
  CerrLog& operator<<(const T& t) {
    has_logged_ = true;
    std::cerr << t;
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
      : CerrLog(ARROW_FATAL){}           // NOLINT

            [[noreturn]] ~FatalLog() {
    if (has_logged_) { std::cerr << std::endl; }
    std::exit(1);
  }
};

}  // namespace internal

}  // namespace arrow

#endif  // ARROW_UTIL_LOGGING_H
