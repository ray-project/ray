// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

// Adapted from Apache Arrow, Apache Kudu, TensorFlow

#ifndef RAY_STATUS_H_
#define RAY_STATUS_H_

#include <cstring>
#include <iosfwd>
#include <string>

#include "ray/util/logging.h"
#include "ray/util/macros.h"
#include "ray/util/visibility.h"

namespace boost {

namespace system {

class error_code;

}  // namespace system

}  // namespace boost

// Return the given status if it is not OK.
#define RAY_RETURN_NOT_OK(s)           \
  do {                                 \
    ::ray::Status _s = (s);            \
    if (RAY_PREDICT_FALSE(!_s.ok())) { \
      return _s;                       \
    }                                  \
  } while (0)

// If 'to_call' returns a bad status, CHECK immediately with a logged message
// of 'msg' followed by the status.
#define RAY_CHECK_OK_PREPEND(to_call, msg)                \
  do {                                                    \
    ::ray::Status _s = (to_call);                         \
    RAY_CHECK(_s.ok()) << (msg) << ": " << _s.ToString(); \
  } while (0)

// If the status is bad, CHECK immediately, appending the status to the
// logged message.
#define RAY_CHECK_OK(s) RAY_CHECK_OK_PREPEND(s, "Bad status")

// This macro is used to replace the "ARROW_CHECK_OK_PREPEND" macro.
#define RAY_ARROW_CHECK_OK_PREPEND(to_call, msg)          \
  do {                                                    \
    ::arrow::Status _s = (to_call);                       \
    RAY_CHECK(_s.ok()) << (msg) << ": " << _s.ToString(); \
  } while (0)

// This macro is used to replace the "ARROW_CHECK_OK" macro.
#define RAY_ARROW_CHECK_OK(s) RAY_ARROW_CHECK_OK_PREPEND(s, "Bad status")

// If arrow status is not ok, return a ray IOError status
// with the error message.
#define RAY_ARROW_RETURN_NOT_OK(s)               \
  do {                                           \
    ::arrow::Status _s = (s);                    \
    if (RAY_PREDICT_FALSE(!_s.ok())) {           \
      return ray::Status::IOError(_s.message()); \
      ;                                          \
    }                                            \
  } while (0)

namespace ray {

enum class StatusCode : char {
  OK = 0,
  OutOfMemory = 1,
  KeyError = 2,
  TypeError = 3,
  Invalid = 4,
  IOError = 5,
  ObjectExists = 6,
  ObjectStoreFull = 7,
  UnknownError = 9,
  NotImplemented = 10,
  RedisError = 11,
  TimedOut = 12,
  Interrupted = 13,
  IntentionalSystemExit = 14,
  UnexpectedSystemExit = 15,
};

#if defined(__clang__)
// Only clang supports warn_unused_result as a type annotation.
class RAY_MUST_USE_RESULT RAY_EXPORT Status;
#endif

class RAY_EXPORT Status {
 public:
  // Create a success status.
  Status() : state_(NULL) {}
  ~Status() { delete state_; }

  Status(StatusCode code, const std::string &msg);

  // Copy the specified status.
  Status(const Status &s);
  void operator=(const Status &s);

  // Return a success status.
  static Status OK() { return Status(); }

  // Return error status of an appropriate type.
  static Status OutOfMemory(const std::string &msg) {
    return Status(StatusCode::OutOfMemory, msg);
  }

  static Status KeyError(const std::string &msg) {
    return Status(StatusCode::KeyError, msg);
  }

  static Status TypeError(const std::string &msg) {
    return Status(StatusCode::TypeError, msg);
  }

  static Status UnknownError(const std::string &msg) {
    return Status(StatusCode::UnknownError, msg);
  }

  static Status NotImplemented(const std::string &msg) {
    return Status(StatusCode::NotImplemented, msg);
  }

  static Status Invalid(const std::string &msg) {
    return Status(StatusCode::Invalid, msg);
  }

  static Status IOError(const std::string &msg) {
    return Status(StatusCode::IOError, msg);
  }

  static Status ObjectExists(const std::string &msg) {
    return Status(StatusCode::ObjectExists, msg);
  }

  static Status ObjectStoreFull(const std::string &msg) {
    return Status(StatusCode::ObjectStoreFull, msg);
  }

  static Status RedisError(const std::string &msg) {
    return Status(StatusCode::RedisError, msg);
  }

  static Status TimedOut(const std::string &msg) {
    return Status(StatusCode::TimedOut, msg);
  }

  static Status Interrupted(const std::string &msg) {
    return Status(StatusCode::Interrupted, msg);
  }

  static Status IntentionalSystemExit() {
    return Status(StatusCode::IntentionalSystemExit, "intentional system exit");
  }

  static Status UnexpectedSystemExit() {
    return Status(StatusCode::UnexpectedSystemExit, "user code caused exit");
  }

  // Returns true iff the status indicates success.
  bool ok() const { return (state_ == NULL); }

  bool IsOutOfMemory() const { return code() == StatusCode::OutOfMemory; }
  bool IsKeyError() const { return code() == StatusCode::KeyError; }
  bool IsInvalid() const { return code() == StatusCode::Invalid; }
  bool IsIOError() const { return code() == StatusCode::IOError; }
  bool IsObjectExists() const { return code() == StatusCode::ObjectExists; }
  bool IsObjectStoreFull() const { return code() == StatusCode::ObjectStoreFull; }
  bool IsTypeError() const { return code() == StatusCode::TypeError; }
  bool IsUnknownError() const { return code() == StatusCode::UnknownError; }
  bool IsNotImplemented() const { return code() == StatusCode::NotImplemented; }
  bool IsRedisError() const { return code() == StatusCode::RedisError; }
  bool IsTimedOut() const { return code() == StatusCode::TimedOut; }
  bool IsInterrupted() const { return code() == StatusCode::Interrupted; }
  bool IsSystemExit() const {
    return code() == StatusCode::IntentionalSystemExit ||
           code() == StatusCode::UnexpectedSystemExit;
  }
  bool IsIntentionalSystemExit() const {
    return code() == StatusCode::IntentionalSystemExit;
  }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

  // Return a string representation of the status code, without the message
  // text or posix code information.
  std::string CodeAsString() const;

  StatusCode code() const { return ok() ? StatusCode::OK : state_->code; }

  std::string message() const { return ok() ? "" : state_->msg; }

 private:
  struct State {
    StatusCode code;
    std::string msg;
  };
  // OK status has a `NULL` state_.  Otherwise, `state_` points to
  // a `State` structure containing the error code and message(s)
  State *state_;

  void CopyFrom(const State *s);
};

static inline std::ostream &operator<<(std::ostream &os, const Status &x) {
  os << x.ToString();
  return os;
}

inline Status::Status(const Status &s)
    : state_((s.state_ == NULL) ? NULL : new State(*s.state_)) {}

inline void Status::operator=(const Status &s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (state_ != s.state_) {
    CopyFrom(s.state_);
  }
}

Status boost_to_ray_status(const boost::system::error_code &error);

}  // namespace ray

#endif  // RAY_STATUS_H_
