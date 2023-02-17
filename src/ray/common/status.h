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

#pragma once

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

#define RAY_RETURN_NOT_OK_ELSE(s, else_) \
  do {                                   \
    ::ray::Status _s = (s);              \
    if (!_s.ok()) {                      \
      else_;                             \
      return _s;                         \
    }                                    \
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

namespace ray {

enum class StatusCode : char {
  OK = 0,
  OutOfMemory = 1,
  KeyError = 2,
  TypeError = 3,
  Invalid = 4,
  IOError = 5,
  UnknownError = 9,
  NotImplemented = 10,
  RedisError = 11,
  TimedOut = 12,
  Interrupted = 13,
  IntentionalSystemExit = 14,
  UnexpectedSystemExit = 15,
  CreationTaskError = 16,
  NotFound = 17,
  Disconnected = 18,
  SchedulingCancelled = 19,
  // object store status
  ObjectExists = 21,
  ObjectNotFound = 22,
  ObjectAlreadySealed = 23,
  ObjectStoreFull = 24,
  TransientObjectStoreFull = 25,
  // grpc status
  // This represents UNAVAILABLE status code
  // returned by grpc.
  GrpcUnavailable = 26,
  // This represents all other status codes
  // returned by grpc that are not defined above.
  GrpcUnknown = 27,
  // Object store is both out of memory and
  // out of disk.
  OutOfDisk = 28,
  ObjectUnknownOwner = 29,
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

  static Status RedisError(const std::string &msg) {
    return Status(StatusCode::RedisError, msg);
  }

  static Status TimedOut(const std::string &msg) {
    return Status(StatusCode::TimedOut, msg);
  }

  static Status Interrupted(const std::string &msg) {
    return Status(StatusCode::Interrupted, msg);
  }

  static Status IntentionalSystemExit(const std::string &msg) {
    return Status(StatusCode::IntentionalSystemExit, msg);
  }

  static Status UnexpectedSystemExit(const std::string &msg) {
    return Status(StatusCode::UnexpectedSystemExit, msg);
  }

  static Status CreationTaskError(const std::string &msg) {
    return Status(StatusCode::CreationTaskError, msg);
  }

  static Status NotFound(const std::string &msg) {
    return Status(StatusCode::NotFound, msg);
  }

  static Status Disconnected(const std::string &msg) {
    return Status(StatusCode::Disconnected, msg);
  }

  static Status SchedulingCancelled(const std::string &msg) {
    return Status(StatusCode::SchedulingCancelled, msg);
  }

  static Status ObjectExists(const std::string &msg) {
    return Status(StatusCode::ObjectExists, msg);
  }

  static Status ObjectNotFound(const std::string &msg) {
    return Status(StatusCode::ObjectNotFound, msg);
  }

  static Status ObjectUnknownOwner(const std::string &msg) {
    return Status(StatusCode::ObjectUnknownOwner, msg);
  }

  static Status ObjectAlreadySealed(const std::string &msg) {
    return Status(StatusCode::ObjectAlreadySealed, msg);
  }

  static Status ObjectStoreFull(const std::string &msg) {
    return Status(StatusCode::ObjectStoreFull, msg);
  }

  static Status TransientObjectStoreFull(const std::string &msg) {
    return Status(StatusCode::TransientObjectStoreFull, msg);
  }

  static Status OutOfDisk(const std::string &msg) {
    return Status(StatusCode::OutOfDisk, msg);
  }

  static Status GrpcUnavailable(const std::string &msg) {
    return Status(StatusCode::GrpcUnavailable, msg);
  }

  static Status GrpcUnknown(const std::string &msg) {
    return Status(StatusCode::GrpcUnknown, msg);
  }

  static StatusCode StringToCode(const std::string &str);

  // Returns true iff the status indicates success.
  bool ok() const { return (state_ == NULL); }

  bool IsOutOfMemory() const { return code() == StatusCode::OutOfMemory; }
  bool IsOutOfDisk() const { return code() == StatusCode::OutOfDisk; }
  bool IsKeyError() const { return code() == StatusCode::KeyError; }
  bool IsInvalid() const { return code() == StatusCode::Invalid; }
  bool IsIOError() const { return code() == StatusCode::IOError; }
  bool IsTypeError() const { return code() == StatusCode::TypeError; }
  bool IsUnknownError() const { return code() == StatusCode::UnknownError; }
  bool IsNotImplemented() const { return code() == StatusCode::NotImplemented; }
  bool IsRedisError() const { return code() == StatusCode::RedisError; }
  bool IsTimedOut() const { return code() == StatusCode::TimedOut; }
  bool IsInterrupted() const { return code() == StatusCode::Interrupted; }
  bool ShouldExitWorker() const {
    return code() == StatusCode::IntentionalSystemExit ||
           code() == StatusCode::UnexpectedSystemExit ||
           code() == StatusCode::CreationTaskError;
  }
  bool IsIntentionalSystemExit() const {
    return code() == StatusCode::IntentionalSystemExit;
  }
  bool IsCreationTaskError() const { return code() == StatusCode::CreationTaskError; }
  bool IsUnexpectedSystemExit() const {
    return code() == StatusCode::UnexpectedSystemExit;
  }
  bool IsNotFound() const { return code() == StatusCode::NotFound; }
  bool IsDisconnected() const { return code() == StatusCode::Disconnected; }
  bool IsSchedulingCancelled() const { return code() == StatusCode::SchedulingCancelled; }
  bool IsObjectExists() const { return code() == StatusCode::ObjectExists; }
  bool IsObjectNotFound() const { return code() == StatusCode::ObjectNotFound; }
  bool IsObjectUnknownOwner() const { return code() == StatusCode::ObjectUnknownOwner; }
  bool IsObjectAlreadySealed() const { return code() == StatusCode::ObjectAlreadySealed; }
  bool IsObjectStoreFull() const { return code() == StatusCode::ObjectStoreFull; }
  bool IsTransientObjectStoreFull() const {
    return code() == StatusCode::TransientObjectStoreFull;
  }
  bool IsGrpcUnavailable() const { return code() == StatusCode::GrpcUnavailable; }
  bool IsGrpcUnknown() const { return code() == StatusCode::GrpcUnknown; }

  bool IsGrpcError() const { return IsGrpcUnknown() || IsGrpcUnavailable(); }

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
