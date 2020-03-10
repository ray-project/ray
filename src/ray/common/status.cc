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

#include "ray/common/status.h"
#include <map>

#include <assert.h>

namespace ray {

static const std::string kStatusCodeOK = "OK";
static const std::string kStatusCodeOutOfMemory = "Out of memory";
static const std::string kStatusCodeKeyError = "Key error";
static const std::string kStatusCodeTypeError = "Type error";
static const std::string kStatusCodeInvalid = "Invalid";
static const std::string kStatusCodeIOError = "IOError";
static const std::string kStatusCodeObjectExists = "ObjectExists";
static const std::string kStatusCodeObjectStoreFull = "ObjectStoreFull";
static const std::string kStatusCodeUnknownError = "Unknown error";
static const std::string kStatusCodeNotImplemented = "NotImplemented";
static const std::string kStatusCodeRedisError = "RedisError";
static const std::string kStatusCodeTimedOut = "TimedOut";
static const std::string kStatusCodeInterrupted = "Interrupted";
static const std::string kStatusCodeUnknown = "Unknown";
static const std::string kStatusCodeIntentionalSystemExit = "IntentionalSystemExit";
static const std::string kStatusCodeUnexpectedSystemExit = "UnexpectedSystemExit";
static const std::string kStatusSeparator = ": ";

Status::Status(StatusCode code, const std::string &msg) {
  assert(code != StatusCode::OK);
  state_ = new State;
  state_->code = code;
  state_->msg = msg;
}

void Status::CopyFrom(const State *state) {
  delete state_;
  if (state == nullptr) {
    state_ = nullptr;
  } else {
    state_ = new State(*state);
  }
}

std::string Status::CodeAsString() const {
  if (state_ == NULL) {
    return kStatusCodeOK;
  }

  switch (code()) {
  case StatusCode::OK:
    return kStatusCodeOK;
  case StatusCode::OutOfMemory:
    return kStatusCodeOutOfMemory;
  case StatusCode::KeyError:
    return kStatusCodeKeyError;
  case StatusCode::TypeError:
    return kStatusCodeTypeError;
  case StatusCode::Invalid:
    return kStatusCodeInvalid;
  case StatusCode::IOError:
    return kStatusCodeIOError;
  case StatusCode::ObjectExists:
    return kStatusCodeObjectExists;
  case StatusCode::ObjectStoreFull:
    return kStatusCodeObjectStoreFull;
  case StatusCode::UnknownError:
    return kStatusCodeUnknownError;
  case StatusCode::NotImplemented:
    return kStatusCodeNotImplemented;
  case StatusCode::RedisError:
    return kStatusCodeRedisError;
  case StatusCode::TimedOut:
    return kStatusCodeTimedOut;
  case StatusCode::Interrupted:
    return kStatusCodeInterrupted;
  case StatusCode::IntentionalSystemExit:
    return kStatusCodeIntentionalSystemExit;
  case StatusCode::UnexpectedSystemExit:
    return kStatusCodeUnexpectedSystemExit;
  default:
    return kStatusCodeUnknown;
  }
}

std::string Status::ToString() const {
  std::string result(CodeAsString());
  if (state_ == NULL) {
    return result;
  }
  result += kStatusSeparator;
  result += state_->msg;
  return result;
}

Status Status::FromString(const std::string &value) {
  static std::map<std::string, StatusCode> str_to_code = {
      {kStatusCodeOK, StatusCode::OK},
      {kStatusCodeOutOfMemory, StatusCode::OutOfMemory},
      {kStatusCodeKeyError, StatusCode::KeyError},
      {kStatusCodeTypeError, StatusCode::TypeError},
      {kStatusCodeInvalid, StatusCode::Invalid},
      {kStatusCodeIOError, StatusCode::IOError},
      {kStatusCodeObjectExists, StatusCode::ObjectExists},
      {kStatusCodeObjectStoreFull, StatusCode::ObjectStoreFull},
      {kStatusCodeUnknownError, StatusCode::UnknownError},
      {kStatusCodeNotImplemented, StatusCode::NotImplemented},
      {kStatusCodeRedisError, StatusCode::RedisError},
      {kStatusCodeTimedOut, StatusCode::TimedOut},
      {kStatusCodeInterrupted, StatusCode::Interrupted},
      {kStatusCodeIntentionalSystemExit, StatusCode::IntentionalSystemExit},
      {kStatusCodeUnexpectedSystemExit, StatusCode::UnexpectedSystemExit}};

  size_t pos = value.find(kStatusSeparator);
  if (pos != std::string::npos) {
    std::string code_str = value.substr(0, pos);
    RAY_CHECK(str_to_code.count(code_str));
    StatusCode code = str_to_code[code_str];
    return Status(code, value.substr(pos + kStatusSeparator.size()));
  } else {
    // Status ok does not include ":".
    return Status();
  }
}

}  // namespace ray
