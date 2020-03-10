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

#include <assert.h>

namespace ray {

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
    return "OK";
  }

  const char *type;
  switch (code()) {
  case StatusCode::OK:
    type = "OK";
    break;
  case StatusCode::OutOfMemory:
    type = "Out of memory";
    break;
  case StatusCode::KeyError:
    type = "Key error";
    break;
  case StatusCode::TypeError:
    type = "Type error";
    break;
  case StatusCode::Invalid:
    type = "Invalid";
    break;
  case StatusCode::IOError:
    type = "IOError";
    break;
  case StatusCode::ObjectExists:
    type = "ObjectExists";
    break;
  case StatusCode::ObjectStoreFull:
    type = "ObjectStoreFull";
    break;
  case StatusCode::UnknownError:
    type = "Unknown error";
    break;
  case StatusCode::NotImplemented:
    type = "NotImplemented";
    break;
  case StatusCode::RedisError:
    type = "RedisError";
    break;
  case StatusCode::TimedOut:
    type = "TimedOut";
    break;
  case StatusCode::Interrupted:
    type = "Interrupted";
    break;
  default:
    type = "Unknown";
    break;
  }
  return std::string(type);
}

std::string Status::ToString() const {
  std::string result(CodeAsString());
  if (state_ == NULL) {
    return result;
  }
  result += ": ";
  result += state_->msg;
  return result;
}

}  // namespace ray
