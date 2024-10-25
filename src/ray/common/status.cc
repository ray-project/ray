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

#include <boost/system/error_code.hpp>

#include "absl/container/flat_hash_map.h"

namespace ray {

#define STATUS_CODE_OK "OK"
#define STATUS_CODE_OUT_OF_MEMORY "Out of memory"
#define STATUS_CODE_KEY_ERROR "Key error"
#define STATUS_CODE_TYPE_ERROR "Type error"
#define STATUS_CODE_INVALID "Invalid"
#define STATUS_CODE_IO_ERROR "IOError"
#define STATUS_CODE_UNKNOWN_ERROR "Unknown error"
#define STATUS_CODE_NOT_IMPLEMENTED "NotImplemented"
#define STATUS_CODE_REDIS_ERROR "RedisError"
#define STATUS_CODE_TIMED_OUT "TimedOut"
#define STATUS_CODE_INTERRUPTED "Interrupted"
#define STATUS_CODE_INTENTIONAL_SYSTEM_EXIT "IntentionalSystemExit"
#define STATUS_CODE_UNEXPECTED_SYSTEM_EXIT "UnexpectedSystemExit"
#define STATUS_CODE_CREATION_TASK_ERROR "CreationTaskError"
#define STATUS_CODE_NOT_FOUND "NotFound"
#define STATUS_CODE_DISCONNECTED "Disconnected"
#define STATUS_CODE_SCHEDULING_CANCELLED "SchedulingCancelled"
#define STATUS_CODE_OBJECT_EXISTS "ObjectExists"
#define STATUS_CODE_OBJECT_NOT_FOUND "ObjectNotFound"
#define STATUS_CODE_OBJECT_ALREADY_SEALED "ObjectAlreadySealed"
#define STATUS_CODE_OBJECT_STORE_FULL "ObjectStoreFull"
#define STATUS_CODE_TRANSIENT_OBJECT_STORE_FULL "TransientObjectStoreFull"
#define STATUS_CODE_OUT_OF_DISK "OutOfDisk"
#define STATUS_CODE_OBJECT_UNKNOWN_OWNER "ObjectUnknownOwner"
#define STATUS_CODE_RPC_ERROR "RpcError"
#define STATUS_CODE_OUT_OF_RESOURCE "OutOfResource"
#define STATUS_CODE_OBJECT_REF_END_OF_STREAM "ObjectRefEndOfStream"
#define STATUS_CODE_AUTH_ERROR "AuthError"
#define STATUS_CODE_INVALID_ARGUMENT "InvalidArgument"
#define STATUS_CODE_CHANNEL_ERROR "ChannelError"
#define STATUS_CODE_CHANNEL_TIMEOUT_ERROR "ChannelTimeoutError"

// not a real status (catch all for codes not known)
#define STATUS_CODE_UNKNOWN "Unknown"

namespace {

// Code <-> String mappings.

const absl::flat_hash_map<StatusCode, std::string> kCodeToStr = {
    {StatusCode::OK, STATUS_CODE_OK},
    {StatusCode::OutOfMemory, STATUS_CODE_OUT_OF_MEMORY},
    {StatusCode::KeyError, STATUS_CODE_KEY_ERROR},
    {StatusCode::TypeError, STATUS_CODE_TYPE_ERROR},
    {StatusCode::Invalid, STATUS_CODE_INVALID},
    {StatusCode::IOError, STATUS_CODE_IO_ERROR},
    {StatusCode::UnknownError, STATUS_CODE_UNKNOWN_ERROR},
    {StatusCode::NotImplemented, STATUS_CODE_NOT_IMPLEMENTED},
    {StatusCode::RedisError, STATUS_CODE_REDIS_ERROR},
    {StatusCode::TimedOut, STATUS_CODE_TIMED_OUT},
    {StatusCode::Interrupted, STATUS_CODE_INTERRUPTED},
    {StatusCode::IntentionalSystemExit, STATUS_CODE_INTENTIONAL_SYSTEM_EXIT},
    {StatusCode::UnexpectedSystemExit, STATUS_CODE_UNEXPECTED_SYSTEM_EXIT},
    {StatusCode::CreationTaskError, STATUS_CODE_CREATION_TASK_ERROR},
    {StatusCode::NotFound, STATUS_CODE_NOT_FOUND},
    {StatusCode::Disconnected, STATUS_CODE_DISCONNECTED},
    {StatusCode::SchedulingCancelled, STATUS_CODE_SCHEDULING_CANCELLED},
    {StatusCode::ObjectExists, STATUS_CODE_OBJECT_EXISTS},
    {StatusCode::ObjectNotFound, STATUS_CODE_OBJECT_NOT_FOUND},
    {StatusCode::ObjectAlreadySealed, STATUS_CODE_OBJECT_ALREADY_SEALED},
    {StatusCode::ObjectStoreFull, STATUS_CODE_OBJECT_STORE_FULL},
    {StatusCode::TransientObjectStoreFull, STATUS_CODE_TRANSIENT_OBJECT_STORE_FULL},
    {StatusCode::OutOfDisk, STATUS_CODE_OUT_OF_DISK},
    {StatusCode::ObjectUnknownOwner, STATUS_CODE_OBJECT_UNKNOWN_OWNER},
    {StatusCode::RpcError, STATUS_CODE_RPC_ERROR},
    {StatusCode::OutOfResource, STATUS_CODE_OUT_OF_RESOURCE},
    {StatusCode::ObjectRefEndOfStream, STATUS_CODE_OBJECT_REF_END_OF_STREAM},
    {StatusCode::AuthError, STATUS_CODE_AUTH_ERROR},
    {StatusCode::InvalidArgument, STATUS_CODE_INVALID_ARGUMENT},
    {StatusCode::ChannelError, STATUS_CODE_CHANNEL_ERROR},
    {StatusCode::ChannelTimeoutError, STATUS_CODE_CHANNEL_TIMEOUT_ERROR},
};

const absl::flat_hash_map<std::string, StatusCode> kStrToCode = []() {
  absl::flat_hash_map<std::string, StatusCode> str_to_code;
  for (const auto &pair : kCodeToStr) {
    str_to_code[pair.second] = pair.first;
  }
  return str_to_code;
}();

}  // namespace

Status::Status(StatusCode code, const std::string &msg, int rpc_code) {
  assert(code != StatusCode::OK);
  state_ = new State;
  state_->code = code;
  state_->msg = msg;
  state_->rpc_code = rpc_code;
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
  if (state_ == nullptr) {
    return STATUS_CODE_OK;
  }

  auto it = kCodeToStr.find(code());
  if (it == kCodeToStr.end()) {
    return STATUS_CODE_UNKNOWN;
  }
  return it->second;
}

StatusCode Status::StringToCode(const std::string &str) {
  // Note: unknown string is mapped to IOError, while unknown code is mapped to "Unknown"
  // which is not an error. This means code -> string -> code is not identity.
  auto it = kStrToCode.find(str);
  if (it == kStrToCode.end()) {
    return StatusCode::IOError;
  }
  return it->second;
}

std::string Status::ToString() const {
  std::string result(CodeAsString());
  if (state_ == nullptr) {
    return result;
  }
  result += ": ";
  result += state_->msg;
  return result;
}

Status boost_to_ray_status(const boost::system::error_code &error) {
  switch (error.value()) {
  case boost::system::errc::success:
    return Status::OK();
  default:
    return Status::IOError(strerror(error.value()));
  }
}

}  // namespace ray
