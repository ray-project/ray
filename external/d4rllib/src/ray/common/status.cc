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

#include <boost/system/error_code.hpp>
#include <cassert>
#include <sstream>
#include <string_view>

#include "absl/container/flat_hash_map.h"

namespace ray {

constexpr std::string_view kStatusCodeOk = "OK";
// not a real status (catch all for codes not known)
constexpr std::string_view kStatusCodeUnknown = "Unknown";

namespace {

// Code <-> String mappings.

const absl::flat_hash_map<StatusCode, std::string_view> kCodeToStr = {
    {StatusCode::OK, kStatusCodeOk},
    {StatusCode::OutOfMemory, "Out of memory"},
    {StatusCode::KeyError, "Key error"},
    {StatusCode::TypeError, "Type error"},
    {StatusCode::Invalid, "Invalid"},
    {StatusCode::IOError, "IOError"},
    {StatusCode::UnknownError, "Unknown error"},
    {StatusCode::NotImplemented, "NotImplemented"},
    {StatusCode::RedisError, "RedisError"},
    {StatusCode::TimedOut, "TimedOut"},
    {StatusCode::Interrupted, "Interrupted"},
    {StatusCode::IntentionalSystemExit, "IntentionalSystemExit"},
    {StatusCode::UnexpectedSystemExit, "UnexpectedSystemExit"},
    {StatusCode::CreationTaskError, "CreationTaskError"},
    {StatusCode::NotFound, "NotFound"},
    {StatusCode::Disconnected, "Disconnected"},
    {StatusCode::SchedulingCancelled, "SchedulingCancelled"},
    {StatusCode::AlreadyExists, "AlreadyExists"},
    {StatusCode::ObjectExists, "ObjectExists"},
    {StatusCode::ObjectNotFound, "ObjectNotFound"},
    {StatusCode::ObjectAlreadySealed, "ObjectAlreadySealed"},
    {StatusCode::ObjectStoreFull, "ObjectStoreFull"},
    {StatusCode::TransientObjectStoreFull, "TransientObjectStoreFull"},
    {StatusCode::OutOfDisk, "OutOfDisk"},
    {StatusCode::ObjectUnknownOwner, "ObjectUnknownOwner"},
    {StatusCode::RpcError, "RpcError"},
    {StatusCode::OutOfResource, "OutOfResource"},
    {StatusCode::ObjectRefEndOfStream, "ObjectRefEndOfStream"},
    {StatusCode::AuthError, "AuthError"},
    {StatusCode::InvalidArgument, "InvalidArgument"},
    {StatusCode::ChannelError, "ChannelError"},
    {StatusCode::ChannelTimeoutError, "ChannelTimeoutError"},
    {StatusCode::PermissionDenied, "PermissionDenied"},
};

const absl::flat_hash_map<std::string_view, StatusCode> kStrToCode = []() {
  absl::flat_hash_map<std::string_view, StatusCode> str_to_code;
  for (const auto &pair : kCodeToStr) {
    str_to_code[pair.second] = pair.first;
  }
  return str_to_code;
}();

}  // namespace

Status::Status(StatusCode code, const std::string &msg, int rpc_code)
    : Status(code, msg, SourceLocation{}, rpc_code) {}

Status::Status(StatusCode code,
               const std::string &msg,
               SourceLocation loc,
               int rpc_code) {
  assert(code != StatusCode::OK);
  state_ = new State;
  state_->code = code;
  state_->msg = msg;
  state_->loc = loc;
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
    return std::string(kStatusCodeOk);
  }

  auto it = kCodeToStr.find(code());
  if (it == kCodeToStr.end()) {
    return std::string(kStatusCodeUnknown);
  }
  return std::string(it->second);
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

  if (IsRpcError()) {
    result += " rpc_code: ";
    result += absl::StrFormat("%d", state_->rpc_code);
  }

  if (IsValidSourceLoc(state_->loc)) {
    std::stringstream ss;
    ss << state_->loc;
    result += " at ";
    result += ss.str();
  }
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
