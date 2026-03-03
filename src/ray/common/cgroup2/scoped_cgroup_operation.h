// Copyright 2025 The Ray Authors.
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
#pragma once

#include <utility>

namespace ray {

/**
  A simple RAII style guard that calls the registered callback on destruction.
  ScopedCgroupOperation instances can be moved, but they cannot be copied.

  Usage:
    ScopedCgroupOperation say_hello_on_death([]() {
      RAY_INFO(INFO) << "Hi, I'm dying!";
    });
*/
class ScopedCgroupOperation {
 public:
  explicit ScopedCgroupOperation(std::function<void()> cleanup_fcn)
      : cleanup_fcn_(std::move(cleanup_fcn)) {}

  ~ScopedCgroupOperation() { cleanup_fcn_(); }

  ScopedCgroupOperation(const ScopedCgroupOperation &) = delete;
  ScopedCgroupOperation &operator=(const ScopedCgroupOperation &other) = delete;

  ScopedCgroupOperation(ScopedCgroupOperation &&other) noexcept
      : cleanup_fcn_(std::move(other.cleanup_fcn_)) {
    other.cleanup_fcn_ = []() {};
  }

  ScopedCgroupOperation &operator=(ScopedCgroupOperation &&other) noexcept {
    cleanup_fcn_ = std::move(other.cleanup_fcn_);
    other.cleanup_fcn_ = []() {};
    return *this;
  }

 private:
  // Defaults to no cleanup.
  std::function<void()> cleanup_fcn_ = []() {};
};
}  // namespace ray
