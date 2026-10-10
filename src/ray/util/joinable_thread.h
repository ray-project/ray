// Copyright 2026 The Ray Authors.
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

#include <thread>
#include <utility>

namespace ray {
class JoinableThread {
 public:
  /// Create a JoinableThread from an existing std::thread.
  ///
  /// \param[in] thread The thread to wrap
  explicit JoinableThread(std::thread thread) noexcept : thread_(std::move(thread)) {}

  ~JoinableThread() {
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  // Support moving, but disallow copying to prevent multiple joins
  JoinableThread(JoinableThread &&) noexcept = default;
  JoinableThread &operator=(JoinableThread &&) noexcept = default;
  JoinableThread(const JoinableThread &) = delete;
  JoinableThread &operator=(const JoinableThread &) = delete;

 private:
  std::thread thread_;
};

}  // namespace ray
