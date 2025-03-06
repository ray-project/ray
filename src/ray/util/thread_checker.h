// Copyright 2024 The Ray Authors.
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

// Used to sanity check threading issues by checking current thread id.
//
// Example usage:
// ThreadChecker thread_checker{};
//
// // Initialize on the thread at first usage.
// RAY_CHECK(thread_checker.IsOnSameThread());
//
// // Check it's on the same thread.
// RAY_CHECK(thread_checker.IsOnSameThread());

#pragma once

#include <atomic>
#include <thread>

namespace ray {

class ThreadChecker {
 public:
  // Return true at initialization, or current invocation happens on the same thread as
  // initialization.
  [[nodiscard]] bool IsOnSameThread() const;

 private:
  mutable std::atomic<std::thread::id> thread_id_{};
};

}  // namespace ray
