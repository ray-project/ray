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

#include "ray/raylet/worker_pool/cache_policy.h"

#include "ray/util/logging.h"

#include <algorithm>

namespace {}  // namespace

namespace ray {

namespace raylet {

IdlePoolSizePolicy::IdlePoolSizePolicy(size_t desired_cache_size,
                                       size_t max_starting_size) : desired_cache_size_(desired_cache_size), max_starting_size_(max_starting_size) {}

const size_t IdlePoolSizePolicy::GetNumIdleProcsToCreate(size_t idle_size,
                                                         size_t running_size,
                                                         size_t starting_size) {
  // Running: means that the process is running
  // Idle: Usually running. Means that the process doesn't have any task.
  // Starting: Process that was started but hasn't finished initialization.
  // Pending exit: Process that is exiting but hasn't confirmed.

  RAY_LOG(DEBUG) << "Idle size: " << idle_size << ", running_size: " << running_size << ", starting_size: " << starting_size;
  auto to_add = desired_cache_size_ - running_size;

  auto to_add_capped = (size_t) std::min(max_starting_size_, (size_t) std::max((size_t) 0, to_add));
  return to_add_capped;
}

// How to get this in incrementally?
// The worker killing poller operates on a per-worker basis.
// We need to determine which workers to kill...
// So we can either go with a very simple policy, or very simple policy plus policy which determines
//      which workers to kill. 
const size_t IdlePoolSizePolicy::GetNumIdleProcsToKill(size_t idle_size,
                                                       size_t running_size,
                                                       size_t starting_size) {
  // 
  return 0;
}

}  // namespace raylet

}  // namespace ray
