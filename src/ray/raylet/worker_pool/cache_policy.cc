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

namespace {}  // namespace

namespace ray {

namespace raylet {

IdlePoolSizePolicy::IdlePoolSizePolicy(size_t max_total_size,
                                       size_t desired_cache_size,
                                       size_t max_starting_size) {}

const size_t IdlePoolSizePolicy::GetNumIdleProcsToCreate(size_t idle_size,
                                                         size_t running_size,
                                                         size_t starting_size,
                                                         size_t terminating_size) {
  return 0;
}

const size_t IdlePoolSizePolicy::GetNumIdleProcsToKill(size_t idle_size,
                                                       size_t running_size,
                                                       size_t starting_size,
                                                       size_t terminating_size) {
  return 0;
}

}  // namespace raylet

}  // namespace ray
