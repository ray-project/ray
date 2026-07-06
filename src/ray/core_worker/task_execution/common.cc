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

#include "ray/core_worker/task_execution/common.h"

#include <string>
#include <utility>
#include <vector>

namespace ray {
namespace core {

ActorTaskExecutionArgWaiter::ActorTaskExecutionArgWaiter(
    AsyncWaitForArgs async_wait_for_args)
    : async_wait_for_args_(async_wait_for_args) {}

void ActorTaskExecutionArgWaiter::AsyncWait(const std::vector<rpc::ObjectReference> &args,
                                            std::function<void()> on_args_ready) {
  auto tag = next_tag_++;
  in_flight_waits_.emplace(tag, std::move(on_args_ready));
  async_wait_for_args_(args, tag);
}

void ActorTaskExecutionArgWaiter::MarkReady(int64_t tag) {
  auto it = in_flight_waits_.find(tag);
  RAY_CHECK(it != in_flight_waits_.end())
      << "MarkReady called on a non-existent tag. This likely means it was called twice "
         "for the same tag mistakenly.";
  it->second();
  in_flight_waits_.erase(it);
}
}  // namespace core
}  // namespace ray
