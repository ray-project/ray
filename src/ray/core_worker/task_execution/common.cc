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
    : async_wait_for_args_(std::move(async_wait_for_args)) {}

void ActorTaskExecutionArgWaiter::BeginArgsFetch(
    const std::vector<rpc::ObjectReference> &args, const TaskAttempt &task_attempt) {
  async_wait_for_args_(args, task_attempt.first, task_attempt.second);
}

void ActorTaskExecutionArgWaiter::OnArgsReady(const TaskAttempt &task_attempt,
                                              std::function<void()> on_args_ready) {
  RAY_CHECK(static_cast<bool>(on_args_ready))
      << "OnArgsReady called with empty callback for task " << task_attempt.first
      << " attempt " << task_attempt.second;
  auto it = in_flight_waits_.find(task_attempt);
  if (it != in_flight_waits_.end()) {
    // The only valid state we can find an existing entry in here is one parked
    // by MarkReady arriving first: callback empty + execute_immediate true.
    RAY_CHECK(it->second.execute_immediate &&
              !static_cast<bool>(it->second.on_args_ready))
        << "OnArgsReady called twice for task " << task_attempt.first << " attempt "
        << task_attempt.second;

    // Remove the entry and run the callback immediately
    in_flight_waits_.erase(it);
    on_args_ready();
    return;
  }

  // No prior entry: store the callback. Invariant — exactly one of `on_args_ready`
  // or `execute_immediate` is set on every entry. Here the callback is set.
  auto [_, inserted] = in_flight_waits_.emplace(
      task_attempt, WaitEntry{std::move(on_args_ready), /*execute_immediate=*/false});
  RAY_CHECK(inserted);
}

void ActorTaskExecutionArgWaiter::MarkReady(const TaskAttempt &task_attempt) {
  auto it = in_flight_waits_.find(task_attempt);
  if (it == in_flight_waits_.end()) {
    // OnArgsReady hasn't been called yet (deferred-attempt path in the unordered
    // queue, or the worker began exiting between BeginArgsFetch and the
    // bookkeeping post). Park the "ready" flag; OnArgsReady will pick it up.
    // Invariant — exactly one of `on_args_ready` or `execute_immediate` is set.
    auto [_, inserted] = in_flight_waits_.emplace(
        task_attempt, WaitEntry{/*on_args_ready=*/{}, /*execute_immediate=*/true});
    RAY_CHECK(inserted);
    return;
  }

  // A duplicate (task_id, attempt_number) at the executor is not possible.
  RAY_CHECK(static_cast<bool>(it->second.on_args_ready) && !it->second.execute_immediate)
      << "MarkReady called twice for task " << task_attempt.first << " attempt "
      << task_attempt.second;
  auto callback = std::move(it->second.on_args_ready);
  in_flight_waits_.erase(it);
  callback();
}
}  // namespace core
}  // namespace ray
