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

TaskToExecute::TaskToExecute(
    std::function<void(const TaskSpecification &)> execute_callback,
    std::function<void(const TaskSpecification &, const Status &)> cancel_callback,
    TaskSpecification task_spec)
    : execute_callback_(std::move(execute_callback)),
      cancel_callback_(std::move(cancel_callback)),
      task_spec_(std::move(task_spec)),
      pending_dependencies_(task_spec_.GetDependencies()) {}

void TaskToExecute::Execute() { execute_callback_(task_spec_); }

void TaskToExecute::Cancel(const Status &status) { cancel_callback_(task_spec_, status); }

ray::TaskID TaskToExecute::TaskID() const { return task_spec_.TaskId(); }

uint64_t TaskToExecute::AttemptNumber() const { return task_spec_.AttemptNumber(); }

bool TaskToExecute::IsRetry() const { return task_spec_.IsRetry(); }

const std::string &TaskToExecute::ConcurrencyGroupName() const {
  return task_spec_.ConcurrencyGroupName();
}

ray::FunctionDescriptor TaskToExecute::FunctionDescriptor() const {
  return task_spec_.FunctionDescriptor();
}

const std::vector<rpc::ObjectReference> &TaskToExecute::PendingDependencies() const {
  return pending_dependencies_;
};

bool TaskToExecute::DependenciesResolved() const { return pending_dependencies_.empty(); }

void TaskToExecute::MarkDependenciesResolved() { pending_dependencies_.clear(); }

const TaskSpecification &TaskToExecute::TaskSpec() const { return task_spec_; }

ActorTaskExecutionArgWaiter::ActorTaskExecutionArgWaiter(
    AsyncWaitForArgs async_wait_for_args)
    : async_wait_for_args_(std::move(async_wait_for_args)) {}

int64_t ActorTaskExecutionArgWaiter::BeginArgsFetch(
    const std::vector<rpc::ObjectReference> &args) {
  int64_t tag = next_tag_.fetch_add(1, std::memory_order_relaxed);
  async_wait_for_args_(args, tag);
  return tag;
}

void ActorTaskExecutionArgWaiter::OnArgsReady(int64_t tag,
                                              std::function<void()> on_args_ready) {
  RAY_CHECK(static_cast<bool>(on_args_ready))
      << "OnArgsReady called with empty callback for tag " << tag;
  auto it = in_flight_waits_.find(tag);
  if (it != in_flight_waits_.end()) {
    // The only valid state we can find an existing entry in here is one parked
    // by MarkReady arriving first: callback empty + execute_immediate true.
    RAY_CHECK(it->second.execute_immediate &&
              !static_cast<bool>(it->second.on_args_ready))
        << "OnArgsReady called twice for tag " << tag;

    // Remove the entry and  run the callback immediately
    in_flight_waits_.erase(it);
    on_args_ready();
    return;
  }

  // No prior entry: store the callback. Invariant — exactly one of `on_args_ready`
  // or `execute_immediate` is set on every entry. Here the callback is set.
  auto [_, inserted] = in_flight_waits_.emplace(
      tag, WaitEntry{std::move(on_args_ready), /*execute_immediate=*/false});
  RAY_CHECK(inserted);
}

void ActorTaskExecutionArgWaiter::MarkReady(int64_t tag) {
  auto it = in_flight_waits_.find(tag);
  if (it == in_flight_waits_.end()) {
    // OnArgsReady hasn't been called yet (deferred-attempt path in the unordered
    // queue, or the worker began exiting between BeginArgsFetch and the
    // bookkeeping post). Park the "ready" flag; OnArgsReady will pick it up.
    // Invariant — exactly one of `on_args_ready` or `execute_immediate` is set.
    auto [_, inserted] = in_flight_waits_.emplace(
        tag, WaitEntry{/*on_args_ready=*/{}, /*execute_immediate=*/true});
    RAY_CHECK(inserted);
    return;
  }

  // The only valid state we can find here is one parked by OnArgsReady arriving
  // first: callback set + execute_immediate false.
  RAY_CHECK(static_cast<bool>(it->second.on_args_ready) && !it->second.execute_immediate)
      << "MarkReady called twice for tag " << tag;
  auto callback = std::move(it->second.on_args_ready);
  in_flight_waits_.erase(it);
  callback();
}
}  // namespace core
}  // namespace ray
