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

TaskToExecute::TaskToExecute() {}

TaskToExecute::TaskToExecute(
    std::function<void(const TaskSpecification &, rpc::SendReplyCallback)>
        accept_callback,
    std::function<void(const TaskSpecification &, const Status &, rpc::SendReplyCallback)>
        reject_callback,
    rpc::SendReplyCallback send_reply_callback,
    TaskSpecification task_spec)
    : accept_callback_(std::move(accept_callback)),
      reject_callback_(std::move(reject_callback)),
      send_reply_callback_(std::move(send_reply_callback)),
      task_spec_(std::move(task_spec)),
      pending_dependencies_(task_spec_.GetDependencies()) {}

void TaskToExecute::Accept() {
  accept_callback_(task_spec_, std::move(send_reply_callback_));
}

void TaskToExecute::Cancel(const Status &status) {
  reject_callback_(task_spec_, status, std::move(send_reply_callback_));
}

ray::TaskID TaskToExecute::TaskID() const { return task_spec_.TaskId(); }

uint64_t TaskToExecute::AttemptNumber() const { return task_spec_.AttemptNumber(); }

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
    : async_wait_for_args_(async_wait_for_args) {}

void ActorTaskExecutionArgWaiter::AsyncWait(const std::vector<rpc::ObjectReference> &args,
                                            std::function<void()> on_args_ready) {
  auto tag = next_tag_++;
  in_flight_waits_[tag] = on_args_ready;
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
