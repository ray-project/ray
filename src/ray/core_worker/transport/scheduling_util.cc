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

#include "ray/core_worker/transport/scheduling_util.h"

#include <string>
#include <utility>
#include <vector>

namespace ray {
namespace core {

QueuedTask::QueuedTask() {}

QueuedTask::QueuedTask(
    std::function<void(const TaskSpecification &, rpc::SendReplyCallback)>
        execute_task_callback,
    std::function<void(const TaskSpecification &, const Status &, rpc::SendReplyCallback)>
        cancel_task_callback,
    rpc::SendReplyCallback send_reply_callback,
    TaskSpecification task_spec)
    : execute_task_callback_(std::move(execute_task_callback)),
      cancel_task_callback_(std::move(cancel_task_callback)),
      send_reply_callback_(std::move(send_reply_callback)),
      task_spec_(std::move(task_spec)),
      pending_dependencies_(task_spec_.GetDependencies()) {}

void QueuedTask::Accept() {
  execute_task_callback_(task_spec_, std::move(send_reply_callback_));
}
void QueuedTask::Cancel(const Status &status) {
  cancel_task_callback_(task_spec_, status, std::move(send_reply_callback_));
}

bool QueuedTask::CanExecute() const { return pending_dependencies_.empty(); }
ray::TaskID QueuedTask::TaskID() const { return task_spec_.TaskId(); }
uint64_t QueuedTask::AttemptNumber() const { return task_spec_.AttemptNumber(); }
const std::string &QueuedTask::ConcurrencyGroupName() const {
  return task_spec_.ConcurrencyGroupName();
}
ray::FunctionDescriptor QueuedTask::FunctionDescriptor() const {
  return task_spec_.FunctionDescriptor();
}
const std::vector<rpc::ObjectReference> &QueuedTask::PendingDependencies() const {
  return pending_dependencies_;
};
void QueuedTask::MarkDependenciesSatisfied() { pending_dependencies_.clear(); }
const TaskSpecification &QueuedTask::TaskSpec() const { return task_spec_; }

DependencyWaiterImpl::DependencyWaiterImpl(DependencyWaiterInterface &dependency_client)
    : dependency_client_(dependency_client) {}

void DependencyWaiterImpl::Wait(const std::vector<rpc::ObjectReference> &dependencies,
                                std::function<void()> on_dependencies_available) {
  auto tag = next_request_id_++;
  requests_[tag] = on_dependencies_available;
  RAY_CHECK_OK(dependency_client_.WaitForActorCallArgs(dependencies, tag));
}

/// Fulfills the callback stored by Wait().
void DependencyWaiterImpl::OnWaitComplete(int64_t tag) {
  auto it = requests_.find(tag);
  RAY_CHECK(it != requests_.end());
  it->second();
  requests_.erase(it);
}
}  // namespace core
}  // namespace ray
