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

#include "ray/core_worker/task_execution/scheduling_util.h"

#include <string>
#include <utility>
#include <vector>

namespace ray {
namespace core {

InboundRequest::InboundRequest() {}

InboundRequest::InboundRequest(
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

void InboundRequest::Accept() {
  accept_callback_(task_spec_, std::move(send_reply_callback_));
}

void InboundRequest::Cancel(const Status &status) {
  reject_callback_(task_spec_, status, std::move(send_reply_callback_));
}

ray::TaskID InboundRequest::TaskID() const { return task_spec_.TaskId(); }

uint64_t InboundRequest::AttemptNumber() const { return task_spec_.AttemptNumber(); }

const std::string &InboundRequest::ConcurrencyGroupName() const {
  return task_spec_.ConcurrencyGroupName();
}

ray::FunctionDescriptor InboundRequest::FunctionDescriptor() const {
  return task_spec_.FunctionDescriptor();
}

const std::vector<rpc::ObjectReference> &InboundRequest::PendingDependencies() const {
  return pending_dependencies_;
};

bool InboundRequest::DependenciesResolved() const {
  return pending_dependencies_.empty();
}

void InboundRequest::MarkDependenciesResolved() { pending_dependencies_.clear(); }

const TaskSpecification &InboundRequest::TaskSpec() const { return task_spec_; }

DependencyWaiterImpl::DependencyWaiterImpl(WaitForActorCallArgs wait_for_actor_call_args)
    : wait_for_actor_call_args_(wait_for_actor_call_args) {}

void DependencyWaiterImpl::Wait(const std::vector<rpc::ObjectReference> &dependencies,
                                std::function<void()> on_dependencies_available) {
  auto tag = next_request_id_++;
  requests_[tag] = on_dependencies_available;
  RAY_CHECK_OK(wait_for_actor_call_args_(dependencies, tag));
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
