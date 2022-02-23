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

#include "ray/core_worker/transport/actor_scheduling_queue.h"

namespace ray {
namespace core {

InboundRequest::InboundRequest() {}

InboundRequest::InboundRequest(
    std::function<void(rpc::SendReplyCallback)> accept_callback,
    std::function<void(rpc::SendReplyCallback)> reject_callback,
    rpc::SendReplyCallback send_reply_callback, class TaskID task_id,
    bool has_dependencies, const std::string &concurrency_group_name,
    const ray::FunctionDescriptor &function_descriptor)
    : accept_callback_(std::move(accept_callback)),
      reject_callback_(std::move(reject_callback)),
      send_reply_callback_(std::move(send_reply_callback)),
      task_id_(task_id),
      concurrency_group_name_(concurrency_group_name),
      function_descriptor_(function_descriptor),
      has_pending_dependencies_(has_dependencies) {}

void InboundRequest::Accept() { accept_callback_(std::move(send_reply_callback_)); }
void InboundRequest::Cancel() { reject_callback_(std::move(send_reply_callback_)); }

bool InboundRequest::CanExecute() const { return !has_pending_dependencies_; }
ray::TaskID InboundRequest::TaskID() const { return task_id_; }
const std::string &InboundRequest::ConcurrencyGroupName() const {
  return concurrency_group_name_;
}
const ray::FunctionDescriptor &InboundRequest::FunctionDescriptor() const {
  return function_descriptor_;
}
void InboundRequest::MarkDependenciesSatisfied() { has_pending_dependencies_ = false; }

DependencyWaiterImpl::DependencyWaiterImpl(DependencyWaiterInterface &dependency_client)
    : dependency_client_(dependency_client) {}

void DependencyWaiterImpl::Wait(const std::vector<rpc::ObjectReference> &dependencies,
                                std::function<void()> on_dependencies_available) {
  auto tag = next_request_id_++;
  requests_[tag] = on_dependencies_available;
  RAY_CHECK_OK(dependency_client_.WaitForDirectActorCallArgs(dependencies, tag));
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
