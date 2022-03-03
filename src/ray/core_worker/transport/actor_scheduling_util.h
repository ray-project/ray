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

#pragma once

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

/// Object dependency and RPC state of an inbound request.
class InboundRequest {
 public:
  InboundRequest();
  InboundRequest(std::function<void(rpc::SendReplyCallback)> accept_callback,
                 std::function<void(rpc::SendReplyCallback)> reject_callback,
                 rpc::SendReplyCallback send_reply_callback, TaskID task_id,
                 bool has_dependencies, const std::string &concurrency_group_name,
                 const ray::FunctionDescriptor &function_descriptor);

  void Accept();
  void Cancel();
  bool CanExecute() const;
  ray::TaskID TaskID() const;
  const std::string &ConcurrencyGroupName() const;
  const ray::FunctionDescriptor &FunctionDescriptor() const;
  void MarkDependenciesSatisfied();

 private:
  std::function<void(rpc::SendReplyCallback)> accept_callback_;
  std::function<void(rpc::SendReplyCallback)> reject_callback_;
  rpc::SendReplyCallback send_reply_callback_;

  ray::TaskID task_id_;
  std::string concurrency_group_name_;
  ray::FunctionDescriptor function_descriptor_;
  bool has_pending_dependencies_;
};

/// Waits for an object dependency to become available. Abstract for testing.
class DependencyWaiter {
 public:
  /// Calls `callback` once the specified objects become available.
  virtual void Wait(const std::vector<rpc::ObjectReference> &dependencies,
                    std::function<void()> on_dependencies_available) = 0;

  virtual ~DependencyWaiter(){};
};

class DependencyWaiterImpl : public DependencyWaiter {
 public:
  DependencyWaiterImpl(DependencyWaiterInterface &dependency_client);

  void Wait(const std::vector<rpc::ObjectReference> &dependencies,
            std::function<void()> on_dependencies_available) override;

  /// Fulfills the callback stored by Wait().
  void OnWaitComplete(int64_t tag);

 private:
  int64_t next_request_id_ = 0;
  std::unordered_map<int64_t, std::function<void()>> requests_;
  DependencyWaiterInterface &dependency_client_;
};

}  // namespace core
}  // namespace ray
