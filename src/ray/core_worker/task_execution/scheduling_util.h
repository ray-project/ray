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

#include <string>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace core {

/// Object dependency and RPC state of an inbound request.
class InboundRequest {
 public:
  InboundRequest();
  InboundRequest(std::function<void(const TaskSpecification &, rpc::SendReplyCallback)>
                     accept_callback,
                 std::function<void(const TaskSpecification &,
                                    const Status &,
                                    rpc::SendReplyCallback)> reject_callback,
                 rpc::SendReplyCallback send_reply_callback,
                 TaskSpecification task_spec);

  void Accept();
  void Cancel(const Status &status);
  ray::TaskID TaskID() const;
  uint64_t AttemptNumber() const;
  const std::string &ConcurrencyGroupName() const;
  ray::FunctionDescriptor FunctionDescriptor() const;
  bool DependenciesResolved() const;
  void MarkDependenciesResolved();
  const std::vector<rpc::ObjectReference> &PendingDependencies() const;
  const TaskSpecification &TaskSpec() const;

 private:
  std::function<void(const TaskSpecification &, rpc::SendReplyCallback)> accept_callback_;
  std::function<void(const TaskSpecification &, const Status &, rpc::SendReplyCallback)>
      reject_callback_;
  rpc::SendReplyCallback send_reply_callback_;

  TaskSpecification task_spec_;
  std::vector<rpc::ObjectReference> pending_dependencies_;
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
  using WaitForActorCallArgs = std::function<Status(
      const std::vector<rpc::ObjectReference> &dependencies, int64_t tag)>;

  explicit DependencyWaiterImpl(WaitForActorCallArgs wait_for_actor_call_args);

  void Wait(const std::vector<rpc::ObjectReference> &dependencies,
            std::function<void()> on_dependencies_available) override;

  /// Fulfills the callback stored by Wait().
  void OnWaitComplete(int64_t tag);

 private:
  int64_t next_request_id_ = 0;
  absl::flat_hash_map<int64_t, std::function<void()>> requests_;
  WaitForActorCallArgs wait_for_actor_call_args_;
};

}  // namespace core
}  // namespace ray
