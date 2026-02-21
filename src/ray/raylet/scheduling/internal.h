// Copyright 2020-2021 The Ray Authors.
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

#include <memory>
#include <utility>

#include "ray/common/lease/lease.h"
#include "ray/common/scheduling/cluster_resource_data.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray::raylet::internal {

enum class WorkStatus {
  /// Waiting to be scheduled.
  WAITING,
  /// Waiting for a worker to start.
  WAITING_FOR_WORKER,
  /// Queued task has been cancelled.
  CANCELLED,
};

/// This enum represents the cause of why work hasn't been scheduled yet.
enum class UnscheduledWorkCause {
  /// Waiting for acquiring resources.
  WAITING_FOR_RESOURCE_ACQUISITION,
  /// Waiting for more plasma store memory to be available. This is set when we can't pin
  /// task arguments due to the lack of memory.
  WAITING_FOR_AVAILABLE_PLASMA_MEMORY,
  /// Pending because there's no node that satisfies the resources in the cluster.
  WAITING_FOR_RESOURCES_AVAILABLE,
  /// Waiting because the worker wasn't available since job config for the worker wasn't
  /// registered yet.
  WORKER_NOT_FOUND_JOB_CONFIG_NOT_EXIST,
  /// Waiting because the worker wasn't available since its registration timed out.
  WORKER_NOT_FOUND_REGISTRATION_TIMEOUT,
};

/// Work represents all the information needed to make a scheduling decision.
/// This includes the lease, the information we need to communicate to
/// dispatch/spillback and the callbacks to trigger it.
struct ReplyCallback {
  ReplyCallback(rpc::SendReplyCallback send_reply_callback,
                rpc::RequestWorkerLeaseReply *reply)
      : send_reply_callback_(std::move(send_reply_callback)), reply_(reply) {}
  rpc::SendReplyCallback send_reply_callback_;
  rpc::RequestWorkerLeaseReply *reply_;
};

class Work {
 public:
  RayLease lease_;
  bool grant_or_reject_;
  bool is_selected_based_on_locality_;
  // All the callbacks will be triggered when the lease is scheduled.
  std::vector<ReplyCallback> reply_callbacks_;
  std::shared_ptr<TaskResourceInstances> allocated_instances_;

  Work(RayLease lease,
       bool grant_or_reject,
       bool is_selected_based_on_locality,
       std::vector<ReplyCallback> reply_callbacks,
       WorkStatus status = WorkStatus::WAITING)
      : lease_(std::move(lease)),
        grant_or_reject_(grant_or_reject),
        is_selected_based_on_locality_(is_selected_based_on_locality),
        reply_callbacks_(std::move(reply_callbacks)),
        allocated_instances_(nullptr),
        status_(status){};

  Work(const Work &Work) = delete;
  Work &operator=(const Work &work) = delete;
  ~Work() = default;

  /// Set the state as waiting with the cause.
  void SetStateWaiting(const UnscheduledWorkCause &cause) {
    status_ = WorkStatus::WAITING;
    unscheduled_work_cause_ = cause;
  }

  /// Set the state as waiting for workers, meaning it is waiting for workers to start.
  void SetStateWaitingForWorker() { status_ = WorkStatus::WAITING_FOR_WORKER; }

  /// Set the state as cancelled, meaning this task has to be unqueued from the node.
  void SetStateCancelled() { status_ = WorkStatus::CANCELLED; }

  WorkStatus GetState() const { return status_; }

  UnscheduledWorkCause GetUnscheduledCause() const { return unscheduled_work_cause_; }

  bool PrioritizeLocalNode() const {
    return grant_or_reject_ || is_selected_based_on_locality_;
  }

 private:
  WorkStatus status_ = WorkStatus::WAITING;
  UnscheduledWorkCause unscheduled_work_cause_ =
      UnscheduledWorkCause::WAITING_FOR_RESOURCE_ACQUISITION;
};

using NodeInfoGetter =
    std::function<std::optional<rpc::GcsNodeAddressAndLiveness>(const NodeID &node_id)>;

}  // namespace ray::raylet::internal
