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

#include "ray/common/ray_object.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {
namespace raylet {

namespace internal {

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
  /// Waiting becasue the worker wasn't available since its registration timed out.
  WORKER_NOT_FOUND_REGISTRATION_TIMEOUT,
};

/// Work represents all the information needed to make a scheduling decision.
/// This includes the task, the information we need to communicate to
/// dispatch/spillback and the callback to trigger it.
class Work {
 public:
  RayTask task;
  const bool grant_or_reject;
  const bool is_selected_based_on_locality;
  rpc::RequestWorkerLeaseReply *reply;
  std::function<void(void)> callback;
  std::shared_ptr<TaskResourceInstances> allocated_instances;
  Work(RayTask task,
       bool grant_or_reject,
       bool is_selected_based_on_locality,
       rpc::RequestWorkerLeaseReply *reply,
       std::function<void(void)> callback,
       WorkStatus status = WorkStatus::WAITING)
      : task(task),
        grant_or_reject(grant_or_reject),
        is_selected_based_on_locality(is_selected_based_on_locality),
        reply(reply),
        callback(callback),
        allocated_instances(nullptr),
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
    return grant_or_reject || is_selected_based_on_locality;
  }

 private:
  WorkStatus status_ = WorkStatus::WAITING;
  UnscheduledWorkCause unscheduled_work_cause_ =
      UnscheduledWorkCause::WAITING_FOR_RESOURCE_ACQUISITION;
};

typedef std::function<const rpc::GcsNodeInfo *(const NodeID &node_id)> NodeInfoGetter;

}  // namespace internal

}  // namespace raylet
}  // namespace ray
