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

#include "ray/raylet/node_manager.h"

namespace ray {

namespace raylet {

void NodeManager::CancelMismatchedLocalTasks(
    const std::string &local_virtual_cluster_id) {
  bool skip_actor = RayConfig::instance().gcs_actor_scheduling_enabled();
  auto predicate = [&local_virtual_cluster_id,
                    skip_actor](const std::shared_ptr<internal::Work> &work) {
    if (skip_actor && work->task.GetTaskSpecification().IsActorCreationTask()) {
      return false;
    }
    if (work->task.GetTaskSpecification().GetSchedulingStrategy().virtual_cluster_id() !=
        local_virtual_cluster_id) {
      return true;
    }
    return false;
  };
  auto tasks_canceled =
      cluster_task_manager_->CancelTasks(predicate,
                                         rpc::RequestWorkerLeaseReply::SCHEDULING_FAILED,
                                         "The node is removed from a virtual cluster.");
  if (tasks_canceled) {
    RAY_LOG(INFO) << "Tasks are cleaned up from cluster_task_manager because the "
                     "node is removed from virtual cluster.";
  }
  tasks_canceled =
      local_task_manager_->CancelTasks(predicate,
                                       rpc::RequestWorkerLeaseReply::SCHEDULING_FAILED,
                                       "The node is removed from a virtual cluster.");
  if (tasks_canceled) {
    RAY_LOG(INFO) << "Tasks are cleaned up from local_task_manager because the "
                     "node is removed from virtual cluster.";
  }
  if (!cluster_resource_scheduler_->GetLocalResourceManager().IsLocalNodeIdle()) {
    for (auto iter = leased_workers_.begin(); iter != leased_workers_.end();) {
      auto curr_iter = iter++;
      auto worker = curr_iter->second;
      const auto &task_spec = worker->GetAssignedTask().GetTaskSpecification();
      if (skip_actor && task_spec.IsActorCreationTask()) {
        continue;
      }
      if (task_spec.GetSchedulingStrategy().virtual_cluster_id() !=
          local_virtual_cluster_id) {
        RAY_LOG(INFO).WithField(worker->WorkerId())
            << "Worker is cleaned because the node is removed from virtual cluster.";
        DestroyWorker(
            worker,
            rpc::WorkerExitType::INTENDED_SYSTEM_EXIT,
            "Worker is cleaned because the node is removed from virtual cluster.");
      }
    }
  }
}

}  // namespace raylet

}  // namespace ray
