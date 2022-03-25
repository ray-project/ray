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

#include <memory>

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_actor_scheduler.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/raylet/scheduling/cluster_resource_manager.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

using ClusterResourceScheduler = gcs::GcsResourceScheduler;

/// `GcsActorWorkerAssignment` represents the assignment from one or multiple actors to a
/// worker process.
/// TODO(Chong-Li): It contains multiple slots, and each of them can bind to an actor.
class GcsActorWorkerAssignment
    : public std::enable_shared_from_this<GcsActorWorkerAssignment> {
 public:
  /// Construct a GcsActorWorkerAssignment.
  ///
  /// \param node_id ID of node on which this gcs actor worker assignment is allocated.
  /// \param acquired_resources Resources owned by this gcs actor worker assignment.
  GcsActorWorkerAssignment(const NodeID &node_id,
                           const ResourceRequest &acquired_resources);
  const NodeID &GetNodeID() const;

  const ResourceRequest &GetResources() const;

 private:
  /// ID of node on which this actor worker assignment is allocated.
  const NodeID node_id_;
  /// Resources owned by this actor worker assignment.
  const ResourceRequest acquired_resources_;
};

/// GcsBasedActorScheduler inherits from GcsActorScheduler. Its scheduling strategy is
/// based on a resource-based node selection. Any rescheduling is also based on GCS,
/// instead of Raylet-based spillback.
class GcsBasedActorScheduler : public GcsActorScheduler {
 public:
  /// Create a GcsBasedActorScheduler
  ///
  /// \param io_context The main event loop.
  /// \param gcs_actor_table Used to flush actor info to storage.
  /// \param gcs_node_manager The node manager which is used when scheduling.
  /// \param cluster_resource_scheduler The scheduler to select nodes based on cluster
  /// resources.
  /// \param schedule_failure_handler Invoked when there are no available nodes to
  /// schedule actors.
  /// \param schedule_success_handler Invoked when actors are created on the worker
  /// successfully.
  /// \param raylet_client_pool Raylet client pool to construct connections to raylets.
  /// \param client_factory Factory to create remote core worker client, default factor
  /// will be used if not set.
  explicit GcsBasedActorScheduler(
      instrumented_io_context &io_context,
      GcsActorTable &gcs_actor_table,
      const GcsNodeManager &gcs_node_manager,
      std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler,
      GcsActorSchedulerFailureCallback schedule_failure_handler,
      GcsActorSchedulerSuccessCallback schedule_success_handler,
      std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
      rpc::ClientFactoryFn client_factory = nullptr,
      std::function<void(const NodeID &, const rpc::ResourcesData &)>
          normal_task_resources_changed_callback = nullptr);

  virtual ~GcsBasedActorScheduler() = default;

  /// Handle the destruction of an actor.
  ///
  /// \param actor The actor to be destoryed.
  void OnActorDestruction(std::shared_ptr<GcsActor> actor) override;

  /// Add resources changed event handler.
  void AddResourcesChangedListener(std::function<void()> listener);

 protected:
  /// Select a node for the actor based on cluster resources.
  ///
  /// \param actor The actor to be scheduled.
  /// \return The selected node's ID. If the selection fails, NodeID::Nil() is returned.
  NodeID SelectNode(std::shared_ptr<GcsActor> actor) override;

  /// Handler to process a worker lease reply.
  /// If a rejection is received, it means resources were preempted by normal
  /// tasks. Then update the the cluster resource view and reschedule immediately.
  ///
  /// \param actor The actor to be scheduled.
  /// \param node The selected node at which a worker is to be leased.
  /// \param status Status of the reply of `RequestWorkerLeaseRequest`.
  /// \param reply The reply of `RequestWorkerLeaseRequest`.
  void HandleWorkerLeaseReply(std::shared_ptr<GcsActor> actor,
                              std::shared_ptr<rpc::GcsNodeInfo> node,
                              const Status &status,
                              const rpc::RequestWorkerLeaseReply &reply) override;

 private:
  /// Allocate a new actor worker assignment.
  ///
  /// \param task_spec The specification of the task.
  std::unique_ptr<GcsActorWorkerAssignment> AllocateActorWorkerAssignment(
      const TaskSpecification &task_spec);

  /// TODO(Chong-Li): This is to accommodate the Raylet scheduling's behavior (different
  /// resources for scheduling and allocation). We need to unify these two at the end.
  /// Allocate resources for the actor.
  ///
  /// \param required_placement_resources The required resources of the task for
  /// scheduling. \param required_resources The required resources of the task for
  /// allocation. \return ID of the node from which the resources are allocated.
  scheduling::NodeID AllocateResources(
      const ResourceRequest &required_placement_resources,
      const ResourceRequest &required_resources);

  scheduling::NodeID GetHighestScoreNodeResource(
      const ResourceRequest &required_resources) const;

  void WarnResourceAllocationFailure(const TaskSpecification &task_spec,
                                     const ResourceRequest &required_resources) const;

  /// A rejected rely means resources were preempted by normal tasks. Then
  /// update the the cluster resource view and reschedule immediately.
  void HandleWorkerLeaseRejectedReply(std::shared_ptr<GcsActor> actor,
                                      const rpc::RequestWorkerLeaseReply &reply);

  /// Reset the actor's current assignment, while releasing acquired resources.
  void ResetActorWorkerAssignment(GcsActor *actor);

  /// Notify that the cluster resources are changed.
  void NotifyClusterResourcesChanged();

  /// The resource changed listeners.
  std::vector<std::function<void()>> resource_changed_listeners_;

  /// Gcs resource scheduler
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;

  /// Normal task resources changed callback.
  std::function<void(const NodeID &, const rpc::ResourcesData &)>
      normal_task_resources_changed_callback_;
};
}  // namespace gcs
}  // namespace ray
