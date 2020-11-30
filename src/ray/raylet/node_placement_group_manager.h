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
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/bundle_spec.h"


#include "ray/rpc/grpc_client.h"
#include "ray/rpc/node_manager/node_manager_server.h"
#include "ray/rpc/node_manager/node_manager_client.h"
#include "ray/common/task/task.h"
#include "ray/common/ray_object.h"
#include "ray/common/client_connection.h"
#include "ray/common/task/task_common.h"
#include "ray/object_manager/object_manager.h"
#include "ray/raylet/actor_registration.h"
#include "ray/raylet/agent_manager.h"
#include "ray/raylet/local_object_manager.h"
#include "ray/raylet/scheduling/scheduling_ids.h"
#include "ray/raylet/scheduling/cluster_resource_scheduler.h"
#include "ray/raylet/scheduling/cluster_task_manager.h"
#include "ray/raylet/scheduling_policy.h"
#include "ray/raylet/scheduling_queue.h"
#include "ray/raylet/reconstruction_policy.h"
#include "ray/raylet/task_dependency_manager.h"
#include "ray/raylet/worker_pool.h"
#include "ray/rpc/worker/core_worker_client_pool.h"
#include "ray/util/ordered_set.h"
#include "ray/raylet/node_placement_group_manager.h"

namespace ray {

namespace raylet {

enum CommitState {
  /// Resources are prepared.
  PREPARED,
  /// Resources are COMMITTED.
  COMMITTED
};

struct BundleState {
  /// Leasing state for 2PC protocol.
  CommitState state;
  /// Resources that are acquired at preparation stage.
  ResourceIdSet acquired_resources;
};

struct pair_hash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &pair) const {
    return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
  }
};

class NodePlacementGroupManager {

 public:
  NodePlacementGroupManager(std::shared_ptr<ResourceIdSet> local_available_resources_,
                            std::shared_ptr<std::unordered_map<NodeID, SchedulingResources>> cluster_resource_map_,
                            const NodeID &self_node_id_);

  bool PrepareBundleResources(const BundleSpecification &bundle_spec);

  void CommitBundleResources(const BundleSpecification &bundle_spec);

  /// Return back all the bundle resource.
  ///
  /// \param bundle_spec: Specification of bundle whose resources will be returned.
  /// \return Whether the resource is returned successfully.
  void ReturnBundleResources(const BundleSpecification &bundle_spec);

  void ReturnUnusedBundleResources(const std::unordered_set<BundleID, pair_hash> &in_use_bundles);

  const ResourceIdSet &GetAllResourceIdSet() const { return *local_available_resources_; };

  const SchedulingResources &GetAllResourceSetWithoutId() const { return (*cluster_resource_map_)[self_node_id_]; }

 private:
  /// The resources (and specific resource IDs) that are currently available.
  std::shared_ptr<ResourceIdSet> local_available_resources_;
  std::shared_ptr<std::unordered_map<NodeID, SchedulingResources>> cluster_resource_map_;
  
  NodeID self_node_id_;
  
  /// This map represents the commit state of 2PC protocol for atomic placement group
  /// creation.
  absl::flat_hash_map<BundleID, std::shared_ptr<BundleState>, pair_hash>
      bundle_state_map_;

  /// Save `BundleSpecification` for cleaning leaked bundles after GCS restart.
  absl::flat_hash_map<BundleID, std::shared_ptr<BundleSpecification>, pair_hash>
      bundle_spec_map_;
};

}  // namespace raylet

}  // end namespace ray