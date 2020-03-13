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

#ifndef RAY_RAYLET_SCHEDULING_POLICY_H
#define RAY_RAYLET_SCHEDULING_POLICY_H

#include <random>
#include <unordered_map>

#include "ray/common/task/scheduling_resources.h"
#include "ray/raylet/scheduling_queue.h"

namespace ray {

namespace raylet {

/// \class SchedulingPolicy
/// \brief Implements a scheduling policy for the node manager.
class SchedulingPolicy {
 public:
  /// \brief SchedulingPolicy constructor.
  ///
  /// \param scheduling_queue: reference to a scheduler queues object for access to
  /// tasks.
  /// \return Void.
  SchedulingPolicy(const SchedulingQueue &scheduling_queue);

  /// \brief Perform a scheduling operation, given a set of cluster resources and
  /// producing a mapping of tasks to raylets.
  ///
  /// \param cluster_resources: a set of cluster resources containing resource and load
  /// information for some subset of the cluster. For all client IDs in the returned
  /// placement map, the corresponding SchedulingResources::resources_load_ is
  /// incremented by the aggregate resource demand of the tasks assigned to it.
  /// \param local_client_id The ID of the node manager that owns this
  /// SchedulingPolicy object.
  /// \return Scheduling decision, mapping tasks to raylets for placement.
  std::unordered_map<TaskID, ClientID> Schedule(
      std::unordered_map<ClientID, SchedulingResources> &cluster_resources,
      const ClientID &local_client_id);

  /// \brief Given a set of cluster resources perform a spill-over scheduling operation.
  ///
  /// \param cluster_resources: a set of cluster resources containing resource and load
  /// information for some subset of the cluster. For all client IDs in the returned
  /// placement map, the corresponding SchedulingResources::resources_load_ is
  /// incremented by the aggregate resource demand of the tasks assigned to it.
  /// \return Scheduling decision, mapping tasks to raylets for placement.
  std::vector<TaskID> SpillOver(SchedulingResources &remote_scheduling_resources) const;

  /// \brief SchedulingPolicy destructor.
  virtual ~SchedulingPolicy();

 private:
  /// An immutable reference to the scheduling task queues.
  const SchedulingQueue &scheduling_queue_;
  /// Internally maintained random number generator.
  std::mt19937_64 gen_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_SCHEDULING_POLICY_H
