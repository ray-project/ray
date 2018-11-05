#ifndef RAY_RAYLET_SCHEDULING_POLICY_H
#define RAY_RAYLET_SCHEDULING_POLICY_H

#include <random>
#include <unordered_map>

#include "ray/raylet/scheduling_queue.h"
#include "ray/raylet/scheduling_resources.h"

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


  /// \brief Given a set of cluster resources and subset of cluster nodes,
  /// perform a spill-over scheduling operation. This is a light-weight decision.
  ///
  /// \param cluster_resources: a set of cluster resources containing resource and load
  /// information for some subset of the cluster. For all client IDs in the returned
  /// placement map, the corresponding SchedulingResources::resources_load_ is
  /// incremented by the aggregate resource demand of the tasks assigned to it.
  /// \param remote_client_ids a list of IDs corresponding to raylets that participate
  /// in this scheduling decision.
  /// \return Scheduling decision, mapping tasks to raylets for placement.
  std::unordered_map<TaskID, ClientID> SpillOver(
      std::unordered_map<ClientID, SchedulingResources> &cluster_resources,
      std::vector<ClientID> &remote_client_ids);

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
