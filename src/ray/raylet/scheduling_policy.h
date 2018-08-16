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

  /// Perform a scheduling operation, given a set of cluster resources and
  /// producing a mapping of tasks to node managers.
  ///
  ///  \param cluster_resources: a set of cluster resources representing
  ///         configured and current resource capacity on each node.
  /// \return Scheduling decision, mapping tasks to node managers for placement.
  std::unordered_map<TaskID, ClientID> Schedule(
      std::unordered_map<ClientID, SchedulingResources> &cluster_resources,
      const ClientID &local_client_id);
      //, const std::vector<ClientID> &others);

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
