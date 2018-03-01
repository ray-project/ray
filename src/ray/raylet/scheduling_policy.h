#ifndef LS_POLICY_H
#define LS_POLICY_H


#include <unordered_map>

#include "scheduling_queue.h"
#include "LsResources.h"


namespace ray {
class SchedulingPolicy {
public:
  // A constructor that initializes the policy with a reference to queue state.
  SchedulingPolicy(const SchedulingQueue &sched_queue) : sched_queue_(sched_queue) {}
  // Perform a scheduling operation, given a set of cluster resources.
  // Produces a mapping of tasks to one of three possible destinations:
  // (1) local raylet (to be executed locally), (2) remote raylet,
  /// (3) global scheduler.
  std::unordered_map<TaskID, ClientID, UniqueIDHasher> Schedule(
      const std::unordered_map<ClientID, LsResources, UniqueIDHasher> &cluster_resources);
  // Perform a scheduling operation for a single task, given a set of

  virtual ~SchedulingPolicy() {}
private:
  // Constant reference to Raylet queue state.
  const SchedulingQueue &sched_queue_;
};
}
#endif
