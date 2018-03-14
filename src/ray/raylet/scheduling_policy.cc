#include "scheduling_policy.h"

namespace ray {

namespace raylet {

SchedulingPolicy::SchedulingPolicy(const SchedulingQueue &scheduling_queue)
    : scheduling_queue_(scheduling_queue) {}

std::unordered_map<TaskID, ClientID, UniqueIDHasher> SchedulingPolicy::Schedule(
    const std::unordered_map<ClientID, SchedulingResources, UniqueIDHasher>
    &cluster_resources,
      const ClientID &me,
      const std::vector<ClientID> &others) {
  std::unordered_map<TaskID, ClientID, UniqueIDHasher> decision;
  // TODO(atumanov): consider all cluster resources.
  SchedulingResources resource_supply = cluster_resources.at(me);
  const auto &resource_supply_set = resource_supply.GetAvailableResources();

  // Iterate over running tasks, get their resource demand and try to schedule.
  for (const auto &t : scheduling_queue_.GetReadyTasks()) {
    // Get task's resource demand
    const auto &resource_demand = t.GetTaskSpecification().GetRequiredResources();
    bool task_feasible = resource_demand.IsSubset(resource_supply_set);
    if (task_feasible) {
      ClientID node = me;
      if (scheduling_queue_.GetScheduledTasks().size() > 0 && others.size() > 0) {
        node = others.front();
      }
      const TaskID &task_id = t.GetTaskSpecification().TaskId();
      decision[task_id] = node;
    }
  }
  return decision;
}

SchedulingPolicy::~SchedulingPolicy() {}

}

}  // namespace ray
