#include "scheduling_policy.h"

#include <random>

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

SchedulingPolicy::SchedulingPolicy(const SchedulingQueue &scheduling_queue)
    : scheduling_queue_(scheduling_queue) {}

std::unordered_map<TaskID, ClientID, UniqueIDHasher> SchedulingPolicy::Schedule(
    const std::unordered_map<ClientID, SchedulingResources, UniqueIDHasher>
    &cluster_resources,
      const ClientID &me,
      const std::vector<ClientID> &others) {
  // The policy decision to be returned.
  std::unordered_map<TaskID, ClientID, UniqueIDHasher> decision;
  // Random number generator.
  std::default_random_engine gen;

  // Get all the client id keys and randomly pick.
  std::vector<ClientID> client_keys;
  for (const auto &pair : cluster_resources) {
    // pair = ClientID, SchedulingResources
    ClientID key = pair.first;
    client_keys.push_back(key);
  }
  RAY_LOG(INFO) << "[SchedulingPolicy] resource map key count : " << client_keys.size();
  // Initialize a uniform integer distribution over the key space.
  std::uniform_int_distribution<int> distribution(0, client_keys.size());

  // Iterate over running tasks, get their resource demand and try to schedule.
  for (const auto &t : scheduling_queue_.GetReadyTasks()) {
    // Get task's resource demand
//    const auto &resource_demand = t.GetTaskSpecification().GetRequiredResources();
    const TaskID &task_id = t.GetTaskSpecification().TaskId();
    // Choose index at random.
    int client_key_index = distribution(gen);
    decision[task_id] = client_keys[client_key_index];
    RAY_LOG(INFO) << "[SchedulingPolicy] assigned: " << task_id.hex() << " --> " << client_keys[client_key_index].hex();
//    SchedulingResources resource_supply = cluster_resources.at(client_keys[client_key_index]);
//    const auto &resource_supply_set = resource_supply.GetAvailableResources();
//
//    bool task_feasible = resource_demand.IsSubset(resource_supply_set);
//    if (task_feasible) {
//      ClientID node = me;
//      if (scheduling_queue_.GetScheduledTasks().size() > 0 && others.size() > 0) {
//        node = others.front();
//      }
//
//      decision[task_id] = node;
//    }
  }
  return decision;
}

SchedulingPolicy::~SchedulingPolicy() {}

}

}  // namespace ray
