#include "scheduling_policy.h"

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

SchedulingPolicy::SchedulingPolicy(const SchedulingQueue &scheduling_queue)
    : scheduling_queue_(scheduling_queue), gen_(rd_()) {}

std::unordered_map<TaskID, ClientID, UniqueIDHasher> SchedulingPolicy::Schedule(
    const std::unordered_map<ClientID, SchedulingResources, UniqueIDHasher>
        &cluster_resources,
    const ClientID &local_client_id, const std::vector<ClientID> &others) {
  // The policy decision to be returned.
  std::unordered_map<TaskID, ClientID, UniqueIDHasher> decision;
  // TODO(atumanov): protect DEBUG code blocks with ifdef DEBUG
  RAY_LOG(DEBUG) << "[Schedule] cluster resource map: ";
  for (const auto &client_resource_pair : cluster_resources) {
    // pair = ClientID, SchedulingResources
    const ClientID &client_id = client_resource_pair.first;
    const SchedulingResources &resources = client_resource_pair.second;
    RAY_LOG(DEBUG) << "client_id: " << client_id << " "
                   << resources.GetAvailableResources().ToString();
  }

  // Iterate over running tasks, get their resource demand and try to schedule.
  for (const auto &t : scheduling_queue_.GetReadyTasks()) {
    // Get task's resource demand
    const auto &resource_demand = t.GetTaskSpecification().GetRequiredResources();
    const TaskID &task_id = t.GetTaskSpecification().TaskId();
    RAY_LOG(DEBUG) << "[SchedulingPolicy]: task=" << task_id
                   << " numforwards=" << t.GetTaskExecutionSpecReadonly().NumForwards()
                   << " resources="
                   << t.GetTaskSpecification().GetRequiredResources().ToString();
    // TODO(atumanov): replace the simple spillback policy with exponential backoff based
    // policy.
    if (t.GetTaskExecutionSpecReadonly().NumForwards() >= 1) {
      decision[task_id] = local_client_id;
      continue;
    }
    // Construct a set of viable node candidates and randomly pick between them.
    // Get all the client id keys and randomly pick.
    std::vector<ClientID> client_keys;
    for (const auto &client_resource_pair : cluster_resources) {
      // pair = ClientID, SchedulingResources
      ClientID node_client_id = client_resource_pair.first;
      SchedulingResources node_resources = client_resource_pair.second;
      RAY_LOG(DEBUG) << "client_id " << node_client_id << " resources: "
                     << node_resources.GetAvailableResources().ToString();
      if (resource_demand.IsSubset(node_resources.GetTotalResources())) {
        // This node is a feasible candidate.
        client_keys.push_back(node_client_id);
      }
    }
    RAY_CHECK(!client_keys.empty());

    // Choose index at random.
    // Initialize a uniform integer distribution over the key space.
    // TODO(atumanov): change uniform random to discrete, weighted by resource capacity.
    std::uniform_int_distribution<int> distribution(0, client_keys.size() - 1);
    int client_key_index = distribution(gen_);
    decision[task_id] = client_keys[client_key_index];
    RAY_LOG(DEBUG) << "[SchedulingPolicy] idx=" << client_key_index << " " << task_id
                   << " --> " << client_keys[client_key_index];
  }
  return decision;
}

SchedulingPolicy::~SchedulingPolicy() {}

}  // namespace raylet

}  // namespace ray
