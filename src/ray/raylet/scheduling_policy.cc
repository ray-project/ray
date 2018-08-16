#include "scheduling_policy.h"

#include <chrono>

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

SchedulingPolicy::SchedulingPolicy(const SchedulingQueue &scheduling_queue)
    : scheduling_queue_(scheduling_queue),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()) {}

std::unordered_map<TaskID, ClientID> SchedulingPolicy::Schedule(
    std::unordered_map<ClientID, SchedulingResources> &cluster_resources,
    const ClientID &local_client_id) {
  // The policy decision to be returned.
  std::unordered_map<TaskID, ClientID> decision;
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
  for (const auto &t : scheduling_queue_.GetPlaceableTasks()) {
    // Get task's resource demand
    const auto &resource_demand = t.GetTaskSpecification().GetRequiredResources();
    const TaskID &task_id = t.GetTaskSpecification().TaskId();
    RAY_LOG(DEBUG) << "[SchedulingPolicy]: task=" << task_id
                   << " numforwards=" << t.GetTaskExecutionSpec().NumForwards()
                   << " resources="
                   << t.GetTaskSpecification().GetRequiredResources().ToString();

    // TODO(atumanov): try to place tasks locally first.
    // Construct a set of viable node candidates and randomly pick between them.
    // Get all the client id keys and randomly pick.
    std::vector<ClientID> client_keys;
    for (const auto &client_resource_pair : cluster_resources) {
      // pair = ClientID, SchedulingResources
      ClientID node_client_id = client_resource_pair.first;
      const auto &node_resources = client_resource_pair.second;
      ResourceSet available_node_resources = ResourceSet(node_resources.GetAvailableResources());
      available_node_resources.SubtractResources(node_resources.GetLoadResources());
      RAY_LOG(INFO) << "client_id " << node_client_id
                    << " avail: " << node_resources.GetAvailableResources().ToString()
                    << " load: "  << node_resources.GetLoadResources().ToString()
                    << " avail-load: " << available_node_resources.ToString();

      if (resource_demand.IsSubset(available_node_resources)) {
        // This node is a feasible candidate.
        client_keys.push_back(node_client_id);
      }
    }

    if (!client_keys.empty()) {
      // Choose index at random.
      // Initialize a uniform integer distribution over the key space.
      // TODO(atumanov): change uniform random to discrete, weighted by resource capacity.
      std::uniform_int_distribution<int> distribution(0, client_keys.size() - 1);
      int client_key_index = distribution(gen_);
      const ClientID &dst_client_id = client_keys[client_key_index];
      decision[task_id] = dst_client_id;
      // Update dst_client_id's load to keep track of remote task load until
      // the next heartbeat.
      ResourceSet new_load(cluster_resources[dst_client_id].GetLoadResources());
      new_load.OuterJoin(resource_demand);
      cluster_resources[dst_client_id].SetLoadResources(std::move(new_load));
      RAY_LOG(DEBUG) << "[SchedulingPolicy] idx=" << client_key_index << " " << task_id
                     << " --> " << client_keys[client_key_index];
    } else {
      // There are no nodes that can feasibly execute this task. The task remains
      // placeable until cluster capacity becomes available.
      // TODO(rkn): Propagate a warning to the user.
      RAY_LOG(DEBUG) << "This task requires "
                       << t.GetTaskSpecification().GetRequiredResources().ToString()
                       << ", but no nodes have the necessary resources.";
    }
  }
  return decision;
}

SchedulingPolicy::~SchedulingPolicy() {}

}  // namespace raylet

}  // namespace ray
