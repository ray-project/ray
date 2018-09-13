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

#ifndef NDEBUG
  for (const auto &client_resource_pair : cluster_resources) {
    // pair = ClientID, SchedulingResources
    const ClientID &client_id = client_resource_pair.first;
    const SchedulingResources &resources = client_resource_pair.second;
    RAY_LOG(DEBUG) << "client_id: " << client_id << " "
                   << resources.GetAvailableResources().ToString();
  }
#endif

  // We expect all placeable tasks to be placed on exit from this policy method.
  RAY_CHECK(scheduling_queue_.GetPlaceableTasks().size() <= 1);
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
      ResourceSet available_node_resources =
          ResourceSet(node_resources.GetAvailableResources());
      available_node_resources.SubtractResourcesStrict(node_resources.GetLoadResources());
      RAY_LOG(DEBUG) << "client_id " << node_client_id
                     << " avail: " << node_resources.GetAvailableResources().ToString()
                     << " load: " << node_resources.GetLoadResources().ToString()
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
      new_load.AddResources(resource_demand);
      cluster_resources[dst_client_id].SetLoadResources(std::move(new_load));
    } else {
      // If the task doesn't fit, place randomly subject to hard constraints.
      for (const auto &client_resource_pair2 : cluster_resources) {
        // pair = ClientID, SchedulingResources
        ClientID node_client_id = client_resource_pair2.first;
        const auto &node_resources = client_resource_pair2.second;
        if (resource_demand.IsSubset(node_resources.GetTotalResources())) {
          // This node is a feasible candidate.
          client_keys.push_back(node_client_id);
        }
      }
      // client candidate list constructed, pick randomly.
      if (!client_keys.empty()) {
        // Choose index at random.
        // Initialize a uniform integer distribution over the key space.
        // TODO(atumanov): change uniform random to discrete, weighted by resource
        // capacity.
        std::uniform_int_distribution<int> distribution(0, client_keys.size() - 1);
        int client_key_index = distribution(gen_);
        const ClientID &dst_client_id = client_keys[client_key_index];
        decision[t.GetTaskSpecification().TaskId()] = dst_client_id;
        // Update dst_client_id's load to keep track of remote task load until
        // the next heartbeat.
        ResourceSet new_load(cluster_resources[dst_client_id].GetLoadResources());
        new_load.AddResources(resource_demand);
        cluster_resources[dst_client_id].SetLoadResources(std::move(new_load));
      } else {
        // There are no nodes that can feasibly execute this task. The task remains
        // placeable until cluster capacity becomes available.
        // TODO(rkn): Propagate a warning to the user.
        RAY_LOG(INFO) << "This task requires "
                      << t.GetTaskSpecification().GetRequiredResources().ToString()
                      << ", but no nodes have the necessary resources.";
      }
    }
  }

  return decision;
}

std::vector<TaskID> SchedulingPolicy::SpillOver(
    SchedulingResources &remote_scheduling_resources) const {
  // The policy decision to be returned.
  std::vector<TaskID> decision;

  ResourceSet new_load(remote_scheduling_resources.GetLoadResources());

  // Check if we can accommodate an infeasible task.
  for (const auto &task : scheduling_queue_.GetInfeasibleTasks()) {
    if (task.GetTaskSpecification().GetRequiredResources().IsSubset(
            remote_scheduling_resources.GetTotalResources())) {
      decision.push_back(task.GetTaskSpecification().TaskId());
      new_load.AddResources(task.GetTaskSpecification().GetRequiredResources());
    }
  }

  for (const auto &task : scheduling_queue_.GetReadyTasks()) {
    if (!task.GetTaskSpecification().IsActorTask()) {
      if (task.GetTaskSpecification().GetRequiredResources().IsSubset(
              remote_scheduling_resources.GetTotalResources())) {
        decision.push_back(task.GetTaskSpecification().TaskId());
        new_load.AddResources(task.GetTaskSpecification().GetRequiredResources());
        break;
      }
    }
  }
  remote_scheduling_resources.SetLoadResources(std::move(new_load));

  return decision;
}

SchedulingPolicy::~SchedulingPolicy() {}

}  // namespace raylet

}  // namespace ray
