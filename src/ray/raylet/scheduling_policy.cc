#include <algorithm>
#include <chrono>
#include <random>

#include "scheduling_policy.h"

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
    const auto &spec = t.GetTaskSpecification();
    const auto &resource_demand = spec.GetRequiredPlacementResources();
    const TaskID &task_id = spec.TaskId();

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
        decision[task_id] = dst_client_id;
        // Update dst_client_id's load to keep track of remote task load until
        // the next heartbeat.
        ResourceSet new_load(cluster_resources[dst_client_id].GetLoadResources());
        new_load.AddResources(resource_demand);
        cluster_resources[dst_client_id].SetLoadResources(std::move(new_load));
      } else {
        // There are no nodes that can feasibly execute this task. The task remains
        // placeable until cluster capacity becomes available.
        // TODO(rkn): Propagate a warning to the user.
        RAY_LOG(INFO) << "The task with ID " << task_id << " requires "
                      << spec.GetRequiredResources().ToString() << " for execution and "
                      << spec.GetRequiredPlacementResources().ToString()
                      << " for placement, but no nodes have the necessary resources. "
                      << "Check the client table to view node resources.";
      }
    }
  }

  return decision;
}

std::unordered_map<TaskID, ClientID> SchedulingPolicy::SpillOver(
    std::unordered_map<ClientID, SchedulingResources> &cluster_resources,
    std::vector<ClientID> &remote_client_ids) {
  // Shuffle the nodes to prevent forwarding cycles.
  std::shuffle(remote_client_ids.begin(), remote_client_ids.end(), gen_);
  std::list<ClientID> remote_client_ids_list(remote_client_ids.begin(),
                                             remote_client_ids.end());
  // The policy decision to be returned.
  std::unordered_map<TaskID, ClientID> decision;

  // Try to accommodate all infeasible tasks and load-balance across nodes.
  for (const auto &task : scheduling_queue_.GetInfeasibleTasks()) {
    const auto &spec = task.GetTaskSpecification();
    const auto &placement_resources = spec.GetRequiredPlacementResources();
    // Find the first node that can accommodate the resources.
    for (auto it = remote_client_ids_list.begin();
         it != remote_client_ids_list.end(); ++it) {
      const ClientID client_id = *it;
      const auto &remote_scheduling_resources = cluster_resources[client_id];
      if (placement_resources.IsSubset(remote_scheduling_resources.GetTotalResources())) {
        decision[spec.TaskId()] = client_id;
        // Update load for the destination raylet.
        ResourceSet new_load(cluster_resources[client_id].GetLoadResources());
        new_load.AddResources(spec.GetRequiredResources());
        cluster_resources[client_id].SetLoadResources(std::move(new_load));
        // Move this client id to the end of the list.
        remote_client_ids_list.splice(remote_client_ids_list.end(),
                                      remote_client_ids_list,
                                      it);
        break;
      }
    }
  }

  // Try to distribute up to n tasks over n nodes.
  size_t num_forwarded = 0;
  for (const auto &task : scheduling_queue_.GetReadyTasks()) {
    const auto &spec = task.GetTaskSpecification();
    const auto &placement_resources = spec.GetRequiredPlacementResources();
    for (auto it = remote_client_ids_list.begin();
         it != remote_client_ids_list.end(); ++it) {
      ClientID client_id = *it;
      const auto &remote_scheduling_resources = cluster_resources[client_id];
      if (!spec.IsActorTask()) {
        const auto &task_id = spec.TaskId();
        if (placement_resources.IsSubset(remote_scheduling_resources.GetTotalResources())) {
          decision[task_id] = client_id;
          ResourceSet new_load(cluster_resources[client_id].GetLoadResources());
          new_load.AddResources(spec.GetRequiredResources());
          cluster_resources[client_id].SetLoadResources(std::move(new_load));
          // Move this client id to the end of the list.
          remote_client_ids_list.splice(remote_client_ids_list.end(),
                                        remote_client_ids_list,
                                        it);
          num_forwarded++;
          break;
        }
      }
    }
    if (num_forwarded == remote_client_ids.size()) {
      break;
    }
  }

  return decision;
#endif
}

SchedulingPolicy::~SchedulingPolicy() {}

}  // namespace raylet

}  // namespace ray
