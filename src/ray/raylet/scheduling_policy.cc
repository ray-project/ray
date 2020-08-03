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

#include "ray/raylet/scheduling_policy.h"

#include <algorithm>
#include <chrono>
#include <random>

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
#ifndef NDEBUG
  RAY_LOG(DEBUG) << "Cluster resource map: ";
  for (const auto &client_resource_pair : cluster_resources) {
    // pair = ClientID, SchedulingResources
    const ClientID &client_id = client_resource_pair.first;
    const SchedulingResources &resources = client_resource_pair.second;
    RAY_LOG(DEBUG) << "client_id: " << client_id << " "
                   << resources.GetAvailableResources().ToString();
  }
#endif

  // We expect all placeable tasks to be placed on exit from this policy method.
  RAY_CHECK(scheduling_queue_.GetTasks(TaskState::PLACEABLE).size() <= 1);
  // Iterate over running tasks, get their resource demand and try to schedule.
  for (const auto &t : scheduling_queue_.GetTasks(TaskState::PLACEABLE)) {
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
      // We have to subtract the current "load" because we set the current "load"
      // to be the resources used by tasks that are in the
      // `SchedulingQueue::ready_queue_` in NodeManager::HandleWorkerAvailable's
      // call to SchedulingQueue::GetResourceLoad.
      available_node_resources.SubtractResources(node_resources.GetLoadResources());
      RAY_LOG(DEBUG) << "client_id " << node_client_id
                     << " avail: " << node_resources.GetAvailableResources().ToString()
                     << " load: " << node_resources.GetLoadResources().ToString();

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

bool SchedulingPolicy::ScheduleBundle(
    std::unordered_map<ClientID, SchedulingResources> &cluster_resources,
    const ClientID &local_client_id, const ray::BundleSpecification &bundle_spec) {
#ifndef NDEBUG
  RAY_LOG(DEBUG) << "Cluster resource map: ";
  for (const auto &client_resource_pair : cluster_resources) {
    const ClientID &client_id = client_resource_pair.first;
    const SchedulingResources &resources = client_resource_pair.second;
    RAY_LOG(DEBUG) << "client_id: " << client_id << " "
                   << resources.GetAvailableResources().ToString();
  }
#endif
  const auto &client_resource_pair = cluster_resources.find(local_client_id);
  if (client_resource_pair == cluster_resources.end()) {
    return false;
  }
  const auto &resource_demand = bundle_spec.GetRequiredResources();
  ClientID node_client_id = client_resource_pair->first;
  const auto &node_resources = client_resource_pair->second;
  ResourceSet available_node_resources =
      ResourceSet(node_resources.GetAvailableResources());
  available_node_resources.SubtractResources(node_resources.GetLoadResources());
  RAY_LOG(DEBUG) << "Scheduling bundle, client id = " << node_client_id
                 << ", available resources = "
                 << node_resources.GetAvailableResources().ToString()
                 << ", resources load = " << node_resources.GetLoadResources().ToString()
                 << ", the resource needed = " << resource_demand.ToString();
  /// If the resource_demand is subset of the whole available_node_resources, this bundle
  /// can be set in this node, return true.
  return resource_demand.IsSubset(available_node_resources);
}

std::vector<TaskID> SchedulingPolicy::SpillOver(
    SchedulingResources &remote_scheduling_resources) const {
  // The policy decision to be returned.
  std::vector<TaskID> decision;

  ResourceSet new_load(remote_scheduling_resources.GetLoadResources());

  // Check if we can accommodate infeasible tasks.
  for (const auto &task : scheduling_queue_.GetTasks(TaskState::INFEASIBLE)) {
    const auto &spec = task.GetTaskSpecification();
    const auto &placement_resources = spec.GetRequiredPlacementResources();
    if (placement_resources.IsSubset(remote_scheduling_resources.GetTotalResources())) {
      decision.push_back(spec.TaskId());
      new_load.AddResources(spec.GetRequiredResources());
    }
  }

  // Try to accommodate up to a single ready task.
  for (const auto &task : scheduling_queue_.GetTasks(TaskState::READY)) {
    const auto &spec = task.GetTaskSpecification();
    if (!spec.IsActorTask()) {
      // Make sure the node has enough available resources to prevent forwarding cycles.
      if (spec.GetRequiredPlacementResources().IsSubset(
              remote_scheduling_resources.GetAvailableResources())) {
        decision.push_back(spec.TaskId());
        new_load.AddResources(spec.GetRequiredResources());
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
