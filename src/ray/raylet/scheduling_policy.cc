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

std::unordered_map<TaskID, NodeID> SchedulingPolicy::Schedule(
    std::unordered_map<NodeID, SchedulingResources> &cluster_resources,
    const NodeID &local_node_id) {
  // The policy decision to be returned.
  std::unordered_map<TaskID, NodeID> decision;
#ifndef NDEBUG
  RAY_LOG(DEBUG) << "Cluster resource map: ";
  for (const auto &node_resource_pair : cluster_resources) {
    // pair = NodeID, SchedulingResources
    const NodeID &node_id = node_resource_pair.first;
    const SchedulingResources &resources = node_resource_pair.second;
    RAY_LOG(DEBUG) << "node_id: " << node_id << " "
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

    // Try to place tasks locally first.
    const auto &local_resources = cluster_resources[local_node_id];
    ResourceSet available_local_resources =
        ResourceSet(local_resources.GetAvailableResources());
    // We have to subtract the current "load" because we set the current "load"
    // to be the resources used by tasks that are in the
    // `SchedulingQueue::ready_queue_` in NodeManager::HandleWorkerAvailable's
    // call to SchedulingQueue::GetResourceLoad.
    available_local_resources.SubtractResources(local_resources.GetLoadResources());
    if (resource_demand.IsSubset(available_local_resources)) {
      // This node is a feasible candidate.
      decision[task_id] = local_node_id;

      ResourceSet new_load(cluster_resources[local_node_id].GetLoadResources());
      new_load.AddResources(resource_demand);
      cluster_resources[local_node_id].SetLoadResources(std::move(new_load));
      continue;
    }

    // Construct a set of viable node candidates and randomly pick between them.
    // Get all the node id keys and randomly pick.
    std::vector<NodeID> node_keys;
    for (const auto &node_resource_pair : cluster_resources) {
      // pair = NodeID, SchedulingResources
      NodeID node_id = node_resource_pair.first;
      const auto &node_resources = node_resource_pair.second;
      ResourceSet available_node_resources =
          ResourceSet(node_resources.GetAvailableResources());
      // We have to subtract the current "load" because we set the current "load"
      // to be the resources used by tasks that are in the
      // `SchedulingQueue::ready_queue_` in NodeManager::HandleWorkerAvailable's
      // call to SchedulingQueue::GetResourceLoad.
      available_node_resources.SubtractResources(node_resources.GetLoadResources());
      RAY_LOG(DEBUG) << "node_id " << node_id
                     << " avail: " << node_resources.GetAvailableResources().ToString()
                     << " load: " << node_resources.GetLoadResources().ToString();

      if (resource_demand.IsSubset(available_node_resources)) {
        // This node is a feasible candidate.
        node_keys.push_back(node_id);
      }
    }

    if (!node_keys.empty()) {
      // Choose index at random.
      // Initialize a uniform integer distribution over the key space.
      // TODO(atumanov): change uniform random to discrete, weighted by resource capacity.
      std::uniform_int_distribution<int> distribution(0, node_keys.size() - 1);
      int node_key_index = distribution(gen_);
      const NodeID &dst_node_id = node_keys[node_key_index];
      decision[task_id] = dst_node_id;
      // Update dst_node_id's load to keep track of remote task load until
      // the next heartbeat.
      ResourceSet new_load(cluster_resources[dst_node_id].GetLoadResources());
      new_load.AddResources(resource_demand);
      cluster_resources[dst_node_id].SetLoadResources(std::move(new_load));
    } else {
      // If the task doesn't fit, place randomly subject to hard constraints.
      for (const auto &node_resource_pair2 : cluster_resources) {
        // pair = NodeID, SchedulingResources
        NodeID node_id = node_resource_pair2.first;
        const auto &node_resources = node_resource_pair2.second;
        if (resource_demand.IsSubset(node_resources.GetTotalResources())) {
          // This node is a feasible candidate.
          node_keys.push_back(node_id);
        }
      }
      // node candidate list constructed, pick randomly.
      if (!node_keys.empty()) {
        // Choose index at random.
        // Initialize a uniform integer distribution over the key space.
        // TODO(atumanov): change uniform random to discrete, weighted by resource
        // capacity.
        std::uniform_int_distribution<int> distribution(0, node_keys.size() - 1);
        int node_key_index = distribution(gen_);
        const NodeID &dst_node_id = node_keys[node_key_index];
        decision[task_id] = dst_node_id;
        // Update dst_node_id's load to keep track of remote task load until
        // the next heartbeat.
        ResourceSet new_load(cluster_resources[dst_node_id].GetLoadResources());
        new_load.AddResources(resource_demand);
        cluster_resources[dst_node_id].SetLoadResources(std::move(new_load));
      } else {
        // There are no nodes that can feasibly execute this task. The task remains
        // placeable until cluster capacity becomes available.
        // TODO(rkn): Propagate a warning to the user.
        RAY_LOG(INFO) << "The task with ID " << task_id << " requires "
                      << spec.GetRequiredResources().ToString() << " for execution and "
                      << spec.GetRequiredPlacementResources().ToString()
                      << " for placement, but no nodes have the necessary resources. "
                      << "Check the node table to view node resources.";
      }
    }
  }

  return decision;
}

std::vector<TaskID> SchedulingPolicy::SpillOverInfeasibleTasks(
    SchedulingResources &node_resources) const {
  // The policy decision to be returned.
  std::vector<TaskID> decision;
  ResourceSet new_load(node_resources.GetLoadResources());

  // Check if we can accommodate infeasible tasks.
  for (const auto &task : scheduling_queue_.GetTasks(TaskState::INFEASIBLE)) {
    const auto &spec = task.GetTaskSpecification();
    const auto &placement_resources = spec.GetRequiredPlacementResources();
    if (placement_resources.IsSubset(node_resources.GetTotalResources())) {
      decision.push_back(spec.TaskId());
      new_load.AddResources(spec.GetRequiredResources());
    }
  }
  node_resources.SetLoadResources(std::move(new_load));
  return decision;
}

std::vector<TaskID> SchedulingPolicy::SpillOver(
    SchedulingResources &remote_resources, SchedulingResources &local_resources) const {
  // First try to spill infeasible tasks.
  auto decision = SpillOverInfeasibleTasks(remote_resources);

  // Get local available resources.
  ResourceSet available_local_resources =
      ResourceSet(local_resources.GetAvailableResources());
  available_local_resources.SubtractResources(local_resources.GetLoadResources());
  // Try to accommodate up to a single ready task.
  bool task_spilled = false;
  for (const auto &queue : scheduling_queue_.GetReadyTasksByClass()) {
    // Skip tasks for which there are resources available locally.
    const auto &task_resources =
        TaskSpecification::GetSchedulingClassDescriptor(queue.first);
    if (task_resources.IsSubset(available_local_resources)) {
      continue;
    }
    // Try to spill one task.
    for (const auto &task_id : queue.second) {
      const auto &task = scheduling_queue_.GetTaskOfState(task_id, TaskState::READY);
      const auto &spec = task.GetTaskSpecification();
      // Make sure the node has enough available resources to prevent forwarding cycles.
      if (spec.GetRequiredPlacementResources().IsSubset(
              remote_resources.GetAvailableResources())) {
        // Update the scheduling resources.
        ResourceSet new_remote_load(remote_resources.GetLoadResources());
        new_remote_load.AddResources(spec.GetRequiredResources());
        remote_resources.SetLoadResources(std::move(new_remote_load));
        ResourceSet new_local_load(local_resources.GetLoadResources());
        new_local_load.SubtractResources(spec.GetRequiredResources());
        local_resources.SetLoadResources(std::move(new_local_load));

        decision.push_back(spec.TaskId());
        task_spilled = true;
        break;
      }
    }
    if (task_spilled) {
      break;
    }
  }

  return decision;
}

SchedulingPolicy::~SchedulingPolicy() {}

}  // namespace raylet

}  // namespace ray
