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

#pragma once

#include <string>

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet_client/raylet_client.h"
#include "ray/util/util.h"

namespace ray {
namespace core {

using WorkerType = rpc::WorkerType;

// Return a string representation of the worker type.
std::string WorkerTypeString(WorkerType type);

// Return a string representation of the language.
std::string LanguageString(Language language);

// Return a string representation of the named actor to cache, in format of
// `namespace-[job_id-]actor_name`
std::string GenerateCachedActorName(const std::string &ns, const std::string &actor_name);

/// Information about a remote function.
class RayFunction {
 public:
  RayFunction() {}
  RayFunction(Language language, const FunctionDescriptor &function_descriptor)
      : language_(language), function_descriptor_(function_descriptor) {}

  Language GetLanguage() const { return language_; }

  const FunctionDescriptor &GetFunctionDescriptor() const { return function_descriptor_; }

 private:
  Language language_;
  FunctionDescriptor function_descriptor_;
};

/// Options for all tasks (actor and non-actor) except for actor creation.
struct TaskOptions {
  TaskOptions() {}
  TaskOptions(std::string name, int num_returns,
              std::unordered_map<std::string, double> &resources,
              const std::string &concurrency_group_name = "",
              const std::string &serialized_runtime_env = "{}")
      : name(name),
        num_returns(num_returns),
        resources(resources),
        concurrency_group_name(concurrency_group_name),
        serialized_runtime_env(serialized_runtime_env) {}

  /// The name of this task.
  std::string name;
  /// Number of returns of this task.
  int num_returns = 1;
  /// Resources required by this task.
  std::unordered_map<std::string, double> resources;
  /// The name of the concurrency group in which this task will be executed.
  std::string concurrency_group_name;
  // Runtime Env used by this task. Propagated to child actors and tasks.
  std::string serialized_runtime_env;
};

/// Options for actor creation tasks.
struct ActorCreationOptions {
  ActorCreationOptions() {}
  ActorCreationOptions(int64_t max_restarts, int64_t max_task_retries,
                       int max_concurrency,
                       const std::unordered_map<std::string, double> &resources,
                       const std::unordered_map<std::string, double> &placement_resources,
                       const std::vector<std::string> &dynamic_worker_options,
                       std::optional<bool> is_detached, std::string &name,
                       std::string &ray_namespace, bool is_asyncio,
                       const rpc::SchedulingStrategy &scheduling_strategy,
                       const std::string &serialized_runtime_env = "{}",
                       const std::vector<ConcurrencyGroup> &concurrency_groups = {},
                       bool execute_out_of_order = false, int32_t max_pending_calls = -1)
      : max_restarts(max_restarts),
        max_task_retries(max_task_retries),
        max_concurrency(max_concurrency),
        resources(resources),
        placement_resources(placement_resources),
        dynamic_worker_options(dynamic_worker_options),
        is_detached(std::move(is_detached)),
        name(name),
        ray_namespace(ray_namespace),
        is_asyncio(is_asyncio),
        serialized_runtime_env(serialized_runtime_env),
        concurrency_groups(concurrency_groups.begin(), concurrency_groups.end()),
        execute_out_of_order(execute_out_of_order),
        max_pending_calls(max_pending_calls),
        scheduling_strategy(scheduling_strategy){};

  /// Maximum number of times that the actor should be restarted if it dies
  /// unexpectedly. A value of -1 indicates infinite restarts. If it's 0, the
  /// actor won't be restarted.
  const int64_t max_restarts = 0;
  /// Maximum number of times that individual tasks can be retried at the
  /// actor, if the actor dies unexpectedly. If -1, then the task may be
  /// retried infinitely many times.
  const int64_t max_task_retries = 0;
  /// The max number of concurrent tasks to run on this direct call actor.
  const int max_concurrency = 1;
  /// Resources required by the whole lifetime of this actor.
  const std::unordered_map<std::string, double> resources;
  /// Resources required to place this actor.
  const std::unordered_map<std::string, double> placement_resources;
  /// The dynamic options used in the worker command when starting a worker process for
  /// an actor creation task.
  const std::vector<std::string> dynamic_worker_options;
  /// Whether to keep the actor persistent after driver exit. If true, this will set
  /// the worker to not be destroyed after the driver shutdown.
  std::optional<bool> is_detached;
  /// The name to give this detached actor that can be used to get a handle to it from
  /// other drivers. This must be globally unique across the cluster.
  /// This should set if and only if is_detached is true.
  const std::string name;
  /// The namespace to give this detached actor so that the actor is only visible
  /// with the namespace.
  /// This should set if and only if is_detached is true.
  const std::string ray_namespace;
  /// Whether to use async mode of direct actor call.
  const bool is_asyncio = false;
  // Runtime Env used by this actor.  Propagated to child actors and tasks.
  std::string serialized_runtime_env;
  /// The actor concurrency groups to indicate how this actor perform its
  /// methods concurrently.
  const std::vector<ConcurrencyGroup> concurrency_groups;
  /// Wether the actor execute tasks out of order.
  const bool execute_out_of_order = false;
  /// The maxmium actor call pending count.
  const int max_pending_calls = -1;
  // The strategy about how to schedule this actor.
  rpc::SchedulingStrategy scheduling_strategy;
};

using PlacementStrategy = rpc::PlacementStrategy;

struct PlacementGroupCreationOptions {
  PlacementGroupCreationOptions(
      std::string name, PlacementStrategy strategy,
      std::vector<std::unordered_map<std::string, double>> bundles, bool is_detached)
      : name(std::move(name)),
        strategy(strategy),
        bundles(std::move(bundles)),
        is_detached(is_detached) {}

  /// The name of the placement group.
  const std::string name;
  /// The strategy to place the bundle in Placement Group.
  const PlacementStrategy strategy = rpc::PACK;
  /// The resource bundles in this placement group.
  const std::vector<std::unordered_map<std::string, double>> bundles;
  /// Whether to keep the placement group persistent after its creator dead.
  const bool is_detached = false;
};

class ObjectLocation {
 public:
  ObjectLocation(NodeID primary_node_id, uint64_t object_size,
                 std::vector<NodeID> node_ids, bool is_spilled, std::string spilled_url,
                 NodeID spilled_node_id)
      : primary_node_id_(primary_node_id),
        object_size_(object_size),
        node_ids_(std::move(node_ids)),
        is_spilled_(is_spilled),
        spilled_url_(std::move(spilled_url)),
        spilled_node_id_(spilled_node_id) {}

  const NodeID &GetPrimaryNodeID() const { return primary_node_id_; }

  const uint64_t GetObjectSize() const { return object_size_; }

  const std::vector<NodeID> &GetNodeIDs() const { return node_ids_; }

  bool IsSpilled() const { return is_spilled_; }

  const std::string &GetSpilledURL() const { return spilled_url_; }

  const NodeID &GetSpilledNodeID() const { return spilled_node_id_; }

 private:
  /// The ID of the node has the primary copy of the object.
  /// Nil if the object is pending resolution.
  const NodeID primary_node_id_;
  /// The size of the object in bytes.
  const uint64_t object_size_;
  /// The IDs of the nodes that this object appeared on or was evicted by.
  const std::vector<NodeID> node_ids_;
  /// Whether this object has been spilled.
  const bool is_spilled_;
  /// If spilled, the URL of this object's spill location.
  const std::string spilled_url_;
  /// If spilled, the ID of the node that spilled the object. Nil if the object was
  /// spilled to distributed external storage.
  const NodeID spilled_node_id_;
};

}  // namespace core
}  // namespace ray
