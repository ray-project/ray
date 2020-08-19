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

using WorkerType = rpc::WorkerType;
using PlacementOptions = std::pair<PlacementGroupID, int64_t>;

// Return a string representation of the worker type.
std::string WorkerTypeString(WorkerType type);

// Return a string representation of the language.
std::string LanguageString(Language language);

/// Information about a remote function.
class RayFunction {
 public:
  RayFunction() {}
  RayFunction(Language language, const ray::FunctionDescriptor &function_descriptor)
      : language_(language), function_descriptor_(function_descriptor) {}

  Language GetLanguage() const { return language_; }

  const ray::FunctionDescriptor &GetFunctionDescriptor() const {
    return function_descriptor_;
  }

 private:
  Language language_;
  ray::FunctionDescriptor function_descriptor_;
};

/// Options for all tasks (actor and non-actor) except for actor creation.
struct TaskOptions {
  TaskOptions() {}
  TaskOptions(int num_returns, std::unordered_map<std::string, double> &resources)
      : num_returns(num_returns), resources(resources) {}

  /// Number of returns of this task.
  int num_returns = 1;
  /// Resources required by this task.
  std::unordered_map<std::string, double> resources;
};

/// Options for actor creation tasks.
struct ActorCreationOptions {
  ActorCreationOptions() {}
  ActorCreationOptions(
      int64_t max_restarts, int64_t max_task_retries, int max_concurrency,
      const std::unordered_map<std::string, double> &resources,
      const std::unordered_map<std::string, double> &placement_resources,
      const std::vector<std::string> &dynamic_worker_options, bool is_detached,
      std::string &name, bool is_asyncio,
      PlacementOptions placement_options = std::make_pair(PlacementGroupID::Nil(), -1))
      : max_restarts(max_restarts),
        max_task_retries(max_task_retries),
        max_concurrency(max_concurrency),
        resources(resources),
        placement_resources(placement_resources),
        dynamic_worker_options(dynamic_worker_options),
        is_detached(is_detached),
        name(name),
        is_asyncio(is_asyncio),
        placement_options(placement_options){};

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
  const bool is_detached = false;
  /// The name to give this detached actor that can be used to get a handle to it from
  /// other drivers. This must be globally unique across the cluster.
  /// This should set if and only if is_detached is true.
  const std::string name;
  /// Whether to use async mode of direct actor call.
  const bool is_asyncio = false;
  /// The placement_options include placement_group_id and bundle_index.
  /// If the actor doesn't belong to a placement group, the placement_group_id will be
  /// nil, and the bundle_index will be -1.
  PlacementOptions placement_options;
};

using PlacementStrategy = rpc::PlacementStrategy;

struct PlacementGroupCreationOptions {
  PlacementGroupCreationOptions(
      std::string name, PlacementStrategy strategy,
      std::vector<std::unordered_map<std::string, double>> bundles)
      : name(std::move(name)), strategy(strategy), bundles(std::move(bundles)) {}

  /// The name of the placement group.
  const std::string name;
  /// The strategy to place the bundle in Placement Group.
  const PlacementStrategy strategy = rpc::PACK;
  /// The resource bundles in this placement group.
  const std::vector<std::unordered_map<std::string, double>> bundles;
};

}  // namespace ray
