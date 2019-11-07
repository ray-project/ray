#ifndef RAY_CORE_WORKER_COMMON_H
#define RAY_CORE_WORKER_COMMON_H

#include <string>

#include "ray/common/task/task_spec.h"
#include "ray/raylet/raylet_client.h"
#include "ray/util/util.h"

namespace ray {
using WorkerType = rpc::WorkerType;

// Return a string representation of the worker type.
std::string WorkerTypeString(WorkerType type);

// Return a string representation of the language.
std::string LanguageString(Language language);

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
  ActorCreationOptions(uint64_t max_reconstructions, bool is_direct_call,
                       int max_concurrency,
                       const std::unordered_map<std::string, double> &resources,
                       const std::unordered_map<std::string, double> &placement_resources,
                       const std::vector<std::string> &dynamic_worker_options,
                       bool is_detached)
      : max_reconstructions(max_reconstructions),
        is_direct_call(is_direct_call),
        max_concurrency(max_concurrency),
        resources(resources),
        placement_resources(placement_resources),
        dynamic_worker_options(dynamic_worker_options),
        is_detached(is_detached){};

  /// Maximum number of times that the actor should be reconstructed when it dies
  /// unexpectedly. It must be non-negative. If it's 0, the actor won't be reconstructed.
  const uint64_t max_reconstructions = 0;
  /// Whether to use direct actor call. If this is set to true, callers will submit
  /// tasks directly to the created actor without going through raylet.
  const bool is_direct_call = false;
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
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_COMMON_H
