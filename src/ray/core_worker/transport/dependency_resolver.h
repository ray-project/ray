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

#include <memory>

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/actor_creator.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_manager.h"

namespace ray {
namespace core {

// This class is thread-safe.
class LocalDependencyResolver {
 public:
  LocalDependencyResolver(CoreWorkerMemoryStore &store,
                          TaskFinisherInterface &task_finisher,
                          ActorCreatorInterface &actor_creator)
      : in_memory_store_(store),
        task_finisher_(task_finisher),
        actor_creator_(actor_creator),
        num_pending_(0) {}

  /// Resolve all local and remote dependencies for the task, calling the specified
  /// callback when done. Direct call ids in the task specification will be resolved
  /// to concrete values and inlined.
  //
  /// Note: This method **will mutate** the given TaskSpecification.
  ///
  /// Postcondition: all direct call id arguments that haven't been spilled to plasma
  /// are converted to values and all remaining arguments are arguments in the task spec.
  ///
  /// \param[in] task The task whose dependencies we should resolve.
  /// \param[in] on_dependencies_resolved A callback to call once the task's dependencies
  /// have been resolved. Note that we will not call this if the dependency
  /// resolution is cancelled.
  void ResolveDependencies(TaskSpecification &task,
                           std::function<void(Status)> on_dependencies_resolved);

  /// Cancel resolution of the given task's dependencies. Its registered
  /// callback will not be called.
  void CancelDependencyResolution(const TaskID &task_id);

  /// Return the number of tasks pending dependency resolution.
  /// TODO(ekl) this should be exposed in worker stats.
  int64_t NumPendingTasks() const {
    absl::MutexLock lock(&mu_);
    return pending_tasks_.size();
  }

 private:
  struct TaskState {
    TaskState(TaskSpecification t,
              const std::unordered_set<ObjectID> &deps,
              const std::unordered_set<ActorID> &actor_ids,
              std::function<void(Status)> on_dependencies_resolved)
        : task(t),
          local_dependencies(),
          actor_dependencies_remaining(actor_ids.size()),
          status(Status::OK()),
          on_dependencies_resolved(on_dependencies_resolved) {
      for (const auto &dep : deps) {
        local_dependencies.emplace(dep, nullptr);
      }
      obj_dependencies_remaining = local_dependencies.size();
    }
    /// The task to be run.
    TaskSpecification task;
    /// The local dependencies to resolve for this task. Objects are nullptr if not yet
    /// resolved.
    absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> local_dependencies;
    /// Number of local dependencies that aren't yet resolved (have nullptrs in the above
    /// map).
    size_t actor_dependencies_remaining;
    size_t obj_dependencies_remaining;
    Status status;
    std::function<void(Status)> on_dependencies_resolved;
  };

  /// The in-memory store.
  CoreWorkerMemoryStore &in_memory_store_;

  /// Used to complete tasks.
  TaskFinisherInterface &task_finisher_;

  ActorCreatorInterface &actor_creator_;
  /// Number of tasks pending dependency resolution.
  std::atomic<int> num_pending_;

  absl::flat_hash_map<TaskID, std::unique_ptr<TaskState>> pending_tasks_ GUARDED_BY(mu_);

  /// Protects against concurrent access to internal state.
  mutable absl::Mutex mu_;
};

}  // namespace core
}  // namespace ray
