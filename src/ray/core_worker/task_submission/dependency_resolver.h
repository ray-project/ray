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
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/actor_creator.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_manager_interface.h"

namespace ray {
namespace core {

using TensorTransportGetter =
    std::function<std::optional<rpc::TensorTransport>(const ObjectID &object_id)>;

// This class is thread-safe.
class LocalDependencyResolver {
 public:
  LocalDependencyResolver(CoreWorkerMemoryStore &store,
                          TaskManagerInterface &task_manager,
                          ActorCreatorInterface &actor_creator,
                          const TensorTransportGetter &tensor_transport_getter)
      : in_memory_store_(store),
        task_manager_(task_manager),
        actor_creator_(actor_creator),
        tensor_transport_getter_(tensor_transport_getter) {}

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

  /// Cancel resolution of the given task's dependencies.
  /// If cancellation succeeds, the registered callback will not be called.
  /// \return true if dependency resolution was successfully cancelled
  bool CancelDependencyResolution(const TaskID &task_id);

  /// Return the number of tasks pending dependency resolution.
  int64_t NumPendingTasks() const {
    absl::MutexLock lock(&mu_);
    return pending_tasks_.size();
  }

 private:
  struct TaskState {
    TaskState(TaskSpecification t,
              const absl::flat_hash_set<ObjectID> &deps,
              const absl::flat_hash_set<ActorID> &actor_ids,
              std::function<void(Status)> on_dependencies_resolved)
        : task(std::move(t)),
          actor_dependencies_remaining(actor_ids.size()),
          status(Status::OK()),
          on_dependencies_resolved_(std::move(on_dependencies_resolved)) {
      local_dependencies.reserve(deps.size());
      for (const auto &dep : deps) {
        local_dependencies.emplace(dep, /*ray_object=*/nullptr);
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
    /// Dependency resolution status.
    Status status;
    std::function<void(Status)> on_dependencies_resolved_;
  };

  /// The in-memory store.
  CoreWorkerMemoryStore &in_memory_store_;

  /// Used to complete tasks.
  TaskManagerInterface &task_manager_;

  ActorCreatorInterface &actor_creator_;

  /// Used to get the tensor transport for an object.
  /// ObjectRefs with a tensor transport other than OBJECT_STORE will be only
  /// partially inlined. The rest of the data will be transferred via a
  /// different communication backend directly between actors. Thus, for these
  /// objects, we will not clear the ObjectRef metadata, even if the task
  /// executor has inlined the object value.
  const TensorTransportGetter tensor_transport_getter_;

  absl::flat_hash_map<TaskID, std::unique_ptr<TaskState>> pending_tasks_
      ABSL_GUARDED_BY(mu_);

  /// Protects against concurrent access to internal state.
  mutable absl::Mutex mu_;
};

}  // namespace core
}  // namespace ray
