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
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_manager.h"

namespace ray {
namespace core {

// This class is thread-safe.
class LocalDependencyResolver {
 public:
  LocalDependencyResolver(std::shared_ptr<CoreWorkerMemoryStore> store,
                          std::shared_ptr<TaskFinisherInterface> task_finisher)
      : in_memory_store_(store), task_finisher_(task_finisher), num_pending_(0) {}

  /// Resolve all local and remote dependencies for the task, calling the specified
  /// callback when done. Direct call ids in the task specification will be resolved
  /// to concrete values and inlined.
  //
  /// Note: This method **will mutate** the given TaskSpecification.
  ///
  /// Postcondition: all direct call id arguments that haven't been spilled to plasma
  /// are converted to values and all remaining arguments are arguments in the task spec.
  void ResolveDependencies(TaskSpecification &task, std::function<void()> on_complete);

  /// Return the number of tasks pending dependency resolution.
  /// TODO(ekl) this should be exposed in worker stats.
  int NumPendingTasks() const { return num_pending_; }

 private:
  /// The in-memory store.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  /// Used to complete tasks.
  std::shared_ptr<TaskFinisherInterface> task_finisher_;

  /// Number of tasks pending dependency resolution.
  std::atomic<int> num_pending_;

  /// Protects against concurrent access to internal state.
  absl::Mutex mu_;
};

}  // namespace core
}  // namespace ray
