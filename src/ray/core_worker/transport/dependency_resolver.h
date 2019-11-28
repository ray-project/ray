#ifndef RAY_CORE_WORKER_DEPENDENCY_RESOLVER_H
#define RAY_CORE_WORKER_DEPENDENCY_RESOLVER_H

#include <memory>

#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"

namespace ray {

// This class is thread-safe.
class LocalDependencyResolver {
 public:
  LocalDependencyResolver(std::shared_ptr<CoreWorkerMemoryStore> store)
      : in_memory_store_(store), num_pending_(0) {}

  /// Resolve all local and remote dependencies for the task, calling the specified
  /// callback when done. Direct call ids in the task specification will be resolved
  /// to concrete values and inlined.
  //
  /// Note: This method **will mutate** the given TaskSpecification.
  ///
  /// Postcondition: all direct call ids in arguments are converted to values and all
  /// remaining by-reference arguments are TaskTransportType::RAYLET.
  void ResolveDependencies(TaskSpecification &task, std::function<void()> on_complete);

  /// Return the number of tasks pending dependency resolution.
  /// TODO(ekl) this should be exposed in worker stats.
  int NumPendingTasks() const { return num_pending_; }

 private:
  /// The in-memory store.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  /// Number of tasks pending dependency resolution.
  std::atomic<int> num_pending_;

  /// Protects against concurrent access to internal state.
  absl::Mutex mu_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_DEPENDENCY_RESOLVER_H
