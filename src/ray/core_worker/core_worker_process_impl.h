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

namespace ray {
namespace core {
class CoreWorkerProcessImpl {
 public:
  /// Create an `CoreWorkerProcessImpl` with proper options.
  ///
  /// \param[in] options The various initialization options.
  CoreWorkerProcessImpl(const CoreWorkerOptions &options);

  ~CoreWorkerProcessImpl();

  void InitializeSystemConfig();

  /// Check that if the global worker should be created on construction.
  bool ShouldCreateGlobalWorkerOnConstruction() const;

  /// Get the `CoreWorker` instance by worker ID.
  ///
  /// \param[in] workerId The worker ID.
  /// \return The `CoreWorker` instance.
  std::shared_ptr<CoreWorker> GetWorker(const WorkerID &worker_id) const
      LOCKS_EXCLUDED(mutex_);

  /// Create a new `CoreWorker` instance.
  ///
  /// \return The newly created `CoreWorker` instance.
  std::shared_ptr<CoreWorker> CreateWorker() LOCKS_EXCLUDED(mutex_);

  /// Remove an existing `CoreWorker` instance.
  ///
  /// \param[in] The existing `CoreWorker` instance.
  /// \return Void.
  void RemoveWorker(std::shared_ptr<CoreWorker> worker) LOCKS_EXCLUDED(mutex_);

  /// Get the `GlobalWorker` instance, if the number of workers is 1.
  std::shared_ptr<CoreWorker> GetGlobalWorker() const LOCKS_EXCLUDED(mutex_);

  /// Run worker execution loop.
  void RunWorkerTaskExecutionLoop();

  /// Shutdown the driver completely at the process level.
  void ShutdownDriver();

  /// Return the CoreWorker for current thread.
  CoreWorker &GetCoreWorkerForCurrentThread() const;

  /// Set the core worker associated with the current thread by worker ID.
  /// Currently used by Java worker only.
  void SetThreadLocalWorkerById(const WorkerID &worker_id) const;

 private:
  /// The various options.
  const CoreWorkerOptions options_;

  /// The only core worker instance, if the number of workers is 1.
  std::shared_ptr<CoreWorker> global_worker_ GUARDED_BY(mutex_);

  static thread_local std::weak_ptr<CoreWorker> thread_local_core_worker_;

  /// The worker ID of the global worker, if the number of workers is 1.
  const WorkerID global_worker_id_;

  /// Map from worker ID to worker.
  std::unordered_map<WorkerID, std::shared_ptr<CoreWorker>> workers_ GUARDED_BY(mutex_);

  /// To protect access to workers_ and global_worker_
  mutable absl::Mutex mutex_;
};
}  // namespace core
}  // namespace ray
