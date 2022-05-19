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

class CoreWorker;

/// Lifecycle management of one or more `CoreWorker` instances in a process.
///
/// To start a driver in the current process:
///     CoreWorkerOptions options = {
///         WorkerType::DRIVER,             // worker_type
///         ...,                            // other arguments
///     };
///     CoreWorkerProcess::Initialize(options);
///
/// To shutdown a driver in the current process:
///     CoreWorkerProcess::Shutdown();
///
/// To start one or more workers in the current process:
///     CoreWorkerOptions options = {
///         WorkerType::WORKER,             // worker_type
///         ...,                            // other arguments
///     };
///     CoreWorkerProcess::Initialize(options);
///     ...                                 // Do other stuff
///     CoreWorkerProcess::RunTaskExecutionLoop();
///
/// To shutdown a worker in the current process, return a system exit status (with status
/// code `IntentionalSystemExit` or `UnexpectedSystemExit`) in the task execution
/// callback.
///
/// If more than 1 worker is started, only the threads which invoke the
/// `task_execution_callback` will be automatically associated with the corresponding
/// worker. If you started your own threads and you want to use core worker APIs in these
/// threads, remember to call `CoreWorkerProcess::SetCurrentThreadWorkerId(worker_id)`
/// once in the new thread before calling core worker APIs, to associate the current
/// thread with a worker. You can obtain the worker ID via
/// `CoreWorkerProcess::GetCoreWorker()->GetWorkerID()`.
///
/// How does core worker process dealloation work?
///
/// For an individual core worker thread's shutdown process, please check core_worker.h.
/// For the core worker process, it has 2 ways to properly shutdown the worker.
///
/// If it is a driver:
///     If the core worker is initialized at a driver, ShutdownDriver must be called.
///     before deallocating the core worker process.
///
/// If it is a worker:
///    If the core worker is initialized at a worker, it is expected to be shutdown
///    when the task execution loop is terminated from each core worker instance.
///    Core worker ensures this by having a strong check there.
///
class CoreWorkerProcess {
 public:
  ///
  /// Public methods used in both DRIVER and WORKER mode.
  ///

  /// Initialize core workers at the process level.
  ///
  /// \param[in] options The various initialization options.
  static void Initialize(const CoreWorkerOptions &options);

  /// Get the core worker associated with the current thread.
  /// NOTE (kfstorm): Here we return a reference instead of a `shared_ptr` to make sure
  /// `CoreWorkerProcess` has full control of the destruction timing of `CoreWorker`.
  static CoreWorker &GetCoreWorker();

  /// Try to get the `CoreWorker` instance by worker ID.
  /// If the current thread is not associated with a core worker, returns a null pointer.
  ///
  /// \param[in] workerId The worker ID.
  /// \return The `CoreWorker` instance.
  static std::shared_ptr<CoreWorker> TryGetWorker(const WorkerID &worker_id);

  /// Set the core worker associated with the current thread by worker ID.
  /// Currently used by Java worker only.
  ///
  /// \param worker_id The worker ID of the core worker instance.
  static void SetCurrentThreadWorkerId(const WorkerID &worker_id);

  /// Whether the current process has been initialized for core worker.
  static bool IsInitialized();

  ///
  /// Public methods used in DRIVER mode only.
  ///

  /// Shutdown the driver completely at the process level.
  /// It must be only used by drivers, not workers.
  static void Shutdown();

  ///
  /// Public methods used in WORKER mode only.
  ///

  /// Start receiving and executing tasks.
  static void RunTaskExecutionLoop();

 private:
  /// Check that the core worker environment is initialized for this process.
  ///
  /// \param[in] quick_exit If set to true, quick exit if uninitialized without
  /// crash.
  /// \return Void.
  static void EnsureInitialized(bool quick_exit);

  static void HandleAtExit();
};

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
  std::shared_ptr<CoreWorker> GetGlobalWorker() LOCKS_EXCLUDED(mutex_);

  /// Run worker execution loop.
  void RunWorkerTaskExecutionLoop();

  /// Shutdown the driver completely at the process level.
  void ShutdownDriver();

  /// Return the CoreWorker for current thread.
  CoreWorker &GetCoreWorkerForCurrentThread();

  /// Set the core worker associated with the current thread by worker ID.
  /// Currently used by Java worker only.
  void SetThreadLocalWorkerById(const WorkerID &worker_id);

 private:
  /// The various options.
  const CoreWorkerOptions options_;

  /// The only core worker instance, if the number of workers is 1.
  std::shared_ptr<CoreWorker> global_worker_ GUARDED_BY(mutex_);

  /// The core worker instance associated with the current thread.
  /// Use weak_ptr here to avoid memory leak due to multi-threading.
  static thread_local std::weak_ptr<CoreWorker> thread_local_core_worker_;

  /// The worker ID of the global worker, if the number of workers is 1.
  const WorkerID global_worker_id_;

  /// To protect access to workers_ and global_worker_
  mutable absl::Mutex mutex_;
};
}  // namespace core
}  // namespace ray
