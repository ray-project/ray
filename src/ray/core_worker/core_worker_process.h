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

/// Lifecycle management of the `CoreWorker` instance in a process.
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
/// To start a worker in the current process:
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

  /// Get the core worker.
  /// NOTE (kfstorm): Here we return a reference instead of a `shared_ptr` to make sure
  /// `CoreWorkerProcess` has full control of the destruction timing of `CoreWorker`.
  static CoreWorker &GetCoreWorker();

  /// Try to get the `CoreWorker` instance by worker ID.
  /// If the current thread is not associated with a core worker, returns a null pointer.
  ///
  /// \param[in] workerId The worker ID.
  /// \return The `CoreWorker` instance.
  static std::shared_ptr<CoreWorker> TryGetWorker();

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
  explicit CoreWorkerProcessImpl(const CoreWorkerOptions &options);

  ~CoreWorkerProcessImpl();

  void InitializeSystemConfig();

  /// Try to get core worker. Returns nullptr if core worker doesn't exist.
  std::shared_ptr<CoreWorker> TryGetCoreWorker() const;

  /// Get the `CoreWorker` instance. The process will be exited if
  /// the core worker is nullptr.
  ///
  /// \return The `CoreWorker` instance.
  std::shared_ptr<CoreWorker> GetCoreWorker() const;

  /// Run worker execution loop.
  void RunWorkerTaskExecutionLoop();

  /// Shutdown the driver completely at the process level.
  void ShutdownDriver();

 private:
  /// The various options.
  const CoreWorkerOptions options_;

  /// The core worker instance of this worker process.
  std::shared_ptr<CoreWorker> core_worker_ GUARDED_BY(mutex_);

  /// The worker ID of this worker.
  const WorkerID worker_id_;

  /// To protect access to core_worker_
  mutable absl::Mutex mutex_;
};
}  // namespace core
}  // namespace ray
