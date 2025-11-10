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

#include <boost/thread.hpp>
#include <memory>
#include <string>

#include "ray/common/metrics.h"
#include "ray/core_worker/core_worker_options.h"
#include "ray/core_worker/grpc_service.h"
#include "ray/core_worker/metrics.h"
#include "ray/rpc/metrics_agent_client.h"
#include "ray/util/mutex_protected.h"

namespace ray {
namespace core {

class CoreWorker;
class CoreWorkerServiceHandlerProxy;

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

  std::shared_ptr<CoreWorker> CreateCoreWorker(CoreWorkerOptions options,
                                               const WorkerID &worker_id);

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

  /// The worker ID of this worker.
  const WorkerID worker_id_;

  /// Event loop where the IO events are handled. e.g. async GCS operations.
  instrumented_io_context io_service_{/*enable_lag_probe=*/false,
                                      /*running_on_single_thread=*/true};

  /// Keeps the io_service_ alive.
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> io_work_;

  /// Shared client call manager across all gRPC clients in the core worker process.
  /// This is used by the CoreWorker and the MetricsAgentClient.
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;

  /// Event loop where tasks are processed.
  /// task_execution_service_ should be destructed first to avoid
  /// issues like https://github.com/ray-project/ray/issues/18857
  instrumented_io_context task_execution_service_{/*enable_lag_probe=*/false,
                                                  /*running_on_single_thread=*/true};

  /// The asio work to keep task_execution_service_ alive.
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      task_execution_service_work_;

  // Thread that runs a boost::asio service to process IO events.
  boost::thread io_thread_;

  /// The core worker instance of this worker process.
  MutexProtected<std::shared_ptr<CoreWorker>> core_worker_;

  /// The proxy service handler that routes the RPC calls to the core worker.
  std::unique_ptr<CoreWorkerServiceHandlerProxy> service_handler_;

  /// The client to export metrics to the metrics agent.
  std::unique_ptr<ray::rpc::MetricsAgentClient> metrics_agent_client_;

  std::unique_ptr<ray::stats::Gauge> task_by_state_gauge_;
  std::unique_ptr<ray::stats::Gauge> actor_by_state_gauge_;
  std::unique_ptr<ray::stats::Gauge> total_lineage_bytes_gauge_;
  std::unique_ptr<ray::stats::Histogram> scheduler_placement_time_ms_histogram_;
};
}  // namespace core
}  // namespace ray
