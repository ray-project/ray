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

#include <inttypes.h>

#include <algorithm>
#include <boost/asio/io_service.hpp>
#include <boost/functional/hash.hpp>
#include <deque>
#include <list>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/time/time.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/lease/lease.h"
#include "ray/common/runtime_env_manager.h"
#include "ray/gcs_rpc_client/gcs_client.h"
#include "ray/raylet/runtime_env_agent_client.h"
#include "ray/raylet/worker_interface.h"
#include "ray/raylet_ipc_client/client_connection.h"
#include "ray/stats/metric.h"

namespace ray {

namespace raylet {

using WorkerCommandMap =
    absl::flat_hash_map<Language, std::vector<std::string>, std::hash<int>>;

// TODO(#54703): Put this type in a separate target.
using AddProcessToCgroupHook = std::function<void(const std::string &)>;

enum PopWorkerStatus {
  // OK.
  // A registered worker will be returned with callback.
  OK = 0,
  // Job config is not found.
  // A nullptr worker will be returned with callback.
  JobConfigMissing = 1,
  // Worker process startup rate is limited.
  // A nullptr worker will be returned with callback.
  TooManyStartingWorkerProcesses = 2,
  // Worker process has been started, but the worker did not register at the raylet within
  // the timeout.
  // A nullptr worker will be returned with callback.
  WorkerPendingRegistration = 3,
  // Any fails of runtime env creation.
  // A nullptr worker will be returned with callback.
  RuntimeEnvCreationFailed = 4,
  // The lease's job has finished.
  // A nullptr worker will be returned with callback.
  JobFinished = 5,
};

/// \param[in] worker The started worker instance. Nullptr if worker is not started.
/// \param[in] status The pop worker status. OK if things go well. Otherwise, it will
/// contain the error status.
/// \param[in] runtime_env_setup_error_message The error message
/// when runtime env setup is failed. This should be empty unless status ==
/// RuntimeEnvCreationFailed.
/// \return true if the worker was used. Otherwise, return false
/// and the worker will be returned to the worker pool.
using PopWorkerCallback =
    std::function<bool(const std::shared_ptr<WorkerInterface> &worker,
                       PopWorkerStatus status,
                       const std::string &runtime_env_setup_error_message)>;

struct PopWorkerRequest {
  const rpc::Language language_;
  const rpc::WorkerType worker_type_;
  const JobID job_id_;                    // can be Nil
  const ActorID root_detached_actor_id_;  // can be Nil
  const std::optional<bool> is_gpu_;
  const std::optional<bool> is_actor_worker_;
  const rpc::RuntimeEnvInfo runtime_env_info_;
  const int runtime_env_hash_;
  const std::vector<std::string> dynamic_options_;
  std::optional<absl::Duration> worker_startup_keep_alive_duration_;

  PopWorkerCallback callback_;

  PopWorkerRequest(rpc::Language lang,
                   rpc::WorkerType worker_type,
                   JobID job,
                   ActorID root_actor_id,
                   std::optional<bool> gpu,
                   std::optional<bool> actor_worker,
                   rpc::RuntimeEnvInfo runtime_env_info,
                   int runtime_env_hash,
                   std::vector<std::string> options,
                   std::optional<absl::Duration> worker_startup_keep_alive_duration,
                   PopWorkerCallback callback)
      : language_(lang),
        worker_type_(worker_type),
        job_id_(job),
        root_detached_actor_id_(root_actor_id),
        is_gpu_(gpu),
        is_actor_worker_(actor_worker),
        runtime_env_info_(std::move(runtime_env_info)),
        runtime_env_hash_(runtime_env_hash),
        dynamic_options_(std::move(options)),
        worker_startup_keep_alive_duration_(worker_startup_keep_alive_duration),
        callback_(std::move(callback)) {}
};

/// \class IOWorkerPoolInterface
///
/// Used for object spilling manager unit tests.
class IOWorkerPoolInterface {
 public:
  virtual void PushSpillWorker(const std::shared_ptr<WorkerInterface> &worker) = 0;

  virtual void PopSpillWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) = 0;

  virtual void PushRestoreWorker(const std::shared_ptr<WorkerInterface> &worker) = 0;

  virtual void PopRestoreWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) = 0;

  virtual void PushDeleteWorker(const std::shared_ptr<WorkerInterface> &worker) = 0;

  virtual void PopDeleteWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) = 0;

  virtual ~IOWorkerPoolInterface() = default;
};

/// \class WorkerPoolInterface
///
/// Used for new scheduler unit tests.
class WorkerPoolInterface : public IOWorkerPoolInterface {
 public:
  /// Pop an idle worker from the pool. The caller is responsible for pushing
  /// the worker back onto the pool once the worker has completed its work.
  ///
  /// \param lease_spec The returned worker must be able to execute this lease.
  /// \param callback The callback function that executed when gets the result of
  /// worker popping.
  /// The callback will be executed with an empty worker in following cases:
  /// Case 1: Job config not found.
  /// Case 2: Worker process startup rate limited.
  /// Case 3: Worker process has been started, but the worker registered back to raylet
  /// timeout.
  //  Case 4: Any fails of runtime env creation.
  /// Of course, the callback will also be executed when a valid worker found in following
  /// cases:
  /// Case 1: An suitable worker was found in idle worker pool.
  /// Case 2: An suitable worker registered to raylet.
  /// The corresponding PopWorkerStatus will be passed to the callback.
  virtual void PopWorker(const LeaseSpecification &lease_spec,
                         const PopWorkerCallback &callback) = 0;
  /// Add an idle worker to the pool.
  ///
  /// \param The idle worker to add.
  virtual void PushWorker(const std::shared_ptr<WorkerInterface> &worker) = 0;

  /// Get all the registered workers.
  ///
  /// \param filter_dead_workers whether or not if this method will filter dead workers
  /// \param filter_io_workers whether or not if this method will filter io workers
  /// non-retriable workers that are still registered.
  ///
  /// \return A list containing all the workers.
  virtual std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredWorkers(
      bool filter_dead_workers = false, bool filter_io_workers = false) const = 0;

  /// Checks if any registered worker is available for scheduling.
  virtual bool IsWorkerAvailableForScheduling() const = 0;

  /// Get registered worker process by id or nullptr if not found.
  virtual std::shared_ptr<WorkerInterface> GetRegisteredWorker(
      const WorkerID &worker_id) const = 0;

  virtual std::shared_ptr<WorkerInterface> GetRegisteredWorker(
      const std::shared_ptr<ClientConnection> &connection) const = 0;

  /// Get registered driver process by id or nullptr if not found.
  virtual std::shared_ptr<WorkerInterface> GetRegisteredDriver(
      const WorkerID &worker_id) const = 0;

  virtual std::shared_ptr<WorkerInterface> GetRegisteredDriver(
      const std::shared_ptr<ClientConnection> &connection) const = 0;

  virtual ~WorkerPoolInterface() = default;

  virtual void HandleJobStarted(const JobID &job_id,
                                const rpc::JobConfig &job_config) = 0;

  virtual void HandleJobFinished(const JobID &job_id) = 0;

  virtual void Start() = 0;

  virtual void SetNodeManagerPort(int node_manager_port) = 0;

  virtual void SetRuntimeEnvAgentClient(
      std::unique_ptr<RuntimeEnvAgentClient> runtime_env_agent_client) = 0;

  virtual std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredDrivers(
      bool filter_dead_drivers = false, bool filter_system_drivers = false) const = 0;

  virtual Status RegisterDriver(const std::shared_ptr<WorkerInterface> &worker,
                                const rpc::JobConfig &job_config,
                                std::function<void(Status, int)> send_reply_callback) = 0;

  virtual Status RegisterWorker(const std::shared_ptr<WorkerInterface> &worker,
                                pid_t pid,
                                StartupToken worker_startup_token,
                                std::function<void(Status, int)> send_reply_callback) = 0;

  virtual boost::optional<const rpc::JobConfig &> GetJobConfig(
      const JobID &job_id) const = 0;

  virtual void OnWorkerStarted(const std::shared_ptr<WorkerInterface> &worker) = 0;

  virtual void DisconnectWorker(const std::shared_ptr<WorkerInterface> &worker,
                                rpc::WorkerExitType disconnect_type) = 0;

  virtual void DisconnectDriver(const std::shared_ptr<WorkerInterface> &driver) = 0;

  virtual void PrestartWorkers(const LeaseSpecification &lease_spec,
                               int64_t backlog_size) = 0;

  virtual void StartNewWorker(
      const std::shared_ptr<PopWorkerRequest> &pop_worker_request) = 0;

  virtual std::string DebugString() const = 0;
};

class WorkerInterface;
class Worker;

enum class WorkerUnfitForLeaseReason {
  NONE = 0,                      // OK
  ROOT_MISMATCH = 1,             // job ID or root detached actor ID mismatch
  RUNTIME_ENV_MISMATCH = 2,      // runtime env hash mismatch
  DYNAMIC_OPTIONS_MISMATCH = 3,  // dynamic options mismatch
  OTHERS = 4,                    // reasons we don't do stats for (e.g. language)
};
static constexpr std::string_view kWorkerUnfitForLeaseReasonDebugName[] = {
    "NONE",
    "ROOT_MISMATCH",
    "RUNTIME_ENV_MISMATCH",
    "DYNAMIC_OPTIONS_MISMATCH",
    "OTHERS",
};

inline std::ostream &operator<<(std::ostream &os,
                                const WorkerUnfitForLeaseReason &reason) {
  os << kWorkerUnfitForLeaseReasonDebugName[static_cast<int>(reason)];
  return os;
}

/// \class WorkerPool
///
/// The WorkerPool is responsible for managing a pool of Workers. Each Worker
/// is a container for a unit of work.
class WorkerPool : public WorkerPoolInterface {
 public:
  /// Create a pool and asynchronously start at least the specified number of workers per
  /// language.
  /// Once each worker process has registered with an external server, the
  /// process should create and register the specified number of workers, and add them to
  /// the pool.
  ///
  /// \param node_id The id of the current node.
  /// \param node_address The address of the current node.
  /// \param num_prestarted_python_workers The number of prestarted Python
  /// workers.
  /// \param maximum_startup_concurrency The maximum number of worker processes
  /// that can be started in parallel (typically this should be set to the number of CPU
  /// resources on the machine).
  /// \param min_worker_port The lowest port number that workers started will bind on.
  /// If this is set to 0, workers will bind on random ports.
  /// \param max_worker_port The highest port number that workers started will bind on.
  /// If this is not set to 0, min_worker_port must also not be set to 0.
  /// \param worker_ports An explicit list of open ports that workers started will bind
  /// on. This takes precedence over min_worker_port and max_worker_port.
  /// \param worker_commands The commands used to start the worker process, grouped by
  /// language.
  /// \param native_library_path The native library path which includes the core
  /// libraries.
  /// \param starting_worker_timeout_callback The callback that will be triggered once
  /// it times out to start a worker.
  /// \param ray_debugger_external Ray debugger in workers will be started in a way
  /// that they are accessible from outside the node.
  /// \param get_time A callback to get the current time in milliseconds.
  /// \param add_to_cgroup_hook A lifecycle hook that the forked worker process will
  /// execute becoming a worker process. The hook adds a newly forked process into
  /// the appropriate cgroup.
  WorkerPool(
      instrumented_io_context &io_service,
      const NodeID &node_id,
      std::string node_address,
      std::function<int64_t()> get_num_cpus_available,
      int num_prestarted_python_workers,
      int maximum_startup_concurrency,
      int min_worker_port,
      int max_worker_port,
      const std::vector<int> &worker_ports,
      gcs::GcsClient &gcs_client,
      const WorkerCommandMap &worker_commands,
      std::string native_library_path,
      std::function<void()> starting_worker_timeout_callback,
      int ray_debugger_external,
      std::function<absl::Time()> get_time,
      AddProcessToCgroupHook add_to_cgroup_hook = [](const std::string &) {});

  /// Destructor responsible for freeing a set of workers owned by this class.
  ~WorkerPool() override;

  /// Start the worker pool. Could only be called once.
  void Start() override;

  /// Set the node manager port.
  /// \param node_manager_port The port Raylet uses for listening to incoming connections.
  void SetNodeManagerPort(int node_manager_port) override;

  /// Set Runtime Env Manager Client.
  void SetRuntimeEnvAgentClient(
      std::unique_ptr<RuntimeEnvAgentClient> runtime_env_agent_client) override;

  /// Handles the event that a job is started.
  ///
  /// \param job_id ID of the started job.
  /// \param job_config The config of the started job.

  void HandleJobStarted(const JobID &job_id, const rpc::JobConfig &job_config) override;

  /// Handles the event that a job is finished.
  ///
  /// \param job_id ID of the finished job.
  void HandleJobFinished(const JobID &job_id) override;

  /// \brief Get the job config by job id.
  ///
  /// We don't return std::optional because it does not support references.
  ///
  /// \param job_id ID of the job.
  /// \return Job config if given job is running, else nullptr.
  boost::optional<const rpc::JobConfig &> GetJobConfig(
      const JobID &job_id) const override;

  /// Register a new worker. The Worker should be added by the caller to the
  /// pool after it becomes idle (e.g., requests a work assignment).
  ///
  /// \param[in] worker The worker to be registered.
  /// \param[in] pid The PID of the worker.
  /// \param[in] worker_startup_token The startup token of the process assigned to
  /// it during startup as a command line argument.
  /// \param[in] send_reply_callback The callback to invoke after registration is
  /// finished/failed.
  /// Returns 0 if the worker should bind on a random port.
  /// \return If the registration is successful.
  Status RegisterWorker(const std::shared_ptr<WorkerInterface> &worker,
                        pid_t pid,
                        StartupToken worker_startup_token,
                        std::function<void(Status, int)> send_reply_callback) override;

  /// To be invoked when a worker is started. This method should be called when the worker
  /// announces its port.
  ///
  /// \param[in] worker The worker which is started.
  void OnWorkerStarted(const std::shared_ptr<WorkerInterface> &worker) override;

  /// Register a new driver.
  ///
  /// \param[in] worker The driver to be registered.
  /// \param[in] job_config The config of the job.
  /// \param[in] send_reply_callback The callback to invoke after registration is
  /// finished/failed.
  /// \return If the registration is successful.
  Status RegisterDriver(const std::shared_ptr<WorkerInterface> &worker,
                        const rpc::JobConfig &job_config,
                        std::function<void(Status, int)> send_reply_callback) override;

  /// Get the client connection's registered worker.
  ///
  /// \param The client connection owned by a registered worker.
  /// \return The Worker that owns the given client connection. Returns nullptr
  /// if the client has not registered a worker yet.
  std::shared_ptr<WorkerInterface> GetRegisteredWorker(
      const std::shared_ptr<ClientConnection> &connection) const override;

  /// Get the registered worker by worker id or nullptr if not found.
  std::shared_ptr<WorkerInterface> GetRegisteredWorker(
      const WorkerID &worker_id) const override;

  /// Get the client connection's registered driver.
  ///
  /// \param The client connection owned by a registered driver.
  /// \return The Worker that owns the given client connection. Returns nullptr
  /// if the client has not registered a driver.
  std::shared_ptr<WorkerInterface> GetRegisteredDriver(
      const std::shared_ptr<ClientConnection> &connection) const override;

  /// Get the registered driver by worker id or nullptr if not found.
  std::shared_ptr<WorkerInterface> GetRegisteredDriver(
      const WorkerID &worker_id) const override;

  /// Disconnect a registered worker.
  ///
  /// \param worker The worker to disconnect. The worker must be registered.
  /// \param disconnect_type Type of a worker exit.
  void DisconnectWorker(const std::shared_ptr<WorkerInterface> &worker,
                        rpc::WorkerExitType disconnect_type) override;

  /// Disconnect a registered driver.
  ///
  /// \param The driver to disconnect. The driver must be registered.
  void DisconnectDriver(const std::shared_ptr<WorkerInterface> &driver) override;

  /// Add an idle spill I/O worker to the pool.
  ///
  /// \param worker The idle spill I/O worker to add.
  void PushSpillWorker(const std::shared_ptr<WorkerInterface> &worker) override;

  /// Pop an idle spill I/O worker from the pool and trigger a callback when
  /// an spill I/O worker is available.
  /// The caller is responsible for pushing the worker back onto the
  /// pool once the worker has completed its work.
  ///
  /// \param callback The callback that returns an available spill I/O worker.
  void PopSpillWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override;

  /// Add an idle restore I/O worker to the pool.
  ///
  /// \param worker The idle I/O worker to add.
  void PushRestoreWorker(const std::shared_ptr<WorkerInterface> &worker) override;

  /// Pop an idle restore I/O worker from the pool and trigger a callback when
  /// an restore I/O worker is available.
  /// The caller is responsible for pushing the worker back onto the
  /// pool once the worker has completed its work.
  ///
  /// \param callback The callback that returns an available restore I/O worker.
  void PopRestoreWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override;

  /// Add an idle delete I/O worker to the pool.
  ///
  /// NOTE: There's currently no concept of delete workers or delete worker pools.
  /// When deleting objects, it shares the workers within restore or spill worker pools.
  /// This method is just a higher level abstraction to hide that implementation detail.
  ///
  /// \param worker The idle I/O worker. It could be either spill or restore I/O worker.
  void PushDeleteWorker(const std::shared_ptr<WorkerInterface> &worker) override;

  /// Pop an idle delete I/O worker from the pool and trigger a callback when
  /// when delete I/O worker is available.
  /// NOTE: There's currently no concept of delete workers or delete worker pools.
  /// This method just finds more available I/O workers from either spill or restore pool
  /// and pop them out.
  void PopDeleteWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) override;

  /// See interface.
  void PushWorker(const std::shared_ptr<WorkerInterface> &worker) override;

  /// See interface.
  void PopWorker(const LeaseSpecification &lease_spec,
                 const PopWorkerCallback &callback) override;

  /// Try to prestart a number of workers suitable the given lease spec. Prestarting
  /// is needed since core workers request one lease at a time, if starting is slow,
  /// then it means it takes a long time to scale up.
  ///
  /// \param lease_spec The returned worker must be able to execute this lease.
  /// \param backlog_size The number of leases in the client backlog of this shape.
  /// We aim to prestart 1 worker per CPU, up to the backlog size.
  void PrestartWorkers(const LeaseSpecification &lease_spec,
                       int64_t backlog_size) override;

  void PrestartWorkersInternal(const LeaseSpecification &lease_spec, int64_t num_needed);

  /// Return the current size of the worker pool for the requested language. Counts only
  /// idle workers.
  ///
  /// \param language The requested language.
  /// \return The total count of all workers (actor and non-actor) in the pool.
  uint32_t Size(const Language &language) const;

  /// Get all the registered workers.
  ///
  /// \param filter_dead_workers whether or not if this method will filter dead workers
  /// \param filter_io_workers whether or not if this method will filter io workers
  /// non-retriable workers that are still registered.
  ///
  /// \return A list containing all the workers.
  std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredWorkers(
      bool filter_dead_workers = false, bool filter_io_workers = false) const override;

  bool IsWorkerAvailableForScheduling() const override;

  /// Get all the registered drivers.
  ///
  /// \param filter_dead_drivers whether or not if this method will filter dead drivers
  /// that are still registered.
  /// \param filter_system_drivers whether or not if this method will filter system
  /// drivers. A system driver is a driver with job config namespace starting with
  /// "__ray_internal__".
  ///
  /// \return A list containing all the drivers.
  std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredDrivers(
      bool filter_dead_drivers = false,
      bool filter_system_drivers = false) const override;

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const override;

  /// Try killing idle workers to ensure the running workers are in a
  /// reasonable size.
  void TryKillingIdleWorkers();

  /// Get the NodeID of this worker pool.
  const NodeID &GetNodeID() const;

  /// Internal implementation of PopWorker.
  void PopWorker(std::shared_ptr<PopWorkerRequest> pop_worker_request);

  // Find an idle worker that can serve the lease. If found, pop it out and return it.
  // Otherwise, return nullptr.
  std::shared_ptr<WorkerInterface> FindAndPopIdleWorker(
      const PopWorkerRequest &pop_worker_request);

  // Starts a new worker that fulfills `pop_worker_request`. Difference on methods:
  // - PopWorker may reuse idle workers.
  // - StartNewWorker force starts a new worker, with runtime env created.
  // - StartWorkerProcess starts a new worker process, *without* runtime env creation.
  //
  // Note: NONE of these methods guarantee that pop_worker_request.callback will be called
  // with the started worker. It may be called with any fitting workers.
  void StartNewWorker(
      const std::shared_ptr<PopWorkerRequest> &pop_worker_request) override;

 protected:
  void update_worker_startup_token_counter();

  /// Asynchronously start a new worker process. Once the worker process has
  /// registered with an external server, the process should create and
  /// register N workers, then add them to the pool.
  /// Failure to start the worker process is a fatal error. If too many workers
  /// are already being started, then this function will return without starting
  /// any workers.
  ///
  /// \param language Which language this worker process should be.
  /// \param worker_type The type of the worker. This worker type is internal to
  ///                             worker pool abstraction. Outside this class, workers
  ///                             will have rpc::WorkerType instead.
  /// \param job_id The ID of the job to which the started worker process belongs.
  /// \param status The output status of work process starting.
  /// \param dynamic_options The dynamic options that we should add for worker command.
  /// \param runtime_env_hash The hash of runtime env.
  /// \param serialized_runtime_env_context The context of runtime env.
  /// \param runtime_env_info The raw runtime env info.
  /// \param worker_startup_keep_alive_duration If set, the worker will be kept alive for
  ///   this duration even if it's idle. This is only applicable before a lease is
  ///   assigned to the worker.
  /// \return The process that we started and a token. If the token is less than 0,
  /// we didn't start a process.
  std::tuple<Process, StartupToken> StartWorkerProcess(
      const Language &language,
      rpc::WorkerType worker_type,
      const JobID &job_id,
      PopWorkerStatus *status /*output*/,
      const std::vector<std::string> &dynamic_options = {},
      int runtime_env_hash = 0,
      const std::string &serialized_runtime_env_context = "{}",
      const rpc::RuntimeEnvInfo &runtime_env_info = rpc::RuntimeEnvInfo(),
      std::optional<absl::Duration> worker_startup_keep_alive_duration = std::nullopt);

  /// The implementation of how to start a new worker process with command arguments.
  /// The lifetime of the process is tied to that of the returned object,
  /// unless the caller manually detaches the process after the call.
  ///
  /// \param worker_command_args The command arguments of new worker process.
  /// \param[in] env Additional environment variables to be set on this process besides
  /// the environment variables of the parent process.
  /// \return An object representing the started worker process.
  virtual Process StartProcess(const std::vector<std::string> &worker_command_args,
                               const ProcessEnvironment &env);

  /// Push an warning message to user if worker pool is getting to big.
  virtual void WarnAboutSize();

  /// Make this synchronized function for unit test.
  void PopWorkerCallbackInternal(const PopWorkerCallback &callback,
                                 std::shared_ptr<WorkerInterface> worker,
                                 PopWorkerStatus status);

  /// Look up worker's dynamic options by startup token.
  /// TODO(scv119): replace dynamic options by runtime_env.
  const std::vector<std::string> &LookupWorkerDynamicOptions(StartupToken token) const;

  /// Global startup token variable. Incremented once assigned
  /// to a worker process and is added to
  /// state.worker_processes.
  StartupToken worker_startup_token_counter_;

  struct IOWorkerState {
    /// The pool of idle I/O workers.
    std::unordered_set<std::shared_ptr<WorkerInterface>> idle_io_workers;
    /// The queue of pending I/O tasks.
    std::queue<std::function<void(std::shared_ptr<WorkerInterface>)>> pending_io_tasks;
    /// All I/O workers that have registered and are still connected, including both
    /// idle and executing.
    std::unordered_set<std::shared_ptr<WorkerInterface>> started_io_workers;
    /// Number of starting I/O workers.
    int num_starting_io_workers = 0;
  };

  /// Some basic information about the worker process.
  struct WorkerProcessInfo {
    /// Whether this worker is pending registration or is started.
    bool is_pending_registration = true;
    /// The type of the worker.
    rpc::WorkerType worker_type;
    /// The worker process instance.
    Process proc;
    /// The worker process start time.
    std::chrono::high_resolution_clock::time_point start_time;
    /// The runtime env Info.
    rpc::RuntimeEnvInfo runtime_env_info;
    /// The dynamic_options.
    std::vector<std::string> dynamic_options;
    /// The duration to keep the newly created worker alive before it's assigned a lease.
    std::optional<absl::Duration> worker_startup_keep_alive_duration;
  };

  /// An internal data structure that maintains the pool state per language.
  struct State {
    /// The commands and arguments used to start the worker process
    std::vector<std::string> worker_command;
    /// The pool of idle workers.
    std::unordered_set<std::shared_ptr<WorkerInterface>> idle;
    // States for io workers used for python util functions.
    IOWorkerState util_io_worker_state;
    // States for io workers used for spilling objects.
    IOWorkerState spill_io_worker_state;
    // States for io workers used for restoring objects.
    IOWorkerState restore_io_worker_state;
    /// All workers that have registered and are still connected, including both
    /// idle and executing.
    std::unordered_set<std::shared_ptr<WorkerInterface>> registered_workers;
    /// All drivers that have registered and are still connected.
    std::unordered_set<std::shared_ptr<WorkerInterface>> registered_drivers;
    /// A map from the startup tokens of worker processes, assigned by the raylet, to
    /// the extra information of the process. Note that the shim process PID is the
    /// same with worker process PID, except worker process in container.
    absl::flat_hash_map<StartupToken, WorkerProcessInfo> worker_processes;
    /// FIFO queue of pending requests with workers STARTED but pending registration.
    /// If a request stays in this status for >= worker_register_timeout_seconds, we'll
    /// fail the request and kill the worker process.
    std::deque<std::shared_ptr<PopWorkerRequest>> pending_registration_requests;
    /// FIFO queue of pending requests with workers NOT STARTED due to
    /// maximum_startup_concurrency_.
    std::deque<std::shared_ptr<PopWorkerRequest>> pending_start_requests;
    /// We'll push a warning to the user every time a multiple of this many
    /// worker processes has been started.
    int multiple_for_warning;
    /// The last size at which a warning about the number of registered workers
    /// was generated.
    int64_t last_warning_multiple;
  };

  /// Pool states per language.
  absl::flat_hash_map<Language, State, std::hash<int>> states_by_lang_;

  /// The pool of idle non-actor workers of all languages. This is used to kill idle
  /// workers in FIFO order.
  struct IdleWorkerEntry {
    std::shared_ptr<WorkerInterface> worker;
    // Don't kill this worker until this time. Set by:
    // - prestarted workers by Now() + keep alive duration from argument
    // - idle workers by Now() + idle_worker_killing_time_threshold_ms
    absl::Time keep_alive_until;
  };
  void KillIdleWorker(const IdleWorkerEntry &node);
  std::list<IdleWorkerEntry> idle_of_all_languages_;

 private:
  /// A helper function that returns the reference of the pool state
  /// for a given language.
  State &GetStateForLanguage(const Language &language);

  /// Start a timer to monitor the starting worker process.
  ///
  /// If any workers in this process don't register within the timeout
  /// (due to worker process crash or any other reasons), remove them
  /// from `worker_processes`. Otherwise if we'll mistakenly
  /// think there are unregistered workers, and won't start new workers.
  void MonitorStartingWorkerProcess(StartupToken proc_startup_token,
                                    const Language &language,
                                    rpc::WorkerType worker_type);

  /// Start a timer to monitor the starting worker process.
  /// Called when a worker process is started and waiting for registration for the
  /// request. If the registration is not finished within the timeout, we'll failed the
  /// request. Note we don't do anything to the worker process itself, as it's timed out
  /// by MonitorStartingWorkerProcess.
  void MonitorPopWorkerRequestForRegistration(
      std::shared_ptr<PopWorkerRequest> pop_worker_request);

  /// Get the next unallocated port in the free ports list. If a port range isn't
  /// configured, returns 0.
  /// NOTE: Ray does not 'reserve' these ports from being used by other services.
  /// There is a race condition where another service binds to the port sometime
  /// after this function returns and before the Worker/Driver uses the port.
  /// \param[out] port The next available port.
  Status GetNextFreePort(int *port);

  /// Mark this port as free to be used by another worker.
  /// \param[in] port The port to mark as free.
  void MarkPortAsFree(int port);

  /// Try start all I/O workers waiting to be started.
  /// \param language The language of the I/O worker. Currently only Python I/O
  /// workers are effective.
  void TryStartIOWorkers(const Language &language);

  /// Try start spill or restore io workers.
  /// \param language The language of the I/O worker. Currently only Python I/O
  /// workers are effective.
  /// \param worker_type The worker type. It is currently either spill worker or restore
  /// worker.
  void TryStartIOWorkers(const Language &language, const rpc::WorkerType &worker_type);

  /// Try to fulfill pending_start_requests by trying to start more workers.
  /// This happens when we have more room to start workers or an idle worker is pushed.
  /// \param language The language of the PopWorker requests.
  void TryPendingStartRequests(const Language &language);

  /// Get either restore or spill worker state from state based on worker_type.
  ///
  /// \param worker_type IO Worker Type.
  /// \param state Worker pool internal state.
  IOWorkerState &GetIOWorkerStateFromWorkerType(const rpc::WorkerType &worker_type,
                                                State &state) const;

  /// Push IOWorker (e.g., spill worker and restore worker) based on the given
  /// worker_type.
  void PushIOWorkerInternal(const std::shared_ptr<WorkerInterface> &worker,
                            const rpc::WorkerType &worker_type);

  /// Pop IOWorker (e.g., spill worker and restore worker) based on the given worker_type.
  void PopIOWorkerInternal(
      const rpc::WorkerType &worker_type,
      std::function<void(std::shared_ptr<WorkerInterface>)> callback);

  /// Return true if the given worker type is IO worker type. Currently, there are 2 IO
  /// worker types (SPILL_WORKER and RESTORE_WORKER).
  bool IsIOWorkerType(const rpc::WorkerType &worker_type) const;

  /// Call the `PopWorkerCallback` function asynchronously to make sure executed in
  /// different stack.
  virtual void PopWorkerCallbackAsync(PopWorkerCallback callback,
                                      std::shared_ptr<WorkerInterface> worker,
                                      PopWorkerStatus status);

  /// We manage all runtime env resources locally by the two methods:
  /// `GetOrCreateRuntimeEnv` and `DeleteRuntimeEnvIfPossible`.
  ///
  /// `GetOrCreateRuntimeEnv` means increasing the reference count for the runtime env
  /// and `DeleteRuntimeEnvIfPossible` means decreasing the reference count. Note, The
  /// actual ref counting happens in runtime env agent.
  /// We increase or decrease runtime env reference in the cases below:
  /// For the job with an eager installed runtime env:
  /// - Increase reference when job started.
  /// - Decrease reference when job finished.
  /// For the worker process with a valid runtime env:
  /// - Increase reference before worker process started.
  /// - Decrease reference when the worker process is invalid in following cases:
  ///     - Worker process exits normally.
  ///     - Any worker instance registration times out.
  ///     - Worker process isn't started by some reasons(see `StartWorkerProcess`).
  ///
  /// A normal state change flow is:
  ///   job level:
  ///       HandleJobStarted(ref + 1 = 1) -> HandleJobFinshed(ref - 1 = 0)
  ///   worker level:
  ///       StartWorkerProcess(ref + 1 = 1)
  ///       -> DisconnectWorker * 3 (ref - 1 = 0)
  ///
  /// A state change flow for worker timeout case is:
  ///       StartWorkerProcess(ref + 1 = 1)
  ///       -> One worker registration times out, kill worker process (ref - 1 = 0)
  ///
  /// Note: "DisconnectWorker * 3" means that three workers are disconnected. And we
  /// assume that the worker process has tree worker instances totally.

  /// Create runtime env asynchronously by runtime env agent.
  void GetOrCreateRuntimeEnv(const std::string &serialized_runtime_env,
                             const rpc::RuntimeEnvConfig &runtime_env_config,
                             const JobID &job_id,
                             const GetOrCreateRuntimeEnvCallback &callback);

  /// Delete runtime env asynchronously by runtime env agent.
  void DeleteRuntimeEnvIfPossible(const std::string &serialized_runtime_env);

  void AddWorkerProcess(State &state,
                        rpc::WorkerType worker_type,
                        const Process &proc,
                        const std::chrono::high_resolution_clock::time_point &start,
                        const rpc::RuntimeEnvInfo &runtime_env_info,
                        const std::vector<std::string> &dynamic_options,
                        std::optional<absl::Duration> worker_startup_keep_alive_duration);

  void RemoveWorkerProcess(State &state, const StartupToken &proc_startup_token);

  /// Increase worker OOM scores to avoid raylet crashes from heap memory
  /// pressure.
  void AdjustWorkerOomScore(pid_t pid) const;

  std::pair<std::vector<std::string>, ProcessEnvironment> BuildProcessCommandArgs(
      const Language &language,
      rpc::JobConfig *job_config,
      rpc::WorkerType worker_type,
      const JobID &job_id,
      const std::vector<std::string> &dynamic_options,
      int runtime_env_hash,
      const std::string &serialized_runtime_env_context,
      const WorkerPool::State &state) const;

  void ExecuteOnPrestartWorkersStarted(std::function<void()> callback);

  /// Returns if the worker can be used to satisfy the request.
  ///
  /// \param[in] worker The worker.
  /// \param[in] pop_worker_request The pop worker request.
  /// \return WorkerUnfitForLeaseReason::NONE if the worker can be used, else a
  ///         status indicating why it cannot.
  WorkerUnfitForLeaseReason WorkerFitForLease(
      const WorkerInterface &worker, const PopWorkerRequest &pop_worker_request) const;

  /// For Process class for managing subprocesses (e.g. reaping zombies).
  instrumented_io_context *io_service_;
  /// Node ID of the current node.
  const NodeID node_id_;
  /// Address of the current node.
  const std::string node_address_;
  /// Address family for the node IP address (AF_INET or AF_INET6).
  const int node_address_family_;
  /// A callback to get the number of CPUs available. We use this to determine
  /// how many idle workers to keep around.
  std::function<int64_t()> get_num_cpus_available_;
  /// The maximum number of worker processes that can be started concurrently.
  int maximum_startup_concurrency_;
  /// Keeps track of unused ports that newly-created workers can bind on.
  /// If null, workers will not be passed ports and will choose them randomly.
  std::unique_ptr<std::queue<int>> free_ports_;
  /// The port Raylet uses for listening to incoming connections.
  int node_manager_port_ = 0;
  /// A client connection to the GCS.
  gcs::GcsClient &gcs_client_;
  /// The native library path which includes the core libraries.
  std::string native_library_path_;
  /// The callback that will be triggered once it times out to start a worker.
  std::function<void()> starting_worker_timeout_callback_;
  /// If 1, expose Ray debuggers started by the workers externally (to this node).
  int ray_debugger_external_;

  /// If the first job has already been registered.
  bool first_job_registered_ = false;

  /// The callback to send RegisterClientReply to the driver of the first job.
  std::function<void()> first_job_send_register_client_reply_to_driver_;

  /// The number of registered workers of the first job.
  int first_job_registered_python_worker_count_;

  /// The number of initial Python workers to wait for the first job before the driver
  /// receives RegisterClientReply.
  int first_job_driver_wait_num_python_workers_;

  /// The number of prestarted default Python workers.
  const int num_prestart_python_workers;

  /// This map tracks the latest infos of unfinished jobs.
  absl::flat_hash_map<JobID, rpc::JobConfig> all_jobs_;

  /// Set of jobs whose drivers have exited.
  absl::flat_hash_set<JobID> finished_jobs_;

  /// A map of idle workers that are pending exit.
  absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>>
      pending_exit_idle_workers_;

  /// The runner to run function periodically.
  std::shared_ptr<PeriodicalRunner> periodical_runner_;

  /// A callback to get the current time.
  const std::function<absl::Time()> get_time_;
  /// Runtime env manager client.
  std::unique_ptr<RuntimeEnvAgentClient> runtime_env_agent_client_;
  /// Stats
  int64_t process_failed_job_config_missing_ = 0;
  int64_t process_failed_rate_limited_ = 0;
  int64_t process_failed_pending_registration_ = 0;
  int64_t process_failed_runtime_env_setup_failed_ = 0;

  AddProcessToCgroupHook add_to_cgroup_hook_;

  /// Ray metrics
  ray::stats::Sum ray_metric_num_workers_started_{
      /*name=*/"internal_num_processes_started",
      /*description=*/"The total number of worker processes the worker pool has created.",
      /*unit=*/"processes"};

  ray::stats::Sum ray_metric_num_cached_workers_skipped_job_mismatch_{
      /*name=*/"internal_num_processes_skipped_job_mismatch",
      /*description=*/"The total number of cached workers skipped due to job mismatch.",
      /*unit=*/"workers"};

  ray::stats::Sum ray_metric_num_cached_workers_skipped_runtime_environment_mismatch_{
      /*name=*/"internal_num_processes_skipped_runtime_environment_mismatch",
      /*description=*/
      "The total number of cached workers skipped due to runtime environment mismatch.",
      /*unit=*/"workers"};

  ray::stats::Sum ray_metric_num_cached_workers_skipped_dynamic_options_mismatch_{
      /*name=*/"internal_num_processes_skipped_dynamic_options_mismatch",
      /*description=*/
      "The total number of cached workers skipped due to dynamic options mismatch.",
      /*unit=*/"workers"};

  ray::stats::Sum ray_metric_num_workers_started_from_cache_{
      /*name=*/"internal_num_processes_started_from_cache",
      /*description=*/"The total number of workers started from a cached worker process.",
      /*unit=*/"workers"};

  friend class WorkerPoolTest;
  friend class WorkerPoolDriverRegisteredTest;
};

}  // namespace raylet

}  // namespace ray
