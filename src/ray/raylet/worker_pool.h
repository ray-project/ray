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
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/client_connection.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
#include "ray/gcs/gcs_client.h"
#include "ray/raylet/worker.h"

namespace ray {

namespace raylet {

using WorkerCommandMap =
    std::unordered_map<Language, std::vector<std::string>, std::hash<int>>;

/// \class WorkerPoolInterface
///
/// Used for new scheduler unit tests.
class WorkerPoolInterface {
 public:
  /// Pop an idle worker from the pool. The caller is responsible for pushing
  /// the worker back onto the pool once the worker has completed its work.
  ///
  /// \param task_spec The returned worker must be able to execute this task.
  /// \return An idle worker with the requested task spec. Returns nullptr if no
  /// such worker exists.
  virtual std::shared_ptr<WorkerInterface> PopWorker(
      const TaskSpecification &task_spec) = 0;
  /// Add an idle worker to the pool.
  ///
  /// \param The idle worker to add.
  virtual void PushWorker(const std::shared_ptr<WorkerInterface> &worker) = 0;

  virtual ~WorkerPoolInterface(){};
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

  virtual void PushUtilWorker(const std::shared_ptr<WorkerInterface> &worker) = 0;
  virtual void PopUtilWorker(
      std::function<void(std::shared_ptr<WorkerInterface>)> callback) = 0;

  virtual ~IOWorkerPoolInterface(){};
};

class WorkerInterface;
class Worker;

/// \class WorkerPool
///
/// The WorkerPool is responsible for managing a pool of Workers. Each Worker
/// is a container for a unit of work.
class WorkerPool : public WorkerPoolInterface, public IOWorkerPoolInterface {
 public:
  /// Create a pool and asynchronously start at least the specified number of workers per
  /// language.
  /// Once each worker process has registered with an external server, the
  /// process should create and register the specified number of workers, and add them to
  /// the pool.
  ///
  /// \param node_id The id of the current node.
  /// \param node_address The address of the current node.
  /// \param num_workers_soft_limit The soft limit of the number of workers.
  /// \param num_initial_python_workers_for_first_job The number of initial Python
  /// workers for the first job.
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
  /// \param starting_worker_timeout_callback The callback that will be triggered once
  /// it times out to start a worker.
  /// \param get_time A callback to get the current time.
  WorkerPool(instrumented_io_context &io_service, const NodeID node_id,
             const std::string node_address, int num_workers_soft_limit,
             int num_initial_python_workers_for_first_job,
             int maximum_startup_concurrency, int min_worker_port, int max_worker_port,
             const std::vector<int> &worker_ports,
             std::shared_ptr<gcs::GcsClient> gcs_client,
             const WorkerCommandMap &worker_commands,
             std::function<void()> starting_worker_timeout_callback,
             const std::function<double()> get_time);

  /// Destructor responsible for freeing a set of workers owned by this class.
  virtual ~WorkerPool();

  /// Set the node manager port.
  /// \param node_manager_port The port Raylet uses for listening to incoming connections.
  void SetNodeManagerPort(int node_manager_port);

  /// Handles the event that a job is started.
  ///
  /// \param job_id ID of the started job.
  /// \param job_config The config of the started job.
  /// \return Void
  void HandleJobStarted(const JobID &job_id, const rpc::JobConfig &job_config);

  /// Handles the event that a job is finished.
  ///
  /// \param job_id ID of the finished job.
  /// \return Void.
  void HandleJobFinished(const JobID &job_id);

  /// \brief Get the job config by job id.
  ///
  /// \param job_id ID of the job.
  /// \return Job config if given job is running, else nullptr.
  boost::optional<const rpc::JobConfig &> GetJobConfig(const JobID &job_id) const;

  /// Register a new worker. The Worker should be added by the caller to the
  /// pool after it becomes idle (e.g., requests a work assignment).
  ///
  /// \param[in] worker The worker to be registered.
  /// \param[in] pid The PID of the worker.
  /// \param[in] send_reply_callback The callback to invoke after registration is
  /// finished/failed.
  /// Returns 0 if the worker should bind on a random port.
  /// \return If the registration is successful.
  Status RegisterWorker(const std::shared_ptr<WorkerInterface> &worker, pid_t pid,
                        std::function<void(Status, int)> send_reply_callback);

  /// To be invoked when a worker is started. This method should be called when the worker
  /// announces its port.
  ///
  /// \param[in] worker The worker which is started.
  void OnWorkerStarted(const std::shared_ptr<WorkerInterface> &worker);

  /// Register a new driver.
  ///
  /// \param[in] worker The driver to be registered.
  /// \param[in] job_config The config of the job.
  /// \param[in] send_reply_callback The callback to invoke after registration is
  /// finished/failed.
  /// \return If the registration is successful.
  Status RegisterDriver(const std::shared_ptr<WorkerInterface> &worker,
                        const rpc::JobConfig &job_config,
                        std::function<void(Status, int)> send_reply_callback);

  /// Get the client connection's registered worker.
  ///
  /// \param The client connection owned by a registered worker.
  /// \return The Worker that owns the given client connection. Returns nullptr
  /// if the client has not registered a worker yet.
  std::shared_ptr<WorkerInterface> GetRegisteredWorker(
      const std::shared_ptr<ClientConnection> &connection) const;

  /// Get the client connection's registered driver.
  ///
  /// \param The client connection owned by a registered driver.
  /// \return The Worker that owns the given client connection. Returns nullptr
  /// if the client has not registered a driver.
  std::shared_ptr<WorkerInterface> GetRegisteredDriver(
      const std::shared_ptr<ClientConnection> &connection) const;

  /// Disconnect a registered worker.
  ///
  /// \param worker The worker to disconnect. The worker must be registered.
  /// \param disconnect_type Type of a worker exit.
  /// \return Whether the given worker was in the pool of idle workers.
  bool DisconnectWorker(const std::shared_ptr<WorkerInterface> &worker,
                        rpc::WorkerExitType disconnect_type);

  /// Disconnect a registered driver.
  ///
  /// \param The driver to disconnect. The driver must be registered.
  void DisconnectDriver(const std::shared_ptr<WorkerInterface> &driver);

  /// Add an idle spill I/O worker to the pool.
  ///
  /// \param worker The idle spill I/O worker to add.
  void PushSpillWorker(const std::shared_ptr<WorkerInterface> &worker);

  /// Pop an idle spill I/O worker from the pool and trigger a callback when
  /// an spill I/O worker is available.
  /// The caller is responsible for pushing the worker back onto the
  /// pool once the worker has completed its work.
  ///
  /// \param callback The callback that returns an available spill I/O worker.
  void PopSpillWorker(std::function<void(std::shared_ptr<WorkerInterface>)> callback);

  /// Add an idle restore I/O worker to the pool.
  ///
  /// \param worker The idle I/O worker to add.
  void PushRestoreWorker(const std::shared_ptr<WorkerInterface> &worker);

  /// Pop an idle restore I/O worker from the pool and trigger a callback when
  /// an restore I/O worker is available.
  /// The caller is responsible for pushing the worker back onto the
  /// pool once the worker has completed its work.
  ///
  /// \param callback The callback that returns an available restore I/O worker.
  void PopRestoreWorker(std::function<void(std::shared_ptr<WorkerInterface>)> callback);

  /// Add an idle delete I/O worker to the pool.
  ///
  /// NOTE: There's currently no concept of delete workers or delete worker pools.
  /// When deleting objects, it shares the workers within restore or spill worker pools.
  /// This method is just a higher level abstraction to hide that implementation detail.
  ///
  /// \param worker The idle I/O worker. It could be either spill or restore I/O worker.
  void PushDeleteWorker(const std::shared_ptr<WorkerInterface> &worker);

  /// Pop an idle delete I/O worker from the pool and trigger a callback when
  /// when delete I/O worker is available.
  /// NOTE: There's currently no concept of delete workers or delete worker pools.
  /// This method just finds more available I/O workers from either spill or restore pool
  /// and pop them out.
  void PopDeleteWorker(std::function<void(std::shared_ptr<WorkerInterface>)> callback);

  void PushUtilWorker(const std::shared_ptr<WorkerInterface> &worker);
  void PopUtilWorker(std::function<void(std::shared_ptr<WorkerInterface>)> callback);

  /// Add an idle worker to the pool.
  ///
  /// \param The idle worker to add.
  void PushWorker(const std::shared_ptr<WorkerInterface> &worker);

  /// Pop an idle worker from the pool. The caller is responsible for pushing
  /// the worker back onto the pool once the worker has completed its work.
  ///
  /// \param task_spec The returned worker must be able to execute this task.
  /// \return An idle worker with the requested task spec. Returns nullptr if no
  /// such worker exists.
  std::shared_ptr<WorkerInterface> PopWorker(const TaskSpecification &task_spec);

  /// Try to prestart a number of workers suitable the given task spec. Prestarting
  /// is needed since core workers request one lease at a time, if starting is slow,
  /// then it means it takes a long time to scale up.
  ///
  /// \param task_spec The returned worker must be able to execute this task.
  /// \param backlog_size The number of tasks in the client backlog of this shape.
  void PrestartWorkers(const TaskSpecification &task_spec, int64_t backlog_size);

  /// Return the current size of the worker pool for the requested language. Counts only
  /// idle workers.
  ///
  /// \param language The requested language.
  /// \return The total count of all workers (actor and non-actor) in the pool.
  uint32_t Size(const Language &language) const;

  /// Get all the workers which are running tasks for a given job.
  ///
  /// \param job_id The job ID.
  /// \return A list containing all the workers which are running tasks for the job.
  std::vector<std::shared_ptr<WorkerInterface>> GetWorkersRunningTasksForJob(
      const JobID &job_id) const;

  /// Get all the registered workers.
  ///
  /// \param filter_dead_workers whether or not if this method will filter dead workers
  /// that are still registered. \return A list containing all the workers.
  const std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredWorkers(
      bool filter_dead_workers = false) const;

  /// Get all the registered drivers.
  ///
  /// \param filter_dead_drivers whether or not if this method will filter dead drivers
  /// that are still registered. \return A list containing all the drivers.
  const std::vector<std::shared_ptr<WorkerInterface>> GetAllRegisteredDrivers(
      bool filter_dead_drivers = false) const;

  /// Whether there is a pending worker for the given task.
  /// Note that, this is only used for actor creation task with dynamic options.
  /// And if the worker registered but isn't assigned a task,
  /// the worker also is in pending state, and this'll return true.
  ///
  /// \param language The required language.
  /// \param task_id The task that we want to query.
  bool HasPendingWorkerForTask(const Language &language, const TaskID &task_id);

  /// Get the set of active object IDs from all workers in the worker pool.
  /// \return A set containing the active object IDs.
  std::unordered_set<ObjectID> GetActiveObjectIDs() const;

  /// Returns debug string for class.
  ///
  /// \return string.
  std::string DebugString() const;

  /// Try killing idle workers to ensure the running workers are in a
  /// reasonable size.
  void TryKillingIdleWorkers();

 protected:
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
  /// \param dynamic_options The dynamic options that we should add for worker command.
  /// \param serialized_runtime_env The runtime environment for the started worker
  /// process. \return The id of the process that we started if it's positive, otherwise
  /// it means we didn't start a process.
  Process StartWorkerProcess(
      const Language &language, const rpc::WorkerType worker_type, const JobID &job_id,
      const std::vector<std::string> &dynamic_options = {},
      const std::string &serialized_runtime_env = "{}",
      std::unordered_map<std::string, std::string> override_environment_variables = {});

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

  struct IOWorkerState {
    /// The pool of idle I/O workers.
    std::queue<std::shared_ptr<WorkerInterface>> idle_io_workers;
    /// The queue of pending I/O tasks.
    std::queue<std::function<void(std::shared_ptr<WorkerInterface>)>> pending_io_tasks;
    /// All I/O workers that have registered and are still connected, including both
    /// idle and executing.
    std::unordered_set<std::shared_ptr<WorkerInterface>> registered_io_workers;
    /// Number of starting I/O workers.
    int num_starting_io_workers = 0;
  };

  /// An internal data structure that maintains the pool state per language.
  struct State {
    /// The commands and arguments used to start the worker process
    std::vector<std::string> worker_command;
    /// The pool of dedicated workers for actor creation tasks
    /// with prefix or suffix worker command.
    std::unordered_map<TaskID, std::shared_ptr<WorkerInterface>> idle_dedicated_workers;
    /// The pool of idle non-actor workers.
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
    /// All workers that have registered but is about to disconnect. They shouldn't be
    /// popped anymore.
    std::unordered_set<std::shared_ptr<WorkerInterface>> pending_disconnection_workers;
    /// A map from the pids of starting worker processes
    /// to the number of their unregistered workers.
    std::unordered_map<Process, int> starting_worker_processes;
    /// A map for looking up the task with dynamic options by the pid of the pending
    /// worker. Note that this is used for the dedicated worker processes.
    std::unordered_map<Process, TaskID> pending_dedicated_workers_to_tasks;
    /// A map for speeding up looking up the pending worker for the given task.
    std::unordered_map<TaskID, Process> tasks_to_pending_dedicated_workers;
    /// A map for looking up tasks with existing dedicated worker processes (processes
    /// with a specially installed environment) so the processes can be reused.
    std::unordered_map<Process, TaskID> registered_dedicated_workers_to_tasks;
    /// We'll push a warning to the user every time a multiple of this many
    /// worker processes has been started.
    int multiple_for_warning;
    /// The last size at which a warning about the number of registered workers
    /// was generated.
    int64_t last_warning_multiple;
  };

  /// Pool states per language.
  std::unordered_map<Language, State, std::hash<int>> states_by_lang_;

  /// The pool of idle non-actor workers of all languages. This is used to kill idle
  /// workers in FIFO order. The second element of std::pair is the time a worker becomes
  /// idle.
  std::list<std::pair<std::shared_ptr<WorkerInterface>, int64_t>> idle_of_all_languages_;

 private:
  /// A helper function that returns the reference of the pool state
  /// for a given language.
  State &GetStateForLanguage(const Language &language);

  /// Start a timer to monitor the starting worker process.
  ///
  /// If any workers in this process don't register within the timeout
  /// (due to worker process crash or any other reasons), remove them
  /// from `starting_worker_processes`. Otherwise if we'll mistakenly
  /// think there are unregistered workers, and won't start new workers.
  void MonitorStartingWorkerProcess(const Process &proc, const Language &language,
                                    const rpc::WorkerType worker_type);

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

  /// Get all workers of the given process.
  ///
  /// \param process The process of workers.
  /// \return The workers of the given process.
  std::unordered_set<std::shared_ptr<WorkerInterface>> GetWorkersByProcess(
      const Process &process);

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
  /// worker types (SPILL_WORKER and RESTORE_WORKER and UTIL_WORKER).
  bool IsIOWorkerType(const rpc::WorkerType &worker_type);

  /// For Process class for managing subprocesses (e.g. reaping zombies).
  instrumented_io_context *io_service_;
  /// Node ID of the current node.
  const NodeID node_id_;
  /// Address of the current node.
  const std::string node_address_;
  /// The soft limit of the number of registered workers.
  int num_workers_soft_limit_;
  /// The maximum number of worker processes that can be started concurrently.
  int maximum_startup_concurrency_;
  /// Keeps track of unused ports that newly-created workers can bind on.
  /// If null, workers will not be passed ports and will choose them randomly.
  std::unique_ptr<std::queue<int>> free_ports_;
  /// The port Raylet uses for listening to incoming connections.
  int node_manager_port_ = 0;
  /// A client connection to the GCS.
  std::shared_ptr<gcs::GcsClient> gcs_client_;
  /// The callback that will be triggered once it times out to start a worker.
  std::function<void()> starting_worker_timeout_callback_;
  FRIEND_TEST(WorkerPoolTest, InitialWorkerProcessCount);

  /// The Job ID of the firstly received job.
  JobID first_job_;

  /// The callback to send RegisterClientReply to the driver of the first job.
  std::function<void()> first_job_send_register_client_reply_to_driver_;

  /// The number of registered workers of the first job.
  int first_job_registered_python_worker_count_;

  /// The umber of initial Python workers to wait for the first job before the driver
  /// receives RegisterClientReply.
  int first_job_driver_wait_num_python_workers_;

  /// The number of initial Python workers for the first job.
  int num_initial_python_workers_for_first_job_;

  /// This map tracks the latest infos of unfinished jobs.
  absl::flat_hash_map<JobID, rpc::JobConfig> all_jobs_;

  /// This map stores the same data as `idle_of_all_languages_`, but in a map structure
  /// for lookup performance.
  std::unordered_map<std::shared_ptr<WorkerInterface>, int64_t>
      idle_of_all_languages_map_;

  /// A map of idle workers that are pending exit.
  absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerInterface>>
      pending_exit_idle_workers_;

  /// The runner to run function periodically.
  PeriodicalRunner periodical_runner_;

  /// A callback to get the current time.
  const std::function<double()> get_time_;
};

}  // namespace raylet

}  // namespace ray
