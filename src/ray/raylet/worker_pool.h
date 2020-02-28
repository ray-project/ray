#ifndef RAY_RAYLET_WORKER_POOL_H
#define RAY_RAYLET_WORKER_POOL_H

#include <inttypes.h>

#include <boost/asio/io_service.hpp>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/client_connection.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_common.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/raylet/worker.h"

namespace ray {

namespace raylet {

using WorkerCommandMap =
    std::unordered_map<Language, std::vector<std::string>, std::hash<int>>;

class Worker;

/// \class WorkerPool
///
/// The WorkerPool is responsible for managing a pool of Workers. Each Worker
/// is a container for a unit of work.
class WorkerPool {
 public:
  /// Create a pool and asynchronously start at least the specified number of workers per
  /// language.
  /// Once each worker process has registered with an external server, the
  /// process should create and register the specified number of workers, and add them to
  /// the pool.
  ///
  /// \param num_workers The number of workers to start, per language.
  /// \param maximum_startup_concurrency The maximum number of worker processes
  /// that can be started in parallel (typically this should be set to the number of CPU
  /// resources on the machine).
  /// \param worker_commands The commands used to start the worker process, grouped by
  /// language.
  /// \param starting_worker_timeout_callback The callback that will be triggered once
  /// it times out to start a worker.
  WorkerPool(boost::asio::io_service &io_service, int num_workers,
             int maximum_startup_concurrency, std::shared_ptr<gcs::GcsClient> gcs_client,
             const WorkerCommandMap &worker_commands,
             std::function<void()> starting_worker_timeout_callback);

  /// Destructor responsible for freeing a set of workers owned by this class.
  virtual ~WorkerPool();

  /// Register a new worker. The Worker should be added by the caller to the
  /// pool after it becomes idle (e.g., requests a work assignment).
  ///
  /// \param The Worker to be registered.
  /// \return If the registration is successful.
  Status RegisterWorker(const std::shared_ptr<Worker> &worker, pid_t pid);

  /// Register a new driver.
  ///
  /// \param The driver to be registered.
  /// \return If the registration is successful.
  Status RegisterDriver(const std::shared_ptr<Worker> &worker);

  /// Get the client connection's registered worker.
  ///
  /// \param The client connection owned by a registered worker.
  /// \return The Worker that owns the given client connection. Returns nullptr
  /// if the client has not registered a worker yet.
  std::shared_ptr<Worker> GetRegisteredWorker(
      const std::shared_ptr<LocalClientConnection> &connection) const;

  /// Get the client connection's registered driver.
  ///
  /// \param The client connection owned by a registered driver.
  /// \return The Worker that owns the given client connection. Returns nullptr
  /// if the client has not registered a driver.
  std::shared_ptr<Worker> GetRegisteredDriver(
      const std::shared_ptr<LocalClientConnection> &connection) const;

  /// Disconnect a registered worker.
  ///
  /// \param The worker to disconnect. The worker must be registered.
  /// \return Whether the given worker was in the pool of idle workers.
  bool DisconnectWorker(const std::shared_ptr<Worker> &worker);

  /// Disconnect a registered driver.
  ///
  /// \param The driver to disconnect. The driver must be registered.
  void DisconnectDriver(const std::shared_ptr<Worker> &driver);

  /// Add an idle worker to the pool.
  ///
  /// \param The idle worker to add.
  void PushWorker(const std::shared_ptr<Worker> &worker);

  /// Pop an idle worker from the pool. The caller is responsible for pushing
  /// the worker back onto the pool once the worker has completed its work.
  ///
  /// \param task_spec The returned worker must be able to execute this task.
  /// \return An idle worker with the requested task spec. Returns nullptr if no
  /// such worker exists.
  std::shared_ptr<Worker> PopWorker(const TaskSpecification &task_spec);

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
  std::vector<std::shared_ptr<Worker>> GetWorkersRunningTasksForJob(
      const JobID &job_id) const;

  /// Get all the workers.
  ///
  /// \return A list containing all the workers.
  const std::vector<std::shared_ptr<Worker>> GetAllWorkers() const;

  /// Get all the drivers.
  ///
  /// \return A list containing all the drivers.
  const std::vector<std::shared_ptr<Worker>> GetAllDrivers() const;

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

  /// Record metrics.
  void RecordMetrics() const;

 protected:
  /// Asynchronously start a new worker process. Once the worker process has
  /// registered with an external server, the process should create and
  /// register num_workers_per_process workers, then add them to the pool.
  /// Failure to start the worker process is a fatal error. If too many workers
  /// are already being started, then this function will return without starting
  /// any workers.
  ///
  /// \param language Which language this worker process should be.
  /// \param dynamic_options The dynamic options that we should add for worker command.
  /// \return The id of the process that we started if it's positive,
  /// otherwise it means we didn't start a process.
  Process StartWorkerProcess(const Language &language,
                             const std::vector<std::string> &dynamic_options = {});

  /// The implementation of how to start a new worker process with command arguments.
  /// The lifetime of the process is tied to that of the returned object,
  /// unless the caller manually detaches the process after the call.
  ///
  /// \param worker_command_args The command arguments of new worker process.
  /// \return An object representing the started worker process.
  virtual Process StartProcess(const std::vector<std::string> &worker_command_args);

  /// Push an warning message to user if worker pool is getting to big.
  virtual void WarnAboutSize();

  /// An internal data structure that maintains the pool state per language.
  struct State {
    /// The commands and arguments used to start the worker process
    std::vector<std::string> worker_command;
    /// The number of workers per process.
    int num_workers_per_process;
    /// The pool of dedicated workers for actor creation tasks
    /// with prefix or suffix worker command.
    std::unordered_map<TaskID, std::shared_ptr<Worker>> idle_dedicated_workers;
    /// The pool of idle non-actor workers.
    std::unordered_set<std::shared_ptr<Worker>> idle;
    /// The pool of idle actor workers.
    std::unordered_map<ActorID, std::shared_ptr<Worker>> idle_actor;
    /// All workers that have registered and are still connected, including both
    /// idle and executing.
    std::unordered_set<std::shared_ptr<Worker>> registered_workers;
    /// All drivers that have registered and are still connected.
    std::unordered_set<std::shared_ptr<Worker>> registered_drivers;
    /// A map from the pids of starting worker processes
    /// to the number of their unregistered workers.
    std::unordered_map<Process, int> starting_worker_processes;
    /// A map for looking up the task with dynamic options by the pid of
    /// worker. Note that this is used for the dedicated worker processes.
    std::unordered_map<Process, TaskID> dedicated_workers_to_tasks;
    /// A map for speeding up looking up the pending worker for the given task.
    std::unordered_map<TaskID, Process> tasks_to_dedicated_workers;
    /// We'll push a warning to the user every time a multiple of this many
    /// worker processes has been started.
    int multiple_for_warning;
    /// The last size at which a warning about the number of registered workers
    /// was generated.
    int64_t last_warning_multiple;
  };

  /// Pool states per language.
  std::unordered_map<Language, State, std::hash<int>> states_by_lang_;

 private:
  /// Force-start at least num_workers workers for this language. Used for internal and
  /// test purpose only.
  ///
  /// \param num_workers The number of workers to start, per language.
  void Start(int num_workers);

  /// A helper function that returns the reference of the pool state
  /// for a given language.
  State &GetStateForLanguage(const Language &language);

  /// Start a timer to monitor the starting worker process.
  ///
  /// If any workers in this process don't register within the timeout
  /// (due to worker process crash or any other reasons), remove them
  /// from `starting_worker_processes`. Otherwise if we'll mistakenly
  /// think there are unregistered workers, and won't start new workers.
  void MonitorStartingWorkerProcess(const Process &proc, const Language &language);

  /// For Process class for managing subprocesses (e.g. reaping zombies).
  boost::asio::io_service *io_service_;
  /// The maximum number of worker processes that can be started concurrently.
  int maximum_startup_concurrency_;
  /// A client connection to the GCS.
  std::shared_ptr<gcs::GcsClient> gcs_client_;
  /// The callback that will be triggered once it times out to start a worker.
  std::function<void()> starting_worker_timeout_callback_;
  FRIEND_TEST(WorkerPoolTest, InitialWorkerProcessCount);
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_WORKER_POOL_H
