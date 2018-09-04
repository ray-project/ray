#ifndef RAY_RAYLET_WORKER_POOL_H
#define RAY_RAYLET_WORKER_POOL_H

#include <inttypes.h>
#include <list>
#include <unordered_map>
#include <unordered_set>

#include "ray/common/client_connection.h"
#include "ray/gcs/format/util.h"
#include "ray/raylet/task.h"
#include "ray/raylet/worker.h"

namespace ray {

namespace raylet {

class Worker;

/// \class WorkerPool
///
/// The WorkerPool is responsible for managing a pool of Workers. Each Worker
/// is a container for a unit of work.
class WorkerPool {
 public:
  /// Create a pool and asynchronously start the specified number of worker processes.
  /// Once each worker process has registered with an external server,
  /// the process should create and register the specified number of workers,
  /// and add them to the pool.
  ///
  /// \param num_worker_processes The number of worker processes to start, per language.
  /// \param num_workers_per_process The number of workers per process.
  /// \param maximum_startup_concurrency The maximum number of worker processes
  /// that can be started in parallel (typically this should be set to the number of CPU
  /// resources on the machine).
  /// \param worker_commands The commands used to start the worker process, grouped by
  /// language.
  WorkerPool(
      int num_worker_processes, int num_workers_per_process,
      int maximum_startup_concurrency,
      const std::unordered_map<Language, std::vector<std::string>> &worker_commands);

  /// Destructor responsible for freeing a set of workers owned by this class.
  virtual ~WorkerPool();

  /// Asynchronously start a new worker process. Once the worker process has
  /// registered with an external server, the process should create and
  /// register num_workers_per_process_ workers, then add them to the pool.
  /// Failure to start the worker process is a fatal error. If too many workers
  /// are already being started, then this function will return without starting
  /// any workers.
  ///
  /// \param language Which language this worker process should be.
  void StartWorkerProcess(const Language &language);

  /// Register a new worker. The Worker should be added by the caller to the
  /// pool after it becomes idle (e.g., requests a work assignment).
  ///
  /// \param The Worker to be registered.
  void RegisterWorker(std::shared_ptr<Worker> worker);

  /// Register a new driver.
  ///
  /// \param The driver to be registered.
  void RegisterDriver(const std::shared_ptr<Worker> worker);

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
  bool DisconnectWorker(std::shared_ptr<Worker> worker);

  /// Disconnect a registered driver.
  ///
  /// \param The driver to disconnect. The driver must be registered.
  void DisconnectDriver(std::shared_ptr<Worker> driver);

  /// Add an idle worker to the pool.
  ///
  /// \param The idle worker to add.
  void PushWorker(std::shared_ptr<Worker> worker);

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

  /// Get all the workers for a particular driver.
  ///
  /// \param job_id The job ID.
  /// \return A list containing all the workers for a driver.
  std::vector<std::shared_ptr<Worker>> GetDriverWorkers(const DriverID &driver_id) const;

 protected:
  /// A map from the pids of starting worker processes
  /// to the number of their unregistered workers.
  std::unordered_map<pid_t, int> starting_worker_processes_;
  /// The number of workers per process.
  int num_workers_per_process_;

 private:
  /// An internal data structure that maintains the pool state per language.
  struct State {
    /// The commands and arguments used to start the worker process
    std::vector<std::string> worker_command;
    /// The pool of idle non-actor workers.
    std::list<std::shared_ptr<Worker>> idle;
    /// The pool of idle actor workers.
    std::unordered_map<ActorID, std::shared_ptr<Worker>> idle_actor;
    /// All workers that have registered and are still connected, including both
    /// idle and executing.
    // TODO(swang): Make this a map to make GetRegisteredWorker faster.
    std::list<std::shared_ptr<Worker>> registered_workers;
    /// All drivers that have registered and are still connected.
    std::list<std::shared_ptr<Worker>> registered_drivers;
  };

  /// A helper function that returns the reference of the pool state
  /// for a given language.
  inline State &GetStateForLanguage(const Language &language);

  /// The maximum number of workers that can be started concurrently.
  int maximum_startup_concurrency_;
  /// Pool states per language.
  std::unordered_map<Language, State> states_by_lang_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_WORKER_POOL_H
