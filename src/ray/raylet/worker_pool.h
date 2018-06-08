#ifndef RAY_RAYLET_WORKER_POOL_H
#define RAY_RAYLET_WORKER_POOL_H

#include <inttypes.h>
#include <list>
#include <unordered_map>
#include <unordered_set>

#include "ray/common/client_connection.h"
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
  /// \param num_worker_processes The number of worker processes to start.
  /// \param num_workers_per_process The number of workers per process.
  /// \param worker_command The command used to start the worker process.
  WorkerPool(int num_worker_processes, int num_workers_per_process, int num_cpus,
             const std::vector<std::string> &worker_command);

  /// Destructor responsible for freeing a set of workers owned by this class.
  virtual ~WorkerPool();

  /// Asynchronously start a new worker process. Once the worker process has
  /// registered with an external server, the process should create and
  /// register num_workers_per_process_ workers, then add them to the pool.
  /// Failure to start the worker process is a fatal error.
  /// This function will start up to num_cpus many workers in parallel
  /// if it is called multiple times.
  ///
  /// \param force_start Controls whether to force starting a worker regardless of any
  /// workers that have already been started but not yet registered.
  void StartWorkerProcess(bool force_start = false);

  /// Register a new worker. The Worker should be added by the caller to the
  /// pool after it becomes idle (e.g., requests a work assignment).
  ///
  /// \param The Worker to be registered.
  void RegisterWorker(std::shared_ptr<Worker> worker);

  /// Get the client connection's registered worker.
  ///
  /// \param The client connection owned by a registered worker.
  /// \return The Worker that owns the given client connection. Returns nullptr
  /// if the client has not registered a worker yet.
  std::shared_ptr<Worker> GetRegisteredWorker(
      const std::shared_ptr<LocalClientConnection> &connection) const;

  /// Disconnect a registered worker.
  ///
  /// \param The worker to disconnect. The worker must be registered.
  /// \return Whether the given worker was in the pool of idle workers.
  bool DisconnectWorker(std::shared_ptr<Worker> worker);

  /// Add an idle worker to the pool.
  ///
  /// \param The idle worker to add.
  void PushWorker(std::shared_ptr<Worker> worker);

  /// Pop an idle worker from the pool. The caller is responsible for pushing
  /// the worker back onto the pool once the worker has completed its work.
  ///
  /// \param actor_id The returned worker must have this actor ID.
  /// \return An idle worker with the requested actor ID. Returns nullptr if no
  /// such worker exists.
  std::shared_ptr<Worker> PopWorker(const ActorID &actor_id);

  /// Return the current size of the worker pool. Counts only the workers that registered
  /// and requested a task.
  ///
  /// \return The total count of all workers (actor and non-actor) in the pool.
  uint32_t Size() const;

 protected:
  /// Add started worker PID to the internal list of started workers (for testing).
  ///
  /// \param pid A process identifier for the worker being started.
  void AddStartingWorkerProcess(pid_t pid);

  /// A map from the pids of worker processes that are starting
  /// to the number of their unregistered workers.
  std::unordered_map<pid_t, int> starting_worker_processes_;

 private:
  /// The number of workers per process.
  int num_workers_per_process_;
  /// The number of CPUs this Raylet has available.
  int num_cpus_;
  /// The command and arguments used to start the worker.
  std::vector<std::string> worker_command_;
  /// The pool of idle workers.
  std::list<std::shared_ptr<Worker>> pool_;
  /// The pool of idle actor workers.
  std::unordered_map<ActorID, std::shared_ptr<Worker>> actor_pool_;
  /// All workers that have registered and are still connected, including both
  /// idle and executing.
  // TODO(swang): Make this a map to make GetRegisteredWorker faster.
  std::list<std::shared_ptr<Worker>> registered_workers_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_WORKER_POOL_H
