#include "ray/raylet/worker_pool.h"

#include <sys/wait.h>

#include "ray/status.h"
#include "ray/util/logging.h"

namespace {

// A helper function to remove a worker from a list. Returns true if the worker
// was found and removed.
std::shared_ptr<ray::raylet::Worker> GetWorker(
    const std::list<std::shared_ptr<ray::raylet::Worker>> &worker_pool,
    const std::shared_ptr<ray::LocalClientConnection> &connection) {
  for (auto it = worker_pool.begin(); it != worker_pool.end(); it++) {
    if ((*it)->Connection() == connection) {
      return (*it);
    }
  }
  return nullptr;
}

// A helper function to remove a worker from a list. Returns true if the worker
// was found and removed.
bool RemoveWorker(std::list<std::shared_ptr<ray::raylet::Worker>> &worker_pool,
                  const std::shared_ptr<ray::raylet::Worker> &worker) {
  for (auto it = worker_pool.begin(); it != worker_pool.end(); it++) {
    if (*it == worker) {
      worker_pool.erase(it);
      return true;
    }
  }
  return false;
}

}  // namespace

namespace ray {

namespace raylet {

/// A constructor that initializes a worker pool with
/// (num_worker_processes * num_workers_per_process) workers
WorkerPool::WorkerPool(int num_worker_processes, int num_workers_per_process,
                       int num_cpus, const std::vector<std::string> &worker_command)
    : num_workers_per_process_(num_workers_per_process),
      num_cpus_(num_cpus),
      worker_command_(worker_command) {
  RAY_CHECK(num_workers_per_process > 0) << "num_workers_per_process must be positive.";
  // Ignore SIGCHLD signals. If we don't do this, then worker processes will
  // become zombies instead of dying gracefully.
  signal(SIGCHLD, SIG_IGN);
  for (int i = 0; i < num_worker_processes; i++) {
    // Force-start num_workers workers.
    StartWorkerProcess(true);
  }
}

WorkerPool::~WorkerPool() {
  std::unordered_set<pid_t> pids_to_kill;
  // Kill all registered workers. NOTE(swang): This assumes that the registered
  // workers were started by the pool.
  for (const auto &worker : registered_workers_) {
    pids_to_kill.insert(worker->Pid());
  }
  // Kill all the workers that have been started but not registered.
  for (const auto &entry : starting_worker_processes_) {
    pids_to_kill.insert(entry.second);
  }
  for (const auto &pid : pids_to_kill) {
    RAY_CHECK(pid > 0);
    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);
  }
}

uint32_t WorkerPool::Size() const {
  return static_cast<uint32_t>(actor_pool_.size() + pool_.size());
}

void WorkerPool::StartWorkerProcess(bool force_start) {
  RAY_CHECK(!worker_command_.empty()) << "No worker command provided";
  // The first condition makes sure that we are always starting up to
  // num_cpus_ number of processes in parallel.
  if (static_cast<int>(starting_worker_processes_.size()) >= num_cpus_ && !force_start) {
    // Workers have been started, but not registered. Force start disabled -- returning.
    RAY_LOG(DEBUG) << starting_worker_processes_.size()
                   << " worker processes pending registration";
    return;
  }
  // Either there are no workers pending registration or the worker start is being forced.
  RAY_LOG(DEBUG) << "starting worker, actor pool " << actor_pool_.size() << " task pool "
                 << pool_.size();

  // Launch the process to create the worker.
  pid_t pid = fork();
  if (pid != 0) {
    RAY_LOG(DEBUG) << "Started worker process with pid " << pid;
    starting_worker_processes_.emplace(std::make_pair(pid, num_workers_per_process_));
    return;
  }

  // Reset the SIGCHLD handler for the worker.
  signal(SIGCHLD, SIG_DFL);

  // Extract pointers from the worker command to pass into execvp.
  std::vector<const char *> worker_command_args;
  for (auto const &token : worker_command_) {
    worker_command_args.push_back(token.c_str());
  }
  worker_command_args.push_back(nullptr);

  // Try to execute the worker command.
  int rv = execvp(worker_command_args[0],
                  const_cast<char *const *>(worker_command_args.data()));
  // The worker failed to start. This is a fatal error.
  RAY_LOG(FATAL) << "Failed to start worker with return value " << rv;
}

void WorkerPool::RegisterWorker(std::shared_ptr<Worker> worker) {
  auto pid = worker->Pid();
  RAY_LOG(DEBUG) << "Registering worker with pid " << pid;
  registered_workers_.push_back(std::move(worker));

  auto it = starting_worker_processes_.find(pid);
  RAY_CHECK(it != starting_worker_processes_.end());
  it->second--;
  if (it->second == 0) {
    starting_worker_processes_.erase(it);
  }
}

void WorkerPool::RegisterDriver(std::shared_ptr<Worker> driver) {
  RAY_CHECK(!driver->GetAssignedTaskId().is_nil());
  registered_drivers_.push_back(driver);
}

std::shared_ptr<Worker> WorkerPool::GetRegisteredWorker(
    const std::shared_ptr<LocalClientConnection> &connection) const {
  return GetWorker(registered_workers_, connection);
}

std::shared_ptr<Worker> WorkerPool::GetRegisteredDriver(
    const std::shared_ptr<LocalClientConnection> &connection) const {
  return GetWorker(registered_drivers_, connection);
}

void WorkerPool::PushWorker(std::shared_ptr<Worker> worker) {
  // Since the worker is now idle, unset its assigned task ID.
  RAY_CHECK(worker->GetAssignedTaskId().is_nil())
      << "Idle workers cannot have an assigned task ID";
  // Add the worker to the idle pool.
  if (worker->GetActorId().is_nil()) {
    pool_.push_back(std::move(worker));
  } else {
    actor_pool_[worker->GetActorId()] = std::move(worker);
  }
}

std::shared_ptr<Worker> WorkerPool::PopWorker(const ActorID &actor_id) {
  std::shared_ptr<Worker> worker = nullptr;
  if (actor_id.is_nil()) {
    if (!pool_.empty()) {
      worker = std::move(pool_.back());
      pool_.pop_back();
    }
  } else {
    auto actor_entry = actor_pool_.find(actor_id);
    if (actor_entry != actor_pool_.end()) {
      worker = std::move(actor_entry->second);
      actor_pool_.erase(actor_entry);
    }
  }
  return worker;
}

bool WorkerPool::DisconnectWorker(std::shared_ptr<Worker> worker) {
  RAY_CHECK(RemoveWorker(registered_workers_, worker));
  return RemoveWorker(pool_, worker);
}

void WorkerPool::DisconnectDriver(std::shared_ptr<Worker> driver) {
  RAY_CHECK(RemoveWorker(registered_drivers_, driver));
}

}  // namespace raylet

}  // namespace ray
