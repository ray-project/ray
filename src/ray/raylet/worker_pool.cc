#include "ray/raylet/worker_pool.h"

#include <sys/wait.h>

#include "ray/status.h"
#include "ray/util/logging.h"

namespace ray {

namespace raylet {

/// A constructor that initializes a worker pool with num_workers workers.
WorkerPool::WorkerPool(int num_workers, const std::vector<std::string> &worker_command)
    : worker_command_(worker_command) {
  // Ignore SIGCHLD signals. If we don't do this, then worker processes will
  // become zombies instead of dying gracefully.
  signal(SIGCHLD, SIG_IGN);
  for (int i = 0; i < num_workers; i++) {
    StartWorker();
  }
}

WorkerPool::~WorkerPool() {
  // Kill all registered workers. NOTE(swang): This assumes that the registered
  // workers were started by the pool.
  for (const auto &worker : registered_workers_) {
    RAY_CHECK(worker->Pid() > 0);
    kill(worker->Pid(), SIGKILL);
    waitpid(worker->Pid(), NULL, 0);
  }
}

void WorkerPool::StartWorker() {
  RAY_CHECK(!worker_command_.empty()) << "No worker command provided";

  // Launch the process to create the worker.
  pid_t pid = fork();
  if (pid != 0) {
    RAY_LOG(DEBUG) << "Started worker with pid " << pid;
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
  RAY_LOG(DEBUG) << "Registering worker with pid " << worker->Pid();
  registered_workers_.push_back(worker);
}

std::shared_ptr<Worker> WorkerPool::GetRegisteredWorker(
    std::shared_ptr<LocalClientConnection> connection) const {
  for (auto it = registered_workers_.begin(); it != registered_workers_.end(); it++) {
    if ((*it)->Connection() == connection) {
      return (*it);
    }
  }
  return nullptr;
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

// A helper function to remove a worker from a list. Returns true if the worker
// was found and removed.
bool removeWorker(std::list<std::shared_ptr<Worker>> &worker_pool,
                  std::shared_ptr<Worker> worker) {
  for (auto it = worker_pool.begin(); it != worker_pool.end(); it++) {
    if (*it == worker) {
      worker_pool.erase(it);
      return true;
    }
  }
  return false;
}

bool WorkerPool::DisconnectWorker(std::shared_ptr<Worker> worker) {
  RAY_CHECK(removeWorker(registered_workers_, worker));
  return removeWorker(pool_, worker);
}

}  // namespace raylet

}  // namespace ray
