#include "ray/raylet/worker_pool.h"

#include "ray/status.h"
#include "ray/util/logging.h"

namespace ray {

namespace raylet {

/// A constructor that initializes a worker pool with num_workers workers.
WorkerPool::WorkerPool(int num_workers, const std::vector<const char *> &worker_command)
    : worker_command_(worker_command) {
  worker_command_.push_back(NULL);
  // Ignore SIGCHLD signals. If we don't do this, then worker processes will
  // become zombies instead of dying gracefully.
  signal(SIGCHLD, SIG_IGN);
  for (int i = 0; i < num_workers; i++) {
    StartWorker();
  }
}

WorkerPool::~WorkerPool() {
  // TODO(swang): Kill registered workers.
  pool_.clear();
  registered_workers_.clear();
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
  // Try to execute the worker command.

  int rv = execvp(worker_command_[0], (char *const *)worker_command_.data());
  // The worker failed to start. This is a fatal error.
  RAY_LOG(FATAL) << "Failed to start worker with return value " << rv;
}

uint32_t WorkerPool::PoolSize() const { return pool_.size(); }

void WorkerPool::RegisterWorker(std::shared_ptr<Worker> worker) {
  RAY_LOG(DEBUG) << "Registering worker with pid " << worker->Pid();
  registered_workers_.push_back(worker);
}

const std::shared_ptr<Worker> WorkerPool::GetRegisteredWorker(
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
  worker->AssignTaskId(TaskID::nil());
  // Add the worker to the idle pool.
  pool_.push_back(std::move(worker));
}

std::shared_ptr<Worker> WorkerPool::PopWorker() {
  if (pool_.empty()) {
    return nullptr;
  }
  std::shared_ptr<Worker> worker = std::move(pool_.back());
  pool_.pop_back();
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
