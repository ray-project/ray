#ifndef WORKER_POOL_CC
#define WORKER_POOL_CC

#include "WorkerPool.h"

#include "common.h"

using namespace std;
namespace ray {

/// A constructor that initializes a worker pool with num_workers workers.
WorkerPool::WorkerPool(int num_workers) {
  init_size_ = num_workers;
  for (int i = 0; i < num_workers; i++) {
    StartWorker();
  }
}

WorkerPool::~WorkerPool() {
  pool_.clear();
  registered_workers_.clear();
}

/// Create a new worker and add it to the pool
bool WorkerPool::StartWorker() {
  // TODO(swang): Start the worker.
  return true;
}

uint32_t WorkerPool::PoolSize() const{
  return pool_.size();
}

void WorkerPool::RegisterWorker(std::shared_ptr<Worker> worker) {
  LOG_INFO("Registering worker with pid %d", worker->Pid());
  registered_workers_.push_back(worker);
}

const std::shared_ptr<Worker> WorkerPool::GetRegisteredWorker(std::shared_ptr<ClientConnection> connection) const {
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
bool removeWorker(std::list<std::shared_ptr<Worker>> &worker_pool, std::shared_ptr<Worker> worker) {
  for (auto it = worker_pool.begin(); it != worker_pool.end(); it++) {
    if (*it == worker) {
      worker_pool.erase(it);
      return true;
    }
  }
  return false;
}

bool WorkerPool::DisconnectWorker(shared_ptr<Worker> worker) {
  CHECK(removeWorker(registered_workers_, worker));
  return removeWorker(pool_, worker);
}

} // end namespace ray

#endif
