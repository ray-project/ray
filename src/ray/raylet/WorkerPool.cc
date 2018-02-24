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

/// Create a new worker and add it to the pool
bool WorkerPool::StartWorker() {
  // TODO(swang): Start the worker.
  return true;
}

uint32_t WorkerPool::PoolSize() const{
  return pool_.size();
}

void WorkerPool::AddWorker(shared_ptr<ClientConnection> connection) {
  LOG_INFO("Registering worker with pid %d", connection->GetWorker().Pid());
  pool_.push_back(connection);
}

shared_ptr<ClientConnection> WorkerPool::PopWorker() {
  auto worker = std::move(pool_.back());
  pool_.pop_back();
  return worker;
}

void WorkerPool::RemoveWorker(shared_ptr<ClientConnection> connection) {
  for (auto it = pool_.begin(); it != pool_.end(); it++) {
    if (*it == connection) {
      LOG_INFO("Removing worker with pid %d", (*it)->GetWorker().Pid());
      pool_.erase(it);
      return;
    }
  }
}

} // end namespace ray

#endif
