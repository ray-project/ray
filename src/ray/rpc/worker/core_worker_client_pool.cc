#include "ray/rpc/worker/core_worker_client_pool.h"

namespace ray {
namespace rpc {

optional<shared_ptr<CoreWorkerClientInterface>> CoreWorkerClientPool::GetByID(ray::WorkerID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return {};
  }
  return it->second;
}

shared_ptr<CoreWorkerClientInterface> CoreWorkerClientPool::GetOrConnect(const Address& addr_proto) {
  return GetOrConnect(WorkerAddress(addr_proto));
}

shared_ptr<CoreWorkerClientInterface> CoreWorkerClientPool::GetOrConnect(const WorkerAddress& addr) {
  auto existing = GetByID(addr.worker_id);
  if (existing.has_value()) {
    return existing.value();
  }
  absl::MutexLock lock(&mu_);
  auto connection = client_factory_(addr.ToProto());
  client_map_[addr.worker_id] = connection;

  RAY_LOG(INFO) << "Connected to " << addr.ip_address << ":" << addr.port;
  return connection;
}

void CoreWorkerClientPool::Disconnect(ray::WorkerID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return;
  }
  client_map_.erase(it);
}

}  // namespace rpc
}  // namespace ray
