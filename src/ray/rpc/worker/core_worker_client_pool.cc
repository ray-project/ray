#include "ray/rpc/worker/core_worker_client_pool.h"

namespace ray {
namespace rpc {

optional<shared_ptr<CoreWorkerClientInterface>> CoreWorkerClientPool::GetByID(
    ray::WorkerID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return {};
  }
  return it->second;
}

shared_ptr<CoreWorkerClientInterface> CoreWorkerClientPool::GetOrConnect(
    const Address &addr_proto) {
  RAY_CHECK(addr_proto.worker_id() != "");
  absl::MutexLock lock(&mu_);
  auto id = WorkerID::FromBinary(addr_proto.worker_id());
  auto it = client_map_.find(id);
  if (it != client_map_.end()) {
    return it->second;
  }
  auto connection = client_factory_(addr_proto);
  client_map_[id] = connection;

  RAY_LOG(DEBUG) << "Connected to " << addr_proto.ip_address() << ":"
                 << addr_proto.port();
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
