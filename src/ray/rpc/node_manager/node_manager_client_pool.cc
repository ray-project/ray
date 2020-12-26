#include "ray/rpc/node_manager/node_manager_client_pool.h"

namespace ray {
namespace rpc {

shared_ptr<ray::RayletClientInterface> NodeManagerClientPool::GetOrConnectByAddress(
    const rpc::Address &address) {
  RAY_CHECK(address.raylet_id() != "");
  absl::MutexLock lock(&mu_);
  auto raylet_id = NodeID::FromBinary(address.raylet_id());
  auto it = client_map_.find(raylet_id);
  if (it != client_map_.end()) {
    return it->second;
  }
  auto connection = client_factory_(address);
  client_map_[raylet_id] = connection;

  RAY_LOG(DEBUG) << "Connected to " << address.ip_address() << ":" << address.port();
  return connection;
}

optional<shared_ptr<ray::RayletClientInterface>> NodeManagerClientPool::GetOrConnectByID(
    ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return {};
  }
  return it->second;
}

void NodeManagerClientPool::Disconnect(ray::NodeID id) {
  absl::MutexLock lock(&mu_);
  auto it = client_map_.find(id);
  if (it == client_map_.end()) {
    return;
  }
  client_map_.erase(it);
}

}  // namespace rpc
}  // namespace ray
