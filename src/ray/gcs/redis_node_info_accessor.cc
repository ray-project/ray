#include "ray/gcs/redis_node_info_accessor.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

RedisNodeInfoAccessor::RedisNodeInfoAccessor(RedisGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status RedisNodeInfoAccessor::RegisterSelf(const GcsNodeInfo &local_node_info) {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.Connect(local_node_info);
}

Status RedisNodeInfoAccessor::UnregisterSelf() {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.Disconnect();
}

bool RedisNodeInfoAccessor::IsSelfUnregistered() const {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.IsDisconnected();
}

const ClientID &RedisNodeInfoAccessor::GetSelfId() const {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.GetLocalClientId();
}

const GcsNodeInfo &RedisNodeInfoAccessor::GetSelfInfo() const {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.GetLocalClient();
}

Status RedisNodeInfoAccessor::AsyncUnregister(const ClientID &node_id,
                                              const StatusCallback &callback) {
  ClientTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ClientID &id,
                         const GcsNodeInfo &data) { callback(Status::OK()); };
  }
  ClientTable &client_table = client_impl_->client_table();
  return client_table.MarkDisconnected(node_id, on_done);
}

Status RedisNodeInfoAccessor::AsyncSubscribeToNodeChange(
    const SubscribeCallback<ClientID, GcsNodeInfo> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  ClientTable &client_table = client_impl_->client_table();
  return client_table.SubscribeToNodeChange(subscribe, done);
}

Status RedisNodeInfoAccessor::AsyncGetAll(
    const MultiItemCallback<GcsNodeInfo> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](RedisGcsClient *client, const ClientID &id,
                            const std::vector<GcsNodeInfo> &data) {
    callback(Status::OK(), data);
  };
  ClientTable &client_table = client_impl_->client_table();
  return client_table.Lookup(on_done);
}

boost::optional<GcsNodeInfo> RedisNodeInfoAccessor::Get(const ClientID &node_id) const {
  GcsNodeInfo node_info;
  ClientTable &client_table = client_impl_->client_table();
  bool found = client_table.GetClient(node_id, &node_info);
  boost::optional<GcsNodeInfo> optional_node;
  if (found) {
    optional_node = std::move(node_info);
  }
  return optional_node;
}

const std::unordered_map<ClientID, GcsNodeInfo> &RedisNodeInfoAccessor::GetAll() const {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.GetAllClients();
}

bool RedisNodeInfoAccessor::IsRemoved(const ClientID &node_id) const {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.IsRemoved(node_id);
}

}  // namespace gcs

}  // namespace ray
