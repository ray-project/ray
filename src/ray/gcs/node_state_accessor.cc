#include "ray/gcs/node_state_accessor.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

NodeStateAccessor::NodeStateAccessor(RedisGcsClient *client_impl)
    : client_impl_(client_impl) {}

Status NodeStateAccessor::RegisterSelf(const GcsNodeInfo &local_node_info) {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.Connect(local_node_info);
}

Status NodeStateAccessor::UnregisterSelf() {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.Disconnect();
}

const ClientID &NodeStateAccessor::GetSelfId() const {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.GetLocalClientId();
}

const GcsNodeInfo &NodeStateAccessor::GetSelfInfo() const {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.GetLocalClient();
}

Status NodeStateAccessor::AsyncUnregister(const ClientID &node_id,
                                          const StatusCallback &callback) {
  ClientTable::WriteCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ClientID &id,
                         const GcsNodeInfo &data) { callback(Status::OK()); };
  }
  ClientTable &client_table = client_impl_->client_table();
  return client_table.MarkDisconnected(node_id, on_done);
}

Status NodeStateAccessor::AsyncGetAll(const MultiItemCallback<GcsNodeInfo> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](RedisGcsClient *client, const ClientID &id,
                            const std::vector<GcsNodeInfo> &data) {
    callback(Status::OK(), data);
  };
  ClientTable &client_table = client_impl_->client_table();
  return client_table.Lookup(on_done);
}

Status NodeStateAccessor::AsyncSubscribeToNodeChange(
    const SubscribeCallback<ClientID, GcsNodeInfo> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  ClientTable &client_table = client_impl_->client_table();
  return client_table.SubscribeToNodeChange(subscribe, done);
}

boost::optional<GcsNodeInfo> NodeStateAccessor::GetFromCache(
    const ClientID &node_id) const {
  ClientTable &client_table = client_impl_->client_table();
  GcsNodeInfo node_info;
  bool found = client_table.GetClient(node_id, &node_info);
  boost::optional<GcsNodeInfo> optional_node;
  if (found) {
    optional_node = std::move(node_info);
  }
  return optional_node;
}

const std::unordered_map<ClientID, GcsNodeInfo> &NodeStateAccessor::GetAllFromCache()
    const {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.GetAllClients();
}

bool NodeStateAccessor::IsRemoved(const ClientID &node_id) const {
  ClientTable &client_table = client_impl_->client_table();
  return client_table.IsRemoved(node_id);
}

}  // namespace gcs

}  // namespace ray
