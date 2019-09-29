#include "ray/gcs/dynamic_resource_state_accessor.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace gcs {

DynamicResourceStateAccessor::DynamicResourceStateAccessor(RedisGcsClient &client_impl)
    : client_impl_(client_impl), resource_sub_executor_(client_impl_.resource_table()) {}

Status DynamicResourceStateAccessor::AsyncGet(
    const ClientID &node_id,
    const OptionalItemCallback<TagToResourceMap> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](RedisGcsClient *client, const ClientID &id,
                            const TagToResourceMap &data) {
    boost::optional<TagToResourceMap> result;
    if (!data.empty()) {
      result = data;
    }
    callback(Status::OK(), result);
  };

  DynamicResourceTable &resource_table = client_impl_.resource_table();
  return resource_table.Lookup(JobID::Nil(), node_id, on_done);
}

Status DynamicResourceStateAccessor::AsyncUpdate(const ClientID &node_id,
                                                 const TagToResourceMap &resources,
                                                 const StatusCallback &callback) {
  Hash<ClientID, ResourceTableData>::HashCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ClientID &node_id,
                         const TagToResourceMap &resources) {
      callback(Status::OK());
    };
  }

  DynamicResourceTable &resource_table = client_impl_.resource_table();
  return resource_table.Update(JobID::Nil(), node_id, resources, on_done);
}

Status DynamicResourceStateAccessor::AsyncDelete(
    const ClientID &node_id, const std::vector<std::string> &resource_tags,
    const StatusCallback &callback) {
  Hash<ClientID, ResourceTableData>::HashRemoveCallback on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ClientID &node_id,
                         const std::vector<std::string> &resource_tags) {
      callback(Status::OK());
    };
  }

  DynamicResourceTable &resource_table = client_impl_.resource_table();
  return resource_table.RemoveEntries(JobID::Nil(), node_id, resource_tags, on_done);
}

Status DynamicResourceStateAccessor::AsyncSubscribe(
    const SubscribeCallback<ClientID, DynamicResourceNotification> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return resource_sub_executor_.AsyncSubscribe(ClientID::Nil(), subscribe, done);
}

}  // namespace gcs

}  // namespace ray
