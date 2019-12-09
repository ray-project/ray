#include "ray/gcs/object_state_accessor.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace gcs {

ObjectStateAccessor::ObjectStateAccessor(RedisGcsClient &client_impl)
    : client_impl_(client_impl), object_sub_executor_(client_impl.object_table()) {}

Status ObjectStateAccessor::AsyncGetLocations(
    const ObjectID &object_id, const MultiItemCallback<ObjectTableData> &callback) {
  RAY_CHECK(callback != nullptr);
  auto on_done = [callback](RedisGcsClient *client, const ObjectID &object_id,
                            const std::vector<ObjectTableData> &data) {
    callback(Status::OK(), data);
  };

  ObjectTable &object_table = client_impl_.object_table();
  return object_table.Lookup(JobID::Nil(), object_id, on_done);
}

Status ObjectStateAccessor::AsyncAddLocation(const ObjectID &object_id,
                                             const ClientID &node_id,
                                             const StatusCallback &callback) {
  std::function<void(RedisGcsClient * client, const ObjectID &id,
                     const ObjectTableData &data)>
      on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ObjectID &object_id,
                         const ObjectTableData &data) { callback(Status::OK()); };
  }

  std::shared_ptr<ObjectTableData> data_ptr = std::make_shared<ObjectTableData>();
  data_ptr->set_manager(node_id.Binary());

  ObjectTable &object_table = client_impl_.object_table();
  return object_table.Add(JobID::Nil(), object_id, data_ptr, on_done);
}

Status ObjectStateAccessor::AsyncRemoveLocation(const ObjectID &object_id,
                                                const ClientID &node_id,
                                                const StatusCallback &callback) {
  std::function<void(RedisGcsClient * client, const ObjectID &id,
                     const ObjectTableData &data)>
      on_done = nullptr;
  if (callback != nullptr) {
    on_done = [callback](RedisGcsClient *client, const ObjectID &object_id,
                         const ObjectTableData &data) { callback(Status::OK()); };
  }

  std::shared_ptr<ObjectTableData> data_ptr = std::make_shared<ObjectTableData>();
  data_ptr->set_manager(node_id.Binary());

  ObjectTable &object_table = client_impl_.object_table();
  return object_table.Remove(JobID::Nil(), object_id, data_ptr, on_done);
}

Status ObjectStateAccessor::AsyncSubscribeToLocations(
    const ObjectID &object_id,
    const SubscribeCallback<ObjectID, ObjectChangeNotification> &subscribe,
    const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  return object_sub_executor_.AsyncSubscribe(node_id_, object_id, subscribe, done);
}

Status ObjectStateAccessor::AsyncUnsubscribeToLocations(const ObjectID &object_id,
                                                        const StatusCallback &done) {
  return object_sub_executor_.AsyncUnsubscribe(node_id_, object_id, done);
}

}  // namespace gcs

}  // namespace ray
