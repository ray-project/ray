#include "ray/object_manager/object_directory.h"

namespace ray {

ObjectDirectory::ObjectDirectory(std::shared_ptr<gcs::AsyncGcsClient> &gcs_client) {
  gcs_client_ = gcs_client;
}

void ObjectDirectory::RegisterBackend() {
  auto object_notification_callback = [this](gcs::AsyncGcsClient *client,
                                             const ObjectID &object_id,
                                             const std::vector<ObjectTableDataT> &data) {
    // Objects are added to this map in SubscribeObjectLocations.
    auto entry = listeners_.find(object_id);
    // Do nothing for objects we are not listening for.
    if (entry == listeners_.end()) {
      return;
    }
    // Update entries for this object.
    auto client_id_set = entry->second.client_ids;
    for (auto &object_table_data : data) {
      ClientID client_id = ClientID::from_binary(object_table_data.manager);
      if (!object_table_data.is_eviction) {
        client_id_set.insert(client_id);
      } else {
        client_id_set.erase(client_id);
      }
    }
    if (!client_id_set.empty()) {
      // Only call the callback if we have object locations.
      std::vector<ClientID> client_id_vec(client_id_set.begin(), client_id_set.end());
      auto callback = entry->second.locations_found_callback;
      callback(client_id_vec, object_id);
    }
  };
  RAY_CHECK_OK(gcs_client_->object_table().Subscribe(
      UniqueID::nil(), gcs_client_->client_table().GetLocalClientId(),
      object_notification_callback, nullptr));
}

ray::Status ObjectDirectory::ReportObjectAdded(const ObjectID &object_id,
                                               const ClientID &client_id,
                                               const ObjectInfoT &object_info) {
  // Append the addition entry to the object table.
  JobID job_id = JobID::nil();
  auto data = std::make_shared<ObjectTableDataT>();
  data->manager = client_id.binary();
  data->is_eviction = false;
  data->num_evictions = object_evictions_[object_id];
  data->object_size = object_info.data_size;
  ray::Status status =
      gcs_client_->object_table().Append(job_id, object_id, data, nullptr);
  return status;
}

ray::Status ObjectDirectory::ReportObjectRemoved(const ObjectID &object_id,
                                                 const ClientID &client_id) {
  // Append the eviction entry to the object table.
  JobID job_id = JobID::nil();
  auto data = std::make_shared<ObjectTableDataT>();
  data->manager = client_id.binary();
  data->is_eviction = true;
  data->num_evictions = object_evictions_[object_id];
  ray::Status status =
      gcs_client_->object_table().Append(job_id, object_id, data, nullptr);
  // Increment the number of times we've evicted this object. NOTE(swang): This
  // is only necessary because the Ray redis module expects unique entries in a
  // log. We track the number of evictions so that the next eviction, if there
  // is one, is unique.
  object_evictions_[object_id]++;
  return status;
};

ray::Status ObjectDirectory::GetInformation(const ClientID &client_id,
                                            const InfoSuccessCallback &success_callback,
                                            const InfoFailureCallback &fail_callback) {
  const ClientTableDataT &data = gcs_client_->client_table().GetClient(client_id);
  ClientID result_client_id = ClientID::from_binary(data.client_id);
  if (result_client_id == ClientID::nil() || !data.is_insertion) {
    fail_callback(ray::Status::RedisError("ClientID not found."));
  } else {
    const auto &info = RemoteConnectionInfo(client_id, data.node_manager_address,
                                            (uint16_t)data.object_manager_port);
    success_callback(info);
  }
  return ray::Status::OK();
}

ray::Status ObjectDirectory::SubscribeObjectLocations(const ObjectID &object_id,
                                                      const OnLocationsFound &callback) {
  if (listeners_.find(object_id) != listeners_.end()) {
    RAY_LOG(ERROR) << "Duplicate calls to SubscribeObjectLocations for " << object_id;
    return ray::Status::OK();
  }
  listeners_.emplace(object_id, LocationListenerState(callback));
  return gcs_client_->object_table().RequestNotifications(
      JobID::nil(), object_id, gcs_client_->client_table().GetLocalClientId());
}

ray::Status ObjectDirectory::UnsubscribeObjectLocations(const ObjectID &object_id) {
  auto entry = listeners_.find(object_id);
  if (entry == listeners_.end()) {
    return ray::Status::OK();
  }
  ray::Status status = gcs_client_->object_table().CancelNotifications(
      JobID::nil(), object_id, gcs_client_->client_table().GetLocalClientId());
  listeners_.erase(entry);
  return status;
}

}  // namespace ray
