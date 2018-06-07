#include "ray/object_manager/object_directory.h"

namespace ray {

ObjectDirectory::ObjectDirectory(std::shared_ptr<gcs::AsyncGcsClient> &gcs_client) {
  gcs_client_ = gcs_client;
}

std::vector<ClientID> UpdateObjectLocations(
    std::unordered_set<ClientID> &client_ids,
    const std::vector<ObjectTableDataT> &location_history) {
  // location_history contains the history of locations of the object (it is a log),
  // which might look like the following:
  //   client1.is_eviction = false
  //   client1.is_eviction = true
  //   client2.is_eviction = false
  // In such a scenario, we want to indicate client2 is the only client that contains
  // the object, which the following code achieves.
  for (const auto &object_table_data : location_history) {
    ClientID client_id = ClientID::from_binary(object_table_data.manager);
    if (!object_table_data.is_eviction) {
      client_ids.insert(client_id);
    } else {
      client_ids.erase(client_id);
    }
  }
  return std::vector<ClientID>(client_ids.begin(), client_ids.end());
}

void ObjectDirectory::RegisterBackend() {
  auto object_notification_callback = [this](
      gcs::AsyncGcsClient *client, const ObjectID &object_id,
      const std::vector<ObjectTableDataT> &location_history) {
    // Objects are added to this map in SubscribeObjectLocations.
    auto object_id_listener_pair = listeners_.find(object_id);
    // Do nothing for objects we are not listening for.
    if (object_id_listener_pair == listeners_.end()) {
      return;
    }
    // Update entries for this object.
    std::vector<ClientID> client_id_vec = UpdateObjectLocations(
        object_id_listener_pair->second.current_object_locations, location_history);
    if (!client_id_vec.empty()) {
      // Copy the callbacks so that the callbacks can unsubscribe without interrupting
      // looping over the callbacks.
      auto callbacks = object_id_listener_pair->second.callbacks;
      // Call all callbacks associated with the object id locations we have received.
      for (const auto &callback_pair : callbacks) {
        callback_pair.second(client_id_vec, object_id);
      }
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

ray::Status ObjectDirectory::SubscribeObjectLocations(const UniqueID &callback_id,
                                                      const ObjectID &object_id,
                                                      const OnLocationsFound &callback) {
  ray::Status status = ray::Status::OK();
  if (listeners_.find(object_id) == listeners_.end()) {
    listeners_.emplace(object_id, LocationListenerState());
    status = gcs_client_->object_table().RequestNotifications(
        JobID::nil(), object_id, gcs_client_->client_table().GetLocalClientId());
  }
  auto &listener_state = listeners_.find(object_id)->second;
  // TODO(hme): Make this fatal after implementing Pull suppression.
  if (listener_state.callbacks.count(callback_id) > 0) {
    return ray::Status::OK();
  }
  listener_state.callbacks.emplace(callback_id, callback);
  // Immediately notify of found object locations.
  if (!listener_state.current_object_locations.empty()) {
    std::vector<ClientID> client_id_vec(listener_state.current_object_locations.begin(),
                                        listener_state.current_object_locations.end());
    callback(client_id_vec, object_id);
  }
  return status;
}

ray::Status ObjectDirectory::UnsubscribeObjectLocations(const UniqueID &callback_id,
                                                        const ObjectID &object_id) {
  ray::Status status = ray::Status::OK();
  auto entry = listeners_.find(object_id);
  if (entry == listeners_.end()) {
    return status;
  }
  entry->second.callbacks.erase(callback_id);
  if (entry->second.callbacks.empty()) {
    status = gcs_client_->object_table().CancelNotifications(
        JobID::nil(), object_id, gcs_client_->client_table().GetLocalClientId());
    listeners_.erase(entry);
  }
  return status;
}

ray::Status ObjectDirectory::LookupLocations(const ObjectID &object_id,
                                             const OnLocationsFound &callback) {
  JobID job_id = JobID::nil();
  ray::Status status = gcs_client_->object_table().Lookup(
      job_id, object_id,
      [callback](gcs::AsyncGcsClient *client, const ObjectID &object_id,
                 const std::vector<ObjectTableDataT> &location_history) {
        // Build the set of current locations based on the entries in the log.
        std::unordered_set<ClientID> client_ids;
        std::vector<ClientID> locations_vector =
            UpdateObjectLocations(client_ids, location_history);
        callback(locations_vector, object_id);
      });
  return status;
}

}  // namespace ray
