#include "ray/object_manager/object_directory.h"

namespace ray {

ObjectDirectory::ObjectDirectory(std::shared_ptr<gcs::AsyncGcsClient> &gcs_client) {
  gcs_client_ = gcs_client;
}

void ObjectDirectory::RegisterBackend() {
  auto object_notification_callback = [this](gcs::AsyncGcsClient *client,
                                             const ObjectID &object_id,
                                             const std::vector<ObjectTableDataT> data) {
    // Objects are added to this map in SubscribeObjectLocations.
    auto entry = listeners_.find(object_id);
    // Do nothing for objects we are not listening for.
    if (entry == listeners_.end()) {
      return;
    }
    // Obtain reported client ids.
    std::vector<ClientID> client_ids;
    for (auto item : data) {
      if (!item.is_eviction) {
        ClientID client_id = ClientID::from_binary(item.manager);
        client_ids.push_back(client_id);
      }
    }
    entry->second.locations_found_callback(client_ids, object_id);
  };
  gcs_client_->object_table().Subscribe(UniqueID::nil(),
                                        gcs_client_->client_table().GetLocalClientId(),
                                        object_notification_callback, nullptr);
}

ray::Status ObjectDirectory::ReportObjectAdded(const ObjectID &object_id,
                                               const ClientID &client_id,
                                               const ObjectInfoT &object_info) {
  // TODO(hme): Determine whether we need to do lookup to append.
  JobID job_id = JobID::from_random();
  auto data = std::make_shared<ObjectTableDataT>();
  data->manager = client_id.binary();
  data->is_eviction = false;
  data->object_size = object_info.data_size;
  ray::Status status = gcs_client_->object_table().Append(
      job_id, object_id, data,
      [](gcs::AsyncGcsClient *client, const UniqueID &id, const ObjectTableDataT &data) {
        // Do nothing.
      });
  return status;
}

ray::Status ObjectDirectory::ReportObjectRemoved(const ObjectID &object_id,
                                                 const ClientID &client_id) {
  // TODO(hme): Need corresponding remove method in GCS.
  return ray::Status::NotImplemented("ObjectTable.Remove is not implemented");
}

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
  ray::Status status = ray::Status::OK();
  if (listeners_.find(object_id) != listeners_.end()) {
    RAY_LOG(ERROR) << "Duplicate calls to SubscribeObjectLocations for " << object_id;
    return ray::Status::Invalid("Unable to subscribe to the same object twice.");
  }
  listeners_.emplace(object_id, LocationListenerState(callback));
  GetLocations(
      object_id,
      [this, callback](const std::vector<ray::ClientID> &v,
                       const ray::ObjectID &object_id) {
        if (listeners_.count(object_id) > 0) {
          // Make sure we're still interested in this object's locations.
          callback(v, object_id);
        }
      },
      [this](const ray::ObjectID &object_id) {
        auto entry = listeners_.find(object_id);
        if (entry == listeners_.end()) {
          return;
        }
        entry->second.listening = true;
        RAY_CHECK_OK(gcs_client_->object_table().RequestNotifications(
            JobID::nil(), object_id, gcs_client_->client_table().GetLocalClientId()));
      });
  return status;
}

ray::Status ObjectDirectory::UnsubscribeObjectLocations(const ObjectID &object_id) {
  auto entry = listeners_.find(object_id);
  if (entry == listeners_.end()) {
    return ray::Status::OK();
  }
  if (entry->second.listening) {
    RAY_CHECK_OK(gcs_client_->object_table().CancelNotifications(
        JobID::nil(), object_id, gcs_client_->client_table().GetLocalClientId()));
  }
  listeners_.erase(entry);
  return ray::Status::OK();
}

ray::Status ObjectDirectory::GetLocations(const ObjectID &object_id,
                                          const OnLocationsFound &success_callback,
                                          const OnLocationsFailure &fail_callback) {
  JobID job_id = JobID::nil();
  ray::Status status = gcs_client_->object_table().Lookup(
      job_id, object_id, [this, success_callback, fail_callback](
                             gcs::AsyncGcsClient *client, const ObjectID &object_id,
                             const std::vector<ObjectTableDataT> &location_entries) {
        // Build the set of current locations based on the entries in the log.
        std::unordered_set<ClientID> locations;
        for (auto entry : location_entries) {
          ClientID client_id = ClientID::from_binary(entry.manager);
          if (!entry.is_eviction) {
            locations.insert(client_id);
          } else {
            locations.erase(client_id);
          }
        }
        // Invoke the callback.
        std::vector<ClientID> locations_vector(locations.begin(), locations.end());
        if (locations_vector.empty()) {
          fail_callback(object_id);
        } else {
          success_callback(locations_vector, object_id);
        }
      });
  return status;
}

}  // namespace ray
