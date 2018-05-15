#include "ray/object_manager/object_directory.h"

namespace ray {

ObjectDirectory::ObjectDirectory(std::shared_ptr<gcs::AsyncGcsClient> gcs_client) {
  gcs_client_ = gcs_client;
};

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
};

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
};

ray::Status ObjectDirectory::GetLocations(const ObjectID &object_id,
                                          const OnLocationsSuccess &success_callback,
                                          const OnLocationsFailure &fail_callback) {
  ray::Status status_code = ray::Status::OK();
  if (existing_requests_.count(object_id) == 0) {
    existing_requests_[object_id] = ODCallbacks({success_callback, fail_callback});
    status_code = ExecuteGetLocations(object_id);
  } else {
    // Do nothing. A request is in progress.
  }
  return status_code;
};

ray::Status ObjectDirectory::ExecuteGetLocations(const ObjectID &object_id) {
  JobID job_id = JobID::nil();
  // Note: Lookup must be synchronous for thread-safe access.
  // For now, this is only accessed by the main thread.
  ray::Status status = gcs_client_->object_table().Lookup(
      job_id, object_id, [this](gcs::AsyncGcsClient *client, const ObjectID &object_id,
                                const std::vector<ObjectTableDataT> &data) {
        GetLocationsComplete(object_id, data);
      });
  return status;
};

void ObjectDirectory::GetLocationsComplete(
    const ObjectID &object_id, const std::vector<ObjectTableDataT> &location_entries) {
  auto request = existing_requests_.find(object_id);
  // Do not invoke a callback if the request was cancelled.
  if (request == existing_requests_.end()) {
    return;
  }
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
    request->second.fail_cb(object_id);
  } else {
    request->second.success_cb(locations_vector, object_id);
  }
  existing_requests_.erase(request);
}

ray::Status ObjectDirectory::Cancel(const ObjectID &object_id) {
  existing_requests_.erase(object_id);
  return ray::Status::OK();
};

}  // namespace ray
