#include "ray/object_manager/object_directory.h"

namespace ray {

ObjectDirectory::ObjectDirectory(boost::asio::io_service &io_service,
                                 std::shared_ptr<gcs::AsyncGcsClient> &gcs_client)
    : io_service_(io_service), gcs_client_(gcs_client) {}

namespace {

/// Process a suffix of the object table log and store the result in
/// client_ids. This assumes that client_ids already contains the result of the
/// object table log up to but not including this suffix. This also stores a
/// bool in has_been_created indicating whether the object has ever been
/// created before.
void UpdateObjectLocations(const std::vector<ObjectTableDataT> &location_history,
                           const ray::gcs::ClientTable &client_table,
                           std::unordered_set<ClientID> *client_ids,
                           bool *inline_object_flag,
                           std::vector<uint8_t> *inline_object_data,
                           bool *has_been_created) {
  // location_history contains the history of locations of the object (it is a log),
  // which might look like the following:
  //   client1.is_eviction = false
  //   client1.is_eviction = true
  //   client2.is_eviction = false
  // In such a scenario, we want to indicate client2 is the only client that contains
  // the object, which the following code achieves.
  if (!location_history.empty()) {
    // If there are entries, then the object has been created. Once this flag
    // is set to true, it should never go back to false.
    *has_been_created = true;
  }
  for (const auto &object_table_data : location_history) {
    ClientID client_id = ClientID::from_binary(object_table_data.manager);
    if (!object_table_data.is_eviction) {
      client_ids->insert(client_id);
    } else {
      client_ids->erase(client_id);
    }
    if (object_table_data.inline_object_flag) {
      // This is an inlined object. Read object's data from the object's GCS entry.
      *inline_object_flag = object_table_data.inline_object_flag;
      inline_object_data->assign(object_table_data.inline_object_data.begin(),
                                 object_table_data.inline_object_data.end());
      // XXX: Ion: after we filter out the updates, we should just return here.
    }
  }
  // Filter out the removed clients from the object locations.
  for (auto it = client_ids->begin(); it != client_ids->end();) {
    if (client_table.IsRemoved(*it)) {
      it = client_ids->erase(it);
    } else {
      it++;
    }
  }
}

}  // namespace

void ObjectDirectory::RegisterBackend() {
  auto object_notification_callback = [this](
      gcs::AsyncGcsClient *client, const ObjectID &object_id,
      const std::vector<ObjectTableDataT> &location_history) {
    // Objects are added to this map in SubscribeObjectLocations.
    auto it = listeners_.find(object_id);
    // Do nothing for objects we are not listening for.
    if (it == listeners_.end()) {
      return;
    }
    // Update entries for this object.
    UpdateObjectLocations(location_history, gcs_client_->client_table(),
                          &it->second.current_object_locations,
                          &it->second.inline_object_flag,
                          &it->second.inline_object_data,
                          &it->second.has_been_created);
    // Copy the callbacks so that the callbacks can unsubscribe without interrupting
    // looping over the callbacks.
    auto callbacks = it->second.callbacks;
    // Call all callbacks associated with the object id locations we have
    // received.  This notifies the client even if the list of locations is
    // empty, since this may indicate that the objects have been evicted from
    // all nodes.
    for (const auto &callback_pair : callbacks) {
      // It is safe to call the callback directly since this is already running
      // in the subscription callback stack.
      callback_pair.second(object_id, it->second.current_object_locations,
                           it->second.inline_object_flag, it->second.inline_object_data,
                           it->second.has_been_created);
    }
  };
  RAY_CHECK_OK(gcs_client_->object_table().Subscribe(
      UniqueID::nil(), gcs_client_->client_table().GetLocalClientId(),
      object_notification_callback, nullptr));
}

ray::Status ObjectDirectory::ReportObjectAdded(
    const ObjectID &object_id, const ClientID &client_id,
    const object_manager::protocol::ObjectInfoT &object_info,
    bool inline_object_flag, const std::vector<uint8_t> &inline_object_data) {
  // Append the addition entry to the object table.
  auto data = std::make_shared<ObjectTableDataT>();
  data->manager = client_id.binary();
  data->is_eviction = false;
  data->num_evictions = object_evictions_[object_id];
  data->object_size = object_info.data_size;
  data->inline_object_flag = inline_object_flag;
  if (inline_object_flag) {
    // inline onject. Add object's data to its GCS entry.
    data->inline_object_data.assign(inline_object_data.begin(), inline_object_data.end());
  }
  ray::Status status =
      gcs_client_->object_table().Append(JobID::nil(), object_id, data, nullptr);
  return status;
}

ray::Status ObjectDirectory::ReportObjectRemoved(const ObjectID &object_id,
                                                 const ClientID &client_id) {
  // Append the eviction entry to the object table.
  auto data = std::make_shared<ObjectTableDataT>();
  data->manager = client_id.binary();
  data->is_eviction = true;
  data->num_evictions = object_evictions_[object_id];
  ray::Status status =
      gcs_client_->object_table().Append(JobID::nil(), object_id, data, nullptr);
  // Increment the number of times we've evicted this object. NOTE(swang): This
  // is only necessary because the Ray redis module expects unique entries in a
  // log. We track the number of evictions so that the next eviction, if there
  // is one, is unique.
  object_evictions_[object_id]++;
  return status;
};

void ObjectDirectory::LookupRemoteConnectionInfo(
    RemoteConnectionInfo &connection_info) const {
  ClientTableDataT client_data;
  gcs_client_->client_table().GetClient(connection_info.client_id, client_data);
  ClientID result_client_id = ClientID::from_binary(client_data.client_id);
  if (!result_client_id.is_nil()) {
    RAY_CHECK(result_client_id == connection_info.client_id);
    if (client_data.is_insertion) {
      connection_info.ip = client_data.node_manager_address;
      connection_info.port = static_cast<uint16_t>(client_data.object_manager_port);
    }
  }
}

std::vector<RemoteConnectionInfo> ObjectDirectory::LookupAllRemoteConnections() const {
  std::vector<RemoteConnectionInfo> remote_connections;
  const auto &clients = gcs_client_->client_table().GetAllClients();
  for (const auto &client_pair : clients) {
    RemoteConnectionInfo info(client_pair.first);
    LookupRemoteConnectionInfo(info);
    if (info.Connected() &&
        info.client_id != gcs_client_->client_table().GetLocalClientId()) {
      remote_connections.push_back(info);
    }
  }
  return remote_connections;
}

void ObjectDirectory::HandleClientRemoved(const ClientID &client_id) {
  for (auto &listener : listeners_) {
    const ObjectID &object_id = listener.first;
    if (listener.second.current_object_locations.count(client_id) > 0) {
      // If the subscribed object has the removed client as a location, update
      // its locations with an empty log so that the location will be removed.
      UpdateObjectLocations({}, gcs_client_->client_table(),
                            &listener.second.current_object_locations,
                            &listener.second.inline_object_flag,
                            &listener.second.inline_object_data,
                            &listener.second.has_been_created);
      // Re-call all the subscribed callbacks for the object, since its
      // locations have changed.
      for (const auto &callback_pair : listener.second.callbacks) {
        // It is safe to call the callback directly since this is already running
        // in the subscription callback stack.
        callback_pair.second(object_id, listener.second.current_object_locations,
                             listener.second.inline_object_flag,
                             listener.second.inline_object_data,
                             listener.second.has_been_created);
      }
    }
  }
}

ray::Status ObjectDirectory::SubscribeObjectLocations(const UniqueID &callback_id,
                                                      const ObjectID &object_id,
                                                      const OnLocationsFound &callback) {
  ray::Status status = ray::Status::OK();
  auto it = listeners_.find(object_id);
  if (it == listeners_.end()) {
    it = listeners_.emplace(object_id, LocationListenerState()).first;
    status = gcs_client_->object_table().RequestNotifications(
        JobID::nil(), object_id, gcs_client_->client_table().GetLocalClientId());
  }
  auto &listener_state = it->second;
  // TODO(hme): Make this fatal after implementing Pull suppression.
  if (listener_state.callbacks.count(callback_id) > 0) {
    return ray::Status::OK();
  }
  listener_state.callbacks.emplace(callback_id, callback);
  // If we previously received some notifications about the object's locations,
  // immediately notify the caller of the current known locations.
  if (listener_state.has_been_created) {
    auto &locations = listener_state.current_object_locations;
    auto inline_object_flag = listener_state.inline_object_flag;
    auto &inline_object_data = listener_state.inline_object_data;
    io_service_.post([callback, locations, inline_object_flag, inline_object_data, object_id]() {
      callback(object_id, locations, inline_object_flag,
               inline_object_data, /*has_been_created=*/true);
    });
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
  ray::Status status;
  auto it = listeners_.find(object_id);
  if (it == listeners_.end()) {
    status = gcs_client_->object_table().Lookup(
        JobID::nil(), object_id,
        [this, callback](gcs::AsyncGcsClient *client, const ObjectID &object_id,
                         const std::vector<ObjectTableDataT> &location_history) {
          // Build the set of current locations based on the entries in the log.
          std::unordered_set<ClientID> client_ids;
          bool inline_object_flag = false;
          std::vector<uint8_t> inline_object_data;
          bool has_been_created = false;
          UpdateObjectLocations(location_history, gcs_client_->client_table(),
                                &client_ids, &inline_object_flag, &inline_object_data,
                                &has_been_created);
          // It is safe to call the callback directly since this is already running
          // in the GCS client's lookup callback stack.
          callback(object_id, client_ids, inline_object_flag,
                   inline_object_data, has_been_created);
        });
  } else {
    // If we have locations cached due to a concurrent SubscribeObjectLocations
    // call, call the callback immediately with the cached locations.
    auto &locations = it->second.current_object_locations;
    bool has_been_created = it->second.has_been_created;
    bool inline_object_flag = it->second.inline_object_flag;
    std::vector<uint8_t> inline_object_data = it->second.inline_object_data;
    io_service_.post([callback, object_id, locations, inline_object_flag,
                      inline_object_data, has_been_created]() {
      callback(object_id, locations, inline_object_flag,
               inline_object_data, has_been_created);
    });
  }
  return status;
}

ray::ClientID ObjectDirectory::GetLocalClientID() {
  return gcs_client_->client_table().GetLocalClientId();
}

std::string ObjectDirectory::DebugString() const {
  std::stringstream result;
  result << "ObjectDirectory:";
  result << "\n- num listeners: " << listeners_.size();
  result << "\n- num eviction entries: " << object_evictions_.size();
  return result.str();
}

}  // namespace ray
