#include "ray/object_manager/object_directory.h"

namespace ray {

ObjectDirectory::ObjectDirectory(boost::asio::io_service &io_service,
                                 std::shared_ptr<gcs::RedisGcsClient> &gcs_client)
    : io_service_(io_service), gcs_client_(gcs_client) {}

namespace {

using ray::rpc::GcsChangeMode;
using ray::rpc::GcsNodeInfo;
using ray::rpc::ObjectTableData;

/// Process a notification of the object table entries and store the result in
/// client_ids. This assumes that client_ids already contains the result of the
/// object table entries up to but not including this notification.
void UpdateObjectLocations(const GcsChangeMode change_mode,
                           const std::vector<ObjectTableData> &location_updates,
                           const ray::gcs::ClientTable &client_table,
                           std::unordered_set<ClientID> *client_ids) {
  // location_updates contains the updates of locations of the object.
  // with GcsChangeMode, we can determine whether the update mode is
  // addition or deletion.
  for (const auto &object_table_data : location_updates) {
    ClientID client_id = ClientID::FromBinary(object_table_data.manager());
    if (change_mode != GcsChangeMode::REMOVE) {
      client_ids->insert(client_id);
    } else {
      client_ids->erase(client_id);
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
  auto object_notification_callback =
      [this](gcs::RedisGcsClient *client, const ObjectID &object_id,
             const GcsChangeMode change_mode,
             const std::vector<ObjectTableData> &location_updates) {
        // Objects are added to this map in SubscribeObjectLocations.
        auto it = listeners_.find(object_id);
        // Do nothing for objects we are not listening for.
        if (it == listeners_.end()) {
          return;
        }

        // Once this flag is set to true, it should never go back to false.
        it->second.subscribed = true;

        // Update entries for this object.
        UpdateObjectLocations(change_mode, location_updates, gcs_client_->client_table(),
                              &it->second.current_object_locations);
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
          callback_pair.second(object_id, it->second.current_object_locations);
        }
      };
  RAY_CHECK_OK(gcs_client_->object_table().Subscribe(
      JobID::Nil(), gcs_client_->client_table().GetLocalClientId(),
      object_notification_callback, nullptr));
}

ray::Status ObjectDirectory::ReportObjectAdded(
    const ObjectID &object_id, const ClientID &client_id,
    const object_manager::protocol::ObjectInfoT &object_info) {
  RAY_LOG(DEBUG) << "Reporting object added to GCS " << object_id;
  // Append the addition entry to the object table.
  auto data = std::make_shared<ObjectTableData>();
  data->set_manager(client_id.Binary());
  data->set_object_size(object_info.data_size);
  ray::Status status =
      gcs_client_->object_table().Add(JobID::Nil(), object_id, data, nullptr);
  return status;
}

ray::Status ObjectDirectory::ReportObjectRemoved(
    const ObjectID &object_id, const ClientID &client_id,
    const object_manager::protocol::ObjectInfoT &object_info) {
  RAY_LOG(DEBUG) << "Reporting object removed to GCS " << object_id;
  // Append the eviction entry to the object table.
  auto data = std::make_shared<ObjectTableData>();
  data->set_manager(client_id.Binary());
  data->set_object_size(object_info.data_size);
  ray::Status status =
      gcs_client_->object_table().Remove(JobID::Nil(), object_id, data, nullptr);
  return status;
};

void ObjectDirectory::LookupRemoteConnectionInfo(
    RemoteConnectionInfo &connection_info) const {
  GcsNodeInfo node_info;
  gcs_client_->client_table().GetClient(connection_info.client_id, node_info);
  ClientID result_client_id = ClientID::FromBinary(node_info.node_id());
  if (!result_client_id.IsNil()) {
    RAY_CHECK(result_client_id == connection_info.client_id);
    if (node_info.state() == GcsNodeInfo::ALIVE) {
      connection_info.ip = node_info.node_manager_address();
      connection_info.port = static_cast<uint16_t>(node_info.object_manager_port());
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
      // its locations with an empty update so that the location will be removed.
      UpdateObjectLocations(GcsChangeMode::APPEND_OR_ADD, {}, gcs_client_->client_table(),
                            &listener.second.current_object_locations);
      // Re-call all the subscribed callbacks for the object, since its
      // locations have changed.
      for (const auto &callback_pair : listener.second.callbacks) {
        // It is safe to call the callback directly since this is already running
        // in the subscription callback stack.
        callback_pair.second(object_id, listener.second.current_object_locations);
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
        JobID::Nil(), object_id, gcs_client_->client_table().GetLocalClientId());
  }
  auto &listener_state = it->second;
  // TODO(hme): Make this fatal after implementing Pull suppression.
  if (listener_state.callbacks.count(callback_id) > 0) {
    return ray::Status::OK();
  }
  listener_state.callbacks.emplace(callback_id, callback);
  // If we previously received some notifications about the object's locations,
  // immediately notify the caller of the current known locations.
  if (listener_state.subscribed) {
    auto &locations = listener_state.current_object_locations;
    io_service_.post(
        [callback, locations, object_id]() { callback(object_id, locations); });
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
        JobID::Nil(), object_id, gcs_client_->client_table().GetLocalClientId());
    listeners_.erase(entry);
  }
  return status;
}

ray::Status ObjectDirectory::LookupLocations(const ObjectID &object_id,
                                             const OnLocationsFound &callback) {
  ray::Status status;
  auto it = listeners_.find(object_id);
  if (it != listeners_.end() && it->second.subscribed) {
    // If we have locations cached due to a concurrent SubscribeObjectLocations
    // call, and we have received at least one notification from the GCS about
    // the object's creation, then call the callback immediately with the
    // cached locations.
    auto &locations = it->second.current_object_locations;
    io_service_.post(
        [callback, object_id, locations]() { callback(object_id, locations); });
  } else {
    // We do not have any locations cached due to a concurrent
    // SubscribeObjectLocations call, so look up the object's locations
    // directly from the GCS.
    status = gcs_client_->object_table().Lookup(
        JobID::Nil(), object_id,
        [this, callback](gcs::RedisGcsClient *client, const ObjectID &object_id,
                         const std::vector<ObjectTableData> &location_updates) {
          // Build the set of current locations based on the entries in the log.
          std::unordered_set<ClientID> client_ids;
          UpdateObjectLocations(GcsChangeMode::APPEND_OR_ADD, location_updates,
                                gcs_client_->client_table(), &client_ids);
          // It is safe to call the callback directly since this is already running
          // in the GCS client's lookup callback stack.
          callback(object_id, client_ids);
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
  return result.str();
}

}  // namespace ray
