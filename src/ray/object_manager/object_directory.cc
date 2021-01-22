// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/object_manager/object_directory.h"

namespace ray {

ObjectDirectory::ObjectDirectory(boost::asio::io_service &io_service,
                                 std::shared_ptr<gcs::GcsClient> &gcs_client)
    : io_service_(io_service), gcs_client_(gcs_client) {}

namespace {

using ray::rpc::GcsChangeMode;
using ray::rpc::GcsNodeInfo;
using ray::rpc::ObjectTableData;

/// Process a notification of the object table entries and store the result in
/// node_ids. This assumes that node_ids already contains the result of the
/// object table entries up to but not including this notification.
bool UpdateObjectLocations(const std::vector<rpc::ObjectLocationChange> &location_updates,
                           std::shared_ptr<gcs::GcsClient> gcs_client,
                           std::unordered_set<NodeID> *node_ids, std::string *spilled_url,
                           size_t *object_size) {
  // location_updates contains the updates of locations of the object.
  // with GcsChangeMode, we can determine whether the update mode is
  // addition or deletion.
  bool isUpdated = false;
  for (const auto &update : location_updates) {
    // The size can be 0 if the update was a deletion. This assumes that an
    // object's size is always greater than 0.
    // TODO(swang): If that's not the case, we should use a flag to check
    // whether the size is set instead.
    if (update.size() > 0) {
      *object_size = update.size();
    }

    if (!update.node_id().empty()) {
      NodeID node_id = NodeID::FromBinary(update.node_id());
      if (update.is_add() && 0 == node_ids->count(node_id)) {
        node_ids->insert(node_id);
        isUpdated = true;
      } else if (!update.is_add() && 1 == node_ids->count(node_id)) {
        node_ids->erase(node_id);
        isUpdated = true;
      }
    } else {
      RAY_CHECK(!update.spilled_url().empty());
      RAY_LOG(DEBUG) << "Received object spilled at " << update.spilled_url();
      if (update.spilled_url() != *spilled_url) {
        *spilled_url = update.spilled_url();
        isUpdated = true;
      }
    }
  }
  // Filter out the removed nodes from the object locations.
  for (auto it = node_ids->begin(); it != node_ids->end();) {
    if (gcs_client->Nodes().IsRemoved(*it)) {
      it = node_ids->erase(it);
    } else {
      it++;
    }
  }

  return isUpdated;
}

}  // namespace

ray::Status ObjectDirectory::ReportObjectAdded(
    const ObjectID &object_id, const NodeID &node_id,
    const object_manager::protocol::ObjectInfoT &object_info) {
  size_t size = object_info.data_size + object_info.metadata_size;
  RAY_LOG(DEBUG) << "Reporting object added to GCS " << object_id << " size " << size;
  ray::Status status =
      gcs_client_->Objects().AsyncAddLocation(object_id, node_id, size, nullptr);
  return status;
}

ray::Status ObjectDirectory::ReportObjectRemoved(
    const ObjectID &object_id, const NodeID &node_id,
    const object_manager::protocol::ObjectInfoT &object_info) {
  RAY_LOG(DEBUG) << "Reporting object removed to GCS " << object_id;
  ray::Status status =
      gcs_client_->Objects().AsyncRemoveLocation(object_id, node_id, nullptr);
  return status;
};

void ObjectDirectory::LookupRemoteConnectionInfo(
    RemoteConnectionInfo &connection_info) const {
  auto node_info = gcs_client_->Nodes().Get(connection_info.node_id);
  if (node_info) {
    NodeID result_node_id = NodeID::FromBinary(node_info->node_id());
    RAY_CHECK(result_node_id == connection_info.node_id);
    connection_info.ip = node_info->node_manager_address();
    connection_info.port = static_cast<uint16_t>(node_info->object_manager_port());
  }
}

std::vector<RemoteConnectionInfo> ObjectDirectory::LookupAllRemoteConnections() const {
  std::vector<RemoteConnectionInfo> remote_connections;
  const auto &node_map = gcs_client_->Nodes().GetAll();
  for (const auto &item : node_map) {
    RemoteConnectionInfo info(item.first);
    LookupRemoteConnectionInfo(info);
    if (info.Connected() && info.node_id != gcs_client_->Nodes().GetSelfId()) {
      remote_connections.push_back(info);
    }
  }
  return remote_connections;
}

void ObjectDirectory::HandleNodeRemoved(const NodeID &node_id) {
  for (auto &listener : listeners_) {
    const ObjectID &object_id = listener.first;
    if (listener.second.current_object_locations.count(node_id) > 0) {
      // If the subscribed object has the removed node as a location, update
      // its locations with an empty update so that the location will be removed.
      UpdateObjectLocations({}, gcs_client_, &listener.second.current_object_locations,
                            &listener.second.spilled_url, &listener.second.object_size);
      // Re-call all the subscribed callbacks for the object, since its
      // locations have changed.
      for (const auto &callback_pair : listener.second.callbacks) {
        // It is safe to call the callback directly since this is already running
        // in the subscription callback stack.
        callback_pair.second(object_id, listener.second.current_object_locations,
                             listener.second.spilled_url, listener.second.object_size);
      }
    }
  }
}

ray::Status ObjectDirectory::SubscribeObjectLocations(const UniqueID &callback_id,
                                                      const ObjectID &object_id,
                                                      const rpc::Address &owner_address,
                                                      const OnLocationsFound &callback) {
  ray::Status status = ray::Status::OK();
  auto it = listeners_.find(object_id);
  if (it == listeners_.end()) {
    it = listeners_.emplace(object_id, LocationListenerState()).first;

    auto object_notification_callback =
        [this](const ObjectID &object_id,
               const std::vector<rpc::ObjectLocationChange> &object_notifications) {
          // Objects are added to this map in SubscribeObjectLocations.
          auto it = listeners_.find(object_id);
          // Do nothing for objects we are not listening for.
          if (it == listeners_.end()) {
            return;
          }

          // Once this flag is set to true, it should never go back to false.
          it->second.subscribed = true;

          // Update entries for this object.
          if (!UpdateObjectLocations(object_notifications, gcs_client_,
                                     &it->second.current_object_locations,
                                     &it->second.spilled_url, &it->second.object_size)) {
            return;
          }
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
                                 it->second.spilled_url, it->second.object_size);
          }
        };
    status = gcs_client_->Objects().AsyncSubscribeToLocations(
        object_id, object_notification_callback, /*done*/ nullptr);
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
    auto &spilled_url = listener_state.spilled_url;
    auto object_size = it->second.object_size;
    io_service_.post([callback, locations, spilled_url, object_size, object_id]() {
      callback(object_id, locations, spilled_url, object_size);
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
    status = gcs_client_->Objects().AsyncUnsubscribeToLocations(object_id);
    listeners_.erase(entry);
  }
  return status;
}

ray::Status ObjectDirectory::LookupLocations(const ObjectID &object_id,
                                             const rpc::Address &owner_address,
                                             const OnLocationsFound &callback) {
  ray::Status status;
  auto it = listeners_.find(object_id);
  if (it != listeners_.end() && it->second.subscribed) {
    // If we have locations cached due to a concurrent SubscribeObjectLocations
    // call, and we have received at least one notification from the GCS about
    // the object's creation, then call the callback immediately with the
    // cached locations.
    auto &locations = it->second.current_object_locations;
    auto &spilled_url = it->second.spilled_url;
    auto object_size = it->second.object_size;
    io_service_.post([callback, object_id, spilled_url, locations, object_size]() {
      callback(object_id, locations, spilled_url, object_size);
    });
  } else {
    // We do not have any locations cached due to a concurrent
    // SubscribeObjectLocations call, so look up the object's locations
    // directly from the GCS.
    status = gcs_client_->Objects().AsyncGetLocations(
        object_id,
        [this, object_id, callback](
            Status status, const boost::optional<rpc::ObjectLocationInfo> &update) {
          RAY_CHECK(status.ok())
              << "Failed to get object location from GCS: " << status.message();
          // Build the set of current locations based on the entries in the log.
          std::vector<rpc::ObjectLocationChange> notification;
          for (const auto &loc : update->locations()) {
            rpc::ObjectLocationChange change;
            change.set_is_add(true);
            change.set_node_id(loc.manager());
            notification.push_back(change);
          }
          if (!update->spilled_url().empty()) {
            rpc::ObjectLocationChange change;
            change.set_spilled_url(update->spilled_url());
            notification.push_back(change);
          }

          std::unordered_set<NodeID> node_ids;
          std::string spilled_url;
          size_t object_size = 0;
          UpdateObjectLocations(notification, gcs_client_, &node_ids, &spilled_url,
                                &object_size);
          // It is safe to call the callback directly since this is already running
          // in the GCS client's lookup callback stack.
          callback(object_id, node_ids, spilled_url, object_size);
        });
  }
  return status;
}

std::string ObjectDirectory::DebugString() const {
  std::stringstream result;
  result << "ObjectDirectory:";
  result << "\n- num listeners: " << listeners_.size();
  return result.str();
}

}  // namespace ray
