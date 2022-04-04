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

#include "ray/object_manager/ownership_based_object_directory.h"

#include "ray/stats/metric_defs.h"

namespace ray {

OwnershipBasedObjectDirectory::OwnershipBasedObjectDirectory(
    instrumented_io_context &io_service,
    std::shared_ptr<gcs::GcsClient> &gcs_client,
    pubsub::SubscriberInterface *object_location_subscriber,
    rpc::CoreWorkerClientPool *owner_client_pool,
    int64_t max_object_report_batch_size,
    std::function<void(const ObjectID &, const rpc::ErrorType &)> mark_as_failed)
    : io_service_(io_service),
      gcs_client_(gcs_client),
      client_call_manager_(io_service),
      object_location_subscriber_(object_location_subscriber),
      owner_client_pool_(owner_client_pool),
      kMaxObjectReportBatchSize(max_object_report_batch_size),
      mark_as_failed_(mark_as_failed) {}

namespace {

/// Filter out the removed nodes from the object locations.
void FilterRemovedNodes(std::shared_ptr<gcs::GcsClient> gcs_client,
                        std::unordered_set<NodeID> *node_ids) {
  for (auto it = node_ids->begin(); it != node_ids->end();) {
    if (gcs_client->Nodes().IsRemoved(*it)) {
      it = node_ids->erase(it);
    } else {
      it++;
    }
  }
}

/// Update object location data based on response from the owning core worker.
bool UpdateObjectLocations(const rpc::WorkerObjectLocationsPubMessage &location_info,
                           std::shared_ptr<gcs::GcsClient> gcs_client,
                           std::unordered_set<NodeID> *node_ids,
                           std::string *spilled_url,
                           NodeID *spilled_node_id,
                           bool *pending_creation,
                           size_t *object_size) {
  bool is_updated = false;
  std::unordered_set<NodeID> new_node_ids;
  // The size can be 0 if the update was a deletion. This assumes that an
  // object's size is always greater than 0.
  // TODO(swang): If that's not the case, we should use a flag to check
  // whether the size is set instead.
  if (location_info.object_size() > 0 && location_info.object_size() != *object_size) {
    *object_size = location_info.object_size();
    is_updated = true;
  }
  for (auto const &node_id : location_info.node_ids()) {
    new_node_ids.emplace(NodeID::FromBinary(node_id));
  }
  // Filter out the removed nodes from the object locations.
  FilterRemovedNodes(gcs_client, &new_node_ids);
  if (new_node_ids != *node_ids) {
    *node_ids = new_node_ids;
    is_updated = true;
  }
  const std::string &new_spilled_url = location_info.spilled_url();
  if (new_spilled_url != *spilled_url) {
    const auto new_spilled_node_id = NodeID::FromBinary(location_info.spilled_node_id());
    RAY_LOG(DEBUG) << "Received object spilled to " << new_spilled_url << " spilled on "
                   << new_spilled_node_id;
    if (gcs_client->Nodes().IsRemoved(new_spilled_node_id)) {
      *spilled_url = "";
      *spilled_node_id = NodeID::Nil();
    } else {
      *spilled_url = new_spilled_url;
      *spilled_node_id = new_spilled_node_id;
    }
    is_updated = true;
  }
  if (location_info.pending_creation() != *pending_creation) {
    *pending_creation = location_info.pending_creation();
    is_updated = true;
  }

  return is_updated;
}

rpc::Address GetOwnerAddressFromObjectInfo(const ObjectInfo &object_info) {
  rpc::Address owner_address;
  owner_address.set_raylet_id(object_info.owner_raylet_id.Binary());
  owner_address.set_ip_address(object_info.owner_ip_address);
  owner_address.set_port(object_info.owner_port);
  owner_address.set_worker_id(object_info.owner_worker_id.Binary());
  return owner_address;
}

}  // namespace

std::shared_ptr<rpc::CoreWorkerClientInterface> OwnershipBasedObjectDirectory::GetClient(
    const rpc::Address &owner_address) {
  if (WorkerID::FromBinary(owner_address.worker_id()).IsNil()) {
    // If an object does not have owner, return nullptr.
    return nullptr;
  }
  return owner_client_pool_->GetOrConnect(owner_address);
}

void OwnershipBasedObjectDirectory::ReportObjectAdded(const ObjectID &object_id,
                                                      const NodeID &node_id,
                                                      const ObjectInfo &object_info) {
  const WorkerID &worker_id = object_info.owner_worker_id;
  const auto owner_address = GetOwnerAddressFromObjectInfo(object_info);
  auto owner_client = GetClient(owner_address);
  if (owner_client == nullptr) {
    RAY_LOG(DEBUG) << "Object " << object_id << " does not have owner. "
                   << "ReportObjectAdded becomes a no-op."
                   << "This should only happen for Plasma store warmup objects.";
    return;
  }
  metrics_num_object_locations_added_++;
  location_buffers_[worker_id][object_id] = rpc::ObjectLocationState::ADDED;
  SendObjectLocationUpdateBatchIfNeeded(worker_id, node_id, owner_address);
}

void OwnershipBasedObjectDirectory::ReportObjectRemoved(const ObjectID &object_id,
                                                        const NodeID &node_id,
                                                        const ObjectInfo &object_info) {
  const WorkerID &worker_id = object_info.owner_worker_id;
  const auto owner_address = GetOwnerAddressFromObjectInfo(object_info);
  auto owner_client = GetClient(owner_address);
  if (owner_client == nullptr) {
    RAY_LOG(DEBUG) << "Object " << object_id << " does not have owner. "
                   << "ReportObjectRemoved becomes a no-op. "
                   << "This should only happen for Plasma store warmup objects.";
    return;
  }
  metrics_num_object_locations_removed_++;
  location_buffers_[worker_id][object_id] = rpc::ObjectLocationState::REMOVED;
  SendObjectLocationUpdateBatchIfNeeded(worker_id, node_id, owner_address);
};

void OwnershipBasedObjectDirectory::SendObjectLocationUpdateBatchIfNeeded(
    const WorkerID &worker_id, const NodeID &node_id, const rpc::Address &owner_address) {
  if (in_flight_requests_.contains(worker_id)) {
    // If there's an in-flight request, the buffer will be sent once the request is
    // replied from the owner.
    return;
  }

  // Do nothing if there's no update to this owner.
  auto location_buffer_it = location_buffers_.find(worker_id);
  if (location_buffer_it == location_buffers_.end()) {
    return;
  }

  const auto &object_state_buffers = location_buffer_it->second;
  RAY_CHECK(object_state_buffers.size() != 0);

  rpc::UpdateObjectLocationBatchRequest request;
  request.set_intended_worker_id(worker_id.Binary());
  request.set_node_id(node_id.Binary());
  auto object_state_buffers_it = object_state_buffers.begin();
  auto batch_size = 0;
  while (object_state_buffers_it != object_state_buffers.end() &&
         batch_size < kMaxObjectReportBatchSize) {
    const auto &object_id = object_state_buffers_it->first;
    const auto &object_state = object_state_buffers_it->second;

    auto state = request.add_object_location_states();
    state->set_object_id(object_id.Binary());
    state->set_state(object_state);
    batch_size++;
    object_state_buffers_it++;
  }
  location_buffer_it->second.erase(object_state_buffers.begin(), object_state_buffers_it);

  if (object_state_buffers.size() == 0) {
    location_buffers_.erase(location_buffer_it);
  }

  in_flight_requests_.emplace(worker_id);
  auto owner_client = GetClient(owner_address);
  owner_client->UpdateObjectLocationBatch(
      request,
      [this, worker_id, node_id, owner_address](
          Status status, const rpc::UpdateObjectLocationBatchReply &reply) {
        auto in_flight_request_it = in_flight_requests_.find(worker_id);
        RAY_CHECK(in_flight_request_it != in_flight_requests_.end());
        in_flight_requests_.erase(in_flight_request_it);

        // TODO(sang): Handle network failures.
        if (!status.ok()) {
          // Currently we consider the owner is dead if the network is failed.
          // Clean up the metadata. No need to mark objects as failed because
          // that's only needed for the object pulling path (and this RPC is not on
          // pulling path).
          RAY_LOG(DEBUG) << "Owner " << worker_id << " failed to update locations for "
                         << node_id << ". The owner is most likely dead. Status: "
                         << status.ToString();
          auto it = location_buffers_.find(worker_id);
          if (it != location_buffers_.end()) {
            location_buffers_.erase(it);
          }
          owner_client_pool_->Disconnect(worker_id);
          return;
        }

        SendObjectLocationUpdateBatchIfNeeded(worker_id, node_id, owner_address);
      });
}

void OwnershipBasedObjectDirectory::ObjectLocationSubscriptionCallback(
    const rpc::WorkerObjectLocationsPubMessage &location_info,
    const ObjectID &object_id,
    bool location_lookup_failed) {
  // Objects are added to this map in SubscribeObjectLocations.
  auto it = listeners_.find(object_id);
  // Do nothing for objects we are not listening for.
  if (it == listeners_.end()) {
    return;
  }
  // Once this flag is set to true, it should never go back to false.
  it->second.subscribed = true;

  // Update entries for this object.
  for (auto const &node_id_binary : location_info.node_ids()) {
    const auto node_id = NodeID::FromBinary(node_id_binary);
    RAY_LOG(DEBUG) << "Object " << object_id << " is on node " << node_id << " alive? "
                   << !gcs_client_->Nodes().IsRemoved(node_id);
  }
  auto location_updated = UpdateObjectLocations(location_info,
                                                gcs_client_,
                                                &it->second.current_object_locations,
                                                &it->second.spilled_url,
                                                &it->second.spilled_node_id,
                                                &it->second.pending_creation,
                                                &it->second.object_size);

  // If the lookup has failed, that means the object is lost. Trigger the callback in this
  // case to handle failure properly.
  if (location_updated || location_lookup_failed) {
    RAY_LOG(DEBUG) << "Pushing location updates to subscribers for object " << object_id
                   << ": " << it->second.current_object_locations.size()
                   << " locations, spilled_url: " << it->second.spilled_url
                   << ", spilled node ID: " << it->second.spilled_node_id
                   << ", object size: " << it->second.object_size
                   << ", lookup failed: " << location_lookup_failed;
    metrics_num_object_location_updates_++;
    cum_metrics_num_object_location_updates_++;
    // Copy the callbacks so that the callbacks can unsubscribe without interrupting
    // looping over the callbacks.
    auto callbacks = it->second.callbacks;
    // Call all callbacks associated with the object id locations we have
    // received.  This notifies the client even if the list of locations is
    // empty, since this may indicate that the objects have been evicted from
    // all nodes.
    for (const auto &[_, func] : callbacks) {
      // We can call the callback directly without worrying about invalidating caller
      // iterators since this is already running in the subscription callback stack.
      // See https://github.com/ray-project/ray/issues/2959.
      func(object_id,
           it->second.current_object_locations,
           it->second.spilled_url,
           it->second.spilled_node_id,
           it->second.pending_creation,
           it->second.object_size);
    }
  }
}

ray::Status OwnershipBasedObjectDirectory::SubscribeObjectLocations(
    const UniqueID &callback_id,
    const ObjectID &object_id,
    const rpc::Address &owner_address,
    const OnLocationsFound &callback) {
  auto it = listeners_.find(object_id);
  if (it == listeners_.end()) {
    // Create an object eviction subscription message.
    auto request = std::make_unique<rpc::WorkerObjectLocationsSubMessage>();
    request->set_intended_worker_id(owner_address.worker_id());
    request->set_object_id(object_id.Binary());

    auto msg_published_callback = [this, object_id](const rpc::PubMessage &pub_message) {
      RAY_CHECK(pub_message.has_worker_object_locations_message());
      const auto &location_info = pub_message.worker_object_locations_message();
      ObjectLocationSubscriptionCallback(
          location_info,
          object_id,
          /*location_lookup_failed*/ !location_info.ref_removed());
    };

    auto failure_callback = [this, owner_address](const std::string &object_id_binary,
                                                  const Status &status) {
      const auto object_id = ObjectID::FromBinary(object_id_binary);
      rpc::WorkerObjectLocationsPubMessage location_info;
      if (!status.ok()) {
        RAY_LOG(INFO) << "Failed to get the location for " << object_id
                      << status.ToString();
        mark_as_failed_(object_id, rpc::ErrorType::OWNER_DIED);
      } else {
        // Owner is still alive but published a failure because the ref was
        // deleted.
        RAY_LOG(INFO)
            << "Failed to get the location for " << object_id
            << ", object already released by distributed reference counting protocol";
        mark_as_failed_(object_id, rpc::ErrorType::OBJECT_DELETED);
      }
      // Location lookup can fail if the owner is reachable but no longer has a
      // record of this ObjectRef, most likely due to an issue with the
      // distributed reference counting protocol.
      ObjectLocationSubscriptionCallback(location_info,
                                         object_id,
                                         /*location_lookup_failed*/ true);
    };

    auto sub_message = std::make_unique<rpc::SubMessage>();
    sub_message->mutable_worker_object_locations_message()->Swap(request.get());

    RAY_CHECK(object_location_subscriber_->Subscribe(
        std::move(sub_message),
        rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL,
        owner_address,
        object_id.Binary(),
        /*subscribe_done_callback=*/nullptr,
        /*Success callback=*/msg_published_callback,
        /*Failure callback=*/failure_callback));

    auto location_state = LocationListenerState();
    location_state.owner_address = owner_address;
    it = listeners_.emplace(object_id, std::move(location_state)).first;
  }
  auto &listener_state = it->second;

  if (listener_state.callbacks.count(callback_id) > 0) {
    return Status::OK();
  }
  listener_state.callbacks.emplace(callback_id, callback);

  // If we previously received some notifications about the object's locations,
  // immediately notify the caller of the current known locations.
  if (listener_state.subscribed) {
    auto &locations = listener_state.current_object_locations;
    auto &spilled_url = listener_state.spilled_url;
    auto &spilled_node_id = listener_state.spilled_node_id;
    bool pending_creation = listener_state.pending_creation;
    auto object_size = listener_state.object_size;
    RAY_LOG(DEBUG) << "Already subscribed to object's locations, pushing location "
                      "updates to subscribers for object "
                   << object_id << ": " << locations.size()
                   << " locations, spilled_url: " << spilled_url
                   << ", spilled node ID: " << spilled_node_id
                   << ", object size: " << object_size;
    // We post the callback to the event loop in order to avoid mutating data
    // structures shared with the caller and potentially invalidating caller
    // iterators. See https://github.com/ray-project/ray/issues/2959.
    io_service_.post(
        [callback,
         locations,
         spilled_url,
         spilled_node_id,
         pending_creation,
         object_size,
         object_id]() {
          callback(object_id,
                   locations,
                   spilled_url,
                   spilled_node_id,
                   pending_creation,
                   object_size);
        },
        "ObjectDirectory.SubscribeObjectLocations");
  }
  return Status::OK();
}

ray::Status OwnershipBasedObjectDirectory::UnsubscribeObjectLocations(
    const UniqueID &callback_id, const ObjectID &object_id) {
  auto entry = listeners_.find(object_id);
  if (entry == listeners_.end()) {
    return Status::OK();
  }
  entry->second.callbacks.erase(callback_id);
  if (entry->second.callbacks.empty()) {
    object_location_subscriber_->Unsubscribe(
        rpc::ChannelType::WORKER_OBJECT_LOCATIONS_CHANNEL,
        entry->second.owner_address,
        object_id.Binary());
    owner_client_pool_->Disconnect(
        WorkerID::FromBinary(entry->second.owner_address.worker_id()));
    listeners_.erase(entry);
  }
  return Status::OK();
}

void OwnershipBasedObjectDirectory::LookupRemoteConnectionInfo(
    RemoteConnectionInfo &connection_info) const {
  auto node_info = gcs_client_->Nodes().Get(connection_info.node_id);
  if (node_info) {
    NodeID result_node_id = NodeID::FromBinary(node_info->node_id());
    RAY_CHECK(result_node_id == connection_info.node_id);
    connection_info.ip = node_info->node_manager_address();
    connection_info.port = static_cast<uint16_t>(node_info->object_manager_port());
  }
}

std::vector<RemoteConnectionInfo>
OwnershipBasedObjectDirectory::LookupAllRemoteConnections() const {
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

void OwnershipBasedObjectDirectory::HandleNodeRemoved(const NodeID &node_id) {
  for (auto &[object_id, listener] : listeners_) {
    bool updated = listener.current_object_locations.erase(node_id);
    if (listener.spilled_node_id == node_id) {
      listener.spilled_node_id = NodeID::Nil();
      listener.spilled_url = "";
      updated = true;
    }

    if (updated) {
      // Re-call all the subscribed callbacks for the object, since its
      // locations have changed.
      for (const auto &[_, func] : listener.callbacks) {
        // It is safe to call the callback directly since this is already running
        // in the subscription callback stack.
        func(object_id,
             listener.current_object_locations,
             listener.spilled_url,
             listener.spilled_node_id,
             listener.pending_creation,
             listener.object_size);
      }
    }
  }
}

void OwnershipBasedObjectDirectory::RecordMetrics(uint64_t duration_ms) {
  stats::ObjectDirectoryLocationSubscriptions.Record(listeners_.size());

  // Record number of object location updates per second.
  metrics_num_object_location_updates_per_second_ =
      (double)metrics_num_object_location_updates_ * (1000.0 / (double)duration_ms);
  stats::ObjectDirectoryLocationUpdates.Record(
      metrics_num_object_location_updates_per_second_);
  metrics_num_object_location_updates_ = 0;
  // Record number of object location lookups per second.
  metrics_num_object_location_lookups_per_second_ =
      (double)metrics_num_object_location_lookups_ * (1000.0 / (double)duration_ms);
  stats::ObjectDirectoryLocationLookups.Record(
      metrics_num_object_location_lookups_per_second_);
  metrics_num_object_location_lookups_ = 0;
  // Record number of object locations added per second.
  metrics_num_object_locations_added_per_second_ =
      (double)metrics_num_object_locations_added_ * (1000.0 / (double)duration_ms);
  stats::ObjectDirectoryAddedLocations.Record(
      metrics_num_object_locations_added_per_second_);
  metrics_num_object_locations_added_ = 0;
  // Record number of object locations removed per second.
  metrics_num_object_locations_removed_per_second_ =
      (double)metrics_num_object_locations_removed_ * (1000.0 / (double)duration_ms);
  stats::ObjectDirectoryRemovedLocations.Record(
      metrics_num_object_locations_removed_per_second_);
  metrics_num_object_locations_removed_ = 0;
}

std::string OwnershipBasedObjectDirectory::DebugString() const {
  std::stringstream result;
  result << std::fixed << std::setprecision(3);
  result << "OwnershipBasedObjectDirectory:";
  result << "\n- num listeners: " << listeners_.size();
  result << "\n- cumulative location updates: "
         << cum_metrics_num_object_location_updates_;
  result << "\n- num location updates per second: "
         << metrics_num_object_location_updates_per_second_;
  result << "\n- num location lookups per second: "
         << metrics_num_object_location_lookups_per_second_;
  result << "\n- num locations added per second: "
         << metrics_num_object_locations_added_per_second_;
  result << "\n- num locations removed per second: "
         << metrics_num_object_locations_removed_per_second_;
  return result.str();
}

}  // namespace ray
