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
#include "ray/stats/stats.h"

namespace ray {

OwnershipBasedObjectDirectory::OwnershipBasedObjectDirectory(
    instrumented_io_context &io_service, std::shared_ptr<gcs::GcsClient> &gcs_client,
    std::function<void(const ObjectID &)> mark_as_failed)
    : ObjectDirectory(io_service, gcs_client),
      client_call_manager_(io_service),
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
bool UpdateObjectLocations(const rpc::GetObjectLocationsOwnerReply &location_reply,
                           const Status &status, const ObjectID &object_id,
                           std::shared_ptr<gcs::GcsClient> gcs_client,
                           std::function<void(const ObjectID &)> mark_as_failed,
                           std::unordered_set<NodeID> *node_ids, std::string *spilled_url,
                           NodeID *spilled_node_id, size_t *object_size) {
  bool is_updated = false;

  std::unordered_set<NodeID> new_node_ids;

  if (!status.ok()) {
    RAY_LOG(INFO) << "Failed to return location updates to subscribers for " << object_id
                  << ": " << status.ToString()
                  << ", assuming that the object was freed or evicted.";
    // When we can't get location updates from the owner, we assume that the object
    // was freed or evicted, so we send an empty location update to all subscribers.
    *node_ids = new_node_ids;
    // Mark the object as failed immediately here since we know it can never appear.
    mark_as_failed(object_id);
    is_updated = true;
  } else {
    // The size can be 0 if the update was a deletion. This assumes that an
    // object's size is always greater than 0.
    // TODO(swang): If that's not the case, we should use a flag to check
    // whether the size is set instead.
    if (location_reply.object_size() > 0) {
      *object_size = location_reply.object_size();
      is_updated = true;
    }
    for (auto const &node_id : location_reply.node_ids()) {
      new_node_ids.emplace(NodeID::FromBinary(node_id));
    }
    // Filter out the removed nodes from the object locations.
    FilterRemovedNodes(gcs_client, &new_node_ids);
    if (new_node_ids != *node_ids) {
      *node_ids = new_node_ids;
      is_updated = true;
    }
    const std::string &new_spilled_url = location_reply.spilled_url();
    if (new_spilled_url != *spilled_url) {
      const auto new_spilled_node_id =
          NodeID::FromBinary(location_reply.spilled_node_id());
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
  }
  return is_updated;
}

rpc::Address GetOwnerAddressFromObjectInfo(
    const object_manager::protocol::ObjectInfoT &object_info) {
  rpc::Address owner_address;
  owner_address.set_raylet_id(object_info.owner_raylet_id);
  owner_address.set_ip_address(object_info.owner_ip_address);
  owner_address.set_port(object_info.owner_port);
  owner_address.set_worker_id(object_info.owner_worker_id);
  return owner_address;
}

}  // namespace

std::shared_ptr<rpc::CoreWorkerClient> OwnershipBasedObjectDirectory::GetClient(
    const rpc::Address &owner_address) {
  WorkerID worker_id = WorkerID::FromBinary(owner_address.worker_id());
  if (worker_id.IsNil()) {
    // If an object does not have owner, return nullptr.
    return nullptr;
  }
  auto it = worker_rpc_clients_.find(worker_id);
  if (it == worker_rpc_clients_.end()) {
    it = worker_rpc_clients_
             .emplace(worker_id, std::make_shared<rpc::CoreWorkerClient>(
                                     owner_address, client_call_manager_))
             .first;
  }
  return it->second;
}

ray::Status OwnershipBasedObjectDirectory::ReportObjectAdded(
    const ObjectID &object_id, const NodeID &node_id,
    const object_manager::protocol::ObjectInfoT &object_info) {
  WorkerID worker_id = WorkerID::FromBinary(object_info.owner_worker_id);
  rpc::Address owner_address = GetOwnerAddressFromObjectInfo(object_info);
  std::shared_ptr<rpc::CoreWorkerClient> rpc_client = GetClient(owner_address);
  if (rpc_client == nullptr) {
    RAY_LOG(DEBUG) << "Object " << object_id << " does not have owner. "
                   << "ReportObjectAdded becomes a no-op."
                   << "This should only happen for Plasma store warmup objects.";
    return Status::OK();
  }
  rpc::AddObjectLocationOwnerRequest request;
  request.set_intended_worker_id(object_info.owner_worker_id);
  request.set_object_id(object_id.Binary());
  request.set_node_id(node_id.Binary());

  metrics_num_object_locations_added_++;

  rpc_client->AddObjectLocationOwner(
      request, [worker_id, object_id, node_id](
                   Status status, const rpc::AddObjectLocationOwnerReply &reply) {
        if (!status.ok()) {
          RAY_LOG(DEBUG) << "Worker " << worker_id << " failed to add the location "
                         << node_id << " for " << object_id
                         << ", the object has most likely been freed: "
                         << status.ToString();
        } else {
          RAY_LOG(DEBUG) << "Added location " << node_id << " for object " << object_id
                         << " on owner " << worker_id;
        }
      });
  return Status::OK();
}

ray::Status OwnershipBasedObjectDirectory::ReportObjectRemoved(
    const ObjectID &object_id, const NodeID &node_id,
    const object_manager::protocol::ObjectInfoT &object_info) {
  WorkerID worker_id = WorkerID::FromBinary(object_info.owner_worker_id);
  rpc::Address owner_address = GetOwnerAddressFromObjectInfo(object_info);
  std::shared_ptr<rpc::CoreWorkerClient> rpc_client = GetClient(owner_address);
  if (rpc_client == nullptr) {
    RAY_LOG(DEBUG) << "Object " << object_id << " does not have owner. "
                   << "ReportObjectRemoved becomes a no-op. "
                   << "This should only happen for Plasma store warmup objects.";
    return Status::OK();
  }

  rpc::RemoveObjectLocationOwnerRequest request;
  request.set_intended_worker_id(object_info.owner_worker_id);
  request.set_object_id(object_id.Binary());
  request.set_node_id(node_id.Binary());

  metrics_num_object_locations_removed_++;

  rpc_client->RemoveObjectLocationOwner(
      request, [worker_id, object_id, node_id](
                   Status status, const rpc::RemoveObjectLocationOwnerReply &reply) {
        if (!status.ok()) {
          RAY_LOG(DEBUG) << "Worker " << worker_id << " failed to remove the location "
                         << node_id << " for " << object_id
                         << ", the object has most likely been freed: "
                         << status.ToString();
        } else {
          RAY_LOG(DEBUG) << "Removed location " << node_id << " for object " << object_id
                         << " on owner " << worker_id;
        }
      });
  return Status::OK();
};

void OwnershipBasedObjectDirectory::SubscriptionCallback(
    ObjectID object_id, WorkerID worker_id, Status status,
    const rpc::GetObjectLocationsOwnerReply &reply) {
  // Objects are added to this map in SubscribeObjectLocations.
  auto it = listeners_.find(object_id);
  // Do nothing for objects we are not listening for.
  if (it == listeners_.end()) {
    return;
  }
  // Once this flag is set to true, it should never go back to false.
  it->second.subscribed = true;

  // Update entries for this object.
  if (UpdateObjectLocations(reply, status, object_id, gcs_client_, mark_as_failed_,
                            &it->second.current_object_locations, &it->second.spilled_url,
                            &it->second.spilled_node_id, &it->second.object_size)) {
    RAY_LOG(DEBUG) << "Pushing location updates to subscribers for object " << object_id
                   << ": " << it->second.current_object_locations.size()
                   << " locations, spilled_url: " << it->second.spilled_url
                   << ", spilled node ID: " << it->second.spilled_node_id
                   << ", object size: " << it->second.object_size;
    metrics_num_object_location_updates_++;
    // Copy the callbacks so that the callbacks can unsubscribe without interrupting
    // looping over the callbacks.
    auto callbacks = it->second.callbacks;
    // Call all callbacks associated with the object id locations we have
    // received.  This notifies the client even if the list of locations is
    // empty, since this may indicate that the objects have been evicted from
    // all nodes.
    for (const auto &callback_pair : callbacks) {
      // We can call the callback directly without worrying about invalidating caller
      // iterators since this is already running in the subscription callback stack.
      // See https://github.com/ray-project/ray/issues/2959.
      callback_pair.second(object_id, it->second.current_object_locations,
                           it->second.spilled_url, it->second.spilled_node_id,
                           it->second.object_size);
    }
  }

  // Only send the next long-polling RPC if the last one was successful.
  // If the last RPC failed, we consider the object to have been freed.
  if (status.ok()) {
    auto worker_it = worker_rpc_clients_.find(worker_id);
    rpc::GetObjectLocationsOwnerRequest request;
    request.set_intended_worker_id(worker_id.Binary());
    request.set_object_id(object_id.Binary());
    request.set_last_version(reply.current_version());
    worker_it->second->GetObjectLocationsOwner(
        request,
        std::bind(&OwnershipBasedObjectDirectory::SubscriptionCallback, this, object_id,
                  worker_id, std::placeholders::_1, std::placeholders::_2));
  }
}

ray::Status OwnershipBasedObjectDirectory::SubscribeObjectLocations(
    const UniqueID &callback_id, const ObjectID &object_id,
    const rpc::Address &owner_address, const OnLocationsFound &callback) {
  auto it = listeners_.find(object_id);
  if (it == listeners_.end()) {
    WorkerID worker_id = WorkerID::FromBinary(owner_address.worker_id());
    std::shared_ptr<rpc::CoreWorkerClient> rpc_client = GetClient(owner_address);
    if (rpc_client == nullptr) {
      RAY_LOG(WARNING) << "Object " << object_id << " does not have owner. "
                       << "SubscribeObjectLocations becomes a no-op.";
      return Status::OK();
    }
    rpc::GetObjectLocationsOwnerRequest request;
    request.set_intended_worker_id(owner_address.worker_id());
    request.set_object_id(object_id.Binary());
    request.set_last_version(-1);
    rpc_client->GetObjectLocationsOwner(
        request,
        std::bind(&OwnershipBasedObjectDirectory::SubscriptionCallback, this, object_id,
                  worker_id, std::placeholders::_1, std::placeholders::_2));
    it = listeners_.emplace(object_id, LocationListenerState()).first;
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
        [callback, locations, spilled_url, spilled_node_id, object_size, object_id]() {
          callback(object_id, locations, spilled_url, spilled_node_id, object_size);
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
    listeners_.erase(entry);
  }
  return Status::OK();
}

ray::Status OwnershipBasedObjectDirectory::LookupLocations(
    const ObjectID &object_id, const rpc::Address &owner_address,
    const OnLocationsFound &callback) {
  metrics_num_object_location_lookups_++;
  auto it = listeners_.find(object_id);
  if (it != listeners_.end() && it->second.subscribed) {
    // If we have locations cached due to a concurrent SubscribeObjectLocations
    // call, and we have received at least one update from the owner about
    // the object's creation, then call the callback immediately with the
    // cached locations.
    auto &locations = it->second.current_object_locations;
    auto &spilled_url = it->second.spilled_url;
    auto &spilled_node_id = it->second.spilled_node_id;
    auto object_size = it->second.object_size;
    // We post the callback to the event loop in order to avoid mutating data
    // structures shared with the caller and potentially invalidating caller
    // iterators. See https://github.com/ray-project/ray/issues/2959.
    io_service_.post(
        [callback, object_id, locations, spilled_url, spilled_node_id, object_size]() {
          callback(object_id, locations, spilled_url, spilled_node_id, object_size);
        },
        "ObjectDirectory.LookupLocations");
  } else {
    WorkerID worker_id = WorkerID::FromBinary(owner_address.worker_id());
    std::shared_ptr<rpc::CoreWorkerClient> rpc_client = GetClient(owner_address);
    if (rpc_client == nullptr) {
      RAY_LOG(WARNING) << "Object " << object_id << " does not have owner. "
                       << "LookupLocations returns an empty list of locations.";
      // We post the callback to the event loop in order to avoid mutating data structures
      // shared with the caller and potentially invalidating caller iterators.
      // See https://github.com/ray-project/ray/issues/2959.
      io_service_.post(
          [callback, object_id]() {
            callback(object_id, std::unordered_set<NodeID>(), "", NodeID::Nil(), 0);
          },
          "ObjectDirectory.LookupLocations");
      return Status::OK();
    }

    rpc::GetObjectLocationsOwnerRequest request;
    request.set_intended_worker_id(owner_address.worker_id());
    request.set_object_id(object_id.Binary());
    request.set_last_version(-1);

    rpc_client->GetObjectLocationsOwner(
        request, [this, worker_id, object_id, callback](
                     Status status, const rpc::GetObjectLocationsOwnerReply &reply) {
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Worker " << worker_id << " failed to get the location for "
                           << object_id;
          }
          std::unordered_set<NodeID> node_ids;
          std::string spilled_url;
          NodeID spilled_node_id;
          size_t object_size = 0;
          UpdateObjectLocations(reply, status, object_id, gcs_client_, mark_as_failed_,
                                &node_ids, &spilled_url, &spilled_node_id, &object_size);
          RAY_LOG(DEBUG) << "Looked up locations for " << object_id
                         << ", returning: " << node_ids.size()
                         << " locations, spilled_url: " << spilled_url
                         << ", spilled node ID: " << spilled_node_id
                         << ", object size: " << object_size;
          // We can call the callback directly without worrying about invalidating
          // caller iterators since this is already running in the core worker
          // client's lookup callback stack.
          // See https://github.com/ray-project/ray/issues/2959.
          callback(object_id, node_ids, spilled_url, spilled_node_id, object_size);
        });
  }
  return Status::OK();
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
