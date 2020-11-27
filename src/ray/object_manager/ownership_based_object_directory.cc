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

namespace ray {

OwnershipBasedObjectDirectory::OwnershipBasedObjectDirectory(
    boost::asio::io_service &io_service, std::shared_ptr<gcs::GcsClient> &gcs_client)
    : ObjectDirectory(io_service, gcs_client), client_call_manager_(io_service) {}

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
    RAY_LOG(WARNING) << "Object " << object_id << " does not have owner. "
                     << "ReportObjectAdded becomes a no-op.";
    return Status::OK();
  }
  rpc::AddObjectLocationOwnerRequest request;
  request.set_intended_worker_id(object_info.owner_worker_id);
  request.set_object_id(object_id.Binary());
  request.set_node_id(node_id.Binary());

  rpc_client->AddObjectLocationOwner(
      request, [worker_id, object_id](Status status,
                                      const rpc::AddObjectLocationOwnerReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Worker " << worker_id << " failed to add the location for "
                         << object_id;
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
    RAY_LOG(WARNING) << "Object " << object_id << " does not have owner. "
                     << "ReportObjectRemoved becomes a no-op.";
    return Status::OK();
  }

  rpc::RemoveObjectLocationOwnerRequest request;
  request.set_intended_worker_id(object_info.owner_worker_id);
  request.set_object_id(object_id.Binary());
  request.set_node_id(node_id.Binary());

  rpc_client->RemoveObjectLocationOwner(
      request, [worker_id, object_id](Status status,
                                      const rpc::RemoveObjectLocationOwnerReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Worker " << worker_id
                         << " failed to remove the location for " << object_id;
        }
      });
  return Status::OK();
};

void OwnershipBasedObjectDirectory::SubscriptionCallback(
    ObjectID object_id, WorkerID worker_id, Status status,
    const rpc::GetObjectLocationsOwnerReply &reply) {
  auto it = listeners_.find(object_id);
  if (it == listeners_.end()) {
    return;
  }

  std::unordered_set<NodeID> node_ids;
  for (auto const &node_id : reply.node_ids()) {
    node_ids.emplace(NodeID::FromBinary(node_id));
  }
  FilterRemovedNodes(gcs_client_, &node_ids);
  if (node_ids != it->second.current_object_locations) {
    it->second.current_object_locations = std::move(node_ids);
    auto callbacks = it->second.callbacks;
    // Call all callbacks associated with the object id locations we have
    // received.  This notifies the client even if the list of locations is
    // empty, since this may indicate that the objects have been evicted from
    // all nodes.
    for (const auto &callback_pair : callbacks) {
      // It is safe to call the callback directly since this is already running
      // in the subscription callback stack.
      callback_pair.second(object_id, it->second.current_object_locations, "");
    }
  }

  auto worker_it = worker_rpc_clients_.find(worker_id);
  rpc::GetObjectLocationsOwnerRequest request;
  request.set_intended_worker_id(worker_id.Binary());
  request.set_object_id(object_id.Binary());
  // TODO(zhuohan): Fix this infinite loop.
  worker_it->second->GetObjectLocationsOwner(
      request,
      std::bind(&OwnershipBasedObjectDirectory::SubscriptionCallback, this, object_id,
                worker_id, std::placeholders::_1, std::placeholders::_2));
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
  WorkerID worker_id = WorkerID::FromBinary(owner_address.worker_id());
  std::shared_ptr<rpc::CoreWorkerClient> rpc_client = GetClient(owner_address);
  if (rpc_client == nullptr) {
    RAY_LOG(WARNING) << "Object " << object_id << " does not have owner. "
                     << "LookupLocations returns an empty list of locations.";
    io_service_.post([callback, object_id]() {
      callback(object_id, std::unordered_set<NodeID>(), "");
    });
    return Status::OK();
  }

  rpc::GetObjectLocationsOwnerRequest request;
  request.set_intended_worker_id(owner_address.worker_id());
  request.set_object_id(object_id.Binary());

  rpc_client->GetObjectLocationsOwner(
      request, [this, worker_id, object_id, callback](
                   Status status, const rpc::GetObjectLocationsOwnerReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Worker " << worker_id << " failed to get the location for "
                         << object_id;
        }
        std::unordered_set<NodeID> node_ids;
        for (auto const &node_id : reply.node_ids()) {
          node_ids.emplace(NodeID::FromBinary(node_id));
        }
        FilterRemovedNodes(gcs_client_, &node_ids);
        callback(object_id, node_ids, "");
      });
  return Status::OK();
}

std::string OwnershipBasedObjectDirectory::DebugString() const {
  std::stringstream result;
  result << "OwnershipBasedObjectDirectory:";
  result << "\n- num listeners: " << listeners_.size();
  return result.str();
}

}  // namespace ray
