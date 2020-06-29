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

#include "ray/core_worker/object_recovery_manager.h"

#include "ray/util/util.h"

namespace ray {

Status ObjectRecoveryManager::RecoverObject(const ObjectID &object_id) {
  // Check the ReferenceCounter to see if there is a location for the object.
  bool pinned = false;
  bool owned_by_us = reference_counter_->IsPlasmaObjectPinned(object_id, &pinned);
  if (!owned_by_us) {
    return Status::Invalid(
        "Object reference no longer exists or is not owned by us. Either lineage pinning "
        "is disabled or this object ID is borrowed.");
  }

  bool already_pending_recovery = true;
  if (!pinned) {
    {
      absl::MutexLock lock(&mu_);
      // Mark that we are attempting recovery for this object to prevent
      // duplicate restarts of the same object.
      already_pending_recovery = !objects_pending_recovery_.insert(object_id).second;
    }
  }

  if (!already_pending_recovery) {
    RAY_LOG(INFO) << "Starting recovery for object " << object_id;
    in_memory_store_->GetAsync(
        object_id, [this, object_id](std::shared_ptr<RayObject> obj) {
          absl::MutexLock lock(&mu_);
          RAY_CHECK(objects_pending_recovery_.erase(object_id)) << object_id;
          RAY_LOG(INFO) << "Recovery complete for object " << object_id;
        });
    // Lookup the object in the GCS to find another copy.
    RAY_RETURN_NOT_OK(object_lookup_(
        object_id,
        [this](const ObjectID &object_id, const std::vector<rpc::Address> &locations) {
          PinOrReconstructObject(object_id, locations);
        }));
  } else {
    RAY_LOG(DEBUG) << "Recovery already started for object " << object_id;
  }
  return Status::OK();
}

void ObjectRecoveryManager::PinOrReconstructObject(
    const ObjectID &object_id, const std::vector<rpc::Address> &locations) {
  RAY_LOG(INFO) << "Lost object " << object_id << " has " << locations.size()
                << " locations";
  if (!locations.empty()) {
    auto locations_copy = locations;
    const auto location = locations_copy.back();
    locations_copy.pop_back();
    PinExistingObjectCopy(object_id, location, locations_copy);
  } else if (lineage_reconstruction_enabled_) {
    // There are no more copies to pin, try to reconstruct the object.
    ReconstructObject(object_id);
  } else {
    reconstruction_failure_callback_(object_id, /*pin_object=*/true);
  }
}

void ObjectRecoveryManager::PinExistingObjectCopy(
    const ObjectID &object_id, const rpc::Address &raylet_address,
    const std::vector<rpc::Address> &other_locations) {
  // If a copy still exists, pin the object by sending a
  // PinObjectIDs RPC.
  const auto node_id = ClientID::FromBinary(raylet_address.raylet_id());
  RAY_LOG(DEBUG) << "Trying to pin copy of lost object " << object_id << " at node "
                 << node_id;

  std::shared_ptr<PinObjectsInterface> client;
  if (node_id == ClientID::FromBinary(rpc_address_.raylet_id())) {
    client = local_object_pinning_client_;
  } else {
    absl::MutexLock lock(&mu_);
    auto client_it = remote_object_pinning_clients_.find(node_id);
    if (client_it == remote_object_pinning_clients_.end()) {
      RAY_LOG(DEBUG) << "Connecting to raylet " << node_id;
      client_it = remote_object_pinning_clients_
                      .emplace(node_id, client_factory_(raylet_address.ip_address(),
                                                        raylet_address.port()))
                      .first;
    }
    client = client_it->second;
  }

  RAY_UNUSED(client->PinObjectIDs(
      rpc_address_, {object_id},
      [this, object_id, other_locations, node_id](const Status &status,
                                                  const rpc::PinObjectIDsReply &reply) {
        if (status.ok()) {
          // TODO(swang): Make sure that the node is still alive when
          // marking the object as pinned.
          RAY_CHECK(in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                                          object_id));
          reference_counter_->UpdateObjectPinnedAtRaylet(object_id, node_id);
        } else {
          RAY_LOG(INFO) << "Error pinning new copy of lost object " << object_id
                        << ", trying again";
          PinOrReconstructObject(object_id, other_locations);
        }
      }));
}

void ObjectRecoveryManager::ReconstructObject(const ObjectID &object_id) {
  // Notify the task manager that we are retrying the task that created this
  // object.
  const auto task_id = object_id.TaskId();
  std::vector<ObjectID> task_deps;
  auto status = task_resubmitter_->ResubmitTask(task_id, &task_deps);

  if (status.ok()) {
    // Try to recover the task's dependencies.
    for (const auto &dep : task_deps) {
      auto status = RecoverObject(dep);
      if (!status.ok()) {
        RAY_LOG(INFO) << "Failed to reconstruct object " << dep << ": "
                      << status.message();
        // We do not pin the dependency because we may not be the owner.
        reconstruction_failure_callback_(dep, /*pin_object=*/false);
      }
    }
  } else {
    RAY_LOG(INFO) << "Failed to reconstruct object " << object_id;
    reconstruction_failure_callback_(object_id, /*pin_object=*/true);
  }
}

}  // namespace ray
