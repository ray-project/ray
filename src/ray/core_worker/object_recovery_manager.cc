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
namespace core {

bool ObjectRecoveryManager::RecoverObject(const ObjectID &object_id) {
  if (object_id.TaskId().IsForActorCreationTask()) {
    // The GCS manages all actor restarts, so we should never try to
    // reconstruct an actor here.
    return true;
  }
  // Check the ReferenceCounter to see if there is a location for the object.
  bool owned_by_us = false;
  NodeID pinned_at;
  bool spilled = false;
  bool ref_exists = reference_counter_->IsPlasmaObjectPinnedOrSpilled(
      object_id, &owned_by_us, &pinned_at, &spilled);
  if (!ref_exists) {
    // References that have gone out of scope cannot be recovered.
    return false;
  }

  if (!owned_by_us) {
    RAY_LOG(DEBUG) << "Reconstruction for borrowed objects (" << object_id
                   << ") is not supported";
    return false;
  }

  bool already_pending_recovery = true;
  bool requires_recovery = pinned_at.IsNil() && !spilled;
  if (requires_recovery) {
    {
      absl::MutexLock lock(&mu_);
      // Mark that we are attempting recovery for this object to prevent
      // duplicate restarts of the same object.
      already_pending_recovery = !objects_pending_recovery_.insert(object_id).second;
    }
  }

  if (!already_pending_recovery) {
    RAY_LOG(DEBUG) << "Starting recovery for object " << object_id;
    in_memory_store_->GetAsync(
        object_id, [this, object_id](std::shared_ptr<RayObject> obj) {
          absl::MutexLock lock(&mu_);
          RAY_CHECK(objects_pending_recovery_.erase(object_id)) << object_id;
          RAY_LOG(INFO) << "Recovery complete for object " << object_id;
        });
    // Lookup the object in the GCS to find another copy.
    RAY_CHECK_OK(object_lookup_(
        object_id,
        [this](const ObjectID &object_id, const std::vector<rpc::Address> &locations) {
          PinOrReconstructObject(object_id, locations);
        }));
  } else if (requires_recovery) {
    RAY_LOG(DEBUG) << "Recovery already started for object " << object_id;
  } else {
    RAY_LOG(DEBUG) << "Object " << object_id
                   << " has a pinned or spilled location, skipping recovery";
  }
  return true;
}

void ObjectRecoveryManager::PinOrReconstructObject(
    const ObjectID &object_id, const std::vector<rpc::Address> &locations) {
  RAY_LOG(DEBUG) << "Lost object " << object_id << " has " << locations.size()
                 << " locations";
  if (!locations.empty()) {
    auto locations_copy = locations;
    const auto location = locations_copy.back();
    locations_copy.pop_back();
    PinExistingObjectCopy(object_id, location, locations_copy);
  } else {
    // There are no more copies to pin, try to reconstruct the object.
    ReconstructObject(object_id);
  }
}

void ObjectRecoveryManager::PinExistingObjectCopy(
    const ObjectID &object_id,
    const rpc::Address &raylet_address,
    const std::vector<rpc::Address> &other_locations) {
  // If a copy still exists, pin the object by sending a
  // PinObjectIDs RPC.
  const auto node_id = NodeID::FromBinary(raylet_address.raylet_id());
  RAY_LOG(DEBUG) << "Trying to pin copy of lost object " << object_id << " at node "
                 << node_id;

  std::shared_ptr<PinObjectsInterface> client;
  if (node_id == NodeID::FromBinary(rpc_address_.raylet_id())) {
    client = local_object_pinning_client_;
  } else {
    absl::MutexLock lock(&mu_);
    auto client_it = remote_object_pinning_clients_.find(node_id);
    if (client_it == remote_object_pinning_clients_.end()) {
      RAY_LOG(DEBUG) << "Connecting to raylet " << node_id;
      client_it = remote_object_pinning_clients_
                      .emplace(node_id,
                               client_factory_(raylet_address.ip_address(),
                                               raylet_address.port()))
                      .first;
    }
    client = client_it->second;
  }

  client->PinObjectIDs(rpc_address_,
                       {object_id},
                       [this, object_id, other_locations, node_id](
                           const Status &status, const rpc::PinObjectIDsReply &reply) {
                         if (status.ok()) {
                           // TODO(swang): Make sure that the node is still alive when
                           // marking the object as pinned.
                           RAY_CHECK(in_memory_store_->Put(
                               RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
                           reference_counter_->UpdateObjectPinnedAtRaylet(object_id,
                                                                          node_id);
                         } else {
                           RAY_LOG(INFO) << "Error pinning new copy of lost object "
                                         << object_id << ", trying again";
                           PinOrReconstructObject(object_id, other_locations);
                         }
                       });
}

void ObjectRecoveryManager::ReconstructObject(const ObjectID &object_id) {
  bool lineage_evicted = false;
  if (!reference_counter_->IsObjectReconstructable(object_id, &lineage_evicted)) {
    RAY_LOG(DEBUG) << "Object " << object_id << " is not reconstructable";
    if (lineage_evicted) {
      // TODO(swang): We may not report the LINEAGE_EVICTED error (just reports
      // general OBJECT_UNRECONSTRUCTABLE error) if lineage eviction races with
      // reconstruction.
      recovery_failure_callback_(object_id,
                                 rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED,
                                 /*pin_object=*/true);
    } else {
      recovery_failure_callback_(object_id,
                                 rpc::ErrorType::OBJECT_LOST,
                                 /*pin_object=*/true);
    }
    return;
  }

  RAY_LOG(DEBUG) << "Attempting to reconstruct object " << object_id;
  // Notify the task manager that we are retrying the task that created this
  // object.
  const auto task_id = object_id.TaskId();
  std::vector<ObjectID> task_deps;
  auto resubmitted = task_resubmitter_->ResubmitTask(task_id, &task_deps);

  if (resubmitted) {
    // Try to recover the task's dependencies.
    for (const auto &dep : task_deps) {
      auto recovered = RecoverObject(dep);
      if (!recovered) {
        RAY_LOG(INFO) << "Failed to reconstruct object " << dep;
        // This case can happen if the dependency was borrowed from another
        // worker, or if there was a bug in reconstruction that caused us to GC
        // the dependency ref.
        // We do not pin the dependency because we may not be the owner.
        recovery_failure_callback_(dep,
                                   rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE,
                                   /*pin_object=*/false);
      }
    }
  } else {
    RAY_LOG(INFO) << "Failed to reconstruct object " << object_id
                  << " because lineage has already been deleted";
    recovery_failure_callback_(
        object_id,
        rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED,
        /*pin_object=*/true);
  }
}

}  // namespace core
}  // namespace ray
