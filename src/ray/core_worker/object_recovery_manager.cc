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

#include <memory>
#include <utility>
#include <vector>

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
  bool ref_exists = reference_counter_.IsPlasmaObjectPinnedOrSpilled(
      object_id, &owned_by_us, &pinned_at, &spilled);
  if (!ref_exists) {
    // References that have gone out of scope cannot be recovered.
    return false;
  }

  if (!owned_by_us) {
    RAY_LOG(DEBUG).WithField(object_id)
        << "Reconstruction for borrowed object is not supported";
    return false;
  }

  bool already_pending_recovery = true;
  bool requires_recovery = pinned_at.IsNil() && !spilled;
  if (requires_recovery) {
    {
      absl::MutexLock lock(&objects_pending_recovery_mu_);
      // Mark that we are attempting recovery for this object to prevent
      // duplicate restarts of the same object.
      already_pending_recovery = !objects_pending_recovery_.insert(object_id).second;
    }
  }

  if (!already_pending_recovery) {
    RAY_LOG(DEBUG).WithField(object_id) << "Starting recovery for object";
    in_memory_store_.GetAsync(
        object_id, [this, object_id](const std::shared_ptr<RayObject> &obj) {
          {
            absl::MutexLock lock(&objects_pending_recovery_mu_);
            RAY_CHECK(objects_pending_recovery_.erase(object_id)) << object_id;
          }
          RAY_LOG(INFO).WithField(object_id) << "Recovery complete for object";
        });
    // Gets the node ids from reference_counter and then gets addresses from the local
    // gcs_client.
    object_lookup_(
        object_id,
        [this](const ObjectID &object_id_to_lookup, std::vector<rpc::Address> locations) {
          PinOrReconstructObject(object_id_to_lookup, std::move(locations));
        });
  } else if (requires_recovery) {
    RAY_LOG(DEBUG).WithField(object_id) << "Recovery already started for object";
  } else {
    RAY_LOG(INFO).WithField(object_id).WithField(pinned_at)
        << "Object has a pinned or spilled location, skipping recovery";
    // If the object doesn't exist in the memory store
    // (core_worker.cc removes the object from memory store before calling this method),
    // we need to add it back to indicate that it's available.
    // If the object is already in the memory store then the put is a no-op.
    in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                         object_id,
                         reference_counter_.HasReference(object_id));
  }
  return true;
}

void ObjectRecoveryManager::PinOrReconstructObject(const ObjectID &object_id,
                                                   std::vector<rpc::Address> locations) {
  RAY_LOG(DEBUG).WithField(object_id)
      << "Lost object has " << locations.size() << " locations";
  // The object to recovery has secondary copies, pin one copy to promote it to primary
  // one.
  if (!locations.empty()) {
    const auto location = std::move(locations.back());
    locations.pop_back();
    PinExistingObjectCopy(object_id, location, std::move(locations));
  } else {
    // There are no more copies to pin, try to reconstruct the object.
    ReconstructObject(object_id);
  }
}

void ObjectRecoveryManager::PinExistingObjectCopy(
    const ObjectID &object_id,
    const rpc::Address &raylet_address,
    std::vector<rpc::Address> other_locations) {
  // If a copy still exists, pin the object by sending a
  // PinObjectIDs RPC.
  const auto node_id = NodeID::FromBinary(raylet_address.node_id());
  RAY_LOG(DEBUG).WithField(object_id).WithField(node_id)
      << "Trying to pin copy of lost object at node";

  raylet_client_pool_->GetOrConnectByAddress(raylet_address)
      ->PinObjectIDs(
          rpc_address_,
          {object_id},
          /*generator_id=*/ObjectID::Nil(),
          [this, object_id, other_locations = std::move(other_locations), node_id](
              const Status &status, const rpc::PinObjectIDsReply &reply) mutable {
            if (status.ok() && reply.successes(0)) {
              in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                                   object_id,
                                   reference_counter_.HasReference(object_id));
              reference_counter_.UpdateObjectPinnedAtRaylet(object_id, node_id);
            } else {
              RAY_LOG(INFO).WithField(object_id)
                  << "Error pinning secondary copy of lost object due to " << status
                  << ", trying again with other locations";
              PinOrReconstructObject(object_id, std::move(other_locations));
            }
          });
}

void ObjectRecoveryManager::ReconstructObject(const ObjectID &object_id) {
  bool lineage_evicted = false;
  if (!reference_counter_.IsObjectReconstructable(object_id, &lineage_evicted)) {
    RAY_LOG(DEBUG).WithField(object_id) << "Object is not reconstructable";
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

  RAY_LOG(DEBUG).WithField(object_id) << "Attempting to reconstruct object";
  // Notify the task manager that we are retrying the task that created this
  // object.
  const auto task_id = object_id.TaskId();
  std::vector<ObjectID> task_deps;
  // pending_creation needs to be set to true BEFORE calling ResubmitTask,
  // since it might be set back to false inside ResubmitTask if the task is
  // an actor task and the actor is dead. If we set pending_creation to true
  // after ResubmitTask, then it will remain true forever.
  // see https://github.com/ray-project/ray/issues/47606 for more details.
  reference_counter_.UpdateObjectPendingCreation(object_id, true);
  auto error_type_optional = task_manager_.ResubmitTask(task_id, &task_deps);

  if (!error_type_optional.has_value()) {
    // Try to recover the task's dependencies.
    for (const auto &dep : task_deps) {
      auto recovered = RecoverObject(dep);
      if (!recovered) {
        RAY_LOG(INFO).WithField(dep) << "Failed to reconstruct object";
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
    RAY_LOG(INFO).WithField(object_id)
        << "Failed to reconstruct object because lineage has already been deleted";
    reference_counter_.UpdateObjectPendingCreation(object_id, false);
    recovery_failure_callback_(object_id,
                               *error_type_optional,
                               /*pin_object=*/true);
  }
}

}  // namespace core
}  // namespace ray
