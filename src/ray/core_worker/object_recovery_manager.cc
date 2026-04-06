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

std::optional<rpc::ErrorType> ObjectRecoveryManager::RecoverObject(
    const ObjectID &object_id) {
  RAY_LOG(INFO) << "ObjectRecoveryDebug recover-object object_id=" << object_id;
  if (object_id.TaskId().IsForActorCreationTask()) {
    // The GCS manages all actor restarts, so we should never try to
    // reconstruct an actor here.
    return std::nullopt;
  }
  // Check the ReferenceCounter to see if there is a location for the object.
  bool owned_by_us = false;
  NodeID pinned_at;
  bool spilled = false;
  bool ref_exists = reference_counter_.IsPlasmaObjectPinnedOrSpilled(
      object_id, &owned_by_us, &pinned_at, &spilled);
  if (!ref_exists) {
    RAY_LOG(INFO).WithField(object_id) << "Cannot recover object: reference not found";
    return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_REF_NOT_FOUND;
  }

  if (!owned_by_us) {
    RAY_LOG(INFO).WithField(object_id)
        << "Cannot recover object: "
        << "borrowed objects cannot be reconstructed by non-owner";
    return rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE_BORROWED;
  }

  bool already_pending_recovery = true;
  bool requires_recovery = pinned_at.IsNil() && !spilled;
  RAY_LOG(INFO) << "ObjectRecoveryDebug recover-object-state object_id=" << object_id
                << " owned_by_us=" << owned_by_us << " pinned_at=" << pinned_at
                << " spilled=" << spilled << " requires_recovery=" << requires_recovery;
  if (requires_recovery) {
    {
      absl::MutexLock lock(&objects_pending_recovery_mu_);
      // Mark that we are attempting recovery for this object to prevent
      // duplicate restarts of the same object.
      already_pending_recovery = !objects_pending_recovery_.insert(object_id).second;
    }
  }

  if (!already_pending_recovery) {
    RAY_LOG(INFO).WithField(object_id) << "ObjectRecoveryDebug start-recovery";
    in_memory_store_.GetAsync(
        object_id, [this, object_id](const std::shared_ptr<RayObject> &obj) {
          {
            absl::MutexLock lock(&objects_pending_recovery_mu_);
            RAY_CHECK(objects_pending_recovery_.erase(object_id)) << object_id;
          }
          RAY_LOG(INFO).WithField(object_id) << "ObjectRecoveryDebug recovery-complete";
        });
    // Gets the node ids from reference_counter and then gets addresses from the local
    // gcs_client.
    object_lookup_(
        object_id,
        [this](const ObjectID &object_id_to_lookup, std::vector<rpc::Address> locations) {
          PinOrReconstructObject(object_id_to_lookup, std::move(locations));
        });
  } else if (requires_recovery) {
    RAY_LOG(INFO).WithField(object_id) << "ObjectRecoveryDebug recovery-already-pending";
  } else {
    RAY_LOG(INFO).WithField(object_id).WithField(pinned_at)
        << "ObjectRecoveryDebug skip-recovery-existing-location";
    // If the object doesn't exist in the memory store
    // (core_worker.cc removes the object from memory store before calling this method),
    // we need to add it back to indicate that it's available.
    // If the object is already in the memory store then the put is a no-op.
    in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                         object_id,
                         reference_counter_.HasReference(object_id));
  }
  return std::nullopt;
}

void ObjectRecoveryManager::PinOrReconstructObject(const ObjectID &object_id,
                                                   std::vector<rpc::Address> locations) {
  RAY_LOG(INFO).WithField(object_id)
      << "ObjectRecoveryDebug pin-or-reconstruct num_locations=" << locations.size();
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
  RAY_LOG(INFO).WithField(object_id).WithField(node_id)
      << "ObjectRecoveryDebug try-pin-existing-copy";

  raylet_client_pool_->GetOrConnectByAddress(raylet_address)
      ->PinObjectIDs(
          rpc_address_,
          {object_id},
          /*generator_id=*/ObjectID::Nil(),
          [this, object_id, other_locations = std::move(other_locations), node_id](
              const Status &status, const rpc::PinObjectIDsReply &reply) mutable {
            if (status.ok() && reply.successes(0)) {
              RAY_LOG(INFO).WithField(object_id).WithField(node_id)
                  << "ObjectRecoveryDebug pin-existing-copy-success";
              in_memory_store_.Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                                   object_id,
                                   reference_counter_.HasReference(object_id));
              reference_counter_.UpdateObjectPinnedAtRaylet(object_id, node_id);
            } else {
              RAY_LOG(INFO).WithField(object_id)
                  << "ObjectRecoveryDebug pin-existing-copy-failed status=" << status;
              PinOrReconstructObject(object_id, std::move(other_locations));
            }
          });
}

void ObjectRecoveryManager::ReconstructObject(const ObjectID &object_id) {
  LineageReconstructionEligibility eligibility =
      reference_counter_.GetLineageReconstructionEligibility(object_id);

  if (eligibility != LineageReconstructionEligibility::ELIGIBLE) {
    auto error_type_opt = ToErrorType(eligibility);
    rpc::ErrorType error_type = error_type_opt.value_or(rpc::ErrorType::OBJECT_LOST);
    RAY_LOG(INFO).WithField(object_id)
        << "Cannot recover object: " << rpc::ErrorType_Name(error_type);
    recovery_failure_callback_(object_id, error_type, /*pin_object=*/true);
    return;
  }

  RAY_LOG(INFO).WithField(object_id) << "ObjectRecoveryDebug reconstruct-object";
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
  RAY_LOG(INFO).WithField(object_id)
      << "ObjectRecoveryDebug reconstruct-resubmit-result task_id=" << task_id
      << " success=" << !error_type_optional.has_value()
      << " num_task_deps=" << task_deps.size()
      << (error_type_optional.has_value()
              ? std::string(" error_type=") + rpc::ErrorType_Name(*error_type_optional)
              : std::string());

  if (!error_type_optional.has_value()) {
    // Try to recover the task's dependencies.
    for (const auto &dep : task_deps) {
      auto error = RecoverObject(dep);
      if (error.has_value()) {
        RAY_LOG(INFO).WithField(dep)
            << "Cannot recover dependency: " << rpc::ErrorType_Name(*error);
        // This case can happen if the dependency was borrowed from another
        // worker, or if there was a bug in reconstruction that caused us to GC
        // the dependency ref.
        // We do not pin the dependency because we may not be the owner.
        recovery_failure_callback_(dep, *error, /*pin_object=*/false);
      }
    }
  } else {
    RAY_LOG(INFO).WithField(object_id)
        << "Cannot recover object: " << rpc::ErrorType_Name(*error_type_optional);
    reference_counter_.UpdateObjectPendingCreation(object_id, false);
    recovery_failure_callback_(object_id,
                               *error_type_optional,
                               /*pin_object=*/true);
  }
}

}  // namespace core
}  // namespace ray
