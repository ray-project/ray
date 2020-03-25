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
  bool pinned;
  if (!pinned_objects_->IsPlasmaObjectPinned(object_id, &pinned)) {
    return Status::Invalid(
        "Object reference no longer exists or is not owned by us. Either lineage pinning "
        "is disabled or this object ID is borrowed.");
  }

  bool already_pending_recovery = true;
  if (!pinned) {
    {
      absl::MutexLock lock(&mu_);
      // Mark that we are attempting recovery for this object to prevent
      // duplicate reconstructions of the same object.
      bool inserted = objects_pending_recovery_.insert(object_id).second;
      RAY_LOG(DEBUG) << "Starting recovery for object " << object_id;
      if (inserted) {
        in_memory_store_->GetAsync(
            object_id, [this, object_id](std::shared_ptr<RayObject> obj) {
              absl::MutexLock lock(&mu_);
              RAY_CHECK(objects_pending_recovery_.erase(object_id)) << object_id;
              RAY_LOG(DEBUG) << "Recovery complete for object " << object_id;
            });
        already_pending_recovery = false;
      }
    }
  }

  if (!already_pending_recovery) {
    AttemptObjectRecovery(object_id);
  }
  return Status::OK();
}

Status ObjectRecoveryManager::AttemptObjectRecovery(const ObjectID &object_id) {
  // Lookup the object in the GCS to find another copy.
  return object_directory_.AsyncGetLocations(
      object_id,
      [this, object_id](Status status, const std::vector<rpc::ObjectTableData> &result) {
        RAY_CHECK_OK(status);
        bool pinned = PinNewObjectCopy(object_id, result);
        if (!pinned) {
          if (RayConfig::instance().lineage_pinning_enabled()) {
            // If we could not find another copy to pin, try to reconstruct the
            // object.
            ReconstructObject(object_id);
          } else {
            reconstruction_failure_callback_(object_id, /*pin_object=*/true);
          }
        }
      });
}

bool ObjectRecoveryManager::PinNewObjectCopy(const ObjectID &object_id,
                                  const std::vector<rpc::ObjectTableData> &locations) {
  // If a copy still exists, pin the object by sending a
  // PinObjectIDs RPC.
  RAY_LOG(INFO) << "Lost object " << object_id << " has " << locations.size() << " locations";
  for (auto it = locations.begin(); it != locations.end(); it++) {
    const auto new_node_id = ClientID::FromBinary(it->manager());
    auto new_node = node_directory_.Get(new_node_id);
    RAY_CHECK(new_node.has_value());
    RAY_LOG(INFO) << "Lost object " << object_id << " on node " << new_node_id << " alive? " << ((new_node->state() == rpc::GcsNodeInfo::ALIVE) ? 1 : 0);
    if (new_node->state() == rpc::GcsNodeInfo::ALIVE) {
      RAY_LOG(DEBUG) << "Trying to pin copy of lost object " << object_id << " at node "
                     << new_node_id;

      std::shared_ptr<ObjectPinningInterface> client;
      if (new_node_id == ClientID::FromBinary(rpc_address_.raylet_id())) {
        client = local_object_pinning_client_;
      } else {
        auto client_it = remote_object_pinning_clients_.find(new_node_id);
        if (client_it == remote_object_pinning_clients_.end()) {
          RAY_LOG(DEBUG) << "Connecting to raylet " << new_node_id;
          client_it = remote_object_pinning_clients_
                   .emplace(new_node_id, client_factory_(new_node->node_manager_address(), new_node->node_manager_port()))
                   .first;
        }
        client = client_it->second;
      }

      auto status = client->PinObjectIDs(
          rpc_address_, {object_id},
          [this, object_id, new_node_id](const Status &status,
                                         const rpc::PinObjectIDsReply &reply) {
            if (status.ok()) {
              // TODO(swang): Make sure that the node is still alive when
              // marking the object as pinned.
              pinned_objects_->MarkPlasmaObjectsPinnedAt({object_id}, new_node_id);
              RAY_CHECK(in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                      object_id));
            } else {
              RAY_LOG(INFO) << "Error pinning new copy of lost object " << object_id
                            << ", trying again";
              auto status = AttemptObjectRecovery(object_id);
              if (!status.ok()) {
                reconstruction_failure_callback_(object_id, /*pin_object=*/true);
              }
            }
          });
      if (status.ok()) {
        return true;
      }
    }
  }

  return false;
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
        RAY_LOG(INFO) << "Failed to reconstruct object " << dep << ": " << status.message();
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
