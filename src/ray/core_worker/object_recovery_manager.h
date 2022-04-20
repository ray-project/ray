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

#pragma once

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_manager.h"
#include "ray/raylet_client/raylet_client.h"

namespace ray {
namespace core {

typedef std::function<std::shared_ptr<PinObjectsInterface>(const std::string &ip_address,
                                                           int port)>
    ObjectPinningClientFactoryFn;

typedef std::function<void(const ObjectID &object_id,
                           const std::vector<rpc::Address> &raylet_locations)>
    ObjectLookupCallback;

// A callback for if we fail to recover an object.
typedef std::function<void(
    const ObjectID &object_id, rpc::ErrorType reason, bool pin_object)>
    ObjectRecoveryFailureCallback;

class ObjectRecoveryManager {
 public:
  ObjectRecoveryManager(
      const rpc::Address &rpc_address,
      ObjectPinningClientFactoryFn client_factory,
      std::shared_ptr<PinObjectsInterface> local_object_pinning_client,
      std::function<Status(const ObjectID &object_id,
                           const ObjectLookupCallback &callback)> object_lookup,
      std::shared_ptr<TaskResubmissionInterface> task_resubmitter,
      std::shared_ptr<ReferenceCounter> reference_counter,
      std::shared_ptr<CoreWorkerMemoryStore> in_memory_store,
      const ObjectRecoveryFailureCallback &recovery_failure_callback)
      : task_resubmitter_(task_resubmitter),
        reference_counter_(reference_counter),
        rpc_address_(rpc_address),
        client_factory_(client_factory),
        local_object_pinning_client_(local_object_pinning_client),
        object_lookup_(object_lookup),
        in_memory_store_(in_memory_store),
        recovery_failure_callback_(recovery_failure_callback) {}

  /// Recover an object that was stored in plasma. This will only succeed for
  /// objects that are lost from memory and that this process owns (returns
  /// Status::Invalid if false).  This method is idempotent for overlapping
  /// recovery operations on the same object. This class will guarantee that
  /// each recovery operation ends in either success (by storing a new value
  /// for the object in the direct memory/plasma store) or failure (by calling
  /// the given reconstruction failure callback).
  ///
  /// Algorithm:
  /// 1. Check that the object is missing from the direct memory store and that
  /// we own the object. If either is false, then fail the recovery operation.
  /// 2. Look up the object in the global directory to check for other
  /// locations of the object. If another location exists, attempt to pin it.
  /// If the pinning is successful, then mark the recovery as a success by
  /// storing a new value for the object in the direct memory store.
  /// 3. If pinning fails at all locations for the object (or there are no
  /// locations), attempt to reconstruct the object by resubmitting the task
  /// that created the object. If the task resubmission fails, then the
  /// fail the recovery operation.
  /// 4. If task resubmission succeeds, recursively attempt to recover any
  /// plasma arguments to the task. The recovery operation will succeed once
  /// the task completes and stores a new value for its return object.
  ///
  /// \return True if recovery for the object has successfully started, false
  /// if the object is not recoverable because we do not have any metadata
  /// about the object. If this returns true, then eventually recovery will
  /// either succeed (a value will be put into the memory store) or fail (the
  /// reconstruction failure callback will be called for this object).
  bool RecoverObject(const ObjectID &object_id);

 private:
  /// Pin a new copy for a lost object from the given locations or, if that
  /// fails, attempt to reconstruct it by resubmitting the task that created
  /// the object.
  void PinOrReconstructObject(const ObjectID &object_id,
                              const std::vector<rpc::Address> &locations);

  /// Pin a new copy for the object at the given location. If that fails, then
  /// try one of the other locations.
  void PinExistingObjectCopy(const ObjectID &object_id,
                             const rpc::Address &raylet_address,
                             const std::vector<rpc::Address> &other_locations);

  /// Reconstruct an object by resubmitting the task that created it.
  void ReconstructObject(const ObjectID &object_id);

  /// Used to resubmit tasks.
  std::shared_ptr<TaskResubmissionInterface> task_resubmitter_;

  /// Used to check whether we own an object.
  std::shared_ptr<ReferenceCounter> reference_counter_;

  /// Address of our RPC server.
  rpc::Address rpc_address_;

  /// Factory for producing new clients to pin objects at remote nodes.
  const ObjectPinningClientFactoryFn client_factory_;

  // Client that can be used to pin objects from the local raylet.
  std::shared_ptr<PinObjectsInterface> local_object_pinning_client_;

  /// Function to lookup an object's locations from the global database.
  const std::function<Status(const ObjectID &object_id,
                             const ObjectLookupCallback &callback)>
      object_lookup_;

  /// Used to store object values (InPlasmaError) if recovery succeeds.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  /// Callback to call if recovery fails.
  const ObjectRecoveryFailureCallback recovery_failure_callback_;

  /// Protects below fields.
  mutable absl::Mutex mu_;

  /// Cache of gRPC clients to remote raylets for pinning objects.
  absl::flat_hash_map<NodeID, std::shared_ptr<PinObjectsInterface>>
      remote_object_pinning_clients_ GUARDED_BY(mu_);

  /// Objects that are currently pending recovery. Calls to RecoverObject for
  /// objects currently in this set are idempotent.
  absl::flat_hash_set<ObjectID> objects_pending_recovery_ GUARDED_BY(mu_);
};

}  // namespace core
}  // namespace ray
