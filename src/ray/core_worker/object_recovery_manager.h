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

#ifndef RAY_CORE_WORKER_OBJECT_RECOVERY_MANAGER_H
#define RAY_CORE_WORKER_OBJECT_RECOVERY_MANAGER_H

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_manager.h"
#include "ray/raylet/raylet_client.h"

namespace ray {

typedef std::function<std::shared_ptr<PinObjectsInterface>(const std::string &ip_address,
                                                           int port)>
    ObjectPinningClientFactoryFn;

typedef std::function<void(const ObjectID &object_id,
                           const std::vector<rpc::Address> &raylet_locations)>
    ObjectLookupCallback;

class ObjectRecoveryManager {
 public:
  ObjectRecoveryManager(const rpc::Address &rpc_address,
                        ObjectPinningClientFactoryFn client_factory,
                        std::shared_ptr<PinObjectsInterface> local_object_pinning_client,
                        std::function<Status(const ObjectID &object_id,
                                             const ObjectLookupCallback &callback)>
                            object_lookup,
                        std::shared_ptr<TaskResubmissionInterface> task_resubmitter,
                        std::shared_ptr<ReferenceCounter> reference_counter,
                        std::shared_ptr<CoreWorkerMemoryStore> in_memory_store,
                        std::function<void(const ObjectID &object_id, bool pin_object)>
                            reconstruction_failure_callback,
                        bool lineage_reconstruction_enabled)
      : rpc_address_(rpc_address),
        client_factory_(client_factory),
        local_object_pinning_client_(local_object_pinning_client),
        object_lookup_(object_lookup),
        task_resubmitter_(task_resubmitter),
        reference_counter_(reference_counter),
        in_memory_store_(in_memory_store),
        reconstruction_failure_callback_(reconstruction_failure_callback),
        lineage_reconstruction_enabled_(lineage_reconstruction_enabled) {}

  Status RecoverObject(const ObjectID &object_id);

 private:
  Status AttemptObjectRecovery(const ObjectID &object_id);

  void ReconstructObject(const ObjectID &object_id);

  Status PinNewObjectCopy(const ObjectID &object_id, const rpc::Address &raylet_address);

  /// Used to resubmit tasks.
  std::shared_ptr<TaskResubmissionInterface> task_resubmitter_;

  /// Used to get and set pinned objects.
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

  /// Used to store object values (InPlasmaError or UnreconstructableError)
  /// once reconstruction finishes.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  const std::function<void(const ObjectID &object_id, bool pin_object)>
      reconstruction_failure_callback_;

  const bool lineage_reconstruction_enabled_;

  /// Protects below fields.
  mutable absl::Mutex mu_;

  /// Cache of gRPC clients to remote raylets.
  absl::flat_hash_map<ClientID, std::shared_ptr<PinObjectsInterface>>
      remote_object_pinning_clients_ GUARDED_BY(mu_);

  /// Objects that are currently pending recovery.
  absl::flat_hash_set<ObjectID> objects_pending_recovery_ GUARDED_BY(mu_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_OBJECT_RECOVERY_MANAGER_H
