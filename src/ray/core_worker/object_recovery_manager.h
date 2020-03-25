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
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_manager.h"
#include "ray/core_worker/reference_count.h"
#include "ray/raylet/raylet_client.h"

namespace ray {

typedef std::function<std::shared_ptr<ObjectPinningInterface>(const std::string &ip_address,
                                                            int port)>
    ObjectPinningClientFactoryFn;

class ObjectRecoveryManager {
 public:
  ObjectRecoveryManager(
                  const rpc::Address &rpc_address,
                  ObjectPinningClientFactoryFn client_factory,
                  std::shared_ptr<ObjectPinningInterface> local_object_pinning_client,
                  gcs::ObjectInfoAccessor &object_directory,
                  gcs::NodeInfoAccessor &node_directory,
                  std::shared_ptr<TaskResubmissionInterface> task_resubmitter,
                  std::shared_ptr<PinnedObjectsInterface> pinned_objects,
                  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store,
                  std::function<void(const ObjectID &object_id, bool pin_object)> reconstruction_failure_callback
                  )
    : rpc_address_(rpc_address),
    client_factory_(client_factory),
    local_object_pinning_client_(local_object_pinning_client),
    object_directory_(object_directory),
    node_directory_(node_directory),
    task_resubmitter_(task_resubmitter),
    pinned_objects_(pinned_objects),
    in_memory_store_(in_memory_store),
    reconstruction_failure_callback_(reconstruction_failure_callback) {}

  Status RecoverObject(const ObjectID &object_id);

 private:
  Status AttemptObjectRecovery(const ObjectID &object_id);

  void ReconstructObject(const ObjectID &object_id);

  bool PinNewObjectCopy(const ObjectID &object_id,
                        const std::vector<rpc::ObjectTableData> &locations);

  /// Used to resubmit tasks.
  std::shared_ptr<TaskResubmissionInterface> task_resubmitter_;

  /// Used to get and set pinned objects.
  std::shared_ptr<PinnedObjectsInterface> pinned_objects_;

  /// Address of our RPC server.
  rpc::Address rpc_address_;

  /// Factory for producing new clients to pin objects at remote nodes.
  const ObjectPinningClientFactoryFn client_factory_;

  // Client that can be used to pin objects from the local raylet.
  std::shared_ptr<ObjectPinningInterface> local_object_pinning_client_;

  /// Global database of objects.
  gcs::ObjectInfoAccessor &object_directory_;

  /// Global database of raylet nodes.
  gcs::NodeInfoAccessor &node_directory_;

  /// Used to store object values (InPlasmaError or UnreconstructableError)
  /// once reconstruction finishes.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  std::function<void(const ObjectID &object_id)> reconstruction_success_callback_;

  std::function<void(const ObjectID &object_id, bool pin_object)> reconstruction_failure_callback_;

  /// Protects below fields.
  mutable absl::Mutex mu_;

  /// Cache of gRPC clients to remote raylets.
  absl::flat_hash_map<ClientID, std::shared_ptr<ObjectPinningInterface>>
      remote_object_pinning_clients_ GUARDED_BY(mu_);

  /// Objects that are currently pending recovery.
  absl::flat_hash_set<ObjectID> objects_pending_recovery_ GUARDED_BY(mu_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_OBJECT_RECOVERY_MANAGER_H
