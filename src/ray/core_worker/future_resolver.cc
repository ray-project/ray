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

#include "ray/core_worker/future_resolver.h"

namespace ray {

void FutureResolver::ResolveFutureAsync(const ObjectID &object_id,
                                        const rpc::Address &owner_address) {
  if (rpc_address_.worker_id() == owner_address.worker_id()) {
    // We do not need to resolve objects that we own. This can happen if a task
    // with a borrowed reference executes on the object's owning worker.
    return;
  }
  auto conn = owner_clients_->GetOrConnect(owner_address);

  rpc::GetObjectStatusRequest request;
  request.set_object_id(object_id.Binary());
  request.set_owner_worker_id(owner_address.worker_id());
  conn->GetObjectStatus(
      request,
      [this, object_id](const Status &status, const rpc::GetObjectStatusReply &reply) {
        if (!status.ok()) {
          RAY_LOG(WARNING) << "Error retrieving the value of object ID " << object_id
                           << " that was deserialized: " << status.ToString();
        }

        if (!status.ok() || reply.status() == rpc::GetObjectStatusReply::OUT_OF_SCOPE) {
          // The owner is gone or the owner replied that the object has gone
          // out of scope (this is an edge case in the distributed ref counting
          // protocol where a borrower dies before it can notify the owner of
          // another borrower). Store an error so that an exception will be
          // thrown immediately when the worker tries to get the value.
          RAY_UNUSED(in_memory_store_->Put(
              RayObject(rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE), object_id));
        } else {
          // We can now try to fetch the object via plasma. If the owner later
          // fails or the object is released, the raylet will eventually store
          // an error in plasma on our behalf.
          RAY_UNUSED(in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                                           object_id));
        }
      });
}

}  // namespace ray
