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

void FutureResolver::ResolveFutureAsync(const ObjectID &object_id, const TaskID &owner_id,
                                        const rpc::Address &owner_address) {
  RAY_CHECK(object_id.IsDirectCallType());
  absl::MutexLock lock(&mu_);
  auto it = owner_clients_.find(owner_id);
  if (it == owner_clients_.end()) {
    auto client =
        std::shared_ptr<rpc::CoreWorkerClientInterface>(client_factory_(owner_address));
    it = owner_clients_.emplace(owner_id, std::move(client)).first;
  }

  rpc::GetObjectStatusRequest request;
  request.set_object_id(object_id.Binary());
  request.set_owner_id(owner_id.Binary());
  RAY_CHECK_OK(it->second->GetObjectStatus(
      request,
      [this, object_id](const Status &status, const rpc::GetObjectStatusReply &reply) {
        if (!status.ok()) {
          RAY_LOG(WARNING) << "Error retrieving the value of object ID " << object_id
                           << " that was deserialized: " << status.ToString();
        }
        // Either the owner is gone or the owner replied that the object has
        // been created. In both cases, we can now try to fetch the object via
        // plasma.
        RAY_UNUSED(in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                                         object_id));
      }));
}

}  // namespace ray
