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
namespace core {

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
      [this, object_id, owner_address](const Status &status,
                                       const rpc::GetObjectStatusReply &reply) {
        ProcessResolvedObject(object_id, owner_address, status, reply);
      });
}

void FutureResolver::ProcessResolvedObject(const ObjectID &object_id,
                                           const rpc::Address &owner_address,
                                           const Status &status,
                                           const rpc::GetObjectStatusReply &reply) {
  if (!status.ok()) {
    RAY_LOG(WARNING) << "Error retrieving the value of object ID " << object_id
                     << " that was deserialized: " << status.ToString();
  }

  if (!status.ok()) {
    // The owner is unreachable. Store an error so that an exception will be
    // thrown immediately when the worker tries to get the value.
    RAY_UNUSED(in_memory_store_->Put(RayObject(rpc::ErrorType::OWNER_DIED), object_id));
  } else if (reply.status() == rpc::GetObjectStatusReply::OUT_OF_SCOPE) {
    // The owner replied that the object has gone out of scope (this is an edge
    // case in the distributed ref counting protocol where a borrower dies
    // before it can notify the owner of another borrower). Store an error so
    // that an exception will be thrown immediately when the worker tries to
    // get the value.
    RAY_UNUSED(
        in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_DELETED), object_id));
  } else if (reply.status() == rpc::GetObjectStatusReply::CREATED) {
    // The object is either an indicator that the object is in Plasma, or
    // the object has been returned directly in the reply. In either
    // case, we put the corresponding RayObject into the in-memory store.
    // If the owner later fails or the object is released, the raylet
    // will eventually store an error in Plasma on our behalf.

    // We save the returned locality data first in order to ensure that it
    // is available for any tasks whose submission is triggered by the in-memory
    // store Put().
    absl::flat_hash_set<NodeID> locations;
    for (const auto &node_id : reply.node_ids()) {
      locations.emplace(NodeID::FromBinary(node_id));
    }
    report_locality_data_callback_(object_id, locations, reply.object_size());

    // Put the RayObject into the in-memory store.
    const auto &data = reply.object().data();
    std::shared_ptr<LocalMemoryBuffer> data_buffer;
    if (data.size() > 0) {
      RAY_LOG(DEBUG) << "Object returned directly in GetObjectStatus reply, putting "
                     << object_id << " in memory store";
      data_buffer = std::make_shared<LocalMemoryBuffer>(
          const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(data.data())),
          data.size());
    } else {
      RAY_LOG(DEBUG) << "Object not returned directly in GetObjectStatus reply, "
                     << object_id << " will have to be fetched from Plasma";
    }
    const auto &metadata = reply.object().metadata();
    std::shared_ptr<LocalMemoryBuffer> metadata_buffer;
    if (metadata.size() > 0) {
      metadata_buffer = std::make_shared<LocalMemoryBuffer>(
          const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(metadata.data())),
          metadata.size());
    }
    auto inlined_refs =
        VectorFromProtobuf<rpc::ObjectReference>(reply.object().nested_inlined_refs());
    for (const auto &inlined_ref : inlined_refs) {
      reference_counter_->AddBorrowedObject(ObjectID::FromBinary(inlined_ref.object_id()),
                                            object_id,
                                            inlined_ref.owner_address());
    }
    RAY_UNUSED(in_memory_store_->Put(
        RayObject(data_buffer, metadata_buffer, inlined_refs), object_id));
  }
}

}  // namespace core
}  // namespace ray
