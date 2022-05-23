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

#include "ray/core_worker/object_barrier.h"

namespace ray {

void ObjectBarrier::AddAssignOwnerRequest(
    const ObjectID object_id, const rpc::Address &owner_address,
    const rpc::Address &current_address,
    const std::vector<ObjectID> &contained_object_ids,
    const std::string &current_call_site, const size_t data_size,
    AssignOwnerReplyCallback callback) {
  io_service_.post(
      [this, object_id, owner_address, current_address, contained_object_ids,
       current_call_site, data_size, callback = std::move(callback)]() {
        AddAssignOwnerRequestInternal(object_id, owner_address, current_address,
                                      contained_object_ids, current_call_site, data_size,
                                      std::move(callback));
      },
      "ObjectBarrier.AddAssignOwnerRequest");
}

void ObjectBarrier::AddAssignOwnerRequestInternal(
    const ObjectID &object_id, const rpc::Address &owner_address,
    const rpc::Address &current_address,
    const std::vector<ObjectID> &contained_object_ids,
    const std::string &current_call_site, const size_t data_size,
    AssignOwnerReplyCallback callback) {
  rpc::WorkerAddress owner_worker_address(owner_address);
  auto it = assign_requests_->find(owner_worker_address);
  if (it == assign_requests_->end()) {
    rpc::BatchAssignObjectOwnerRequest request;
    request.mutable_borrower_address()->CopyFrom(current_address);

    auto timer =
        execute_after(io_service_,
                      [this, object_id, owner_address]() {
                        TrySendAssignOwnerRequest(object_id, owner_address);
                      },
                      ::RayConfig::instance().delay_send_assign_owner_request_ms());

    absl::flat_hash_set<ObjectID> objects_set = {};
    auto emplace_result = assign_requests_->try_emplace(
        owner_address, std::make_tuple(request, objects_set, timer));
    it = emplace_result.first;
    RAY_CHECK(emplace_result.second);
  }

  // Modify request.
  auto &request = std::get<0>(it->second);
  auto &object_set = std::get<1>(it->second);
  request.add_object_ids(object_id.Binary());
  request.add_object_sizes(data_size);
  request.add_call_sites(current_call_site);
  rpc::ContainedObjects contained_objects;
  for (auto &contained_object_id : contained_object_ids) {
    contained_objects.add_object_ids(contained_object_id.Binary());
  }
  request.add_contained_objects()->CopyFrom(contained_objects);
  RAY_CHECK(object_set.insert(object_id).second);

  (*object_callbacks_)[object_id].emplace_back(std::move(callback));

  if (object_set.size() >= ::RayConfig::instance().assign_owner_batch_size()) {
    SendAssignOwnerRequest(owner_address);
  }
}

void ObjectBarrier::TrySendAssignOwnerRequest(const ObjectID &object_id,
                                              const rpc::Address &owner_address) {
  rpc::WorkerAddress owner_worker_address(owner_address);
  auto pending_request_it = assign_requests_->find(owner_worker_address);
  if (pending_request_it == assign_requests_->end() ||
      std::get<1>(pending_request_it->second).count(object_id) == 0)
    return;

  SendAssignOwnerRequest(owner_address);
}

void ObjectBarrier::AsyncWaitForAssignmentFinish(const ObjectID &object_id,
                                                 AssignOwnerReplyCallback callback) {
  auto it = object_callbacks_->find(object_id);
  if (it == object_callbacks_->end()) {
    callback(Status::OK());
    return;
  }
  it->second.emplace_back(std::move(callback));
}

void ObjectBarrier::InvokeAllReplyCallbacks(const ObjectID &object_id,
                                            const Status &status) {
  auto it = object_callbacks_->find(object_id);
  if (it == object_callbacks_->end()) return;
  for (auto &cb : it->second) {
    cb(status);
  }
  object_callbacks_->erase(it);
}

void ObjectBarrier::SendAssignOwnerRequest(const rpc::Address &owner_address) {
  rpc::WorkerAddress owner_worker_address(owner_address);
  auto it = assign_requests_->find(owner_worker_address);
  RAY_CHECK(it != assign_requests_->end());

  // Cancel any asynchronous operations that are waiting on the timer.
  std::get<2>(it->second)->cancel();

  std::vector<ObjectID> object_ids;
  for (const auto &object_id : std::get<1>(it->second)) {
    object_ids.emplace_back(object_id);
  }

  auto conn = core_worker_client_pool_->GetOrConnect(owner_address);
  conn->BatchAssignObjectOwner(
      std::get<0>(it->second),
      [this, object_ids = std::move(object_ids)](
          const Status &status, const rpc::BatchAssignObjectOwnerReply &reply) {
        for (const auto &object_id : object_ids) {
          InvokeAllReplyCallbacks(object_id, status);
        }
      });
  assign_requests_->erase(owner_worker_address);
}

}  // namespace ray