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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/rpc/worker/core_worker_client_pool.h"
#include "ray/util/util.h"

namespace ray {

typedef std::function<void(const Status &status)> AssignOwnerReplyCallback;

// Allows waiting at the specified place for the reply of the asynchronous
// request to avoid race-condition.
class ObjectBarrier {
 public:
  explicit ObjectBarrier(
      instrumented_io_context &io_service,
      std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool)
      : io_service_(io_service),
        core_worker_client_pool_(std::move(core_worker_client_pool)) {}

  /// Add one object to BatchAssignObjectOwnerRequest.
  /// When the number of objects reaches the threshold or the time since the
  /// first object joined the request exceeds the threshold, it will be sent to the owner.
  ///
  /// \param object_id The ID of the adding object.
  /// \param owner_address The owner address of the adding object.
  /// \param current_address The current worker address.
  /// \param contained_object_ids contained_object_ids The IDs serialized in this object.
  /// \param current_call_site The caller ID used to submit tasks from this worker to an
  /// actor. \param data_size The data size of the adding object. \param callback The
  /// callback will invoke when the request reply. \return void
  void AddAssignOwnerRequest(const ObjectID object_id, const rpc::Address &owner_address,
                             const rpc::Address &current_address,
                             const std::vector<ObjectID> &contained_object_ids,
                             const std::string &current_call_site, const size_t data_size,
                             AssignOwnerReplyCallback callback);

  /// Send the BatchAssignObjectOwnerRequest which contain the object to the owner.
  ///
  /// \param object_id The ID of the adding object.
  /// \param owner_address The owner address of the adding object.
  /// \return void
  void TrySendAssignOwnerRequest(const ObjectID &object_id,
                                 const rpc::Address &owner_address);

  /// Asynchronously wait until the object owner assigned successfully.
  ///
  /// \param object_id The ID of the adding object.
  /// \param callback The callback that will be called after actor registered
  /// \return void
  void AsyncWaitForAssignmentFinish(const ObjectID &object_id,
                                    AssignOwnerReplyCallback callback);

 private:
  void AddAssignOwnerRequestInternal(const ObjectID &object_id,
                                     const rpc::Address &owner_address,
                                     const rpc::Address &current_address,
                                     const std::vector<ObjectID> &contained_object_ids,
                                     const std::string &current_call_site,
                                     const size_t data_size,
                                     AssignOwnerReplyCallback callback);

  void InvokeAllReplyCallbacks(const ObjectID &object_id, const Status &status);

  void SendAssignOwnerRequest(const rpc::Address &owner_address);

  instrumented_io_context &io_service_;

  std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool_;

  ThreadPrivate<absl::flat_hash_map<ObjectID, std::vector<AssignOwnerReplyCallback>>>
      object_callbacks_;
  ThreadPrivate<absl::flat_hash_map<
      rpc::WorkerAddress,
      std::tuple<rpc::BatchAssignObjectOwnerRequest, absl::flat_hash_set<ObjectID>,
                 std::shared_ptr<boost::asio::deadline_timer>>>>
      assign_requests_;
};

}  // namespace ray