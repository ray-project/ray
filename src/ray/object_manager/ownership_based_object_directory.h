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

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_client.h"
#include "ray/object_manager/format/object_manager_generated.h"
#include "ray/object_manager/object_directory.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace ray {

/// Ray OwnershipBasedObjectDirectory declaration.
class OwnershipBasedObjectDirectory : public ObjectDirectory {
 public:
  /// Create an ownership based object directory.
  ///
  /// \param io_service The event loop to dispatch callbacks to. This should
  /// usually be the same event loop that the given gcs_client runs on.
  /// \param gcs_client A Ray GCS client to request object and node
  /// information from.
  OwnershipBasedObjectDirectory(boost::asio::io_service &io_service,
                                std::shared_ptr<gcs::GcsClient> &gcs_client);

  virtual ~OwnershipBasedObjectDirectory() {}

  ray::Status LookupLocations(const ObjectID &object_id,
                              const rpc::Address &owner_address,
                              const OnLocationsFound &callback) override;

  ray::Status SubscribeObjectLocations(const UniqueID &callback_id,
                                       const ObjectID &object_id,
                                       const rpc::Address &owner_address,
                                       const OnLocationsFound &callback) override;
  ray::Status UnsubscribeObjectLocations(const UniqueID &callback_id,
                                         const ObjectID &object_id) override;

  ray::Status ReportObjectAdded(
      const ObjectID &object_id, const NodeID &node_id,
      const object_manager::protocol::ObjectInfoT &object_info) override;
  ray::Status ReportObjectRemoved(
      const ObjectID &object_id, const NodeID &node_id,
      const object_manager::protocol::ObjectInfoT &object_info) override;

  std::string DebugString() const override;

  /// OwnershipBasedObjectDirectory should not be copied.
  RAY_DISALLOW_COPY_AND_ASSIGN(OwnershipBasedObjectDirectory);

 private:
  /// The client call manager used to create the RPC clients.
  rpc::ClientCallManager client_call_manager_;
  /// Cache of gRPC clients to workers (not necessarily running on this node).
  /// Also includes the number of inflight requests to each worker - when this
  /// reaches zero, the client will be deleted and a new one will need to be created
  /// for any subsequent requests.
  absl::flat_hash_map<WorkerID, std::shared_ptr<rpc::CoreWorkerClient>>
      worker_rpc_clients_;

  /// Get or create the rpc client in the worker_rpc_clients.
  std::shared_ptr<rpc::CoreWorkerClient> GetClient(const rpc::Address &owner_address);

  /// Internal callback function used by SubscribeObjectLocations.
  void SubscriptionCallback(ObjectID object_id, WorkerID worker_id, Status status,
                            const rpc::GetObjectLocationsOwnerReply &reply);
};

}  // namespace ray
