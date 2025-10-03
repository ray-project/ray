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
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/gcs_callback_types.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// \class NodeResourceInfoAccessorInterface
/// Interface for NodeResourceInfo operations.
class NodeResourceInfoAccessorInterface {
 public:
  virtual ~NodeResourceInfoAccessorInterface() = default;

  /// Get available resources of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  virtual void AsyncGetAllAvailableResources(
      const MultiItemCallback<rpc::AvailableResources> &callback) = 0;

  /// Get total resources of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  virtual void AsyncGetAllTotalResources(
      const MultiItemCallback<rpc::TotalResources> &callback) = 0;

  /// Get draining nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  virtual void AsyncGetDrainingNodes(
      const ItemCallback<std::unordered_map<NodeID, int64_t>> &callback) = 0;

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  virtual void AsyncResubscribe() = 0;

  /// Get newest resource usage of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  virtual void AsyncGetAllResourceUsage(
      const ItemCallback<rpc::ResourceUsageBatchData> &callback) = 0;

  /// Get newest resource usage of all nodes from GCS synchronously.
  ///
  /// \param timeout_ms -1 means infinite.
  /// \param resource_usage_batch_data The resource usage of all nodes.
  /// \return Status
  virtual Status GetAllResourceUsage(int64_t timeout_ms,
                                     rpc::GetAllResourceUsageReply &reply) = 0;
};

}  // namespace gcs
}  // namespace ray
