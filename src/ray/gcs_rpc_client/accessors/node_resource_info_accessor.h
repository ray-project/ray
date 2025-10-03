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

#include "ray/gcs_rpc_client/accessors/node_resource_info_accessor_interface.h"
#include "ray/gcs_rpc_client/gcs_client_context.h"
#include "ray/util/sequencer.h"

namespace ray {
namespace gcs {

using SubscribeOperation = std::function<Status(const StatusCallback &done)>;
using ResourceMap = std::unordered_map<std::string, double>;

/// \class NodeResourceInfoAccessor
/// Implementation of NodeResourceInfoAccessorInterface.
class NodeResourceInfoAccessor : public NodeResourceInfoAccessorInterface {
 public:
  NodeResourceInfoAccessor() = default;
  explicit NodeResourceInfoAccessor(GcsClientContext *context);
  virtual ~NodeResourceInfoAccessor() = default;

  /// Get available resources of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  virtual void AsyncGetAllAvailableResources(
      const MultiItemCallback<rpc::AvailableResources> &callback) override;

  /// Get total resources of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  virtual void AsyncGetAllTotalResources(
      const MultiItemCallback<rpc::TotalResources> &callback) override;

  /// Get draining nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  virtual void AsyncGetDrainingNodes(
      const ItemCallback<std::unordered_map<NodeID, int64_t>> &callback) override;

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  /// PubSub server restart will cause GCS server restart. In this case, we need to
  /// resubscribe from PubSub server, otherwise we only need to fetch data from GCS
  /// server.
  void AsyncResubscribe() override;

  /// Get newest resource usage of all nodes from GCS asynchronously.
  ///
  /// \param callback Callback that will be called after lookup finishes.
  void AsyncGetAllResourceUsage(
      const ItemCallback<rpc::ResourceUsageBatchData> &callback) override;

  /// Get newest resource usage of all nodes from GCS synchronously.
  ///
  /// \param timeout_ms -1 means infinite.
  /// \param resource_usage_batch_data The resource usage of all nodes.
  /// \return Status
  virtual Status GetAllResourceUsage(int64_t timeout_ms,
                                     rpc::GetAllResourceUsageReply &reply) override;

 private:
  /// Save the subscribe operation in this function, so we can call it again when PubSub
  /// server restarts from a failure.
  SubscribeOperation subscribe_resource_operation_;
  SubscribeOperation subscribe_batch_resource_usage_operation_;

  GcsClientContext *context_;

  Sequencer<NodeID> sequencer_;
};

}  // namespace gcs
}  // namespace ray
