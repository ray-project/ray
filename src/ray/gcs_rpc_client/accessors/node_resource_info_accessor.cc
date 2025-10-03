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

#include "ray/gcs_rpc_client/accessors/node_resource_info_accessor.h"

#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/pubsub/gcs_subscriber.h"
#include "ray/util/container_util.h"

namespace ray {
namespace gcs {

NodeResourceInfoAccessor::NodeResourceInfoAccessor(GcsClientContext *context)
    : context_(context) {}

void NodeResourceInfoAccessor::AsyncGetAllAvailableResources(
    const MultiItemCallback<rpc::AvailableResources> &callback) {
  rpc::GetAllAvailableResourcesRequest request;
  context_->GetGcsRpcClient().GetAllAvailableResources(
      std::move(request),
      [callback](const Status &status, rpc::GetAllAvailableResourcesReply &&reply) {
        callback(status, VectorFromProtobuf(std::move(*reply.mutable_resources_list())));
        RAY_LOG(DEBUG) << "Finished getting available resources of all nodes, status = "
                       << status;
      });
}

void NodeResourceInfoAccessor::AsyncGetAllTotalResources(
    const MultiItemCallback<rpc::TotalResources> &callback) {
  rpc::GetAllTotalResourcesRequest request;
  context_->GetGcsRpcClient().GetAllTotalResources(
      std::move(request),
      [callback](const Status &status, rpc::GetAllTotalResourcesReply &&reply) {
        callback(status, VectorFromProtobuf(std::move(*reply.mutable_resources_list())));
        RAY_LOG(DEBUG) << "Finished getting total resources of all nodes, status = "
                       << status;
      });
}

void NodeResourceInfoAccessor::AsyncGetDrainingNodes(
    const ItemCallback<std::unordered_map<NodeID, int64_t>> &callback) {
  rpc::GetDrainingNodesRequest request;
  context_->GetGcsRpcClient().GetDrainingNodes(
      std::move(request),
      [callback](const Status &status, rpc::GetDrainingNodesReply &&reply) {
        RAY_CHECK_OK(status);
        std::unordered_map<NodeID, int64_t> draining_nodes;
        for (const auto &draining_node : reply.draining_nodes()) {
          draining_nodes[NodeID::FromBinary(draining_node.node_id())] =
              draining_node.draining_deadline_timestamp_ms();
        }
        callback(std::move(draining_nodes));
      });
}

void NodeResourceInfoAccessor::AsyncResubscribe() {
  RAY_LOG(DEBUG) << "Reestablishing subscription for node resource info.";
  if (subscribe_resource_operation_ != nullptr) {
    RAY_CHECK_OK(subscribe_resource_operation_(nullptr));
  }
  if (subscribe_batch_resource_usage_operation_ != nullptr) {
    RAY_CHECK_OK(subscribe_batch_resource_usage_operation_(nullptr));
  }
}

void NodeResourceInfoAccessor::AsyncGetAllResourceUsage(
    const ItemCallback<rpc::ResourceUsageBatchData> &callback) {
  rpc::GetAllResourceUsageRequest request;
  context_->GetGcsRpcClient().GetAllResourceUsage(
      std::move(request),
      [callback](const Status &status, rpc::GetAllResourceUsageReply &&reply) {
        callback(std::move(*reply.mutable_resource_usage_data()));
        RAY_LOG(DEBUG) << "Finished getting resource usage of all nodes, status = "
                       << status;
      });
}

Status NodeResourceInfoAccessor::GetAllResourceUsage(
    int64_t timeout_ms, rpc::GetAllResourceUsageReply &reply) {
  rpc::GetAllResourceUsageRequest request;
  return context_->GetGcsRpcClient().SyncGetAllResourceUsage(
      std::move(request), &reply, timeout_ms);
}

}  // namespace gcs
}  // namespace ray
