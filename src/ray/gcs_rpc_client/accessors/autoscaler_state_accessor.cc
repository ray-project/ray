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

#include "ray/gcs_rpc_client/accessors/autoscaler_state_accessor.h"

#include "ray/gcs_rpc_client/rpc_client.h"

namespace ray {
namespace gcs {

AutoscalerStateAccessor::AutoscalerStateAccessor(GcsClientContext *context)
    : context_(context) {}

Status AutoscalerStateAccessor::RequestClusterResourceConstraint(
    int64_t timeout_ms,
    const std::vector<std::unordered_map<std::string, double>> &bundles,
    const std::vector<int64_t> &count_array) {
  rpc::autoscaler::RequestClusterResourceConstraintRequest request;
  rpc::autoscaler::RequestClusterResourceConstraintReply reply;
  RAY_CHECK_EQ(bundles.size(), count_array.size());
  for (size_t i = 0; i < bundles.size(); ++i) {
    const auto &bundle = bundles[i];
    auto count = count_array[i];

    auto new_resource_requests_by_count =
        request.mutable_cluster_resource_constraint()->add_resource_requests();

    new_resource_requests_by_count->mutable_request()->mutable_resources_bundle()->insert(
        bundle.begin(), bundle.end());
    new_resource_requests_by_count->set_count(count);
  }

  return context_->GetGcsRpcClient().SyncRequestClusterResourceConstraint(
      std::move(request), &reply, timeout_ms);
}

Status AutoscalerStateAccessor::GetClusterResourceState(int64_t timeout_ms,
                                                        std::string &serialized_reply) {
  rpc::autoscaler::GetClusterResourceStateRequest request;
  rpc::autoscaler::GetClusterResourceStateReply reply;

  RAY_RETURN_NOT_OK(context_->GetGcsRpcClient().SyncGetClusterResourceState(
      std::move(request), &reply, timeout_ms));

  if (!reply.SerializeToString(&serialized_reply)) {
    return Status::IOError("Failed to serialize GetClusterResourceState");
  }
  return Status::OK();
}

Status AutoscalerStateAccessor::GetClusterStatus(int64_t timeout_ms,
                                                 std::string &serialized_reply) {
  rpc::autoscaler::GetClusterStatusRequest request;
  rpc::autoscaler::GetClusterStatusReply reply;

  RAY_RETURN_NOT_OK(context_->GetGcsRpcClient().SyncGetClusterStatus(
      std::move(request), &reply, timeout_ms));

  if (!reply.SerializeToString(&serialized_reply)) {
    return Status::IOError("Failed to serialize GetClusterStatusReply");
  }
  return Status::OK();
}

void AutoscalerStateAccessor::AsyncGetClusterStatus(
    int64_t timeout_ms,
    const OptionalItemCallback<rpc::autoscaler::GetClusterStatusReply> &callback) {
  rpc::autoscaler::GetClusterStatusRequest request;
  context_->GetGcsRpcClient().GetClusterStatus(
      std::move(request),
      [callback](const Status &status, rpc::autoscaler::GetClusterStatusReply &&reply) {
        if (!status.ok()) {
          callback(status, std::nullopt);
          return;
        }
        callback(Status::OK(), std::move(reply));
      },
      timeout_ms);
}

Status AutoscalerStateAccessor::ReportAutoscalingState(
    int64_t timeout_ms, const std::string &serialized_state) {
  rpc::autoscaler::ReportAutoscalingStateRequest request;
  rpc::autoscaler::ReportAutoscalingStateReply reply;

  if (!request.mutable_autoscaling_state()->ParseFromString(serialized_state)) {
    return Status::IOError("Failed to parse ReportAutoscalingState");
  }
  return context_->GetGcsRpcClient().SyncReportAutoscalingState(
      std::move(request), &reply, timeout_ms);
}

Status AutoscalerStateAccessor::ReportClusterConfig(
    int64_t timeout_ms, const std::string &serialized_cluster_config) {
  rpc::autoscaler::ReportClusterConfigRequest request;
  rpc::autoscaler::ReportClusterConfigReply reply;

  if (!request.mutable_cluster_config()->ParseFromString(serialized_cluster_config)) {
    return Status::IOError("Failed to parse ClusterConfig");
  }
  return context_->GetGcsRpcClient().SyncReportClusterConfig(
      std::move(request), &reply, timeout_ms);
}

Status AutoscalerStateAccessor::DrainNode(const std::string &node_id,
                                          int32_t reason,
                                          const std::string &reason_message,
                                          int64_t deadline_timestamp_ms,
                                          int64_t timeout_ms,
                                          bool &is_accepted,
                                          std::string &rejection_reason_message) {
  rpc::autoscaler::DrainNodeRequest request;
  request.set_node_id(NodeID::FromHex(node_id).Binary());
  request.set_reason(static_cast<rpc::autoscaler::DrainNodeReason>(reason));
  request.set_reason_message(reason_message);
  request.set_deadline_timestamp_ms(deadline_timestamp_ms);

  rpc::autoscaler::DrainNodeReply reply;

  RAY_RETURN_NOT_OK(
      context_->GetGcsRpcClient().SyncDrainNode(std::move(request), &reply, timeout_ms));

  is_accepted = reply.is_accepted();
  if (!is_accepted) {
    rejection_reason_message = reply.rejection_reason_message();
  }
  return Status::OK();
}

}  // namespace gcs
}  // namespace ray
