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

#include "ray/gcs_rpc_client/accessors/placement_group_info_accessor.h"

#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/util/container_util.h"

namespace ray {
namespace gcs {

namespace {
int64_t GetGcsTimeoutMs() {
  return absl::ToInt64Milliseconds(
      absl::Seconds(RayConfig::instance().gcs_server_request_timeout_seconds()));
}
}  // namespace

PlacementGroupInfoAccessor::PlacementGroupInfoAccessor(GcsClientContext *context)
    : context_(context) {}

Status PlacementGroupInfoAccessor::SyncCreatePlacementGroup(
    const ray::PlacementGroupSpecification &placement_group_spec) {
  rpc::CreatePlacementGroupRequest request;
  rpc::CreatePlacementGroupReply reply;
  request.mutable_placement_group_spec()->CopyFrom(placement_group_spec.GetMessage());
  auto status = context_->GetGcsRpcClient().SyncCreatePlacementGroup(
      std::move(request), &reply, GetGcsTimeoutMs());
  if (status.ok()) {
    RAY_LOG(DEBUG).WithField(placement_group_spec.PlacementGroupId())
        << "Finished registering placement group.";
  } else {
    RAY_LOG(ERROR).WithField(placement_group_spec.PlacementGroupId())
        << "Failed to be registered. " << status;
  }
  return status;
}

Status PlacementGroupInfoAccessor::SyncRemovePlacementGroup(
    const ray::PlacementGroupID &placement_group_id) {
  rpc::RemovePlacementGroupRequest request;
  rpc::RemovePlacementGroupReply reply;
  request.set_placement_group_id(placement_group_id.Binary());
  auto status = context_->GetGcsRpcClient().SyncRemovePlacementGroup(
      std::move(request), &reply, GetGcsTimeoutMs());
  return status;
}

Status PlacementGroupInfoAccessor::SyncGet(
    const PlacementGroupID &placement_group_id,
    PlacementGroupSpecification *placement_group_spec) {
  // TODO: Implement this method
  return Status::NotImplemented("SyncGet not implemented yet");
}

Status PlacementGroupInfoAccessor::SyncGetByName(
    const std::string &placement_group_name,
    const std::string &ray_namespace,
    PlacementGroupSpecification *placement_group_spec) {
  // TODO: Implement this method
  return Status::NotImplemented("SyncGetByName not implemented yet");
}

Status PlacementGroupInfoAccessor::SyncGetAll(
    std::vector<rpc::PlacementGroupTableData> *placement_group_info_list,
    int64_t timeout_ms) {
  // TODO: Implement this method
  return Status::NotImplemented("SyncGetAll not implemented yet");
}

Status PlacementGroupInfoAccessor::SyncWaitUntilReady(
    const PlacementGroupID &placement_group_id, int64_t timeout_seconds) {
  rpc::WaitPlacementGroupUntilReadyRequest request;
  rpc::WaitPlacementGroupUntilReadyReply reply;
  request.set_placement_group_id(placement_group_id.Binary());
  auto status = context_->GetGcsRpcClient().SyncWaitPlacementGroupUntilReady(
      std::move(request),
      &reply,
      absl::ToInt64Milliseconds(absl::Seconds(timeout_seconds)));
  RAY_LOG(DEBUG).WithField(placement_group_id)
      << "Finished waiting placement group until ready";
  return status;
}

void PlacementGroupInfoAccessor::AsyncGet(
    const PlacementGroupID &placement_group_id,
    const OptionalItemCallback<rpc::PlacementGroupTableData> &callback) {
  RAY_LOG(DEBUG).WithField(placement_group_id) << "Getting placement group info";
  rpc::GetPlacementGroupRequest request;
  request.set_placement_group_id(placement_group_id.Binary());
  context_->GetGcsRpcClient().GetPlacementGroup(
      std::move(request),
      [placement_group_id, callback](const Status &status,
                                     rpc::GetPlacementGroupReply &&reply) {
        if (reply.has_placement_group_table_data()) {
          callback(status, reply.placement_group_table_data());
        } else {
          callback(status, std::nullopt);
        }
        RAY_LOG(DEBUG).WithField(placement_group_id)
            << "Finished getting placement group info";
      });
}

void PlacementGroupInfoAccessor::AsyncGetByName(
    const std::string &name,
    const std::string &ray_namespace,
    const OptionalItemCallback<rpc::PlacementGroupTableData> &callback,
    int64_t timeout_ms) {
  RAY_LOG(DEBUG) << "Getting named placement group info, name = " << name;
  rpc::GetNamedPlacementGroupRequest request;
  request.set_name(name);
  request.set_ray_namespace(ray_namespace);
  context_->GetGcsRpcClient().GetNamedPlacementGroup(
      std::move(request),
      [name, callback](const Status &status, rpc::GetNamedPlacementGroupReply &&reply) {
        if (reply.has_placement_group_table_data()) {
          callback(status, reply.placement_group_table_data());
        } else {
          callback(status, std::nullopt);
        }
        RAY_LOG(DEBUG) << "Finished getting named placement group info, status = "
                       << status << ", name = " << name;
      },
      timeout_ms);
}

void PlacementGroupInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::PlacementGroupTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting all placement group info.";
  rpc::GetAllPlacementGroupRequest request;
  context_->GetGcsRpcClient().GetAllPlacementGroup(
      std::move(request),
      [callback](const Status &status, rpc::GetAllPlacementGroupReply &&reply) {
        callback(
            status,
            VectorFromProtobuf(std::move(*reply.mutable_placement_group_table_data())));
        RAY_LOG(DEBUG) << "Finished getting all placement group info, status = "
                       << status;
      });
}

}  // namespace gcs
}  // namespace ray
