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

#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"

namespace ray {
namespace gcs {

void GcsVirtualClusterManager::Initialize(const GcsInitData &gcs_init_data) {
  // TODO(Shanly): To be implement.
}

void GcsVirtualClusterManager::HandleCreateOrUpdateVirtualCluster(
    rpc::CreateOrUpdateVirtualClusterRequest request,
    rpc::CreateOrUpdateVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &virtual_cluster_id = request.virtual_cluster_id();
  RAY_LOG(INFO) << "Start creating or updating virtual cluster " << virtual_cluster_id;
  // TODO(Shanly): To be implement.
}

void GcsVirtualClusterManager::HandleRemoveVirtualCluster(
    rpc::RemoveVirtualClusterRequest request,
    rpc::RemoveVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &virtual_cluster_id = request.virtual_cluster_id();
  RAY_LOG(INFO) << "Start removing virtual cluster " << virtual_cluster_id;
  // TODO(Shanly): To be implement.
}

void GcsVirtualClusterManager::HandleGetAllVirtualClusters(
    rpc::GetAllVirtualClustersRequest request,
    rpc::GetAllVirtualClustersReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all virtual clusters.";
  // TODO(Shanly): To be implement.
}

}  // namespace gcs
}  // namespace ray