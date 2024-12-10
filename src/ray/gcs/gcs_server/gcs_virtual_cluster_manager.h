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

#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

/// This implementation class of `VirtualClusterInfoHandler`.
class GcsVirtualClusterManager : public rpc::VirtualClusterInfoHandler {
 public:
  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

 protected:
  void HandleCreateOrUpdateVirtualCluster(
      rpc::CreateOrUpdateVirtualClusterRequest request,
      rpc::CreateOrUpdateVirtualClusterReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemoveVirtualCluster(rpc::RemoveVirtualClusterRequest request,
                                  rpc::RemoveVirtualClusterReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllVirtualClusters(rpc::GetAllVirtualClustersRequest request,
                                   rpc::GetAllVirtualClustersReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;
};

}  // namespace gcs
}  // namespace ray