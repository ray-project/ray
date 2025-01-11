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

#include "ray/gcs/gcs_server/gcs_server.h"

#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"

namespace ray {
namespace gcs {
void GcsServer::InitGcsVirtualClusterManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_publisher_);
  gcs_virtual_cluster_manager_ = std::make_shared<gcs::GcsVirtualClusterManager>(
      *gcs_table_storage_,
      *gcs_publisher_,
      cluster_resource_scheduler_->GetClusterResourceManager(),
      periodical_runner_);
  // Initialize by gcs tables data.
  gcs_virtual_cluster_manager_->Initialize(gcs_init_data);
  // Register service.
  gcs_virtual_cluster_service_.reset(new rpc::VirtualClusterInfoGrpcService(
      io_context_provider_.GetDefaultIOContext(), *gcs_virtual_cluster_manager_));
  rpc_server_.RegisterService(*gcs_virtual_cluster_service_);
}
}  // namespace gcs
}  // namespace ray