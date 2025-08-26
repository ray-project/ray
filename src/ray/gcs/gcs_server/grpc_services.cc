// Copyright 2025 The Ray Authors.
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
#include "ray/gcs/gcs_server/grpc_services.h"

#include <memory>
#include <vector>

namespace ray {
namespace rpc {

void NodeInfoGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  // We only allow one cluster ID in the lifetime of a client.
  // So, if a client connects, it should not have a pre-existing different ID.
  RPC_SERVICE_HANDLER_CUSTOM_AUTH(NodeInfoGcsService,
                                  GetClusterId,
                                  max_active_rpcs_per_handler_,
                                  AuthType::EMPTY_AUTH);
  RPC_SERVICE_HANDLER(NodeInfoGcsService, RegisterNode, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(NodeInfoGcsService, UnregisterNode, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(NodeInfoGcsService, DrainNode, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(NodeInfoGcsService, GetAllNodeInfo, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(NodeInfoGcsService, CheckAlive, max_active_rpcs_per_handler_)
}

void JobInfoGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  RPC_SERVICE_HANDLER(JobInfoGcsService, AddJob, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(JobInfoGcsService, MarkJobFinished, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(JobInfoGcsService, GetAllJobInfo, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(JobInfoGcsService, ReportJobError, max_active_rpcs_per_handler_)
  RPC_SERVICE_HANDLER(JobInfoGcsService, GetNextJobID, max_active_rpcs_per_handler_)
}

void RuntimeEnvGrpcService::InitServerCallFactories(
    const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
    std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
    const ClusterID &cluster_id) {
  RPC_SERVICE_HANDLER(
      RuntimeEnvGcsService, PinRuntimeEnvURI, max_active_rpcs_per_handler_)
}

}  // namespace rpc
}  // namespace ray
