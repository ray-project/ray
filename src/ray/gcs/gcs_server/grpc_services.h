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

/*
 * This file defines the gRPC service handlers for the GCS server binary.
 * Subcomponents that implement a given interface should inherit from the relevant
 * class in grpc_service_interfaces.h.
 *
 * The GCS server main binary should be the only user of this target.
 */

#pragma once

#include <memory>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_server/grpc_service_interfaces.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

class NodeInfoGrpcService : public GrpcService {
 public:
  explicit NodeInfoGrpcService(instrumented_io_context &io_service,
                               NodeInfoGcsServiceHandler &service_handler,
                               int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(service_handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id) override;

 private:
  NodeInfoGcsService::AsyncService service_;
  NodeInfoGcsServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

class NodeResourceInfoGrpcService : public GrpcService {
 public:
  explicit NodeResourceInfoGrpcService(instrumented_io_context &io_service,
                                       NodeResourceInfoGcsServiceHandler &handler,
                                       int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id) override;

 private:
  NodeResourceInfoGcsService::AsyncService service_;
  NodeResourceInfoGcsServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

}  // namespace rpc
}  // namespace ray
