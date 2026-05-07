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
#include <optional>
#include <vector>

#include "ray/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/gcs/grpc_service_interfaces.h"
#include "ray/rpc/authentication/authentication_token.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/rpc_callback_types.h"
#include "src/proto/grpc/health/v1/health.grpc.pb.h"
#include "src/ray/protobuf/autoscaler.grpc.pb.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {
namespace rpc {

class ActorInfoGrpcService : public GrpcService {
 public:
  explicit ActorInfoGrpcService(instrumented_io_context &io_service,
                                ActorInfoGcsServiceHandler &service_handler,
                                int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(service_handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

 private:
  ActorInfoGcsService::AsyncService service_;
  ActorInfoGcsServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

class NodeInfoGrpcService : public GrpcService {
 public:
  explicit NodeInfoGrpcService(instrumented_io_context &io_service,
                               NodeInfoGcsServiceHandler &service_handler,
                               int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(service_handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

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
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

 private:
  NodeResourceInfoGcsService::AsyncService service_;
  NodeResourceInfoGcsServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

class ControlPlanePubSubGrpcService : public GrpcService {
 public:
  ControlPlanePubSubGrpcService(instrumented_io_context &io_service,
                                ControlPlanePubSubGcsServiceHandler &handler,
                                int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

 private:
  ControlPlanePubSubGcsService::AsyncService service_;
  ControlPlanePubSubGcsServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

class ObservabilityPubSubGrpcService : public GrpcService {
 public:
  ObservabilityPubSubGrpcService(instrumented_io_context &io_service,
                                 ObservabilityPubSubServiceHandler &handler,
                                 int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

 private:
  ObservabilityPubSubService::AsyncService service_;
  ObservabilityPubSubServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

class JobInfoGrpcService : public GrpcService {
 public:
  explicit JobInfoGrpcService(instrumented_io_context &io_service,
                              JobInfoGcsServiceHandler &handler,
                              int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

 private:
  JobInfoGcsService::AsyncService service_;
  JobInfoGcsServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

class RuntimeEnvGrpcService : public GrpcService {
 public:
  explicit RuntimeEnvGrpcService(instrumented_io_context &io_service,
                                 RuntimeEnvGcsServiceHandler &handler,
                                 int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

 private:
  RuntimeEnvGcsService::AsyncService service_;
  RuntimeEnvGcsServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

class WorkerInfoGrpcService : public GrpcService {
 public:
  explicit WorkerInfoGrpcService(instrumented_io_context &io_service,
                                 WorkerInfoGcsServiceHandler &handler,
                                 int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

 private:
  WorkerInfoGcsService::AsyncService service_;
  WorkerInfoGcsServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

class InternalKVGrpcService : public GrpcService {
 public:
  explicit InternalKVGrpcService(instrumented_io_context &io_service,
                                 InternalKVGcsServiceHandler &handler,
                                 int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

 private:
  InternalKVGcsService::AsyncService service_;
  InternalKVGcsServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

class TaskInfoGrpcService : public GrpcService {
 public:
  explicit TaskInfoGrpcService(instrumented_io_context &io_service,
                               TaskInfoGcsServiceHandler &handler,
                               int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

 private:
  TaskInfoGcsService::AsyncService service_;
  TaskInfoGcsServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

class PlacementGroupInfoGrpcService : public GrpcService {
 public:
  explicit PlacementGroupInfoGrpcService(instrumented_io_context &io_service,
                                         PlacementGroupInfoGcsServiceHandler &handler,
                                         int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

 private:
  PlacementGroupInfoGcsService::AsyncService service_;
  PlacementGroupInfoGcsServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

namespace autoscaler {

class AutoscalerStateGrpcService : public GrpcService {
 public:
  explicit AutoscalerStateGrpcService(instrumented_io_context &io_service,
                                      AutoscalerStateServiceHandler &handler,
                                      int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

 private:
  AutoscalerStateService::AsyncService service_;
  AutoscalerStateServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

}  // namespace autoscaler

namespace events {

class RayEventExportGrpcService : public GrpcService {
 public:
  explicit RayEventExportGrpcService(instrumented_io_context &io_service,
                                     RayEventExportGcsServiceHandler &handler,
                                     int64_t max_active_rpcs_per_handler)
      : GrpcService(io_service),
        service_handler_(handler),
        max_active_rpcs_per_handler_(max_active_rpcs_per_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

 private:
  RayEventExportGcsService::AsyncService service_;
  RayEventExportGcsServiceHandler &service_handler_;
  int64_t max_active_rpcs_per_handler_;
};

}  // namespace events

/// gRPC Health Check service that dispatches to the threads running boost::asio
/// event loops to ensure they are alive and not overloaded.
///
/// Unlike the default gRPC health check service (which responds directly from gRPC
/// threads), this service's handler runs on the io_context event loop. If the event loop
/// is stuck, the health check will not respond and the client will time out.
///
/// NOTE: we currently ignore the `service` field, which is part of the default
/// health check protocol. In the future, we may want to implement this as per-service
/// health checks (which could check the relevant boost::asio event loop).
class HealthCheckGrpcService : public GrpcService {
 public:
  explicit HealthCheckGrpcService(instrumented_io_context &io_service)
      : GrpcService(io_service) {}

  void HandleCheck(grpc::health::v1::HealthCheckRequest request,
                   grpc::health::v1::HealthCheckResponse *reply,
                   SendReplyCallback send_reply_callback) {
    reply->set_status(grpc::health::v1::HealthCheckResponse::SERVING);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories,
      const ClusterID &cluster_id,
      std::shared_ptr<const AuthenticationToken> auth_token) override;

 private:
  grpc::health::v1::Health::AsyncService service_;
};

}  // namespace rpc
}  // namespace ray
