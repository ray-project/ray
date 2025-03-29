#pragma once

#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/head_node_service.grpc.pb.h"

namespace ray {
namespace rpc {

class HeadNodeServiceHandler : public HeadNodeService::Service {
 public:
  explicit HeadNodeServiceHandler(gcs::GcsServer &gcs_server)
      : gcs_server_(gcs_server) {}

  grpc::Status RegisterService(grpc::ServerContext *context,
                             const RegisterServiceRequest *request,
                             RegisterServiceReply *reply) override;

  grpc::Status DiscoverService(grpc::ServerContext *context,
                             const DiscoverServiceRequest *request,
                             DiscoverServiceReply *reply) override;

  grpc::Status BalanceLoad(grpc::ServerContext *context,
                          const BalanceLoadRequest *request,
                          BalanceLoadReply *reply) override;

  grpc::Status GetLoadStats(grpc::ServerContext *context,
                           const GetLoadStatsRequest *request,
                           GetLoadStatsReply *reply) override;

  grpc::Status UpdateLoadStats(grpc::ServerContext *context,
                             const UpdateLoadStatsRequest *request,
                             UpdateLoadStatsReply *reply) override;

  grpc::Status SyncState(grpc::ServerContext *context,
                        const SyncStateRequest *request,
                        SyncStateReply *reply) override;

  grpc::Status GetState(grpc::ServerContext *context,
                       const GetStateRequest *request,
                       GetStateReply *reply) override;

  grpc::Status UpdateState(grpc::ServerContext *context,
                          const UpdateStateRequest *request,
                          UpdateStateReply *reply) override;

  grpc::Status GetConfig(grpc::ServerContext *context,
                        const GetConfigRequest *request,
                        GetConfigReply *reply) override;

  grpc::Status UpdateConfig(grpc::ServerContext *context,
                           const UpdateConfigRequest *request,
                           UpdateConfigReply *reply) override;

  grpc::Status ValidateConfig(grpc::ServerContext *context,
                             const ValidateConfigRequest *request,
                             ValidateConfigReply *reply) override;

  grpc::Status ApplyConfig(grpc::ServerContext *context,
                          const ApplyConfigRequest *request,
                          ApplyConfigReply *reply) override;

  grpc::Status CollectMetrics(grpc::ServerContext *context,
                             const CollectMetricsRequest *request,
                             CollectMetricsReply *reply) override;

  grpc::Status GetMetrics(grpc::ServerContext *context,
                         const GetMetricsRequest *request,
                         GetMetricsReply *reply) override;

  grpc::Status SetAlert(grpc::ServerContext *context,
                       const SetAlertRequest *request,
                       SetAlertReply *reply) override;

  grpc::Status GetAlerts(grpc::ServerContext *context,
                        const GetAlertsRequest *request,
                        GetAlertsReply *reply) override;

 private:
  gcs::GcsServer &gcs_server_;
};

}  // namespace rpc
}  // namespace ray 