#include "ray/rpc/head_node_client.h"

namespace ray {
namespace rpc {

void HeadNodeClient::RegisterService(const RegisterServiceRequest &request,
                                   const ClientCallback<RegisterServiceReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(RegisterService, request, callback);
}

void HeadNodeClient::DiscoverService(const DiscoverServiceRequest &request,
                                   const ClientCallback<DiscoverServiceReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(DiscoverService, request, callback);
}

void HeadNodeClient::BalanceLoad(const BalanceLoadRequest &request,
                               const ClientCallback<BalanceLoadReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(BalanceLoad, request, callback);
}

void HeadNodeClient::GetLoadStats(const GetLoadStatsRequest &request,
                                const ClientCallback<GetLoadStatsReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(GetLoadStats, request, callback);
}

void HeadNodeClient::UpdateLoadStats(const UpdateLoadStatsRequest &request,
                                   const ClientCallback<UpdateLoadStatsReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(UpdateLoadStats, request, callback);
}

void HeadNodeClient::SyncState(const SyncStateRequest &request,
                             const ClientCallback<SyncStateReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(SyncState, request, callback);
}

void HeadNodeClient::GetState(const GetStateRequest &request,
                            const ClientCallback<GetStateReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(GetState, request, callback);
}

void HeadNodeClient::UpdateState(const UpdateStateRequest &request,
                               const ClientCallback<UpdateStateReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(UpdateState, request, callback);
}

void HeadNodeClient::GetConfig(const GetConfigRequest &request,
                             const ClientCallback<GetConfigReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(GetConfig, request, callback);
}

void HeadNodeClient::UpdateConfig(const UpdateConfigRequest &request,
                                const ClientCallback<UpdateConfigReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(UpdateConfig, request, callback);
}

void HeadNodeClient::ValidateConfig(const ValidateConfigRequest &request,
                                  const ClientCallback<ValidateConfigReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(ValidateConfig, request, callback);
}

void HeadNodeClient::ApplyConfig(const ApplyConfigRequest &request,
                               const ClientCallback<ApplyConfigReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(ApplyConfig, request, callback);
}

void HeadNodeClient::CollectMetrics(const CollectMetricsRequest &request,
                                  const ClientCallback<CollectMetricsReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(CollectMetrics, request, callback);
}

void HeadNodeClient::GetMetrics(const GetMetricsRequest &request,
                              const ClientCallback<GetMetricsReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(GetMetrics, request, callback);
}

void HeadNodeClient::SetAlert(const SetAlertRequest &request,
                            const ClientCallback<SetAlertReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(SetAlert, request, callback);
}

void HeadNodeClient::GetAlerts(const GetAlertsRequest &request,
                             const ClientCallback<GetAlertsReply> &callback) {
  grpc::ClientContext context;
  INVOKE_RPC_CALL(GetAlerts, request, callback);
}

}  // namespace rpc
}  // namespace ray 