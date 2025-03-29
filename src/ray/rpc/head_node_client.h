#pragma once

#include <memory>

#include "ray/common/status.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/head_node_service.grpc.pb.h"

namespace ray {
namespace rpc {

class HeadNodeClient : public GrpcClient<HeadNodeService> {
 public:
  HeadNodeClient(const std::string &address, int port,
                 ClientCallManager &call_manager)
      : GrpcClient<HeadNodeService>(address, port, call_manager) {}

  void RegisterService(const RegisterServiceRequest &request,
                      const ClientCallback<RegisterServiceReply> &callback);

  void DiscoverService(const DiscoverServiceRequest &request,
                      const ClientCallback<DiscoverServiceReply> &callback);

  void BalanceLoad(const BalanceLoadRequest &request,
                  const ClientCallback<BalanceLoadReply> &callback);

  void GetLoadStats(const GetLoadStatsRequest &request,
                   const ClientCallback<GetLoadStatsReply> &callback);

  void UpdateLoadStats(const UpdateLoadStatsRequest &request,
                      const ClientCallback<UpdateLoadStatsReply> &callback);

  void SyncState(const SyncStateRequest &request,
                const ClientCallback<SyncStateReply> &callback);

  void GetState(const GetStateRequest &request,
               const ClientCallback<GetStateReply> &callback);

  void UpdateState(const UpdateStateRequest &request,
                  const ClientCallback<UpdateStateReply> &callback);

  void GetConfig(const GetConfigRequest &request,
                const ClientCallback<GetConfigReply> &callback);

  void UpdateConfig(const UpdateConfigRequest &request,
                   const ClientCallback<UpdateConfigReply> &callback);

  void ValidateConfig(const ValidateConfigRequest &request,
                     const ClientCallback<ValidateConfigReply> &callback);

  void ApplyConfig(const ApplyConfigRequest &request,
                  const ClientCallback<ApplyConfigReply> &callback);

  void CollectMetrics(const CollectMetricsRequest &request,
                     const ClientCallback<CollectMetricsReply> &callback);

  void GetMetrics(const GetMetricsRequest &request,
                 const ClientCallback<GetMetricsReply> &callback);

  void SetAlert(const SetAlertRequest &request,
               const ClientCallback<SetAlertReply> &callback);

  void GetAlerts(const GetAlertsRequest &request,
                const ClientCallback<GetAlertsReply> &callback);
};

}  // namespace rpc
}  // namespace ray 