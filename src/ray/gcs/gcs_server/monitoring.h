#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/rpc/head_node_service.h"

namespace ray {
namespace gcs {

class Monitoring {
 public:
  Monitoring(GcsServer &gcs_server, instrumented_io_context &io_context)
      : gcs_server_(gcs_server), io_context_(io_context) {}

  void Start();
  void Stop();

  Status CollectMetrics(const rpc::CollectMetricsRequest &request,
                       rpc::CollectMetricsReply *reply);

  Status GetMetrics(const rpc::GetMetricsRequest &request,
                   rpc::GetMetricsReply *reply);

  Status SetAlert(const rpc::SetAlertRequest &request,
                 rpc::SetAlertReply *reply);

  Status GetAlerts(const rpc::GetAlertsRequest &request,
                  rpc::GetAlertsReply *reply);

 private:
  GcsServer &gcs_server_;
  instrumented_io_context &io_context_;

  // Map of service ID to metrics
  std::unordered_map<std::string, rpc::ServiceMetrics> service_metrics_;

  // Map of service type to list of service IDs
  std::unordered_map<std::string, std::vector<std::string>> service_type_to_ids_;

  // Map of alert ID to alert configuration
  std::unordered_map<std::string, rpc::AlertConfig> alerts_;

  // Collection interval in seconds
  const int64_t collection_interval_ = 15;
};

}  // namespace gcs
}  // namespace ray 