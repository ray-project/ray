#pragma once

#include <chrono>
#include <string>
#include <vector>

#include "ray/common/status.h"
#include "ray/gcs/gcs_server/gcs_server_fwd.h"
#include "ray/gcs/gcs_server/load_balancer.h"
#include "ray/gcs/gcs_server/metrics_manager.h"
#include "ray/gcs/gcs_server/service_discovery.h"

namespace ray {
namespace gcs {

struct ServiceInfo {
  std::string name;
  std::string address;
  int port;
  std::string status;
  std::chrono::system_clock::time_point last_heartbeat;
};

struct LoadStats {
  double cpu_usage;
  double memory_usage;
  int active_requests;
  int queued_requests;
};

struct Alert {
  std::string severity;
  std::string message;
  std::chrono::system_clock::time_point timestamp;
};

class GcsDebugTools {
 public:
  explicit GcsDebugTools(GcsServer* gcs_server);
  ~GcsDebugTools() = default;

  // Service discovery related methods
  std::vector<ServiceInfo> GetRegisteredServices() const;
  ServiceInfo GetServiceInfo(const std::string& service_name) const;
  
  // Load balancing related methods
  LoadStats GetLoadStats() const;
  std::vector<std::string> GetActiveRequests() const;
  std::vector<std::string> GetQueuedRequests() const;
  
  // State sync related methods
  bool IsStateSynced() const;
  std::string GetLastSyncTime() const;
  
  // Config related methods
  std::string GetCurrentConfig() const;
  std::vector<std::string> GetConfigHistory() const;
  
  // Metrics related methods
  std::vector<Alert> GetActiveAlerts() const;
  std::string GetMetricsSnapshot() const;
  double GetAverageLatency() const;
  double GetThroughput() const;
  double GetErrorRate() const;
  std::vector<std::string> GetRecentErrors() const;
  std::vector<std::string> GetRecentWarnings() const;
  
  // Queue related methods
  int GetQueueSize() const;
  double GetAverageQueueTime() const;
  
  // Resource usage methods
  double GetMemoryUsage() const;
  std::string GetMemoryBreakdown() const;
  double GetCPUUsage() const;
  std::string GetCPUBreakdown() const;
  double GetNetworkIn() const;
  double GetNetworkOut() const;
  
  // Health check methods
  Status CheckHealth() const;
  std::string GetHealthReport() const;

 private:
  GcsServer* gcs_server_;
};

}  // namespace gcs
}  // namespace ray 