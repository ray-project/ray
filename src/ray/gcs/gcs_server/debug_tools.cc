#include "src/ray/gcs/gcs_server/debug_tools.h"

#include <chrono>
#include <sstream>

#include "src/ray/common/status.h"
#include "src/ray/gcs/gcs_server/gcs_server.h"
#include "src/ray/util/logging.h"

namespace ray {
namespace gcs {

GcsDebugTools::GcsDebugTools(GcsServer* gcs_server) : gcs_server_(gcs_server) {}

std::string GcsDebugTools::GetServiceState() const {
  std::stringstream ss;
  auto& service_discovery = gcs_server_->GetServiceDiscovery();
  auto services = service_discovery->GetAllServices();
  ss << "Registered Services: " << services.size() << "\n";
  for (const auto& service : services) {
    ss << "  - " << service.service_id << " (" << service.service_type << ")\n";
    ss << "    Address: " << service.address << ":" << service.port << "\n";
  }
  return ss.str();
}

std::string GcsDebugTools::GetLoadBalancingState() const {
  std::stringstream ss;
  auto& load_balancer = gcs_server_->GetLoadBalancer();
  auto stats = load_balancer->GetAllLoadStats();
  ss << "Node Load Statistics: " << stats.size() << "\n";
  for (const auto& stat : stats) {
    ss << "  - " << stat.first << "\n";
    ss << "    CPU: " << stat.second.cpu_usage << "\n";
    ss << "    Memory: " << stat.second.memory_usage << "\n";
    ss << "    Network: " << stat.second.network_usage << "\n";
  }
  return ss.str();
}

std::string GcsDebugTools::GetStateSyncState() const {
  std::stringstream ss;
  auto& state_sync = gcs_server_->GetStateSync();
  auto state = state_sync->GetAllState();
  ss << "State Keys: " << state.size() << "\n";
  for (const auto& kv : state) {
    ss << "  - " << kv.first << ": " << kv.second << "\n";
  }
  return ss.str();
}

std::string GcsDebugTools::GetConfigState() const {
  std::stringstream ss;
  auto& config_manager = gcs_server_->GetConfigManager();
  auto config = config_manager->GetAllConfig();
  ss << "Config Keys: " << config.size() << "\n";
  for (const auto& kv : config) {
    ss << "  - " << kv.first << ": " << kv.second << "\n";
  }
  return ss.str();
}

std::string GcsDebugTools::GetMetricsState() const {
  std::stringstream ss;
  auto& metrics_manager = gcs_server_->GetMetricsManager();
  auto metrics = metrics_manager->GetAllMetrics();
  ss << "Metrics: " << metrics.size() << "\n";
  for (const auto& kv : metrics) {
    ss << "  - " << kv.first << ": " << kv.second << "\n";
  }
  return ss.str();
}

std::vector<ServiceInfo> GcsDebugTools::GetRegisteredServices() const {
  std::vector<ServiceInfo> services;
  auto service_discovery = gcs_server_->GetServiceDiscovery();
  if (service_discovery) {
    services = service_discovery->GetAllServices();
  }
  return services;
}

ServiceInfo GcsDebugTools::GetServiceInfo(const std::string& service_name) const {
  auto service_discovery = gcs_server_->GetServiceDiscovery();
  if (service_discovery) {
    return service_discovery->GetServiceInfo(service_name);
  }
  return ServiceInfo{};
}

LoadStats GcsDebugTools::GetLoadStats() const {
  auto load_balancer = gcs_server_->GetLoadBalancer();
  if (load_balancer) {
    return load_balancer->GetCurrentLoadStats();
  }
  return LoadStats{};
}

std::vector<std::string> GcsDebugTools::GetActiveRequests() const {
  auto load_balancer = gcs_server_->GetLoadBalancer();
  if (load_balancer) {
    return load_balancer->GetActiveRequests();
  }
  return {};
}

std::vector<std::string> GcsDebugTools::GetQueuedRequests() const {
  auto load_balancer = gcs_server_->GetLoadBalancer();
  if (load_balancer) {
    return load_balancer->GetQueuedRequests();
  }
  return {};
}

bool GcsDebugTools::IsStateSynced() const {
  auto state_sync = gcs_server_->GetStateSync();
  if (state_sync) {
    return state_sync->IsSynced();
  }
  return false;
}

std::string GcsDebugTools::GetLastSyncTime() const {
  auto state_sync = gcs_server_->GetStateSync();
  if (state_sync) {
    return state_sync->GetLastSyncTime();
  }
  return "";
}

std::string GcsDebugTools::GetCurrentConfig() const {
  auto config_manager = gcs_server_->GetConfigManager();
  if (config_manager) {
    return config_manager->GetCurrentConfig();
  }
  return "";
}

std::vector<std::string> GcsDebugTools::GetConfigHistory() const {
  auto config_manager = gcs_server_->GetConfigManager();
  if (config_manager) {
    return config_manager->GetConfigHistory();
  }
  return {};
}

std::vector<Alert> GcsDebugTools::GetActiveAlerts() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetActiveAlerts();
  }
  return {};
}

std::string GcsDebugTools::GetMetricsSnapshot() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetSnapshot();
  }
  return "";
}

double GcsDebugTools::GetAverageLatency() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetAverageLatency();
  }
  return 0.0;
}

double GcsDebugTools::GetThroughput() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetThroughput();
  }
  return 0.0;
}

int GcsDebugTools::GetErrorRate() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetErrorRate();
  }
  return 0;
}

std::vector<std::string> GcsDebugTools::GetRecentErrors() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetRecentErrors();
  }
  return {};
}

std::vector<std::string> GcsDebugTools::GetRecentWarnings() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetRecentWarnings();
  }
  return {};
}

int GcsDebugTools::GetQueueSize() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetQueueSize();
  }
  return 0;
}

double GcsDebugTools::GetAverageQueueTime() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetAverageQueueTime();
  }
  return 0.0;
}

double GcsDebugTools::GetMemoryUsage() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetMemoryUsage();
  }
  return 0.0;
}

std::string GcsDebugTools::GetMemoryBreakdown() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetMemoryBreakdown();
  }
  return "";
}

double GcsDebugTools::GetCPUUsage() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetCPUUsage();
  }
  return 0.0;
}

std::string GcsDebugTools::GetCPUBreakdown() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetCPUBreakdown();
  }
  return "";
}

double GcsDebugTools::GetNetworkIn() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetNetworkIn();
  }
  return 0.0;
}

double GcsDebugTools::GetNetworkOut() const {
  auto metrics_manager = gcs_server_->GetMetricsManager();
  if (metrics_manager) {
    return metrics_manager->GetNetworkOut();
  }
  return 0.0;
}

Status GcsDebugTools::CheckHealth() const {
  // Check all components
  auto service_discovery = gcs_server_->GetServiceDiscovery();
  auto load_balancer = gcs_server_->GetLoadBalancer();
  auto state_sync = gcs_server_->GetStateSync();
  auto config_manager = gcs_server_->GetConfigManager();
  auto metrics_manager = gcs_server_->GetMetricsManager();

  if (!service_discovery || !load_balancer || !state_sync || !config_manager || !metrics_manager) {
    return Status::Invalid("One or more components are not initialized");
  }

  // Check if state is synced
  if (!IsStateSynced()) {
    return Status::Invalid("State is not synced");
  }

  // Check load stats
  auto load_stats = GetLoadStats();
  if (load_stats.cpu_usage > 90.0 || load_stats.memory_usage > 90.0) {
    return Status::Invalid("System is overloaded");
  }

  // Check active alerts
  auto alerts = GetActiveAlerts();
  if (!alerts.empty()) {
    std::stringstream ss;
    ss << "Active alerts present: ";
    for (const auto& alert : alerts) {
      ss << "[" << alert.severity << "] " << alert.message << "; ";
    }
    return Status::Invalid(ss.str());
  }

  return Status::OK();
}

std::string GcsDebugTools::GetHealthReport() const {
  std::stringstream ss;
  ss << "GCS Server Health Report\n";
  ss << "=======================\n\n";

  // Component status
  ss << "Component Status:\n";
  ss << "- Service Discovery: " << (gcs_server_->GetServiceDiscovery() ? "OK" : "Not initialized") << "\n";
  ss << "- Load Balancer: " << (gcs_server_->GetLoadBalancer() ? "OK" : "Not initialized") << "\n";
  ss << "- State Sync: " << (gcs_server_->GetStateSync() ? "OK" : "Not initialized") << "\n";
  ss << "- Config Manager: " << (gcs_server_->GetConfigManager() ? "OK" : "Not initialized") << "\n";
  ss << "- Metrics Manager: " << (gcs_server_->GetMetricsManager() ? "OK" : "Not initialized") << "\n\n";

  // Performance metrics
  ss << "Performance Metrics:\n";
  ss << "- CPU Usage: " << GetCPUUsage() << "%\n";
  ss << "- Memory Usage: " << GetMemoryUsage() << "%\n";
  ss << "- Average Latency: " << GetAverageLatency() << "ms\n";
  ss << "- Throughput: " << GetThroughput() << " req/s\n";
  ss << "- Error Rate: " << GetErrorRate() << "%\n\n";

  // Queue statistics
  ss << "Queue Statistics:\n";
  ss << "- Queue Size: " << GetQueueSize() << "\n";
  ss << "- Average Queue Time: " << GetAverageQueueTime() << "ms\n\n";

  // Network statistics
  ss << "Network Statistics:\n";
  ss << "- Network In: " << GetNetworkIn() << " MB/s\n";
  ss << "- Network Out: " << GetNetworkOut() << " MB/s\n\n";

  // Active alerts
  auto alerts = GetActiveAlerts();
  ss << "Active Alerts: " << alerts.size() << "\n";
  for (const auto& alert : alerts) {
    ss << "- [" << alert.severity << "] " << alert.message << "\n";
  }

  return ss.str();
}

}  // namespace gcs
}  // namespace ray 