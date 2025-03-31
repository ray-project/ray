#include <memory>
#include <string>
#include <unordered_map>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/gcs_server/gcs_server_fwd.h"
#include "ray/gcs/gcs_server/debug_tools.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

// Forward declarations
class GcsServer;
class ServiceDiscovery;
class LoadBalancer;
class StateSync;
class ConfigManager;
class MetricsManager;

class GcsServerTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    // Set up logging
    ray::util::RayLog::StartRayLog("gcs_server_test", ray::util::RayLogLevel::INFO);

    // Create GCS server
    gcs_server_ = std::make_unique<ray::gcs::GcsServer>("127.0.0.1", 0);
  }

  virtual void TearDown() {
    // Stop GCS server
    if (gcs_server_) {
      auto status = gcs_server_->Stop();
      ASSERT_TRUE(status.ok());
    }

    // Clean up logging
    ray::util::RayLog::ShutDownRayLog();
  }

  std::unique_ptr<ray::gcs::GcsServer> gcs_server_;
};

TEST_F(GcsServerTest, TestStartStop) {
  // Start GCS server
  auto status = gcs_server_->Start();
  ASSERT_TRUE(status.ok());

  // Stop GCS server
  status = gcs_server_->Stop();
  ASSERT_TRUE(status.ok());
}

TEST_F(GcsServerTest, TestServiceDiscovery) {
  // Start GCS server
  auto status = gcs_server_->Start();
  ASSERT_TRUE(status.ok());

  // Get service discovery instance
  auto& service_discovery = gcs_server_->GetServiceDiscovery();

  // Register a service
  std::unordered_map<std::string, std::string> metadata = {
      {"service_type", "test_type"},
      {"version", "1.0.0"}
  };
  status = service_discovery.RegisterService("test_service", "127.0.0.1", 1234, metadata);
  ASSERT_TRUE(status.ok());

  // Discover the service
  auto service_info = service_discovery.GetServiceInfo("test_service");
  ASSERT_TRUE(service_info.ok());
  ASSERT_EQ(service_info.value().name, "test_service");
  ASSERT_EQ(service_info.value().address, "127.0.0.1");
  ASSERT_EQ(service_info.value().port, 1234);

  // Stop GCS server
  status = gcs_server_->Stop();
  ASSERT_TRUE(status.ok());
}

TEST_F(GcsServerTest, TestLoadBalancer) {
  // Start GCS server
  auto status = gcs_server_->Start();
  ASSERT_TRUE(status.ok());

  // Get load balancer instance
  auto& load_balancer = gcs_server_->GetLoadBalancer();

  // Update load stats
  LoadStats stats;
  stats.cpu_usage = 0.5;
  stats.memory_usage = 0.6;
  stats.active_requests = 10;
  stats.queued_requests = 5;
  status = load_balancer.UpdateLoadStats("test_node", stats);
  ASSERT_TRUE(status.ok());

  // Get load stats
  auto node_stats = load_balancer.GetLoadStats("test_node");
  ASSERT_TRUE(node_stats.ok());
  ASSERT_EQ(node_stats.value().cpu_usage, 0.5);
  ASSERT_EQ(node_stats.value().memory_usage, 0.6);
  ASSERT_EQ(node_stats.value().active_requests, 10);
  ASSERT_EQ(node_stats.value().queued_requests, 5);

  // Stop GCS server
  status = gcs_server_->Stop();
  ASSERT_TRUE(status.ok());
}

TEST_F(GcsServerTest, TestStateSync) {
  // Start GCS server
  auto status = gcs_server_->Start();
  ASSERT_TRUE(status.ok());

  // Get state sync instance
  auto& state_sync = gcs_server_->GetStateSync();

  // Update state
  std::string key = "test_key";
  std::string value = "test_value";
  status = state_sync.UpdateState(key, value);
  ASSERT_TRUE(status.ok());

  // Get state
  auto state = state_sync.GetState(key);
  ASSERT_TRUE(state.ok());
  ASSERT_EQ(state.value(), value);

  // Stop GCS server
  status = gcs_server_->Stop();
  ASSERT_TRUE(status.ok());
}

TEST_F(GcsServerTest, TestConfigManager) {
  // Start GCS server
  auto status = gcs_server_->Start();
  ASSERT_TRUE(status.ok());

  // Get config manager instance
  auto& config_manager = gcs_server_->GetConfigManager();

  // Update config
  std::string key = "test_key";
  std::string value = "test_value";
  status = config_manager.UpdateConfig(key, value);
  ASSERT_TRUE(status.ok());

  // Get config
  auto config = config_manager.GetConfig(key);
  ASSERT_TRUE(config.ok());
  ASSERT_EQ(config.value(), value);

  // Stop GCS server
  status = gcs_server_->Stop();
  ASSERT_TRUE(status.ok());
}

TEST_F(GcsServerTest, TestMetricsManager) {
  // Start GCS server
  auto status = gcs_server_->Start();
  ASSERT_TRUE(status.ok());

  // Get metrics manager instance
  auto& metrics_manager = gcs_server_->GetMetricsManager();

  // Update metrics
  std::string key = "test_key";
  double value = 42.0;
  status = metrics_manager.UpdateMetric(key, value);
  ASSERT_TRUE(status.ok());

  // Get metrics
  auto metric = metrics_manager.GetMetric(key);
  ASSERT_TRUE(metric.ok());
  ASSERT_EQ(metric.value(), value);

  // Stop GCS server
  status = gcs_server_->Stop();
  ASSERT_TRUE(status.ok());
}

TEST_F(GcsServerTest, TestDebugTools) {
  // Start GCS server
  auto status = gcs_server_->Start();
  ASSERT_TRUE(status.ok());

  // Get debug tools instance
  auto& debug_tools = gcs_server_->GetDebugTools();

  // Test service discovery methods
  auto services = debug_tools.GetRegisteredServices();
  ASSERT_TRUE(services.empty());  // No services registered yet

  // Test load stats
  auto load_stats = debug_tools.GetLoadStats();
  ASSERT_EQ(load_stats.cpu_usage, 0.0);
  ASSERT_EQ(load_stats.memory_usage, 0.0);
  ASSERT_EQ(load_stats.active_requests, 0);
  ASSERT_EQ(load_stats.queued_requests, 0);

  // Test state sync
  ASSERT_FALSE(debug_tools.IsStateSynced());  // Not synced initially
  ASSERT_TRUE(debug_tools.GetLastSyncTime().empty());

  // Test config
  ASSERT_TRUE(debug_tools.GetCurrentConfig().empty());
  ASSERT_TRUE(debug_tools.GetConfigHistory().empty());

  // Test metrics
  ASSERT_TRUE(debug_tools.GetActiveAlerts().empty());
  ASSERT_TRUE(debug_tools.GetMetricsSnapshot().empty());
  ASSERT_EQ(debug_tools.GetAverageLatency(), 0.0);
  ASSERT_EQ(debug_tools.GetThroughput(), 0.0);
  ASSERT_EQ(debug_tools.GetErrorRate(), 0);
  ASSERT_TRUE(debug_tools.GetRecentErrors().empty());
  ASSERT_TRUE(debug_tools.GetRecentWarnings().empty());
  ASSERT_EQ(debug_tools.GetQueueSize(), 0);
  ASSERT_EQ(debug_tools.GetAverageQueueTime(), 0.0);
  ASSERT_EQ(debug_tools.GetMemoryUsage(), 0.0);
  ASSERT_TRUE(debug_tools.GetMemoryBreakdown().empty());
  ASSERT_EQ(debug_tools.GetCPUUsage(), 0.0);
  ASSERT_TRUE(debug_tools.GetCPUBreakdown().empty());
  ASSERT_EQ(debug_tools.GetNetworkIn(), 0.0);
  ASSERT_EQ(debug_tools.GetNetworkOut(), 0.0);

  // Test health check
  auto health_status = debug_tools.CheckHealth();
  ASSERT_FALSE(health_status.ok());  // Should fail initially as components are not fully initialized

  // Test health report
  auto health_report = debug_tools.GetHealthReport();
  ASSERT_FALSE(health_report.empty());
  ASSERT_NE(health_report.find("Component Status:"), std::string::npos);
  ASSERT_NE(health_report.find("Performance Metrics:"), std::string::npos);
  ASSERT_NE(health_report.find("Queue Statistics:"), std::string::npos);
  ASSERT_NE(health_report.find("Network Statistics:"), std::string::npos);
  ASSERT_NE(health_report.find("Active Alerts:"), std::string::npos);

  // Stop GCS server
  status = gcs_server_->Stop();
  ASSERT_TRUE(status.ok());
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
} 