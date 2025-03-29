#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/gcs_server/gcs_server_fwd.h"
#include "ray/gcs/gcs_server/debug_tools.h"
#include "ray/rpc/head_node_client.h"
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

class GcsServerIntegrationTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    // Set up logging
    ray::util::RayLog::StartRayLog("gcs_server_integration_test", ray::util::RayLogLevel::INFO);

    // Create GCS server
    gcs_server_ = std::make_unique<ray::gcs::GcsServer>("127.0.0.1", 50051);

    // Start GCS server
    auto status = gcs_server_->Start();
    ASSERT_TRUE(status.ok());

    // Create client
    client_ = std::make_unique<ray::rpc::HeadNodeClient>("127.0.0.1", 50051);
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
  std::unique_ptr<ray::rpc::HeadNodeClient> client_;
};

TEST_F(GcsServerIntegrationTest, TestServiceDiscovery) {
  // Register a service
  std::unordered_map<std::string, std::string> metadata = {
      {"service_type", "test_type"},
      {"version", "1.0.0"}
  };
  auto status = client_->RegisterService("test_service", "127.0.0.1", 1234, metadata);
  ASSERT_TRUE(status.ok());

  // Discover the service
  auto service_info = client_->DiscoverService("test_service");
  ASSERT_TRUE(service_info.ok());
  ASSERT_EQ(service_info.value().name, "test_service");
  ASSERT_EQ(service_info.value().address, "127.0.0.1");
  ASSERT_EQ(service_info.value().port, 1234);
}

TEST_F(GcsServerIntegrationTest, TestLoadBalancing) {
  // Update load stats
  LoadStats stats;
  stats.cpu_usage = 0.5;
  stats.memory_usage = 0.6;
  stats.active_requests = 10;
  stats.queued_requests = 5;
  auto status = client_->UpdateLoadStats("test_node", stats);
  ASSERT_TRUE(status.ok());

  // Get load stats
  auto node_stats = client_->GetLoadStats("test_node");
  ASSERT_TRUE(node_stats.ok());
  ASSERT_EQ(node_stats.value().cpu_usage, 0.5);
  ASSERT_EQ(node_stats.value().memory_usage, 0.6);
  ASSERT_EQ(node_stats.value().active_requests, 10);
  ASSERT_EQ(node_stats.value().queued_requests, 5);

  // Balance load
  auto balance_result = client_->BalanceLoad();
  ASSERT_TRUE(balance_result.ok());
}

TEST_F(GcsServerIntegrationTest, TestStateSync) {
  // Update state
  std::string key = "test_key";
  std::string value = "test_value";
  auto status = client_->UpdateState(key, value);
  ASSERT_TRUE(status.ok());

  // Get state
  auto state = client_->GetState(key);
  ASSERT_TRUE(state.ok());
  ASSERT_EQ(state.value(), value);

  // Sync state
  auto sync_result = client_->SyncState();
  ASSERT_TRUE(sync_result.ok());
}

TEST_F(GcsServerIntegrationTest, TestConfigManagement) {
  // Update config
  std::string key = "test_key";
  std::string value = "test_value";
  auto status = client_->UpdateConfig(key, value);
  ASSERT_TRUE(status.ok());

  // Get config
  auto config = client_->GetConfig(key);
  ASSERT_TRUE(config.ok());
  ASSERT_EQ(config.value(), value);

  // Validate config
  auto validate_result = client_->ValidateConfig(key, value);
  ASSERT_TRUE(validate_result.ok());

  // Apply config
  auto apply_result = client_->ApplyConfig(key);
  ASSERT_TRUE(apply_result.ok());
}

TEST_F(GcsServerIntegrationTest, TestMetricsAndMonitoring) {
  // Update metrics
  std::string key = "test_key";
  double value = 42.0;
  auto status = client_->UpdateMetric(key, value);
  ASSERT_TRUE(status.ok());

  // Get metrics
  auto metric = client_->GetMetric(key);
  ASSERT_TRUE(metric.ok());
  ASSERT_EQ(metric.value(), value);

  // Set alert
  std::string alert_name = "test_alert";
  std::string alert_condition = "value > 40";
  auto alert_status = client_->SetAlert(alert_name, alert_condition);
  ASSERT_TRUE(alert_status.ok());

  // Get alerts
  auto alerts = client_->GetAlerts();
  ASSERT_TRUE(alerts.ok());
  ASSERT_EQ(alerts.value().size(), 1);
  ASSERT_EQ(alerts.value()[0].name, alert_name);
  ASSERT_EQ(alerts.value()[0].condition, alert_condition);
}

TEST_F(GcsServerIntegrationTest, TestDebugTools) {
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

  // Register a service and verify it appears in debug tools
  std::unordered_map<std::string, std::string> metadata = {
      {"service_type", "test_type"},
      {"version", "1.0.0"}
  };
  status = client_->RegisterService("test_service", "127.0.0.1", 1234, metadata);
  ASSERT_TRUE(status.ok());

  services = debug_tools.GetRegisteredServices();
  ASSERT_EQ(services.size(), 1);
  ASSERT_EQ(services[0].name, "test_service");
  ASSERT_EQ(services[0].address, "127.0.0.1");
  ASSERT_EQ(services[0].port, 1234);

  // Update load stats and verify they appear in debug tools
  LoadStats stats;
  stats.cpu_usage = 0.5;
  stats.memory_usage = 0.6;
  stats.active_requests = 10;
  stats.queued_requests = 5;
  status = client_->UpdateLoadStats("test_node", stats);
  ASSERT_TRUE(status.ok());

  load_stats = debug_tools.GetLoadStats();
  ASSERT_EQ(load_stats.cpu_usage, 0.5);
  ASSERT_EQ(load_stats.memory_usage, 0.6);
  ASSERT_EQ(load_stats.active_requests, 10);
  ASSERT_EQ(load_stats.queued_requests, 5);
}

TEST_F(GcsServerIntegrationTest, TestConcurrentOperations) {
  const int num_threads = 10;
  const int num_operations = 100;
  std::vector<std::thread> threads;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([this, i]() {
      for (int j = 0; j < num_operations; ++j) {
        // Service discovery
        std::string service_id = "service_" + std::to_string(i) + "_" + std::to_string(j);
        std::unordered_map<std::string, std::string> metadata = {
            {"service_type", "test_type"},
            {"version", "1.0.0"}
        };
        auto status = client_->RegisterService(service_id, "127.0.0.1", 1234, metadata);
        ASSERT_TRUE(status.ok());

        // Load balancing
        LoadStats stats;
        stats.cpu_usage = 0.5;
        stats.memory_usage = 0.6;
        stats.active_requests = 10;
        stats.queued_requests = 5;
        status = client_->UpdateLoadStats(service_id, stats);
        ASSERT_TRUE(status.ok());

        // State sync
        std::string key = "key_" + std::to_string(i) + "_" + std::to_string(j);
        std::string value = "value_" + std::to_string(i) + "_" + std::to_string(j);
        status = client_->UpdateState(key, value);
        ASSERT_TRUE(status.ok());

        // Config management
        status = client_->UpdateConfig(key, value);
        ASSERT_TRUE(status.ok());

        // Metrics
        status = client_->UpdateMetric(key, j);
        ASSERT_TRUE(status.ok());
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
} 