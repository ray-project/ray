

#include "absl/strings/substitute.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_client/accessor.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"
#include "ray/util/util.h"

namespace ray {

class GcsClientTest : public ::testing::Test {
 public:
  GcsClientTest() : use_gcs_pubsub_(false) {
    RayConfig::instance().initialize(
        absl::Substitute(R"(
{
  "gcs_rpc_server_reconnect_timeout_s": 60,
  "maximum_gcs_destroyed_actor_cached_count": 10,
  "maximum_gcs_dead_node_cached_count": 10,
  "gcs_grpc_based_pubsub": $0
}
  )",
                         use_gcs_pubsub_ ? "true" : "false"));
    TestSetupUtil::StartUpRedisServers(std::vector<int>());
  }

  virtual ~GcsClientTest() { TestSetupUtil::ShutDownRedisServers(); }

 protected:
  void SetUp() override {
    config_.grpc_server_port = 0;
    config_.grpc_server_name = "MockedGcsServer";
    config_.grpc_server_thread_num = 1;
    config_.redis_address = "127.0.0.1";
    config_.node_ip_address = "127.0.0.1";
    config_.enable_sharding_conn = false;
    config_.redis_port = TEST_REDIS_SERVER_PORTS.front();
    // Tests legacy code paths. The poller and broadcaster have their own dedicated unit
    // test targets.
    config_.grpc_based_resource_broadcast = false;
    config_.grpc_pubsub_enabled = use_gcs_pubsub_;

    client_io_service_.reset(new instrumented_io_context());
    client_io_service_thread_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*client_io_service_));
      client_io_service_->run();
    }));

    server_io_service_.reset(new instrumented_io_context());
    gcs_server_.reset(new gcs::GcsServer(config_, *server_io_service_));
    gcs_server_->Start();
    server_io_service_thread_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*server_io_service_));
      server_io_service_->run();
    }));

    // Wait until server starts listening.
    while (!gcs_server_->IsStarted()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    RAY_LOG(INFO) << "GCS service, port = " << gcs_server_->GetPort();

    // Create GCS client.
    gcs::GcsClientOptions options(config_.redis_address, config_.redis_port,
                                  config_.redis_password);
    gcs_client_.reset(new gcs::GcsClient(options));
    RAY_CHECK_OK(gcs_client_->Connect(*client_io_service_));
  }

  void TearDown() override {
    client_io_service_->stop();
    client_io_service_thread_->join();
    gcs_client_->Disconnect();

    server_io_service_->stop();
    server_io_service_thread_->join();
    gcs_server_->Stop();
    gcs_server_.reset();
    TestSetupUtil::FlushAllRedisServers();
  }

  // Test parameter, whether to use GCS pubsub instead of Redis pubsub.
  const bool use_gcs_pubsub_;

  // GCS server.
  gcs::GcsServerConfig config_;
  std::unique_ptr<gcs::GcsServer> gcs_server_;
  std::unique_ptr<std::thread> server_io_service_thread_;
  std::unique_ptr<instrumented_io_context> server_io_service_;

  // GCS client.
  std::unique_ptr<std::thread> client_io_service_thread_;
  std::unique_ptr<instrumented_io_context> client_io_service_;
  std::unique_ptr<gcs::GcsClient> gcs_client_;

  // Timeout waiting for GCS server reply, default is 2s.
  const std::chrono::milliseconds timeout_ms_{2000};
};

TEST_F(GcsClientTest, Test) {
  /// Share the same io_service with gcs client.
  auto periodical_runner_ = std::make_unique<PeriodicalRunner>(*client_io_service_);
  periodical_runner_->RunFnPeriodically(
      [this] {
        /// Mock what the `GcsClient::ReconnectGcsServer` does.
        while (true) {
          rpc::ActorTableData result;
          std::string ray_namespace;
          gcs_client_->Actors().SyncGetByName("test", ray_namespace, result);
          RAY_LOG(INFO) << "Never be called, because the io_service is occupied by periodical_runner_";
        }
      },
      1000, "test");

  while (true) {
    sleep(1);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog, argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
