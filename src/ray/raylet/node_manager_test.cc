// Copyright 2017 The Ray Authors.
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

#include <thread>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/raylet/node_manager.h"

namespace ray {

namespace raylet {

class TestNodeManager : public ::testing::Test {
 public:
  TestNodeManager() {
    // Start redis.
    TestSetupUtil::StartUpRedisServers(std::vector<int>());
    RAY_LOG(INFO) << "Start redis successfully.";

    // Start store.
    store_socket_name_ = TestSetupUtil::StartObjectStore();
    RAY_LOG(INFO) << "Start object store successfully.";

    // Create object manager config.
    object_manager_config_.store_socket_name = store_socket_name_;
    object_manager_config_.pull_timeout_ms = 1;
    object_manager_config_.object_chunk_size = static_cast<uint64_t>(std::pow(10, 3));
    object_manager_config_.push_timeout_ms = 1000;
    object_manager_config_.object_manager_port = 0;
    object_manager_config_.rpc_service_threads_number = 3;

    // Create node manager config.
    node_manager_config_.heartbeat_period_ms = 100000;
    node_manager_config_.store_socket_name = store_socket_name_;
    node_manager_config_.maximum_startup_concurrency = 1;
    node_manager_config_.node_manager_address = "127.0.0.1";
    node_manager_config_.min_worker_port = 1000;
    node_manager_config_.max_worker_port = 30000;
  }

  virtual ~TestNodeManager() {
    // Stop store.
    TestSetupUtil::StopObjectStore(store_socket_name_);
    RAY_LOG(INFO) << "Stop object store successfully.";

    // Stop redis.
    TestSetupUtil::ShutDownRedisServers();
    RAY_LOG(INFO) << "Stop redis successfully.";
  }

  void SetUp() {
    server_io_service_.reset(new boost::asio::io_service());
    server_io_service_thread_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*server_io_service_));
      server_io_service_->run();
    }));

    client_io_service_.reset(new boost::asio::io_service());
    client_io_service_thread_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*client_io_service_));
      client_io_service_->run();
    }));

    // Start redis gcs client.
    gcs::GcsClientOptions client_options("127.0.0.1", TEST_REDIS_SERVER_PORTS.front(),
                                         /*password*/ "",
                                         /*is_test_client=*/true);
    gcs_client_ = std::make_shared<gcs::RedisGcsClient>(client_options);
    RAY_CHECK_OK(gcs_client_->Connect(*client_io_service_));
    RAY_LOG(INFO) << "Start redis gcs client successfully.";

    // Create object manager.
    NodeID node_id = NodeID::FromRandom();
    object_manager_.reset(new ObjectManager(
        *server_io_service_, node_id, object_manager_config_,
        std::make_shared<ObjectDirectory>(*server_io_service_, gcs_client_),
        [](const ObjectID &, const std::string &,
           const std::function<void(const ray::Status &)> &) {}));
    RAY_LOG(INFO) << "Create object manager successfully.";

    // Create node manager.
    node_manager_config_.node_manager_port = 5566;
    node_manager_.reset(new NodeManager(
        *server_io_service_, node_id, node_manager_config_, *object_manager_, gcs_client_,
        std::make_shared<ObjectDirectory>(*server_io_service_, gcs_client_), []() {}));
    RAY_LOG(INFO) << "Create node manager successfully.";

    // Create node manager rpc client.
    client_call_manager_.reset(new rpc::ClientCallManager(*client_io_service_));
    node_manager_rpc_client_ = rpc::NodeManagerWorkerClient::make(
        node_manager_config_.node_manager_address, node_manager_config_.node_manager_port,
        *client_call_manager_);
    RAY_LOG(INFO) << "Create node manager rpc client successfully.";
  }

  void TearDown() {
    server_io_service_->stop();
    server_io_service_thread_->join();
    node_manager_.reset();
    RAY_LOG(INFO) << "Stop node manager successfully.";

    client_io_service_->stop();
    client_io_service_thread_->join();
  }

 protected:
  std::string store_socket_name_;

  std::unique_ptr<std::thread> server_io_service_thread_;
  std::unique_ptr<boost::asio::io_service> server_io_service_;
  std::unique_ptr<std::thread> client_io_service_thread_;
  std::unique_ptr<boost::asio::io_service> client_io_service_;

  ObjectManagerConfig object_manager_config_;
  NodeManagerConfig node_manager_config_;
  std::unique_ptr<ObjectManager> object_manager_;
  std::shared_ptr<gcs::GcsClient> gcs_client_;
  std::unique_ptr<NodeManager> node_manager_;

  // Node manager rpc client.
  std::shared_ptr<rpc::NodeManagerWorkerClient> node_manager_rpc_client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
};

TEST_F(TestNodeManager, PrepareBundleResourcesTest) {
  rpc::Bundle message;
  message.mutable_bundle_id()->set_placement_group_id(
      PlacementGroupID::FromRandom().Binary());
  message.mutable_bundle_id()->set_bundle_index(0);
  rpc::PrepareBundleResourcesRequest request;
  request.mutable_bundle_spec()->CopyFrom(message);
  std::promise<bool> promise;
  node_manager_rpc_client_->PrepareBundleResources(
      request,
      [&promise](const Status &status, const rpc::PrepareBundleResourcesReply &reply) {
        promise.set_value(true);
      });
  promise.get_future().get();
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 5);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::TEST_REDIS_MODULE_LIBRARY_PATH = argv[3];
  ray::TEST_STORE_EXEC_PATH = std::string(argv[4]);
  return RUN_ALL_TESTS();
}
