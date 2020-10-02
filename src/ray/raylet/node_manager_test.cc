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

#include <iostream>
#include <thread>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/status.h"
#include "ray/common/test_util.h"
#include "ray/raylet/node_manager.h"
#include "ray/raylet/raylet.h"
#include "ray/object_manager/object_directory.h"
#include "ray/util/filesystem.h"

extern "C" {
#include "hiredis/hiredis.h"
}

static inline void flushall_redis(void) {
  redisContext *context = redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  redisFree(context);
}

namespace ray {

namespace raylet {

class MockObjectManager : public ObjectManager {
 public:
  MockObjectManager(boost::asio::io_service &main_service,
                    const NodeID &self_node_id,
                    const ObjectManagerConfig &config,
                    std::shared_ptr<ObjectDirectoryInterface> object_directory) :
      ObjectManager(main_service, self_node_id, config, object_directory) {}

  ray::Status SubscribeObjAdded(
      std::function<void(const object_manager::protocol::ObjectInfoT &)> callback) {
    return Status::OK();
  }
};

class MockObjectDirectory : public ObjectDirectoryInterface {
 public:
  MockObjectDirectory() {}

  ray::Status LookupLocations(const ObjectID &object_id,
                              const rpc::Address &owner_address,
                              const OnLocationsFound &callback) override {
    callbacks_.push_back({object_id, callback});
    return ray::Status::OK();
  }

  void FlushCallbacks() {
    for (const auto &callback : callbacks_) {
      const ObjectID object_id = callback.first;
      auto it = locations_.find(object_id);
      if (it == locations_.end()) {
        callback.second(object_id, std::unordered_set<ray::NodeID>());
      } else {
        callback.second(object_id, it->second);
      }
    }
    callbacks_.clear();
  }

  void SetObjectLocations(const ObjectID &object_id,
                          const std::unordered_set<NodeID> &locations) {
    locations_[object_id] = locations;
  }

  void HandleClientRemoved(const NodeID &client_id) override {
    for (auto &locations : locations_) {
      locations.second.erase(client_id);
    }
  }

  std::string DebugString() const override { return ""; }

  MOCK_METHOD0(GetLocalClientID, ray::NodeID());
  MOCK_CONST_METHOD1(LookupRemoteConnectionInfo, void(RemoteConnectionInfo &));
  MOCK_CONST_METHOD0(LookupAllRemoteConnections, std::vector<RemoteConnectionInfo>());
  MOCK_METHOD4(SubscribeObjectLocations,
      ray::Status(const ray::UniqueID &, const ObjectID &,
                  const rpc::Address &owner_address, const OnLocationsFound &));
  MOCK_METHOD2(UnsubscribeObjectLocations,
      ray::Status(const ray::UniqueID &, const ObjectID &));
  MOCK_METHOD3(ReportObjectAdded,
      ray::Status(const ObjectID &, const NodeID &,
                  const object_manager::protocol::ObjectInfoT &));
  MOCK_METHOD3(ReportObjectRemoved,
      ray::Status(const ObjectID &, const NodeID &,
                  const object_manager::protocol::ObjectInfoT &));

 private:
  std::vector<std::pair<ObjectID, OnLocationsFound>> callbacks_;
  std::unordered_map<ObjectID, std::unordered_set<NodeID>> locations_;
};

class TestNodeManager : public ::testing::Test {
 public:
  TestNodeManager() {
    std::vector<int> ports;
    ports.push_back(6379);
    TestSetupUtil::StartUpRedisServers(ports);
    RAY_LOG(INFO) << "Finish StartUpRedisServers...";
    config_.heartbeat_period_ms = 100000;
  }

  virtual ~TestNodeManager() {
    TestSetupUtil::ShutDownRedisServers();
  }

  void SetUp() {
    client_io_service_.reset(new boost::asio::io_service());
    client_io_service_thread_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*client_io_service_));
      client_io_service_->run();
    }));

    RAY_LOG(INFO) << "Begin Finish flushall_redis...";
    flushall_redis();

    RAY_LOG(INFO) << "Begin StartObjectStore...";
    // start store
    socket_name_1 = TestSetupUtil::StartObjectStore();
    RAY_LOG(INFO) << "Finish StartObjectStore... socket_name_1 is " << socket_name_1;

    unsigned int pull_timeout_ms = 1;
    push_timeout_ms = 1000;

    // start first server
    RAY_LOG(INFO) << "Begin start gcs_client_1...";
    gcs::GcsClientOptions client_options("127.0.0.1", 6379, /*password*/ "",
        /*is_test_client=*/true);
    gcs_client_1 = std::make_shared<gcs::RedisGcsClient>(client_options);
    RAY_CHECK_OK(gcs_client_1->Connect(*client_io_service_));
    RAY_LOG(INFO) << "Finish start gcs_client_1...";

    NodeID node_id = NodeID::FromRandom();
    RAY_LOG(INFO) << "Setup 11111";
    object_directory_.reset(new MockObjectDirectory());
    RAY_LOG(INFO) << "Setup 2222";
    io_service_.reset(new boost::asio::io_service());
    RAY_LOG(INFO) << "Setup 3333";
    ObjectManagerConfig om_config_1;
    om_config_1.store_socket_name = socket_name_1;
    om_config_1.pull_timeout_ms = pull_timeout_ms;
    om_config_1.object_chunk_size = object_chunk_size;
    om_config_1.push_timeout_ms = push_timeout_ms;
    om_config_1.object_manager_port = 0;
    om_config_1.rpc_service_threads_number = 3;
    object_manager_.reset(new MockObjectManager(*io_service_,
                                                node_id,
                                                om_config_1,
                                                std::make_shared<ObjectDirectory>(*io_service_, gcs_client_)));
    RAY_LOG(INFO) << "Setup 4444";
    config_.store_socket_name = socket_name_1;
    config_.maximum_startup_concurrency = 1;
    config_.node_manager_address = "127.0.0.1";
    config_.node_manager_port = 5566;
    node_manager_.reset(new NodeManager(*io_service_, node_id, config_, *object_manager_,
                                        gcs_client_, object_directory_));
    RAY_LOG(INFO) << "Setup 5555";
    io_service_thread_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(*io_service_));
      io_service_->run();
    }));

    client_call_manager_.reset(new rpc::ClientCallManager(*client_io_service_));
    node_manager_rpc_client_ = rpc::NodeManagerWorkerClient::make(config_.node_manager_address,
                                                                  config_.node_manager_port,
                                                                  *client_call_manager_);
  }

  void TearDown() {
    io_service_->stop();
    io_service_thread_->join();
    node_manager_.reset();

    client_io_service_->stop();
    client_io_service_thread_->join();

    TestSetupUtil::StopObjectStore(socket_name_1);
  }

 protected:
  std::unique_ptr<std::thread> io_service_thread_;
  std::unique_ptr<boost::asio::io_service> io_service_;

  NodeManagerConfig config_;
  std::shared_ptr<ObjectManager> object_manager_;
  std::shared_ptr<gcs::GcsClient> gcs_client_;
  std::shared_ptr<ObjectDirectoryInterface> object_directory_;
  std::unique_ptr<NodeManager> node_manager_;

  std::unique_ptr<std::thread> client_io_service_thread_;
  std::unique_ptr<boost::asio::io_service> client_io_service_;
  std::shared_ptr<gcs::GcsClient> gcs_client_1;

  std::string socket_name_1;
  unsigned int push_timeout_ms;
  uint64_t object_chunk_size = static_cast<uint64_t>(std::pow(10, 3));


  // Raylet rpc client
  std::shared_ptr<rpc::NodeManagerWorkerClient> node_manager_rpc_client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;
};

TEST_F(TestNodeManager, BaseTest) {
  RAY_LOG(INFO) << "Hello, i am the first testcase...";
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
