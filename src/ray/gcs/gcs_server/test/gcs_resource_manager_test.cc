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

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

using ::testing::_;

class GcsResourceManagerTest : public ::testing::Test {
 public:
  GcsResourceManagerTest() {
    std::promise<bool> promise;
    thread_io_service_.reset(new std::thread([this, &promise] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      promise.set_value(true);
      io_service_.run();
    }));
    promise.get_future().get();

    gcs_pub_sub_ = std::make_shared<GcsServerMocker::MockGcsPubSub>(redis_client_);
    gcs_table_storage_ = std::make_shared<gcs::RedisGcsTableStorage>(redis_client_);
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        io_service_, io_service_, gcs_pub_sub_, gcs_table_storage_);
    gcs_resource_manager_ = std::make_shared<gcs::GcsResourceManager>(*gcs_node_manager_);
  }

  virtual ~GcsResourceManagerTest() {
    io_service_.stop();
    thread_io_service_->join();
    gcs_node_manager_.reset();
  }

  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<gcs::GcsResourceManagerInterface> gcs_resource_manager_;

 private:
  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
  std::shared_ptr<gcs::RedisClient> redis_client_;
  std::shared_ptr<GcsServerMocker::MockGcsPubSub> gcs_pub_sub_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;

  const std::chrono::milliseconds timeout_ms_{2000};
};

TEST_F(GcsResourceManagerTest, TestBasic) {
  // Add node resources.
  auto node_id = NodeID::FromRandom();
  rpc::HeartbeatTableData heartbeat;
  const std::string cpu_resource = "CPU";
  heartbeat.set_client_id(node_id.Binary());
  (*heartbeat.mutable_resources_available())[cpu_resource] = 10;
  gcs_node_manager_->UpdateNodeRealtimeResources(node_id, heartbeat);

  // Get and check cluster resources.
  const auto &cluster_resource = gcs_resource_manager_->GetClusterResources();
  ASSERT_EQ(1, cluster_resource.size());

  // Test `AcquireResource`.
  std::unordered_map<std::string, double> resource_map;
  resource_map[cpu_resource] = 10;
  ResourceSet resource_set(resource_map);
  ASSERT_FALSE(
      gcs_resource_manager_->AcquireResource(NodeID::FromRandom(), resource_set));
  ASSERT_TRUE(gcs_resource_manager_->AcquireResource(node_id, resource_set));
  ASSERT_FALSE(gcs_resource_manager_->AcquireResource(node_id, resource_set));

  // Test `ReleaseResource`.
  ASSERT_FALSE(
      gcs_resource_manager_->ReleaseResource(NodeID::FromRandom(), resource_set));
  ASSERT_TRUE(gcs_resource_manager_->ReleaseResource(node_id, resource_set));
  ASSERT_TRUE(gcs_resource_manager_->AcquireResource(node_id, resource_set));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
