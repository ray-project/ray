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

#include "ray/gcs/cluster_config.h"

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/store_client_kv.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/store_client/store_client.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

class ClusterConfigTest : public ::testing::Test {
 public:
  ClusterConfigTest()
      : options_("127.0.0.1", "", TEST_REDIS_SERVER_PORTS.front(), false) {
    TestSetupUtil::StartUpRedisServers(std::vector<int>());
    RayConfig::instance().initialize(
        R"(
{
  "gcs_rpc_server_reconnect_timeout_s": 60,
  "maximum_gcs_destroyed_actor_cached_count": 10,
  "maximum_gcs_dead_node_cached_count": 10,
  "gcs_storage": redis
}
    )");
  }

  virtual ~ClusterConfigTest() { TestSetupUtil::ShutDownRedisServers(); }

 protected:
  void SetUp() override {}
  void TearDown() override {}

  gcs::RedisClientOptions options_;
};

TEST_F(ClusterConfigTest, TestBasic) {
  RAY_LOG(INFO) << "BBBB";
  gcs::ClusterConfig config;
  config.session_name = "session_xyz";
  config.temp_dir = "/tmp/ray";

  InitClusterConfig(options_, true /* is_head */, config);

  ASSERT_TRUE(true);

  // TODO check that redis has the right values
};

}  // namespace ray