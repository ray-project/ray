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

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/gcs_server/test/gcs_table_storage_test_base.h"
#include "ray/gcs/store_client/redis_store_client.h"

namespace ray {

class RedisGcsTableStorageTest : public gcs::GcsTableStorageTestBase {
 public:
  static void SetUpTestCase() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  static void TearDownTestCase() { TestSetupUtil::ShutDownRedisServers(); }

  void SetUp() override {
    gcs::RedisClientOptions options("127.0.0.1",
                                    TEST_REDIS_SERVER_PORTS.front(),
                                    "",
                                    /*enable_sharding_conn=*/false);
    redis_client_ = std::make_shared<gcs::RedisClient>(options);
    RAY_CHECK_OK(redis_client_->Connect(io_service_pool_->GetAll()));

    gcs_table_storage_ = std::make_shared<gcs::RedisGcsTableStorage>(redis_client_);
  }

  void TearDown() override { redis_client_->Disconnect(); }

 protected:
  std::shared_ptr<gcs::RedisClient> redis_client_;
};

TEST_F(RedisGcsTableStorageTest, TestGcsTableApi) { TestGcsTableApi(); }

TEST_F(RedisGcsTableStorageTest, TestGcsTableWithJobIdApi) { TestGcsTableWithJobIdApi(); }

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
