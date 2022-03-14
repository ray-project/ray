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

#include "ray/gcs/store_client/redis_store_client.h"

#include "ray/common/test_util.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/store_client/test/store_client_test_base.h"

namespace ray {

namespace gcs {

class RedisStoreClientTest : public StoreClientTestBase {
 public:
  RedisStoreClientTest() {}

  virtual ~RedisStoreClientTest() {}

  static void SetUpTestCase() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  static void TearDownTestCase() { TestSetupUtil::ShutDownRedisServers(); }

  void InitStoreClient() override {
    RedisClientOptions options("127.0.0.1",
                               TEST_REDIS_SERVER_PORTS.front(),
                               "",
                               /*enable_sharding_conn=*/false);
    redis_client_ = std::make_shared<RedisClient>(options);
    RAY_CHECK_OK(redis_client_->Connect(io_service_pool_->GetAll()));

    store_client_ = std::make_shared<RedisStoreClient>(redis_client_);
  }

  void DisconnectStoreClient() override { redis_client_->Disconnect(); }

 protected:
  std::shared_ptr<RedisClient> redis_client_;
};

TEST_F(RedisStoreClientTest, AsyncPutAndAsyncGetTest) { TestAsyncPutAndAsyncGet(); }

TEST_F(RedisStoreClientTest, AsyncPutAndDeleteWithIndexTest) {
  TestAsyncPutAndDeleteWithIndex();
}

TEST_F(RedisStoreClientTest, AsyncGetAllAndBatchDeleteTest) {
  TestAsyncGetAllAndBatchDelete();
}

TEST_F(RedisStoreClientTest, TestAsyncDeleteWithIndex) { TestAsyncDeleteWithIndex(); }

TEST_F(RedisStoreClientTest, TestAsyncBatchDeleteWithIndex) {
  TestAsyncBatchDeleteWithIndex();
}

}  // namespace gcs

}  // namespace ray

int main(int argc, char **argv) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog,
                                         argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
