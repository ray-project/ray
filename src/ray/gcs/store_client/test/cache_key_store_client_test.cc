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

#include "ray/gcs/store_client/cache_key_store_client.h"

#include "ray/gcs/store_client/test/store_client_test_base.h"

namespace ray {

namespace gcs {

class CacheKeyStoreClientTest : public StoreClientTestBase {
 public:
  void InitStoreClient() override {
    ::RayConfig::instance().use_redis_keys_cache() = true;

    RedisClientOptions options("127.0.0.1", TEST_REDIS_SERVER_PORTS.front(), "");
    redis_client_ = std::make_shared<RedisClient>(options);
    RAY_CHECK_OK(redis_client_->Connect(*io_service_pool_->Get()));

    auto redis_store_client = std::make_shared<RedisStoreClient>(redis_client_);
    store_client_ = std::make_shared<CacheKeyStoreClient>(redis_store_client);
  }

  void DisconnectStoreClient() override { redis_client_->Disconnect(); }

 protected:
  std::shared_ptr<RedisClient> redis_client_;
};

TEST_F(CacheKeyStoreClientTest, AsyncPutAndAsyncGetTest) { TestAsyncPutAndAsyncGet(); }

TEST_F(CacheKeyStoreClientTest, AsyncGetAllAndBatchDeleteTest) {
  TestAsyncGetAllAndBatchDelete();
}

}  // namespace gcs

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
