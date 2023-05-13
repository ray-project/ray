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

TEST_F(RedisStoreClientTest, AsyncGetAllAndBatchDeleteTest) {
  TestAsyncGetAllAndBatchDelete();
}

TEST_F(RedisStoreClientTest, BasicSimple) {
  // Send 100 times write and then read
  for (size_t i = 0; i < 100; ++i) {
    for (size_t j = 0; j < 20; ++j) {
      store_client_->AsyncPut(
          "T",
          absl::StrCat("A", std::to_string(j)),
          std::to_string(i),
          false,
          [i](auto r) { ASSERT_TRUE((i == 0 && r) || (i != 0 && !r)); });
    }
  }
  store_client_->AsyncGet("T", "A", [](auto s, auto r) {
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(*r, "99");
  });
}

TEST_F(RedisStoreClientTest, Complicated) {
  int window = 10;
  std::atomic<size_t> finished{0};
  std::atomic<size_t> sent{0};

  for (int i = 0; i < 1000; i += window) {
    std::vector<std::string> keys;
    for (int j = i; j < i + window; ++j) {
      ++sent;
      RAY_LOG(INFO) << "S AsyncPut: " << ("P_" + std::to_string(j));
      store_client_->AsyncPut("N",
                              "P_" + std::to_string(j),
                              std::to_string(j),
                              true,
                              [&finished, j](auto r) mutable {
                                RAY_LOG(INFO)
                                    << "F AsyncPut: " << ("P_" + std::to_string(j));
                                ++finished;
                                ASSERT_TRUE(r);
                              });
      keys.push_back(std::to_string(j));
    }

    std::vector<std::string> p_keys;
    for (auto &key : keys) {
      p_keys.push_back("P_" + key);
    }

    std::vector<std::string> n_keys;
    for (auto &key : keys) {
      n_keys.push_back("N_" + key);
    }

    ++sent;
    RAY_LOG(INFO) << "S AsyncMultiGet: " << absl::StrJoin(p_keys, ",");
    store_client_->AsyncMultiGet(
        "N",
        p_keys,
        [&finished, i, keys, window, &sent, p_keys, n_keys, this](auto m) mutable {
          RAY_LOG(INFO) << "F SendAsyncMultiGet: " << absl::StrJoin(p_keys, ",");
          ++finished;
          ASSERT_EQ(keys.size(), m.size());
          for (auto &key : keys) {
            ASSERT_EQ(m["P_" + key], key);
          }

          if ((i / window) % 2 == 0) {
            // Delete non exist keys
            for (size_t i = 0; i < keys.size(); ++i) {
              ++sent;
              RAY_LOG(INFO) << "S AsyncDelete: " << n_keys[i];
              store_client_->AsyncDelete(
                  "N", n_keys[i], [&finished, n_keys, i](auto b) mutable {
                    RAY_LOG(INFO) << "F AsyncDelete: " << n_keys[i];
                    ++finished;
                    ASSERT_FALSE(b);
                  });

              ++sent;
              RAY_LOG(INFO) << "S AsyncExists: " << p_keys[i];
              store_client_->AsyncExists(
                  "N", p_keys[i], [&finished, p_keys, i](auto b) mutable {
                    RAY_LOG(INFO) << "F AsyncExists: " << p_keys[i];
                    ++finished;
                    ASSERT_TRUE(b);
                  });
            }
          } else {
            ++sent;
            RAY_LOG(INFO) << "S AsyncBatchDelete: " << absl::StrJoin(p_keys, ",");
            store_client_->AsyncBatchDelete(
                "N", p_keys, [&finished, p_keys, keys](auto n) mutable {
                  RAY_LOG(INFO) << "F AsyncBatchDelete: " << absl::StrJoin(p_keys, ",");
                  ++finished;
                  ASSERT_EQ(n, keys.size());
                });

            for (auto p_key : p_keys) {
              ++sent;
              RAY_LOG(INFO) << "S AsyncExists: " << p_key;
              store_client_->AsyncExists("N", p_key, [&finished, p_key](auto b) mutable {
                RAY_LOG(INFO) << "F AsyncExists: " << p_key;
                ++finished;
                ASSERT_FALSE(false);
              });
            }
          }
        });

    ASSERT_TRUE(WaitForCondition(
        [&finished, &sent]() {
          RAY_LOG(INFO) << finished << "/" << sent;
          return finished == sent;
        },
        5000));
  }
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
