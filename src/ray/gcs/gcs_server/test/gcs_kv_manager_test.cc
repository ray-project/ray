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

#include "ray/gcs/gcs_server/gcs_kv_manager.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/test_util.h"

class GcsKVManagerTest : public ::testing::TestWithParam<std::string> {
 public:
  GcsKVManagerTest() { ray::TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  void SetUp() override {
    thread_io_service = std::make_unique<std::thread>([this] {
      boost::asio::io_service::work work(io_service);
      io_service.run();
    });
    ASSERT_TRUE(GetParam() == "redis" || GetParam() == "memory");
    ray::gcs::RedisClientOptions redis_client_options(
        "127.0.0.1", ray::TEST_REDIS_SERVER_PORTS.front(), "", false);
    redis_client = std::make_unique<ray::gcs::RedisClient>(redis_client_options);
    auto status = redis_client->Connect(io_service);
    RAY_CHECK(status.ok()) << "Failed to init redis gcs client as " << status;
    if (GetParam() == "redis") {
      kv_instance = std::make_unique<ray::gcs::RedisInternalKV>(redis_client.get());
    } else if (GetParam() == "memory") {
      kv_instance = std::make_unique<ray::gcs::MemoryInternalKV>(io_service);
    }
  }

  void TearDown() override {
    io_service.stop();
    thread_io_service->join();
    redis_client.reset();
    kv_instance.reset();
  }

  std::unique_ptr<ray::gcs::RedisClient> redis_client;
  std::unique_ptr<std::thread> thread_io_service;
  instrumented_io_context io_service;
  std::unique_ptr<ray::gcs::InternalKVInterface> kv_instance;
};

TEST_P(GcsKVManagerTest, TestInternalKV) {
  kv_instance->Get("A", [](auto b) { ASSERT_FALSE(b.has_value()); });
  kv_instance->Put("A", "B", false, [](auto b) { ASSERT_TRUE(b); });
  kv_instance->Put("A", "C", false, [](auto b) { ASSERT_FALSE(b); });
  kv_instance->Get("A", [](auto b) { ASSERT_EQ("B", *b); });
  kv_instance->Put("A", "C", true, [](auto b) { ASSERT_FALSE(b); });
  kv_instance->Get("A", [](auto b) { ASSERT_EQ("C", *b); });

  kv_instance->Put("A_1", "B", false, [](auto b) { ASSERT_TRUE(b); });
  kv_instance->Put("A_2", "C", false, [](auto b) { ASSERT_TRUE(b); });
  kv_instance->Put("A_3", "C", false, [](auto b) { ASSERT_TRUE(b); });

  kv_instance->Keys("A_", [](std::vector<std::string> keys) {
    auto expected = std::vector<std::string>{"A_1", "A_2", "A_3"};
    ASSERT_EQ(expected, keys);
  });
}

INSTANTIATE_TEST_SUITE_P(GcsKVManagerTestFixture, GcsKVManagerTest,
                         ::testing::Values("redis", "memory"));

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
