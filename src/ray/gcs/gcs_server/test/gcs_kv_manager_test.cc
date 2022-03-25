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
    if (GetParam() == "redis") {
      kv_instance = std::make_unique<ray::gcs::RedisInternalKV>(redis_client_options);
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
  kv_instance->Get("N1", "A", [](auto b) { ASSERT_FALSE(b.has_value()); });
  kv_instance->Put("N1", "A", "B", false, [](auto b) { ASSERT_TRUE(b); });
  kv_instance->Put("N1", "A", "C", false, [](auto b) { ASSERT_FALSE(b); });
  kv_instance->Get("N1", "A", [](auto b) { ASSERT_EQ("B", *b); });
  kv_instance->Put("N1", "A", "C", true, [](auto b) { ASSERT_FALSE(b); });
  kv_instance->Get("N1", "A", [](auto b) { ASSERT_EQ("C", *b); });
  kv_instance->Put("N1", "A_1", "B", false, [](auto b) { ASSERT_TRUE(b); });
  kv_instance->Put("N1", "A_2", "C", false, [](auto b) { ASSERT_TRUE(b); });
  kv_instance->Put("N1", "A_3", "C", false, [](auto b) { ASSERT_TRUE(b); });
  kv_instance->Keys("N1", "A_", [](std::vector<std::string> keys) {
    auto expected = std::set<std::string>{"A_1", "A_2", "A_3"};
    ASSERT_EQ(expected, std::set<std::string>(keys.begin(), keys.end()));
  });
  kv_instance->Get("N2", "A_1", [](auto b) { ASSERT_FALSE(b.has_value()); });
  kv_instance->Get("N1", "A_1", [](auto b) { ASSERT_TRUE(b.has_value()); });
  {
    // Delete by prefix are two steps in redis mode, so we need sync here.
    std::promise<void> p;
    kv_instance->Del("N1", "A_", true, [&p](auto b) {
      ASSERT_EQ(3, b);
      p.set_value();
    });
    p.get_future().get();
  }
  {
    // Delete by prefix are two steps in redis mode, so we need sync here.
    std::promise<void> p;
    kv_instance->Del("NX", "A_", true, [&p](auto b) {
      ASSERT_EQ(0, b);
      p.set_value();
    });
    p.get_future().get();
  }

  {
    // Make sure the last cb is called.
    std::promise<void> p;
    kv_instance->Get("N1", "A_1", [&p](auto b) {
      ASSERT_FALSE(b.has_value());
      p.set_value();
    });
    p.get_future().get();
  }
}

INSTANTIATE_TEST_SUITE_P(GcsKVManagerTestFixture,
                         GcsKVManagerTest,
                         ::testing::Values("redis", "memory"));

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
