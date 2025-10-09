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

#include "ray/gcs/gcs_kv_manager.h"

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/gcs/store_client_kv.h"

class GcsKVManagerTest : public ::testing::TestWithParam<std::string> {
 public:
  GcsKVManagerTest() { ray::TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  void SetUp() override {
    thread_io_service = std::make_unique<std::thread>([this] {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          io_service.get_executor());
      io_service.run();
    });
    ray::gcs::RedisClientOptions options{"127.0.0.1",
                                         ray::TEST_REDIS_SERVER_PORTS.front()};
    if (GetParam() == "redis") {
      kv_instance = std::make_unique<ray::gcs::StoreClientInternalKV>(
          std::make_unique<ray::gcs::RedisStoreClient>(io_service, options));
    } else if (GetParam() == "memory") {
      kv_instance = std::make_unique<ray::gcs::StoreClientInternalKV>(
          std::make_unique<ray::gcs::InMemoryStoreClient>());
    }
  }

  void TearDown() override {
    io_service.stop();
    thread_io_service->join();
    kv_instance.reset();
  }

  std::unique_ptr<std::thread> thread_io_service;
  instrumented_io_context io_service;
  std::unique_ptr<ray::gcs::InternalKVInterface> kv_instance;

  /// Synchronous version of Get
  std::optional<std::string> SyncGet(const std::string &ns, const std::string &key) {
    std::promise<std::optional<std::string>> p;
    kv_instance->Get(ns, key, {[&p](auto result) { p.set_value(result); }, io_service});
    return p.get_future().get();
  }

  /// Synchronous version of MultiGet
  absl::flat_hash_map<std::string, std::string> SyncMultiGet(
      const std::string &ns, const std::vector<std::string> &keys) {
    std::promise<absl::flat_hash_map<std::string, std::string>> p;
    kv_instance->MultiGet(
        ns, keys, {[&p](auto result) { p.set_value(result); }, io_service});
    return p.get_future().get();
  }

  /// Synchronous version of Put
  bool SyncPut(const std::string &ns,
               const std::string &key,
               std::string value,
               bool overwrite) {
    std::promise<bool> p;
    kv_instance->Put(ns,
                     key,
                     std::move(value),
                     overwrite,
                     {[&p](auto result) { p.set_value(result); }, io_service});
    return p.get_future().get();
  }

  /// Synchronous version of Del
  int64_t SyncDel(const std::string &ns, const std::string &key, bool del_by_prefix) {
    std::promise<int64_t> p;
    kv_instance->Del(
        ns, key, del_by_prefix, {[&p](auto result) { p.set_value(result); }, io_service});
    return p.get_future().get();
  }

  /// Synchronous version of Exists
  bool SyncExists(const std::string &ns, const std::string &key) {
    std::promise<bool> p;
    kv_instance->Exists(
        ns, key, {[&p](auto result) { p.set_value(result); }, io_service});
    return p.get_future().get();
  }

  /// Synchronous version of Keys
  std::vector<std::string> SyncKeys(const std::string &ns, const std::string &prefix) {
    std::promise<std::vector<std::string>> p;
    kv_instance->Keys(
        ns, prefix, {[&p](auto result) { p.set_value(result); }, io_service});
    return p.get_future().get();
  }
};

// This test is intended to be serialized. To avoid callback hell we define helper
// SyncGet and SyncPut.
TEST_P(GcsKVManagerTest, TestInternalKV) {
  ASSERT_FALSE(SyncGet("N1", "A").has_value());
  ASSERT_TRUE(SyncPut("N1", "A", "B", false));
  ASSERT_FALSE(SyncPut("N1", "A", "C", false));
  ASSERT_EQ("B", *SyncGet("N1", "A"));
  ASSERT_FALSE(SyncPut("N1", "A", "C", true));
  ASSERT_EQ("C", *SyncGet("N1", "A"));
  ASSERT_TRUE(SyncPut("N1", "A_1", "B", false));
  ASSERT_TRUE(SyncPut("N1", "A_2", "C", false));
  ASSERT_TRUE(SyncPut("N1", "A_3", "C", false));

  auto keys = SyncKeys("N1", "A_");
  auto expected = std::set<std::string>{"A_1", "A_2", "A_3"};
  ASSERT_EQ(expected, std::set<std::string>(keys.begin(), keys.end()));

  ASSERT_FALSE(SyncGet("N2", "A_1").has_value());
  ASSERT_TRUE(SyncGet("N1", "A_1").has_value());

  auto multi_get_result = SyncMultiGet("N1", {"A_1", "A_2", "A_3"});
  ASSERT_EQ(3, multi_get_result.size());
  ASSERT_EQ("B", multi_get_result["A_1"]);
  ASSERT_EQ("C", multi_get_result["A_2"]);
  ASSERT_EQ("C", multi_get_result["A_3"]);

  // MultiGet with empty keys.
  ASSERT_EQ(0, SyncMultiGet("N1", {}).size());
  // MultiGet with non-existent keys.
  ASSERT_EQ(0, SyncMultiGet("N1", {"A_4", "A_5"}).size());

  // Delete by prefix
  ASSERT_EQ(3, SyncDel("N1", "A_", true));
  ASSERT_EQ(0, SyncDel("NX", "A_", true));

  // Make sure keys are deleted
  ASSERT_FALSE(SyncGet("N1", "A_1").has_value());
  ASSERT_EQ(0, SyncMultiGet("N1", {"A_1", "A_2", "A_3"}).size());
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
