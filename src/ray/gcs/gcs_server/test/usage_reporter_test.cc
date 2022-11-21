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

#include "ray/gcs/gcs_server/usage_reporter.h"

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/store_client_kv.h"
#include "ray/gcs/store_client/in_memory_store_client.h"

namespace ray {

class UsageReporterTest : public ::testing::Test {
 public:
  void SetUp() override {
    thread_io_service = std::make_unique<std::thread>([this] {
      boost::asio::io_service::work work(io_service);
      io_service.run();
    });

    kv = std::make_shared<ray::gcs::StoreClientInternalKV>(
        std::make_unique<ray::gcs::InMemoryStoreClient>(io_service));
    reporter = std::make_shared<ray::gcs::GcsUsageReporter>(io_service, kv);
  }

  void TearDown() override {
    io_service.stop();
    thread_io_service->join();
    reporter.reset();
    kv.reset();
  }

  void ExecuteIOService() {
    std::promise<int> p;
    std::future<int> f = p.get_future();
    io_service.post([&p]() { p.set_value(1); }, "");
    f.get();
  }

  instrumented_io_context io_service;
  std::unique_ptr<std::thread> thread_io_service;
  std::shared_ptr<ray::gcs::InternalKVInterface> kv;
  std::shared_ptr<ray::gcs::GcsUsageReporter> reporter;
};

TEST_F(UsageReporterTest, SmokeTest) {
  reporter->ReportCounter(ray::usage::TagKey::_TEST1, 3);
  ExecuteIOService();
  kv->Get("usage_stats", "extra_usage_tag__test1", [](auto b) { ASSERT_EQ("3", *b); });
  ExecuteIOService();
  reporter->ReportValue(ray::usage::TagKey::_TEST2, "hello");
  ExecuteIOService();
  kv->Get(
      "usage_stats", "extra_usage_tag__test2", [](auto b) { ASSERT_EQ("hello", *b); });
  ExecuteIOService();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
