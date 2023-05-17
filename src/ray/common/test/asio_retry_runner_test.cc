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

#include <optional>
#include <string>

#include "gtest/gtest.h"
#include "ray/common/asio/asio_util.h"

using namespace std::chrono_literals;

class RetryRunnerTest : public ::testing::Test {
 public:
  RetryRunnerTest() {}
  void SetUp() override {
    t = std::make_unique<std::thread>([this]() {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard(
          io_context.get_executor());
      io_context.run();
    });
  }
  void TearDown() override {
    io_context.stop();
    t->join();
  }

  boost::asio::io_context io_context;
  std::unique_ptr<std::thread> t;
};

TEST_F(RetryRunnerTest, Basic) {
  int count = 0;
  auto fn = [&count]() mutable {
    count++;
    return count == 3;
  };
  // Retry 1 time, wait 5ms between retries.
  ASSERT_FALSE(
      async_retry_until(io_context.get_executor(), fn, 1, 5ms, boost::asio::use_future)
          .get());
  ASSERT_EQ(2, count);

  count = 0;
  // Retry 2 times, wait 5ms between retries.
  ASSERT_TRUE(
      async_retry_until(io_context.get_executor(), fn, 2, 5ms, boost::asio::use_future)
          .get());
  ASSERT_EQ(3, count);

  count = 0;
  // Test default completion token works
  ASSERT_TRUE(async_retry_until(io_context.get_executor(), fn, 2, 5ms).get());
  ASSERT_EQ(3, count);

  count = 0;
  // Test default completion token works
  ASSERT_TRUE(async_retry_until(io_context.get_executor(), fn, std::nullopt, 5ms).get());
  ASSERT_EQ(3, count);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
