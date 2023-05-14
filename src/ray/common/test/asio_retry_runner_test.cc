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

#include <string>

#include "gtest/gtest.h"
#include "ray/common/asio/asio_util.h"

using namespace std::chrono_literals;

class RetryRunnerTest : public ::testing::Test {
 public:
  RetryRunnerTest() {}
  void SetUp() override {
    t = std::make_unique<std::thread>([this]() {
      boost::asio::executor_work_guard<instrumented_io_context::executor_type> work_guard(
          io_context.get_executor());
      io_context.run();
    });
  }
  void TearDown() override {
    io_context.stop();
    t->join();
  }

  instrumented_io_context io_context;
  std::unique_ptr<std::thread> t;
};

TEST_F(RetryRunnerTest, Basic) {
  int count = 0;
  auto fn = [&count]() mutable {
    count++;
    return count == 3;
  };
  // Retry 1 time, wait 5ms between retries.
  ASSERT_FALSE(ray::async_retry(io_context.get_executor(), fn, 1, 5ms, boost::asio::use_future).get());
  ASSERT_EQ(2, count);

  count = 0;
  // Retry 2 times, wait 5ms between retries.
  ASSERT_TRUE(ray::async_retry(io_context.get_executor(), fn, 2, 5ms, boost::asio::use_future).get());
  ASSERT_EQ(3, count);

  count = 0;
  // Test default completion token works
  ASSERT_TRUE(ray::async_retry(io_context.get_executor(), fn, 2, 5ms).get());
  ASSERT_EQ(3, count);

  count = 0;
  // Test default completion token works
  ASSERT_TRUE(ray::async_retry(io_context.get_executor(), fn, -1, 5ms).get());
  ASSERT_EQ(3, count);
}
