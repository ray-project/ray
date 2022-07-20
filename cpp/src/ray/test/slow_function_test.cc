// Copyright 2020-2021 The Ray Authors.
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

#include <gtest/gtest.h>
#include <ray/api.h>

#include <chrono>
#include <thread>

int slow_function(int i) {
  std::this_thread::sleep_for(std::chrono::seconds(i));
  return i;
}

RAY_REMOTE(slow_function);

TEST(RaySlowFunctionTest, BaseTest) {
  ray::RayConfig config;
  config.local_mode = true;
  ray::Init(config);
  auto time1 = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  auto r0 = ray::Task(slow_function).Remote(1);
  auto r1 = ray::Task(slow_function).Remote(2);
  auto r2 = ray::Task(slow_function).Remote(3);
  auto r3 = ray::Task(slow_function).Remote(4);

  int result0 = *(r0.Get());
  int result1 = *(r1.Get());
  int result2 = *(r2.Get());
  int result3 = *(r3.Get());
  auto time2 = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());

  EXPECT_EQ(result0, 1);
  EXPECT_EQ(result1, 2);
  EXPECT_EQ(result2, 3);
  EXPECT_EQ(result3, 4);

  EXPECT_LT(time2.count() - time1.count(), 4200);
}
