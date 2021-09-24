// Copyright 2021 The Ray Authors.
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

#include "ray/util/process.h"

#include <cstdlib>
#include <thread>
#include <vector>

#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gtest/gtest.h"

namespace ray {

TEST(EnvironmentTest, Get) {
  // Can read environment variable.
  setenv("TEST_VAR", "test_val", 1);
  ASSERT_EQ(GetEnvironment("TEST_VAR"), "test_val");

  // Environment variable is cached.
  setenv("TEST_VAR", "test_val_xxx", 1);
  ASSERT_EQ(GetEnvironment("TEST_VAR"), "test_val");

  // Non-existent environment variable.
  ASSERT_FALSE(GetEnvironment("TEST_VAR_NON_EXISTENT").has_value());
}

TEST(EnvironmentTest, ConcurrentGetAndCreateProcess) {
  absl::Notification notif;
  std::thread process([&notif]() {
    const std::vector<std::string> args = {"env"};
    const ProcessEnvironment env = {{"RAY_ADDRESS", "aaa.zzz"}};
    while (!notif.HasBeenNotified()) {
      ASSERT_FALSE(Process::Call(args, env));
    }
  });
  std::thread get_env([&notif]() {
    while (!notif.HasBeenNotified()) {
      if (!GetEnvironment("USER")) {
        FAIL() << "USER environment variable is empty";
      }
    }
  });

  absl::SleepFor(absl::Seconds(5));

  notif.Notify();
  process.join();
  get_env.join();
}

}  // namespace ray
