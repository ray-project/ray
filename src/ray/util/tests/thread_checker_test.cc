// Copyright 2024 The Ray Authors.
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

#include "ray/util/thread_checker.h"

#include <gtest/gtest.h>

#include <thread>

namespace ray {

TEST(ThreadCheckerTest, BasicTest) {
  ThreadChecker thread_checker;
  // Pass at initialization.
  ASSERT_TRUE(thread_checker.IsOnSameThread());
  // Pass when invoked at the same thread.
  ASSERT_TRUE(thread_checker.IsOnSameThread());

  auto thd = std::thread([&]() { ASSERT_FALSE(thread_checker.IsOnSameThread()); });
  thd.join();
}

}  // namespace ray
