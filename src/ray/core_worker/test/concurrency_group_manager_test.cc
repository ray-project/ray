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

#include "ray/core_worker/transport/concurrency_group_manager.h"

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/transport/direct_actor_transport.h"

namespace ray {
namespace core {

TEST(ConcurrencyGroupManagerTest, TestEmptyConcurrencyGroupManager) {
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
// Emulate GCC's __SANITIZE_THREAD__ flag
#define __SANITIZE_THREAD__
#endif
#endif

#ifndef __SANITIZE_THREAD__
  // boost fiber doesn't have tsan support yet
  // https://github.com/boostorg/context/issues/124
  static auto empty = std::make_shared<ray::EmptyFunctionDescriptor>();
  ConcurrencyGroupManager<FiberState> manager;
  auto executor = manager.GetExecutor("", empty);
  ASSERT_EQ(manager.GetDefaultExecutor(), executor);
  manager.Stop();
#endif
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
