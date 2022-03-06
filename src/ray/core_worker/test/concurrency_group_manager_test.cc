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

TEST(ConcurrencyGroupManagerTest, TestBasicConcurrencyGroupManager) {
  static auto empty = std::make_shared<ray::EmptyFunctionDescriptor>();

  auto func_1_in_io_group =
      FunctionDescriptorBuilder::BuildPython("io_module", "io_class", "io_func1", "");
  auto func_2_in_io_group =
      FunctionDescriptorBuilder::BuildPython("io_module", "io_class", "io_func2", "");
  auto func_1_in_executing_group = FunctionDescriptorBuilder::BuildPython(
      "executing_module", "executing_class", "executing_func1", "");
  auto func_2_in_executing_group = FunctionDescriptorBuilder::BuildPython(
      "executing_module", "executing_class", "executing_func2", "");
  const std::vector<ray::FunctionDescriptor> fds_in_io_group = {func_1_in_io_group,
                                                                func_2_in_io_group};

  const std::vector<ray::FunctionDescriptor> fds_in_executing_group = {
      func_1_in_executing_group, func_2_in_executing_group};

  std::vector<ConcurrencyGroup> defined_concurrency_groups = {
      {"io_group", 2, fds_in_io_group}, {"executing_group", 4, fds_in_executing_group}};

  ConcurrencyGroupManager<BoundedExecutor> manager(defined_concurrency_groups, 3);
  ASSERT_EQ(manager.GetDefaultExecutor()->GetMaxConcurrency(), 3);
  ASSERT_EQ(manager.GetExecutor("io_group", empty)->GetMaxConcurrency(), 2);
  ASSERT_EQ(manager.GetExecutor("executing_group", empty)->GetMaxConcurrency(), 4);

  ASSERT_EQ(manager.GetExecutor("", func_1_in_io_group)->GetMaxConcurrency(), 2);
  ASSERT_EQ(manager.GetExecutor("", func_2_in_io_group)->GetMaxConcurrency(), 2);
  auto func_3_in_io_group =
      FunctionDescriptorBuilder::BuildPython("io_module", "io_class", "io_func3", "");
  ASSERT_EQ(manager.GetExecutor("", func_3_in_io_group)->GetMaxConcurrency(), 3);

  ASSERT_EQ(manager.GetExecutor("", func_1_in_executing_group)->GetMaxConcurrency(), 4);
  ASSERT_EQ(manager.GetExecutor("", func_2_in_executing_group)->GetMaxConcurrency(), 4);

  manager.Stop();
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
