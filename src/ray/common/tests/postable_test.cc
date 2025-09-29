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

#include "ray/common/asio/postable.h"

#include <gtest/gtest.h>

#include <utility>

namespace ray {

TEST(PostableTest, TestPostable) {
  instrumented_io_context io_context;

  size_t counter = 0;
  Postable<void(int)> p{[&counter](int a) { counter += a; }, io_context};

  // Postable is not run if it is not posted.
  ASSERT_EQ(counter, 0);

  // Postable is not run if io_context is not running.
  std::move(p).Post("TestPostable", 1);
  ASSERT_EQ(counter, 0);

  // It posts exactly 1 task.
  ASSERT_EQ(io_context.poll(), 1);
  ASSERT_EQ(counter, 1);
}

TEST(PostableTest, TestPostableNoArgs) {
  instrumented_io_context io_context;

  size_t counter = 0;
  Postable<void()> p{[&counter]() { counter += 1; }, io_context};

  // Postable is not run if it is not posted.
  ASSERT_EQ(counter, 0);

  // Postable is not run if io_context is not running.
  std::move(p).Post("TestPostable");
  ASSERT_EQ(counter, 0);

  // It posts exactly 1 task.
  ASSERT_EQ(io_context.poll(), 1);
  ASSERT_EQ(counter, 1);
}

TEST(PostableTest, TestTransformArg) {
  instrumented_io_context io_context;

  size_t counter = 0;
  Postable<void(int)> p{[&counter](int a) { counter += a; }, io_context};

  // arg_mapper takes any number of arguments, and returns a value (or void) that is the
  // argument of the original function.
  auto transformed = std::move(p).TransformArg([](int x, int y) -> int { return x + y; });

  // After transformation, it receives 2 arguments. it first runs arg_mapper and then
  // calls the original function with the result of arg_mapper.
  std::move(transformed).Post("TestTransformArg", 1, 2);

  // Posts exactly 1 task.
  ASSERT_EQ(io_context.poll(), 1);
  ASSERT_EQ(counter, 3);
}

TEST(PostableTest, TestTransformArgNoArgs) {
  instrumented_io_context io_context;

  size_t arg_mapper_counter = 0;
  size_t counter = 0;
  Postable<void()> p{[&counter]() { counter += 1; }, io_context};

  auto transformed = std::move(p).TransformArg(
      [&arg_mapper_counter]() -> void { arg_mapper_counter += 1; });

  std::move(transformed).Post("TestTransformArg");

  // Posts exactly 1 task.
  // arg_mapper is called exactly once.
  ASSERT_EQ(io_context.poll(), 1);
  ASSERT_EQ(counter, 1);
  ASSERT_EQ(arg_mapper_counter, 1);
}

}  // namespace ray
