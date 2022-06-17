// Copyright 2022 The Ray Authors.
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

TEST(RayClusterModeXLangTest, JavaInvocationTest) {
  auto java_actor_handle =
      ray::Actor(ray::JavaActorClass{"io.ray.test.Counter"}).Remote(1);
  EXPECT_TRUE(!java_actor_handle.ID().empty());
  auto java_actor_ret =
      java_actor_handle.Task(ray::JavaActorMethod<int>{"increase"}).Remote(2);
  EXPECT_EQ(3, *java_actor_ret.Get());

  auto java_task_ret =
      ray::Task(ray::JavaFunction<std::string>{"io.ray.test.CrossLanguageInvocationTest",
                                               "returnInputString"})
          .Remote("helloworld");
  EXPECT_EQ("helloworld", *java_task_ret.Get());
}

int main(int argc, char **argv) {
  ray::RayConfig config;
  ray::Init(config, argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  ray::Shutdown();
  return ret;
}
