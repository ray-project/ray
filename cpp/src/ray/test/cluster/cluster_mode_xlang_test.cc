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

#include "cpp/include/ray/api/actor_handle.h"

TEST(RayClusterModeXLangTest, JavaInvocationTest) {
  // Test java nested static class
  ray::ActorHandleXlang java_nested_class_actor_handle =
      ray::Actor(ray::JavaActorClass{"io.ray.test.Counter$NestedActor"}).Remote("hello");
  EXPECT_TRUE(!java_nested_class_actor_handle.ID().empty());
  auto java_actor_ret =
      java_nested_class_actor_handle.Task(ray::JavaActorMethod<std::string>{"concat"})
          .Remote("world");
  EXPECT_EQ("helloworld", *java_actor_ret.Get());

  // Test java static function
  auto java_task_ret =
      ray::Task(ray::JavaFunction<std::string>{"io.ray.test.CrossLanguageInvocationTest",
                                               "returnInputString"})
          .Remote("helloworld");
  EXPECT_EQ("helloworld", *java_task_ret.Get());

  // Test java normal class
  std::string actor_name = "java_actor";
  auto java_class_actor_handle = ray::Actor(ray::JavaActorClass{"io.ray.test.Counter"})
                                     .SetName(actor_name)
                                     .Remote(0);
  auto ref2 =
      java_class_actor_handle.Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *ref2.Get());

  // Test get java actor by actor name.
  boost::optional<ray::ActorHandleXlang> named_actor_handle_optional =
      ray::GetActor(actor_name);
  EXPECT_TRUE(named_actor_handle_optional);
  ray::ActorHandleXlang named_actor_handle = *named_actor_handle_optional;
  auto named_actor_obj1 =
      named_actor_handle.Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *named_actor_obj1.Get());

  // Test get other java actor by actor name.
  auto ref_1 =
      java_class_actor_handle.Task(ray::JavaActorMethod<std::string>{"createChildActor"})
          .Remote("child_actor");
  EXPECT_EQ(*ref_1.Get(), "OK");
  boost::optional<ray::ActorHandleXlang> child_actor_optional =
      ray::GetActor("child_actor");
  EXPECT_TRUE(child_actor_optional);
  ray::ActorHandleXlang &child_actor = *child_actor_optional;
  auto ref_2 = child_actor.Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *ref_2.Get());
}

TEST(RayClusterModeXLangTest, GetXLangActorByNameTest) {
  // Create a named java actor in namespace `isolated_ns`.
  std::string actor_name_in_isolated_ns = "named_actor_in_isolated_ns";
  std::string isolated_ns_name = "isolated_ns";

  auto java_actor_handle = ray::Actor(ray::JavaActorClass{"io.ray.test.Counter"})
                               .SetName(actor_name_in_isolated_ns, isolated_ns_name)
                               .Remote(0);
  auto ref = java_actor_handle.Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *ref.Get());

  // It is invisible to job default namespace.
  boost::optional<ray::ActorHandleXlang> actor_optional =
      ray::GetActor(actor_name_in_isolated_ns);
  EXPECT_TRUE(!actor_optional);
  // It is invisible to any other namespaces.
  actor_optional = ray::GetActor(actor_name_in_isolated_ns, "other_ns");
  EXPECT_TRUE(!actor_optional);
  // It is visible to the namespace it belongs.
  actor_optional = ray::GetActor(actor_name_in_isolated_ns, isolated_ns_name);
  EXPECT_TRUE(actor_optional);
  ref = (*actor_optional).Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *ref.Get());

  // Create a named java actor in job default namespace.
  std::string actor_name_in_default_ns = "actor_name_in_default_ns";
  java_actor_handle = ray::Actor(ray::JavaActorClass{"io.ray.test.Counter"})
                          .SetName(actor_name_in_default_ns)
                          .Remote(0);
  ref = java_actor_handle.Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *ref.Get());
  // It is invisible to any other namespaces.
  actor_optional = ray::GetActor(actor_name_in_default_ns, isolated_ns_name);
  EXPECT_TRUE(!actor_optional);
  // It is visible to job default namespace.
  actor_optional = ray::GetActor(actor_name_in_default_ns);
  EXPECT_TRUE(actor_optional);
  ref = (*actor_optional).Task(ray::JavaActorMethod<int>{"getValue"}).Remote();
  EXPECT_EQ(0, *ref.Get());
}

int main(int argc, char **argv) {
  ray::RayConfig config;
  ray::Init(config, argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  ray::Shutdown();
  return ret;
}
