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

#include <gtest/gtest.h>
#include <ray/api.h>
#include <ray/api/serializer.h>
#include "cpp/src/ray/runtime/task/task_executor.h"
#include "cpp/src/ray/util/function_helper.h"
#include "ray/core.h"

using namespace ray::api;
using namespace ray::internal;

int Return() { return 1; }
int PlusOne(int x) { return x + 1; }
int PlusTwo(int x, int y) { return x + y; }

int out_for_void_func = 0;
int out_for_void_func_no_args = 0;

void VoidFuncNoArgs() { out_for_void_func = 1; }
void VoidFuncWithArgs(int x, int y) { out_for_void_func_no_args = (x + y); }

int NotRegisteredFunc(int x) { return x; }

void ExceptionFunc(int x) { throw std::invalid_argument(std::to_string(x)); }

RAY_REMOTE(PlusOne);
RAY_REMOTE(PlusTwo);
RAY_REMOTE(VoidFuncNoArgs);
RAY_REMOTE(VoidFuncWithArgs);
RAY_REMOTE(ExceptionFunc);

TEST(RayApiTest, DuplicateRegister) {
  bool r = FunctionManager::Instance().RegisterRemoteFunction("Return", Return);
  EXPECT_TRUE(r);

  /// Duplicate register
  bool r1 = FunctionManager::Instance().RegisterRemoteFunction("Return", Return);
  EXPECT_FALSE(r1);

  bool r2 = FunctionManager::Instance().RegisterRemoteFunction("PlusOne", PlusOne);
  EXPECT_FALSE(r2);
}

TEST(RayApiTest, NormalTask) {
  ray::api::RayConfig::GetInstance()->use_ray_remote = true;

  auto r = Ray::Task(Return).Remote();
  EXPECT_EQ(1, *(r.Get()));

  auto r1 = Ray::Task(PlusOne).Remote(1);
  EXPECT_EQ(2, *(r1.Get()));

  ray::api::RayConfig::GetInstance()->use_ray_remote = false;
}

TEST(RayApiTest, VoidFunction) {
  ray::api::RayConfig::GetInstance()->use_ray_remote = true;

  auto r2 = Ray::Task(VoidFuncNoArgs).Remote();
  r2.Get();
  EXPECT_EQ(1, out_for_void_func);

  auto r3 = Ray::Task(VoidFuncWithArgs).Remote(1, 2);
  r3.Get();
  EXPECT_EQ(3, out_for_void_func_no_args);

  ray::api::RayConfig::GetInstance()->use_ray_remote = false;
}

TEST(RayApiTest, CallWithObjectRef) {
  ray::api::RayConfig::GetInstance()->use_ray_remote = true;

  auto rt0 = Ray::Task(Return).Remote();
  auto rt1 = Ray::Task(PlusOne).Remote(rt0);
  auto rt2 = Ray::Task(PlusTwo).Remote(rt1, 3);
  auto rt3 = Ray::Task(PlusOne).Remote(3);
  auto rt4 = Ray::Task(PlusTwo).Remote(rt2, rt3);

  int return0 = *(rt0.Get());
  int return1 = *(rt1.Get());
  int return2 = *(rt2.Get());
  int return3 = *(rt3.Get());
  int return4 = *(rt4.Get());

  EXPECT_EQ(return0, 1);
  EXPECT_EQ(return1, 2);
  EXPECT_EQ(return2, 5);
  EXPECT_EQ(return3, 4);
  EXPECT_EQ(return4, 9);

  ray::api::RayConfig::GetInstance()->use_ray_remote = false;
}

/// We should consider the driver so is not same with the worker so, and find the error
/// reason.
TEST(RayApiTest, NotExistFunction) {
  ray::api::RayConfig::GetInstance()->use_ray_remote = true;

  EXPECT_THROW(Ray::Task(NotRegisteredFunc), RayException);

  ray::api::RayConfig::GetInstance()->use_ray_remote = false;
}

TEST(RayApiTest, ArgumentsNotMatch) {
  ray::api::RayConfig::GetInstance()->use_ray_remote = true;

  /// Arguments number is not match.
  auto r = Ray::Task(PlusOne).Remote();
  EXPECT_THROW(r.Get(), RayException);

  auto r1 = Ray::Task(PlusOne).Remote(1, 2);
  EXPECT_THROW(r1.Get(), RayException);

  auto r2 = Ray::Task(ExceptionFunc).Remote();
  EXPECT_THROW(r2.Get(), RayException);

  auto r3 = Ray::Task(ExceptionFunc).Remote(1, 2);
  EXPECT_THROW(r3.Get(), RayException);

  /// Arguments not match.
  auto r4 = Ray::Task(PlusOne).Remote("invalid argument");
  EXPECT_THROW(r4.Get(), RayException);

  auto r5 = Ray::Task(ExceptionFunc).Remote("invalid argument");
  EXPECT_THROW(r5.Get(), RayException);

  /// Normal task Exception.
  auto r6 = Ray::Task(ExceptionFunc).Remote(2);
  EXPECT_THROW(r6.Get(), RayException);

  ray::api::RayConfig::GetInstance()->use_ray_remote = false;
}

class DummyObject {
 public:
  int count;

  MSGPACK_DEFINE(count);

  DummyObject(int init) {
    std::cout << "construct DummyObject\n";
    count = init;
  }

  ~DummyObject() { std::cout << "destruct DummyObject\n"; }

  static DummyObject *FactoryCreate() { return new DummyObject(0); }
};
RAY_REMOTE(DummyObject::FactoryCreate);

TEST(RayApiTest, CreateActor) {
  ray::api::RayConfig::GetInstance()->use_ray_remote = true;

  ActorHandle<DummyObject> actor = Ray::Actor(DummyObject::FactoryCreate).Remote();
  auto s = actor.ID().Hex();
  EXPECT_FALSE(s.empty());

  ray::api::RayConfig::GetInstance()->use_ray_remote = false;
}
