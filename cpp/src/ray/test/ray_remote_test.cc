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

int Return() { return 1; }
int PlusOne(int x) { return x + 1; }
int PlusTwo(int x, int y) { return x + y; }

int out_for_void_func = 0;
int out_for_void_func_no_args = 0;

void VoidFuncNoArgs() { out_for_void_func = 1; }
void VoidFuncWithArgs(int x, int y) { out_for_void_func_no_args = (x + y); }

int NotRegisteredFunc(int x) { return x; }

void ExceptionFunc(int x) { throw std::invalid_argument(std::to_string(x)); }

std::string Concat1(std::string &&a, std::string &&b) { return a + b; }

std::string Concat2(const std::string &a, std::string &&b) { return a + b; }

std::string Concat3(std::string &a, std::string &b) { return a + b; }

int OverloadFunc() {
  std::cout << "OverloadFunc with no argument\n";
  return 1;
}

int OverloadFunc(int i) {
  std::cout << "OverloadFunc with one argument\n";
  return i + 1;
}

int OverloadFunc(int i, int j) {
  std::cout << "OverloadFunc with two arguments\n";
  return i + j;
}

RAY_REMOTE(RAY_FUNC(OverloadFunc));
RAY_REMOTE(RAY_FUNC(OverloadFunc, int));
RAY_REMOTE(RAY_FUNC(OverloadFunc, int, int));

class DummyObject {
 public:
  int count;

  MSGPACK_DEFINE(count);
  DummyObject() = default;
  DummyObject(int init) { count = init; }

  int Add(int x, int y) { return x + y; }

  std::string Concat1(std::string &&a, std::string &&b) { return a + b; }

  std::string Concat2(const std::string &a, std::string &&b) { return a + b; }

  ~DummyObject() { std::cout << "destruct DummyObject\n"; }

  static DummyObject *FactoryCreate(int init) { return new DummyObject(init); }
};
RAY_REMOTE(DummyObject::FactoryCreate);
RAY_REMOTE(&DummyObject::Add, &DummyObject::Concat1, &DummyObject::Concat2);

RAY_REMOTE(PlusOne, Concat1, Concat2, Concat3);
RAY_REMOTE(PlusTwo, VoidFuncNoArgs, VoidFuncWithArgs, ExceptionFunc);

struct Base {
  static Base *FactoryCreate() { return new Base(); }
  virtual int Foo() { return 1; }
  virtual int Bar() { return 2; }
  virtual ~Base() {}
};

struct Base1 {
  static Base1 *FactoryCreate() { return new Base1(); }
  virtual int Foo() { return 3; }
  virtual int Bar() { return 4; }
  virtual ~Base1() {}
};
RAY_REMOTE(Base::FactoryCreate, &Base::Foo, &Base::Bar);
RAY_REMOTE(Base1::FactoryCreate, &Base1::Foo, &Base1::Bar);

struct Derived : public Base {
  static Derived *FactoryCreate() { return new Derived(); }
  int Foo() override { return 10; }
  int Bar() override { return 20; }
};
RAY_REMOTE(Derived::FactoryCreate);

TEST(RayApiTest, DuplicateRegister) {
  bool r = FunctionManager::Instance().RegisterRemoteFunction("Return", Return);
  EXPECT_TRUE(r);

  /// Duplicate register
  EXPECT_THROW(FunctionManager::Instance().RegisterRemoteFunction("Return", Return),
               ray::internal::RayException);
  EXPECT_THROW(FunctionManager::Instance().RegisterRemoteFunction("PlusOne", PlusOne),
               ray::internal::RayException);
}

TEST(RayApiTest, NormalTask) {
  auto r = ray::Task(Return).Remote();
  EXPECT_EQ(1, *(r.Get()));

  auto r1 = ray::Task(PlusOne).Remote(1);
  EXPECT_EQ(2, *(r1.Get()));
}

TEST(RayApiTest, VoidFunction) {
  auto r2 = ray::Task(VoidFuncNoArgs).Remote();
  r2.Get();
  EXPECT_EQ(1, out_for_void_func);

  auto r3 = ray::Task(VoidFuncWithArgs).Remote(1, 2);
  r3.Get();
  EXPECT_EQ(3, out_for_void_func_no_args);
}

TEST(RayApiTest, ReferenceArgs) {
  auto r = ray::Task(Concat1).Remote("a", "b");
  EXPECT_EQ(*(r.Get()), "ab");
  std::string a = "a";
  std::string b = "b";
  auto r1 = ray::Task(Concat1).Remote(std::move(a), std::move(b));
  EXPECT_EQ(*(r.Get()), *(r1.Get()));

  std::string str = "a";
  std::string str1 = "b";
  auto r2 = ray::Task(Concat2).Remote(str, std::move(str1));

  std::string str2 = "b";
  auto r3 = ray::Task(Concat3).Remote(str, str2);
  EXPECT_EQ(*(r2.Get()), *(r3.Get()));

  ray::ActorHandle<DummyObject> actor = ray::Actor(DummyObject::FactoryCreate).Remote(1);
  auto r4 = actor.Task(&DummyObject::Concat1).Remote("a", "b");
  auto r5 = actor.Task(&DummyObject::Concat2).Remote(str, "b");
  EXPECT_EQ(*(r4.Get()), *(r5.Get()));
}

TEST(RayApiTest, VirtualFunctions) {
  auto actor = ray::Actor(Base::FactoryCreate).Remote();
  auto r = actor.Task(&Base::Foo).Remote();
  auto r1 = actor.Task(&Base::Bar).Remote();
  auto actor1 = ray::Actor(Base1::FactoryCreate).Remote();
  auto r2 = actor1.Task(&Base1::Foo).Remote();
  auto r3 = actor1.Task(&Base1::Bar).Remote();
  EXPECT_EQ(*(r.Get()), 1);
  EXPECT_EQ(*(r1.Get()), 2);
  EXPECT_EQ(*(r2.Get()), 3);
  EXPECT_EQ(*(r3.Get()), 4);

  auto derived = ray::Actor(Derived::FactoryCreate).Remote();
  auto r4 = derived.Task(&Base::Foo).Remote();
  auto r5 = derived.Task(&Base::Bar).Remote();
  EXPECT_EQ(*(r4.Get()), 10);
  EXPECT_EQ(*(r5.Get()), 20);
}

TEST(RayApiTest, CallWithObjectRef) {
  auto rt0 = ray::Task(Return).Remote();
  auto rt1 = ray::Task(PlusOne).Remote(rt0);
  auto rt2 = ray::Task(PlusTwo).Remote(rt1, 3);
  auto rt3 = ray::Task(PlusOne).Remote(3);
  auto rt4 = ray::Task(PlusTwo).Remote(rt2, rt3);

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
}

TEST(RayApiTest, OverloadTest) {
  auto rt0 = ray::Task(RAY_FUNC(OverloadFunc)).Remote();
  auto rt1 = ray::Task(RAY_FUNC(OverloadFunc, int)).Remote(rt0);
  auto rt2 = ray::Task(RAY_FUNC(OverloadFunc, int, int)).Remote(rt1, 3);

  int return0 = *(rt0.Get());
  int return1 = *(rt1.Get());
  int return2 = *(rt2.Get());

  EXPECT_EQ(return0, 1);
  EXPECT_EQ(return1, 2);
  EXPECT_EQ(return2, 5);
}

/// We should consider the driver so is not same with the worker so, and find the error
/// reason.
TEST(RayApiTest, NotExistFunction) {
  EXPECT_THROW(ray::Task(NotRegisteredFunc), ray::internal::RayException);
}

TEST(RayApiTest, ExceptionTask) {
  /// Normal task Exception.
  auto r4 = ray::Task(ExceptionFunc).Remote(2);
  EXPECT_THROW(r4.Get(), ray::internal::RayTaskException);
}

TEST(RayApiTest, GetClassNameByFuncNameTest) {
  using ray::internal::FunctionManager;
  EXPECT_EQ(FunctionManager::GetClassNameByFuncName("RAY_FUNC(Counter::FactoryCreate)"),
            "Counter");
  EXPECT_EQ(FunctionManager::GetClassNameByFuncName("Counter::FactoryCreate"), "Counter");
  EXPECT_EQ(FunctionManager::GetClassNameByFuncName("FactoryCreate"), "");
  EXPECT_EQ(FunctionManager::GetClassNameByFuncName(""), "");
  EXPECT_EQ(FunctionManager::GetClassNameByFuncName("::FactoryCreate"), "");
}
