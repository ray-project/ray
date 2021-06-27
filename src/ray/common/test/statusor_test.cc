// Copyright 2020 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/common/statusor.h"

#include "gtest/gtest.h"

namespace ray {
namespace {

class Base1 {
 public:
  virtual ~Base1() {}
  int pad;
};

class Base2 {
 public:
  virtual ~Base2() {}
  int yetotherpad;
};

class Derived : public Base1, public Base2 {
 public:
  virtual ~Derived() {}
  int evenmorepad;
};

TEST(StatusOr, TestDefaultCtor) {
  StatusOr<int> thing;
  EXPECT_FALSE(thing.ok());
  EXPECT_EQ(Status::UnknownError(""), thing.status());
}

TEST(StatusOr, TestStatusCtor) {
  StatusOr<int> thing(Status::Invalid(""));
  EXPECT_FALSE(thing.ok());
  EXPECT_EQ(Status::Invalid(""), thing.status());
}

TEST(StatusOr, TestValueCtor) {
  const int kI = 4;
  StatusOr<int> thing(kI);
  EXPECT_TRUE(thing.ok());
  EXPECT_EQ(kI, thing.value());
}

TEST(StatusOr, TestCopyCtorStatusOk) {
  const int kI = 4;
  StatusOr<int> original(kI);
  StatusOr<int> copy(original);
  EXPECT_EQ(original.status(), copy.status());
  EXPECT_EQ(original.value(), copy.value());
}

TEST(StatusOr, TestCopyCtorStatusNotOk) {
  StatusOr<int> original(Status::Invalid(""));
  StatusOr<int> copy(original);
  EXPECT_EQ(original.status(), copy.status());
}

TEST(StatusOr, TestCopyCtorStatusOKConverting) {
  const int kI = 4;
  StatusOr<int>    original(kI);
  StatusOr<double> copy(original);
  EXPECT_EQ(original.status(), copy.status());
  EXPECT_EQ(original.value(), copy.value());
}

TEST(StatusOr, TestCopyCtorStatusNotOkConverting) {
  StatusOr<int> original(Status::Invalid(""));
  StatusOr<double> copy(original);
  EXPECT_EQ(original.status(), copy.status());
}

TEST(StatusOr, TestAssignmentStatusOk) {
  const int kI = 4;
  StatusOr<int> source(kI);
  StatusOr<int> target;
  target = source;
  EXPECT_EQ(source.status(), target.status());
  EXPECT_EQ(source.value(), target.value());
}

TEST(StatusOr, TestAssignmentStatusNotOk) {
  StatusOr<int> source(Status::Invalid(""));
  StatusOr<int> target;
  target = source;
  EXPECT_EQ(source.status(), target.status());
}

TEST(StatusOr, TestAssignmentStatusOKConverting) {
  const int kI = 4;
  StatusOr<int>    source(kI);
  StatusOr<double> target;
  target = source;
  EXPECT_EQ(source.status(), target.status());
  EXPECT_DOUBLE_EQ(source.value(), target.value());
}

TEST(StatusOr, TestAssignmentStatusNotOkConverting) {
  StatusOr<int> source(Status::Invalid(""));
  StatusOr<double> target;
  target = source;
  EXPECT_EQ(source.status(), target.status());
}

TEST(StatusOr, TestStatus) {
  StatusOr<int> good(4);
  EXPECT_TRUE(good.ok());
  StatusOr<int> bad(Status::Invalid(""));
  EXPECT_FALSE(bad.ok());
  EXPECT_EQ(Status::Invalid(""), bad.status());
}

TEST(StatusOr, TestValue) {
  const int kI = 4;
  StatusOr<int> thing(kI);
  EXPECT_EQ(kI, thing.value());
}

TEST(StatusOr, TestValueConst) {
  const int kI = 4;
  const StatusOr<int> thing(kI);
  EXPECT_EQ(kI, thing.value());
}

TEST(StatusOr, TestPointerDefaultCtor) {
  StatusOr<int*> thing;
  EXPECT_FALSE(thing.ok());
  EXPECT_EQ(Status::UnknownError(""), thing.status());
}

TEST(StatusOr, TestPointerStatusCtor) {
  StatusOr<int*> thing(Status::Invalid(""));
  EXPECT_FALSE(thing.ok());
  EXPECT_EQ(Status::Invalid(""), thing.status());
}

TEST(StatusOr, TestPointerValueCtor) {
  const int kI = 4;
  StatusOr<const int*> thing(&kI);
  EXPECT_TRUE(thing.ok());
  EXPECT_EQ(&kI, thing.value());
}

TEST(StatusOr, TestPointerCopyCtorStatusOk) {
  const int kI = 0;
  StatusOr<const int*> original(&kI);
  StatusOr<const int*> copy(original);
  EXPECT_EQ(original.status(), copy.status());
  EXPECT_EQ(original.value(), copy.value());
}

TEST(StatusOr, TestPointerCopyCtorStatusNotOk) {
  StatusOr<int*> original(Status::Invalid(""));
  StatusOr<int*> copy(original);
  EXPECT_EQ(original.status(), copy.status());
}

TEST(StatusOr, TestPointerCopyCtorStatusOKConverting) {
  Derived derived;
  StatusOr<Derived*> original(&derived);
  StatusOr<Base2*>   copy(original);
  EXPECT_EQ(original.status(), copy.status());
  EXPECT_EQ(static_cast<const Base2*>(original.value()), copy.value());
}

TEST(StatusOr, TestPointerCopyCtorStatusNotOkConverting) {
  StatusOr<Derived*> original(Status::Invalid(""));
  StatusOr<Base2*>   copy(original);
  EXPECT_EQ(original.status(), copy.status());
}

TEST(StatusOr, TestPointerAssignmentStatusOk) {
  const int kI = 0;
  StatusOr<const int*> source(&kI);
  StatusOr<const int*> target;
  target = source;
  EXPECT_EQ(source.status(), target.status());
  EXPECT_EQ(source.value(), target.value());
}

TEST(StatusOr, TestPointerAssignmentStatusNotOk) {
  StatusOr<int*> source(Status::Invalid(""));
  StatusOr<int*> target;
  target = source;
  EXPECT_EQ(source.status(), target.status());
}

TEST(StatusOr, TestPointerAssignmentStatusOKConverting) {
  Derived derived;
  StatusOr<Derived*> source(&derived);
  StatusOr<Base2*>   target;
  target = source;
  EXPECT_EQ(source.status(), target.status());
  EXPECT_EQ(static_cast<const Base2*>(source.value()), target.value());
}

TEST(StatusOr, TestPointerAssignmentStatusNotOkConverting) {
  StatusOr<Derived*> source(Status::Invalid(""));
  StatusOr<Base2*>   target;
  target = source;
  EXPECT_EQ(source.status(), target.status());
}

TEST(StatusOr, TestPointerStatus) {
  const int kI = 0;
  StatusOr<const int*> good(&kI);
  EXPECT_TRUE(good.ok());
  StatusOr<const int*> bad(Status::Invalid(""));
  EXPECT_EQ(Status::Invalid(""), bad.status());
}

TEST(StatusOr, TestPointerValue) {
  const int kI = 0;
  StatusOr<const int*> thing(&kI);
  EXPECT_EQ(&kI, thing.value());
}

TEST(StatusOr, TestPointerValueConst) {
  const int kI = 0;
  const StatusOr<const int*> thing(&kI);
  EXPECT_EQ(&kI, thing.value());
}

}  // namespace
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}