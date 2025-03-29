// Copyright 2025 The Ray Authors.
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

#include "ray/util/strings/get_type_name.h"

#include <gtest/gtest.h>

namespace ray {

namespace {
struct AnonymousStruct {};
}  // namespace

namespace internal {
struct InternalStruct {};
}  // namespace internal

TEST(GetTypeName, BasicTypes) {
  EXPECT_EQ(GetTypeName<int>(), "int");
  EXPECT_EQ(GetTypeName<double>(), "double");
  EXPECT_EQ(GetTypeName<long>(), "long");
  EXPECT_EQ(GetTypeName<unsigned long long>(), "unsigned long long");
}

TEST(GetTypeName, CVQualified) {
  EXPECT_EQ(GetTypeName<const int>(), "const int");
  EXPECT_EQ(GetTypeName<const int *>(), "const int *");
  EXPECT_EQ(GetTypeName<const int *const>(), "const int *const");

  EXPECT_EQ(GetTypeName<volatile int>(), "volatile int");
  EXPECT_EQ(GetTypeName<volatile int *>(), "volatile int *");
  EXPECT_EQ(GetTypeName<volatile int *const>(), "volatile int *const");
}

TEST(GetTypeName, ReferencesTypes) {
  EXPECT_EQ(GetTypeName<int &>(), "int &");
  EXPECT_EQ(GetTypeName<int &&>(), "int &&");
}

TEST(GetTypeName, Pointers) {
  EXPECT_EQ(GetTypeName<long *>(), "long *");
  EXPECT_EQ(GetTypeName<unsigned *>(), "unsigned int *");
  EXPECT_EQ(GetTypeName<int *>(), "int *");
  EXPECT_EQ(GetTypeName<double *>(), "double *");
}

TEST(GetTypeName, CustomizedStruct) {
  EXPECT_EQ(GetTypeName<AnonymousStruct>(),
            "ray::(anonymous namespace)::AnonymousStruct");
  EXPECT_EQ(GetTypeName<internal::InternalStruct>(), "ray::internal::InternalStruct");
}

}  // namespace ray
