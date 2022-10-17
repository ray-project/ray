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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/executor/executor.h"
#include <iostream>
TEST(Executor, Basic) {
  ray::executor::Executor exec;
  auto f = exec.submit([](){
    return 10; 
  });
  ASSERT_EQ(10, f.get());
  int v = 10;
  exec.submit([&v] () mutable {
    v = 100;
  }).get();
  ASSERT_EQ(100, v);
}
