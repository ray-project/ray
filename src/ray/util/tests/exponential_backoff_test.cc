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

#include "ray/util/exponential_backoff.h"

#include <math.h>

#include "gtest/gtest.h"

namespace ray {

TEST(ExponentialBackOffTest, TestExponentialIncrease) {
  ASSERT_EQ(ExponentialBackOff::GetBackoffMs(0, 157), 157 * 1);
  ASSERT_EQ(ExponentialBackOff::GetBackoffMs(1, 157), 157 * 2);
  ASSERT_EQ(ExponentialBackOff::GetBackoffMs(2, 157), 157 * 4);
  ASSERT_EQ(ExponentialBackOff::GetBackoffMs(3, 157), 157 * 8);

  ASSERT_EQ(ExponentialBackOff::GetBackoffMs(10, 0), 0);
  ASSERT_EQ(ExponentialBackOff::GetBackoffMs(11, 0), 0);
}

TEST(ExponentialBackOffTest, TestExceedMaxBackoffReturnsMaxBackoff) {
  auto backoff = ExponentialBackOff::GetBackoffMs(
      /*attempt*/ 10, /*base_ms*/ 1, /*max_backoff_ms*/ 5);
  ASSERT_EQ(backoff, 5);
}

TEST(ExponentialBackOffTest, TestOverflowReturnsMaxBackoff) {
  // 2 ^ 64+ will overflow.
  for (int i = 64; i < 10000; i++) {
    auto backoff = ExponentialBackOff::GetBackoffMs(
        /*attempt*/ i,
        /*base_ms*/ 1,
        /*max_backoff_ms*/ 1234);
    ASSERT_EQ(backoff, 1234);
  }
}

TEST(ExponentialBackOffTest, GetNext) {
  auto exp = ExponentialBackOff{1, 2, 9};
  ASSERT_EQ(1, exp.Next());
  ASSERT_EQ(2, exp.Next());
  ASSERT_EQ(4, exp.Next());
  ASSERT_EQ(8, exp.Next());
  ASSERT_EQ(9, exp.Next());
  ASSERT_EQ(9, exp.Next());
  exp.Reset();
  ASSERT_EQ(1, exp.Next());
  ASSERT_EQ(2, exp.Next());
  ASSERT_EQ(4, exp.Next());
  ASSERT_EQ(8, exp.Next());
  ASSERT_EQ(9, exp.Next());
  ASSERT_EQ(9, exp.Next());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
