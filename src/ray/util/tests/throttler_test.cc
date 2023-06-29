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

#include "ray/util/throttler.h"

#include <cstdlib>
#include <thread>

#include "gtest/gtest.h"

TEST(ThrottlerTest, BasicTest) {
  int64_t now = 100;
  ray::Throttler throttler(5, [&now] { return now; });
  EXPECT_TRUE(throttler.AbleToRun());
  now += 5;
  EXPECT_TRUE(throttler.AbleToRun());
  now += 1;
  EXPECT_FALSE(throttler.AbleToRun());
  now += 4;
  throttler.RunNow();
  EXPECT_FALSE(throttler.AbleToRun());
  now += 5;
  EXPECT_TRUE(throttler.AbleToRun());
}
