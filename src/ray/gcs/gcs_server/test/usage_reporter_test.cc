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

#include "ray/gcs/gcs_server/usage_reporter.h"

#include "gtest/gtest.h"
#include "ray/common/test_util.h"

namespace ray {

class UsageReporterTest : public ::testing::Test {
 public:
  void SetUp() override {}
};

TEST_F(UsageReporterTest, SmokeTest) {
  auto key = usage::TagKey::_TEST1;
  ASSERT_TRUE(key == usage::TagKey::_TEST1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
