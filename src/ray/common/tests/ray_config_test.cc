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

#include "ray/common/ray_config.h"

#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/grpc_util.h"

namespace ray {
class RayConfigTest : public ::testing::Test {};

TEST_F(RayConfigTest, ConvertValueTrimsVectorElements) {
  const std::string type_string = "std::vector";
  const std::string input = "no_spaces, with spaces ";
  const std::vector<std::string> expected_output{"no_spaces", "with spaces"};
  auto output = ConvertValue<std::vector<std::string>>(type_string, input);
  ASSERT_EQ(output, expected_output);
}

}  // namespace ray
