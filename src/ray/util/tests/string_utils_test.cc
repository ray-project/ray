// Copyright  The Ray Authors.
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

#include "ray/util/string_utils.h"

#include <gtest/gtest.h>

#include <string>

#include "ray/common/status_or.h"

namespace ray {

TEST(StringUtilsTest, StringToIntFailsWhenNonNumberInputWithInvalidArgument) {
  std::string input = "imanumber";
  StatusOr<int> parsed = StringToInt<int>(input);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();
}

TEST(StringUtilsTest, StringToIntFailsWhenEmptyStringWithInvalidArgument) {
  std::string input = "";
  StatusOr<int> parsed = StringToInt<int>(input);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();
}

TEST(StringUtilsTest, StringToIntFailsWhenNumberWithSpacesWithInvalidArgument) {
  std::string leading_space = " 1";
  StatusOr<int> parsed = StringToInt<int>(leading_space);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();

  std::string trailing_space = "1 ";
  parsed = StringToInt<int>(trailing_space);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();

  std::string space_separated = "1 2";
  parsed = StringToInt<int>(space_separated);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();
}

TEST(StringUtilsTest, StringToIntFailsWhenNonIntegerAndIntegerCharsWithInvalidArgumen) {
  std::string input = "123hellodarknessmyoldfriend";
  StatusOr<int> parsed = StringToInt<int>(input);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();
}

TEST(StringUtilsTest, StringToIntFailWhenIntegerTooOverflowsTypeWithInvalidArgument) {
  std::string input = "4294967296";
  StatusOr<int8_t> parsed = StringToInt<int8_t>(input);
  ASSERT_TRUE(parsed.IsInvalidArgument()) << parsed.ToString();
}

TEST(StringUtilsTest, StringToIntSucceedsWithNegativeIntegers) {
  std::string input = "-4294967296";
  StatusOr<int64_t> parsed = StringToInt<int64_t>(input);
  ASSERT_TRUE(parsed.ok()) << parsed.ToString();
}

TEST(StringUtilsTest, StringToIntSucceedsWithPositiveIntegers) {
  std::string input = "4294967296";
  StatusOr<int64_t> parsed = StringToInt<int64_t>(input);
  ASSERT_TRUE(parsed.ok()) << parsed.ToString();
}

}  // namespace ray
