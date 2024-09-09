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

#include "src/ray/common/status.h"

#include <gtest/gtest.h>

#include "absl/strings/str_format.h"

namespace ray {

TEST(StatusTest, StringifyTest) {
  // Test for error status.
  {
    Status s = Status::UnknownError("Unknown");
    const auto str = absl::StrFormat("%v", s);
    EXPECT_EQ(str, "Unknown error: Unknown");
  }
  // Test for OK status.
  {
    Status s = Status::OK();
    const auto str = absl::StrCat(s, "OK status");
    EXPECT_EQ(str, "OKOK status");
  }
}

}  // namespace ray
