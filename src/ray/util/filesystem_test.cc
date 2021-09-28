// Copyright 2020 The Ray Authors.
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

#include "ray/util/filesystem.h"

#include "gtest/gtest.h"

namespace ray {

TEST(FileSystemTest, PathParseTest) {
  ASSERT_EQ(GetFileName("."), ".");
  ASSERT_EQ(GetFileName(".."), "..");
  ASSERT_EQ(GetFileName("foo/bar"), "bar");
  ASSERT_EQ(GetFileName("///bar"), "bar");
  ASSERT_EQ(GetFileName("///bar/"), "");
#ifdef _WIN32
  ASSERT_EQ(GetFileName("C:"), "");
  ASSERT_EQ(GetFileName("C::"), ":");  // just to match Python behavior
  ASSERT_EQ(GetFileName("CC::"), "CC::");
  ASSERT_EQ(GetFileName("C:\\"), "");
#endif
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
