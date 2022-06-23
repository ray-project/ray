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

#include <filesystem>

#include "gtest/gtest.h"
#include "ray/common/file_system_monitor.h"

namespace ray {

namespace testing {
template <class... Paths>
std::string JoinPaths(std::string base, Paths... components) {
  std::string to_append[] = {components...};
  for (size_t i = 0; i < sizeof(to_append) / sizeof(*to_append); ++i) {
    const std::string &s = to_append[i];
    if (!base.empty() && !IsDirSep(base.back()) && !s.empty() && !IsDirSep(s[0])) {
      base += std::filesystem::path::preferred_separator;
    }
    base += s;
  }
  return base;
}
}  // namespace testing

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

TEST(FileSystemTest, JoinPathTest) {
#ifdef _WIN32
  auto old_path =
      testing::JoinPaths(GetUserTempDir(), "hello", "\\subdir", "more", "", "last/");
  auto new_path =
      ray::JoinPaths(GetUserTempDir(), "hello", "\\subdir", "more", "", "last/");
  ASSERT_EQ(old_path, new_path);
#else
  auto old_path =
      testing::JoinPaths(GetUserTempDir(), "hello", "/subdir", "more", "", "last/");
  auto new_path =
      ray::JoinPaths(GetUserTempDir(), "hello", "/subdir", "more", "", "last/");
  ASSERT_EQ(old_path, new_path);
#endif
}

TEST(FileSystemTest, TestFileSystemMonitor) {
  std::string tmp_path = std::filesystem::temp_directory_path().string();
  {
    ray::FileSystemMonitor monitor({tmp_path}, 1);
    ASSERT_FALSE(monitor.OverCapacity());
  }

  {
    FileSystemMonitor monitor({tmp_path}, 0);
    ASSERT_TRUE(monitor.OverCapacity());
  }

  {
    FileSystemMonitor monitor({tmp_path}, 0);
    auto result = monitor.Space(tmp_path);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->available > 0);
    ASSERT_TRUE(result->capacity > 0);
  }

  auto noop_monitor = std::make_unique<ray::FileSystemMonitor>();
  ASSERT_FALSE(noop_monitor->OverCapacity());
}

TEST(FileSystemTest, TestOverCapacity) {
  std::string tmp_path = std::filesystem::temp_directory_path().string();
  FileSystemMonitor monitor({tmp_path}, 0.1);
  ASSERT_FALSE(monitor.OverCapacityImpl(tmp_path, std::nullopt));
  ASSERT_FALSE(monitor.OverCapacityImpl(
      tmp_path,
      {std::filesystem::space_info{
          /* capacity */ 11, /* free */ 10, /* available */ 10}}));
  ASSERT_TRUE(monitor.OverCapacityImpl(
      tmp_path,
      {std::filesystem::space_info{/* capacity */ 11, /* free */ 9, /* available */ 9}}));
  ASSERT_TRUE(monitor.OverCapacityImpl(
      tmp_path,
      {std::filesystem::space_info{/* capacity */ 0, /* free */ 0, /* available */ 0}}));
}

TEST(FileSystemTest, ParseLocalSpillingPaths) {
  {
    std::vector<std::string> expected{"/tmp/spill", "/tmp/spill_1"};
    auto parsed = ParseSpillingPaths(
        "{"
        "  \"type\": \"filesystem\","
        "  \"params\": {"
        "    \"directory_path\": ["
        "      \"/tmp/spill\","
        "      \"/tmp/spill_1\""
        "     ]"
        "  }"
        "}");
    ASSERT_EQ(expected, parsed);
  }

  {
    std::vector<std::string> expected{"/tmp/spill"};
    auto parsed = ParseSpillingPaths(
        "{"
        "  \"type\": \"filesystem\","
        "  \"params\": {"
        "    \"directory_path\": \"/tmp/spill\""
        "  }"
        "}");
    ASSERT_EQ(expected, parsed);
  }

  {
    std::vector<std::string> expected{};
    auto parsed = ParseSpillingPaths(
        "{"
        "  \"type\": \"filesystem\","
        "    \"params\": {"
        "    \"directory_1path\": \"/tmp/spill\""
        "  }"
        "}");
    ASSERT_EQ(expected, parsed);
  }

  {
    std::vector<std::string> expected{};
    auto parsed = ParseSpillingPaths(
        "{"
        "  \"type\": \"filesystem\","
        "    \"params\": {"
        "    \"directory_path\": 3"
        "  }"
        "}");
    ASSERT_EQ(expected, parsed);
  }

  {
    std::vector<std::string> expected{"/tmp/spill", "/tmp/spill_1"};
    auto parsed = ParseSpillingPaths(
        "{"
        "  \"type\": \"filesystem\","
        "  \"params\": {"
        "    \"directory_path\": ["
        "      \"/tmp/spill\","
        "      2,"
        "      \"/tmp/spill_1\""
        "    ]"
        "  }"
        "}");
    ASSERT_EQ(expected, parsed);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
