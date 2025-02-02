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

#include "ray/util/compat.h"

#include <gtest/gtest.h>

#include <string_view>

#include "absl/cleanup/cleanup.h"
#include "ray/common/status_or.h"
#include "ray/util/filesystem.h"
#include "ray/util/temporary_directory.h"
#include "ray/util/util.h"

#if defined(__APPLE__) || defined(__linux__)
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

namespace ray {

namespace {

constexpr std::string_view kContent = "helloworld";

#if defined(__APPLE__) || defined(__linux__)
TEST(CompatTest, WriteTest) {
  ScopedTemporaryDirectory temp_dir;
  const auto file = temp_dir.GetDirectory() / "file";
  const auto file_path_str = file.native();
  const int fd = open(file_path_str.data(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  ASSERT_GT(fd, 0);

  RAY_CHECK_OK(CompleteWrite(fd, kContent.data(), kContent.length()));
  Flush(fd);
  RAY_CHECK_OK(Close(fd));

  RAY_ASSIGN_OR_CHECK(const auto content, ReadEntireFile(file_path_str));
  EXPECT_EQ(content, kContent);
}
#elif defined(_WIN32)
TEST(CompatTest, WriteTest) {
  const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

  HANDLE handle = CreateFile(test_file_path.data(),  // File path
                             GENERIC_WRITE,          // Open for writing
                             0,                      // No sharing
                             NULL,                   // Default security
                             CREATE_ALWAYS,  // Create new file (overwrite if exists)
                             FILE_ATTRIBUTE_NORMAL,  // Normal file attributes
                             NULL                    // No template
  );
  ASSERT_NE(handle, INVALID_HANDLE_VALUE);
  absl::Cleanup cleanup_test_file = [&test_file_path]() {
    EXPECT_TRUE(std::filesystem::remove(test_file_path));
  };

  RAY_CHECK_OK(CompleteWrite(handle, kContent.data(), kContent.length()));
  Flush(handle);
  RAY_CHECK_OK(Close(handle));

  RAY_ASSIGN_OR_CHECK(const auto content, ReadEntireFile(test_file_path));
  EXPECT_EQ(content, kContent);
}
#endif

}  // namespace

}  // namespace ray
