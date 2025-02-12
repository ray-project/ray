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

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <thread>

#include "ray/common/test/testing.h"
#include "ray/util/filesystem.h"
#include "ray/util/stream_redirection_utils.h"
#include "ray/util/util.h"

namespace ray {

namespace {
constexpr std::string_view kLogLine1 = "hello\n";
constexpr std::string_view kLogLine2 = "world\n";

// Output logging files to cleanup at process termination.
std::vector<std::string> log_files;
void CleanupOutputLogFiles() {
  for (const auto &cur_log : log_files) {
    EXPECT_TRUE(std::filesystem::remove(cur_log));
  }
}
}  // namespace

TEST(LoggingUtilTest, RedirectStderr) {
  const std::string test_file_path = absl::StrFormat("%s.err", GenerateUUIDV4());
  log_files.emplace_back(test_file_path);

  // Cleanup generated log files at test completion; because loggers are closed at process
  // termination via exit hook, and hooked functions are executed at the reverse order of
  // their registration, so register cleanup hook before logger close hook.
  ASSERT_EQ(std::atexit(CleanupOutputLogFiles), 0);

  // Redirect stderr for testing, so we could have stdout for debugging.
  StreamRedirectionOption opts;
  opts.file_path = test_file_path;
  RedirectStderr(opts);

  std::cerr << kLogLine1 << std::flush;
  std::cerr << kLogLine2 << std::flush;

  // Check log content after completion.
  const auto actual_content = ReadEntireFile(test_file_path);
  RAY_ASSERT_OK(actual_content);
  EXPECT_EQ(*actual_content, absl::StrFormat("%s%s", kLogLine1, kLogLine2));

  // Make sure flush hook works fine and process terminates with no problem.
}

}  // namespace ray
