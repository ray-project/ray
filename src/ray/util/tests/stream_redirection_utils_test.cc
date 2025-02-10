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

#include "ray/util/stream_redirection_utils.h"

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <thread>

#include "ray/common/test/testing.h"
#include "ray/util/filesystem.h"
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
  const std::string log_file_path1 = test_file_path;
  const std::string log_file_path2 = absl::StrFormat("%s.1", test_file_path);
  log_files.emplace_back(log_file_path1);
  log_files.emplace_back(log_file_path2);

  // Cleanup generated log files at test completion; because loggers are closed at process
  // termination via exit hook, and hooked functions are executed at the reverse order of
  // their registration, so register cleanup hook before logger close hook.
  ASSERT_EQ(std::atexit(CleanupOutputLogFiles), 0);

  // Works via `dup`, so have to execute before we redirect via `dup2` and close stderr.
  testing::internal::CaptureStderr();

  // Redirect stderr for testing, so we could have stdout for debugging.
  StreamRedirectionOption opts;
  opts.file_path = test_file_path;
  opts.tee_to_stderr = true;
  opts.rotation_max_size = 5;
  opts.rotation_max_file_count = 2;
  RedirectStderr(opts);

  std::cerr << kLogLine1 << std::flush;
  std::cerr << kLogLine2 << std::flush;

  // TODO(hjiang): Current implementation is flaky intrinsically, sleep for a while to
  // make sure pipe content has been read over to spdlog.
  std::this_thread::sleep_for(std::chrono::seconds(2));
  FlushOnRedirectedStderr();

  // Check log content after completion.
  const auto actual_content1 = ReadEntireFile(log_file_path1);
  RAY_ASSERT_OK(actual_content1);
  EXPECT_EQ(*actual_content1, kLogLine2);

  const auto actual_content2 = ReadEntireFile(log_file_path2);
  RAY_ASSERT_OK(actual_content2);
  EXPECT_EQ(*actual_content2, kLogLine1);

  // Check tee-ed to stderr content.
  std::string stderr_content = testing::internal::GetCapturedStderr();
  EXPECT_EQ(stderr_content, absl::StrFormat("%s%s", kLogLine1, kLogLine2));

  // Make sure flush hook works fine and process terminates with no problem.
}

}  // namespace ray
