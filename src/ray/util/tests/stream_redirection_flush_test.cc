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
constexpr std::string_view kLogLine1 = "hello";
constexpr std::string_view kLogLine2 = "world";

// Output logging files to cleanup at process termination.
std::vector<std::string> log_files;
void CleanupOutputLogFiles() {
  for (const auto &cur_log : log_files) {
    EXPECT_TRUE(std::filesystem::remove(cur_log)) << "Fails to delete " << cur_log;
  }
}

}  // namespace

TEST(LoggingUtilTest, RedirectStderr) {
  const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());
  log_files.emplace_back(test_file_path);

  // Cleanup generated log files at test completion; because loggers are closed at process
  // termination via exit hook, and hooked functions are executed at the reverse order of
  // their registration, so register cleanup hook before logger close hook.
  ASSERT_EQ(std::atexit(CleanupOutputLogFiles), 0);

  // Works via `dup`, so have to execute before we redirect via `dup2` and close stdout.
  testing::internal::CaptureStdout();

  // Redirect stderr for testing, so we could have stdout for debugging.
  StreamRedirectionOption opts;
  opts.file_path = test_file_path;
  RedirectStdout(opts);

  std::cout << kLogLine1 << std::flush;
  std::cout << kLogLine2 << std::flush;

  // TODO(hjiang): Current implementation is flaky intrinsically, sleep for a while to
  // make sure pipe content has been read over to spdlog.
  std::this_thread::sleep_for(std::chrono::seconds(2));
  FlushOnRedirectedStdout();

  // Check tee-ed to stdout content, it's worth notice at this point we haven't written
  // newliner into the stream, nor did we close the redirection handle.
  std::string stdout_content = testing::internal::GetCapturedStdout();
  EXPECT_EQ(stdout_content, absl::StrFormat("%s%s", kLogLine1, kLogLine2));

  // Make sure flush hook works fine and process terminates with no problem.
}

}  // namespace ray
