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

#if defined(__APPLE__) || defined(__linux__)

#include "ray/util/stream_redirection_utils.h"

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <thread>

#include "ray/util/tests/unix_test_utils.h"
#include "ray/util/util.h"

namespace ray {

namespace {
constexpr std::string_view kLogLine1 = "hello\n";
constexpr std::string_view kLogLine2 = "world\n";
}  // namespace

TEST(LoggingUtilTest, RedirectStderr) {
  // Works via `dup`, so have to execute before we redirect via `dup2` and close stderr.
  testing::internal::CaptureStderr();

  // Redirect stderr for testing, so we could have stdout for debugging.
  const std::string test_file_path = absl::StrFormat("%s.err", GenerateUUIDV4());

  StreamRedirectionOption opts;
  opts.file_path = test_file_path;
  // Manually check whether streamed content is displayed on screen.
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
  const std::string log_file_path1 = test_file_path;
  EXPECT_EQ(CompleteReadFile(test_file_path), kLogLine2);

  const std::string log_file_path2 = absl::StrFormat("%s.1", test_file_path);
  EXPECT_EQ(CompleteReadFile(log_file_path2), kLogLine1);

  // Check tee-ed to stderr content.
  std::string stderr_content = testing::internal::GetCapturedStderr();
  EXPECT_EQ(stderr_content, absl::StrFormat("%s%s", kLogLine1, kLogLine2));

  // Delete temporary file.
  EXPECT_EQ(unlink(log_file_path1.data()), 0);
  EXPECT_EQ(unlink(log_file_path2.data()), 0);

  // Make sure flush hook works fine and process terminates with no problem.
}

}  // namespace ray

#endif
