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

#include "ray/util/pipe_logger.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <future>
#include <string_view>

#include "ray/util/filesystem.h"
#include "ray/util/util.h"

namespace ray {

namespace {

constexpr std::string_view kLogLine1 = "hello\n";
constexpr std::string_view kLogLine2 = "world\n";

TEST(PipeLoggerTestWithTee, PipeWrite) {
  // TODO(core): We should have a better test util, which allows us to create a temporary
  // testing directory.
  const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

  // Take the default option, which doesn't have rotation enabled.
  StreamRedirectionOption logging_option{};
  logging_option.file_path = test_file_path;
  logging_option.rotation_max_size = 5;
  logging_option.rotation_max_file_count = 2;

  auto log_token = CreateRedirectionFileHandle(logging_option);
  ASSERT_EQ(write(log_token.GetWriteHandle(), kLogLine1.data(), kLogLine1.length()),
            kLogLine1.length());
  ASSERT_EQ(write(log_token.GetWriteHandle(), kLogLine2.data(), kLogLine2.length()),
            kLogLine2.length());
  // Write empty line, which is not expected to appear.
  ASSERT_EQ(write(log_token.GetWriteHandle(), "\n", /*count=*/1), 1);
  // Synchronize on log flush completion.
  log_token.Close();

  // Check log content after completion.
  const std::string log_file_path1 = test_file_path;
  EXPECT_EQ(CompleteReadFile(test_file_path), kLogLine2);

  const std::string log_file_path2 = absl::StrFormat("%s.1", test_file_path);
  EXPECT_EQ(CompleteReadFile(log_file_path2), kLogLine1);

  // Delete temporary file.
  EXPECT_EQ(unlink(log_file_path1.data()), 0);
  EXPECT_EQ(unlink(log_file_path2.data()), 0);
}

TEST(PipeLoggerTestWithTee, RedirectionWithTee) {
  // TODO(core): We should have a better test util, which allows us to create a temporary
  // testing directory.
  const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

  StreamRedirectionOption logging_option{};
  logging_option.file_path = test_file_path;
  logging_option.tee_to_stdout = true;

  // Capture stdout via `dup`.
  testing::internal::CaptureStdout();

  auto log_token = CreateRedirectionFileHandle(logging_option);
  ASSERT_EQ(write(log_token.GetWriteHandle(), kLogLine1.data(), kLogLine1.length()),
            kLogLine1.length());
  ASSERT_EQ(write(log_token.GetWriteHandle(), kLogLine2.data(), kLogLine2.length()),
            kLogLine2.length());
  log_token.Close();

  // Check content tee-ed to stdout.
  const std::string stdout_content = testing::internal::GetCapturedStdout();
  EXPECT_EQ(stdout_content, absl::StrFormat("%s%s", kLogLine1, kLogLine2));

  // Check log content after completion.
  EXPECT_EQ(CompleteReadFile(test_file_path),
            absl::StrFormat("%s%s", kLogLine1, kLogLine2));

  // Delete temporary file.
  EXPECT_EQ(unlink(test_file_path.data()), 0);
}

TEST(PipeLoggerTestWithTee, RotatedRedirectionWithTee) {
  // TODO(core): We should have a better test util, which allows us to create a temporary
  // testing directory.
  const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

  StreamRedirectionOption logging_option{};
  logging_option.file_path = test_file_path;
  logging_option.rotation_max_size = 5;
  logging_option.rotation_max_file_count = 2;
  logging_option.tee_to_stdout = true;

  // Capture stdout via `dup`.
  testing::internal::CaptureStdout();

  auto log_token = CreateRedirectionFileHandle(logging_option);
  ASSERT_EQ(write(log_token.GetWriteHandle(), kLogLine1.data(), kLogLine1.length()),
            kLogLine1.length());
  ASSERT_EQ(write(log_token.GetWriteHandle(), kLogLine2.data(), kLogLine2.length()),
            kLogLine2.length());
  // White-box testing: pipe logger reads in lines so manually append a newline to trigger
  // corresponding sink.
  ASSERT_EQ(write(log_token.GetWriteHandle(), "\n", 1), 1);
  log_token.Close();

  // Check content tee-ed to stdout.
  const std::string stdout_content = testing::internal::GetCapturedStdout();
  EXPECT_EQ(stdout_content, absl::StrFormat("%s%s", kLogLine1, kLogLine2));

  // Check log content after completion.
  const std::string log_file_path1 = test_file_path;
  EXPECT_EQ(CompleteReadFile(test_file_path), kLogLine2);

  const std::string log_file_path2 = absl::StrFormat("%s.1", test_file_path);
  EXPECT_EQ(CompleteReadFile(log_file_path2), kLogLine1);

  // Delete temporary file.
  EXPECT_EQ(unlink(log_file_path1.data()), 0);
  EXPECT_EQ(unlink(log_file_path2.data()), 0);
}

// TODO(hjiang): Add a test case which tee to stderr and verify via gtest capturing.

}  // namespace

}  // namespace ray

#endif
