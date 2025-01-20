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

#include "ray/util/pipe_logger.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <future>
#include <string_view>

#include "absl/cleanup/cleanup.h"
#include "ray/util/filesystem.h"
#include "ray/util/util.h"

/////////////////////////////////////////////////
// Unit test for both unix platform only.
/////////////////////////////////////////////////

#if defined(__APPLE__) || defined(__linux__)

#include <unistd.h>

namespace ray {

namespace {

constexpr std::string_view kLogLine1 = "hello\n";
constexpr std::string_view kLogLine2 = "world\n";

class PipeLoggerTest : public ::testing::TestWithParam<size_t> {};

TEST_P(PipeLoggerTest, PipeWrite) {
  const size_t pipe_buffer_size = GetParam();
  setenv(kPipeLogReadBufSizeEnv.data(),
         absl::StrFormat("%d", pipe_buffer_size).data(),
         /*overwrite=*/1);

  // TODO(hjiang): We should have a better test util, which allows us to create a
  // temporary testing directory.
  const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());
  const std::string log_file_path1 = test_file_path;
  const std::string log_file_path2 = absl::StrFormat("%s.1", test_file_path);

  // Delete temporary file.
  absl::Cleanup cleanup_test_file = [&log_file_path1, &log_file_path2]() {
    EXPECT_TRUE(std::filesystem::remove(log_file_path1));
    EXPECT_TRUE(std::filesystem::remove(log_file_path2));
  };

  StreamRedirectionOption stream_redirection_opt{};
  stream_redirection_opt.file_path = test_file_path;
  stream_redirection_opt.rotation_max_size = 5;
  stream_redirection_opt.rotation_max_file_count = 2;

  auto stream_redirection_handle = CreateRedirectionFileHandle(stream_redirection_opt);
  stream_redirection_handle.CompleteWrite(kLogLine1.data(), kLogLine1.length());
  stream_redirection_handle.CompleteWrite(kLogLine2.data(), kLogLine2.length());
  // Write empty line, which is not expected to appear.
  stream_redirection_handle.CompleteWrite("\n", /*count=*/1);
  // Synchronize on log flush completion.
  stream_redirection_handle.Close();

  // Check log content after completion.
  EXPECT_EQ(CompleteReadFile(log_file_path1), kLogLine2);
  EXPECT_EQ(CompleteReadFile(log_file_path2), kLogLine1);
}

INSTANTIATE_TEST_SUITE_P(PipeLoggerTest, PipeLoggerTest, testing::Values(1024, 3));

TEST(PipeLoggerTestWithTee, RedirectionWithTee) {
  // TODO(hjiang): We should have a better test util, which allows us to create a
  // temporary testing directory.
  const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

  // Delete temporary file.
  absl::Cleanup cleanup_test_file = [&test_file_path]() {
    EXPECT_TRUE(std::filesystem::remove(test_file_path));
  };

  StreamRedirectionOption stream_redirection_opt{};
  stream_redirection_opt.file_path = test_file_path;
  stream_redirection_opt.tee_to_stdout = true;

  // Capture stdout via `dup`.
  testing::internal::CaptureStdout();

  auto stream_redirection_handle = CreateRedirectionFileHandle(stream_redirection_opt);
  stream_redirection_handle.CompleteWrite(kLogLine1.data(), kLogLine1.length());
  stream_redirection_handle.CompleteWrite(kLogLine2.data(), kLogLine2.length());
  stream_redirection_handle.Close();

  // Check content tee-ed to stdout.
  const std::string stdout_content = testing::internal::GetCapturedStdout();
  EXPECT_EQ(stdout_content, absl::StrFormat("%s%s", kLogLine1, kLogLine2));

  // Check log content after completion.
  EXPECT_EQ(CompleteReadFile(test_file_path),
            absl::StrFormat("%s%s", kLogLine1, kLogLine2));
}

TEST(PipeLoggerTestWithTee, RotatedRedirectionWithTee) {
  // TODO(hjiang): We should have a better test util, which allows us to create a
  // temporary testing directory.
  const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());
  const std::string log_file_path1 = test_file_path;
  const std::string log_file_path2 = absl::StrFormat("%s.1", test_file_path);

  // Delete temporary file.
  absl::Cleanup cleanup_test_file = [&log_file_path1, &log_file_path2]() {
    EXPECT_TRUE(std::filesystem::remove(log_file_path1));
    EXPECT_TRUE(std::filesystem::remove(log_file_path2));
  };

  StreamRedirectionOption stream_redirection_opt{};
  stream_redirection_opt.file_path = test_file_path;
  stream_redirection_opt.rotation_max_size = 5;
  stream_redirection_opt.rotation_max_file_count = 2;
  stream_redirection_opt.tee_to_stderr = true;

  // Capture stdout via `dup`.
  testing::internal::CaptureStderr();

  auto stream_redirection_handle = CreateRedirectionFileHandle(stream_redirection_opt);
  stream_redirection_handle.CompleteWrite(kLogLine1.data(), kLogLine1.length());
  stream_redirection_handle.CompleteWrite(kLogLine2.data(), kLogLine2.length());
  stream_redirection_handle.Close();

  // Check content tee-ed to stderr.
  const std::string stderr_content = testing::internal::GetCapturedStderr();
  EXPECT_EQ(stderr_content, absl::StrFormat("%s%s", kLogLine1, kLogLine2));

  // Check log content after completion.
  EXPECT_EQ(CompleteReadFile(test_file_path), kLogLine2);
  EXPECT_EQ(CompleteReadFile(log_file_path2), kLogLine1);
}

}  // namespace

}  // namespace ray

#endif

/////////////////////////////////////////////////
// Unit test for both windows and unix platform.
/////////////////////////////////////////////////

namespace ray {

namespace {

TEST(PipeLoggerTestWithTee, RedirectionWithNoTeeAndRotation) {
  // TODO(hjiang): We should have a better test util, which allows us to create a
  // temporary testing directory.
  const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

  // Delete temporary file.
  absl::Cleanup cleanup_test_file = [&test_file_path]() {
    EXPECT_TRUE(std::filesystem::remove(test_file_path));
  };

  StreamRedirectionOption stream_redirection_opt{};
  stream_redirection_opt.file_path = test_file_path;

  auto stream_redirection_handle = CreateRedirectionFileHandle(stream_redirection_opt);
  stream_redirection_handle.CompleteWrite(kLogLine1.data(), kLogLine1.length());
  stream_redirection_handle.CompleteWrite(kLogLine2.data(), kLogLine2.length());
  stream_redirection_handle.Close();

  // Check log content after completion.
  EXPECT_EQ(CompleteReadFile(test_file_path),
            absl::StrFormat("%s%s", kLogLine1, kLogLine2));
}

}  // namespace

}  // namespace ray
