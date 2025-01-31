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

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <string_view>

#include "absl/cleanup/cleanup.h"
#include "ray/common/test/testing.h"
#include "ray/util/filesystem.h"
#include "ray/util/util.h"

/////////////////////////////////////////////////
// Unit test for both windows and unix platform.
/////////////////////////////////////////////////

namespace ray {

namespace {

constexpr std::string_view kLogLine1 = "hello\n";
constexpr std::string_view kLogLine2 = "world\n";

class PipeLoggerTest : public ::testing::TestWithParam<size_t> {};

TEST_P(PipeLoggerTest, NoPipeWrite) {
  const size_t pipe_buffer_size = GetParam();
  setEnv(kPipeLogReadBufSizeEnv.data(), absl::StrFormat("%d", pipe_buffer_size));

  // TODO(hjiang): We should have a better test util, which allows us to create a
  // temporary testing directory.
  const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

  // Delete temporary file.
  absl::Cleanup cleanup_test_file = [&test_file_path]() {
    EXPECT_TRUE(std::filesystem::remove(test_file_path));
  };

  // Take the default option, which doesn't have rotation enabled.
  StreamRedirectionOption stream_redirection_opt{};
  stream_redirection_opt.file_path = test_file_path;
  auto stream_redirection_handle = CreateRedirectionFileHandle(stream_redirection_opt);
  stream_redirection_handle.CompleteWrite(kLogLine1.data(), kLogLine1.length());
  stream_redirection_handle.CompleteWrite(kLogLine2.data(), kLogLine2.length());
  stream_redirection_handle.Close();

  // Check log content after completion.
  const auto actual_content = ReadEntireFile(test_file_path);
  RAY_ASSERT_OK(actual_content);
  const std::string expected_content = absl::StrFormat("%s%s", kLogLine1, kLogLine2);
  EXPECT_EQ(*actual_content, expected_content);
}

INSTANTIATE_TEST_SUITE_P(PipeLoggerTest, PipeLoggerTest, testing::Values(1024, 3));

}  // namespace

}  // namespace ray

/////////////////////////////////////////////////
// Unit test for both unix platform only.
/////////////////////////////////////////////////

#if defined(__APPLE__) || defined(__linux__)

#include <unistd.h>

namespace ray {

namespace {

TEST_P(PipeLoggerTest, PipeWrite) {
  const size_t pipe_buffer_size = GetParam();
  setEnv(kPipeLogReadBufSizeEnv.data(), absl::StrFormat("%d", pipe_buffer_size));

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
  // Synchronize on log flush completion.
  stream_redirection_handle.Close();

  // Check log content after completion.
  const auto actual_content1 = ReadEntireFile(log_file_path1);
  RAY_ASSERT_OK(actual_content1);
  EXPECT_EQ(*actual_content1, kLogLine2);

  const auto actual_content2 = ReadEntireFile(log_file_path2);
  RAY_ASSERT_OK(actual_content1);
  EXPECT_EQ(*actual_content2, kLogLine1);
}

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
  const auto actual_content = ReadEntireFile(test_file_path);
  RAY_ASSERT_OK(actual_content);
  EXPECT_EQ(*actual_content, absl::StrFormat("%s%s", kLogLine1, kLogLine2));
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
  const auto actual_content1 = ReadEntireFile(log_file_path1);
  RAY_ASSERT_OK(actual_content1);
  EXPECT_EQ(*actual_content1, kLogLine2);

  const auto actual_content2 = ReadEntireFile(log_file_path2);
  RAY_ASSERT_OK(actual_content2);
  EXPECT_EQ(*actual_content2, kLogLine1);
}

// Testing senario: log to stdout and file; check whether these two sinks generate
// expected output.
TEST(PipeLoggerCompatTest, CompatibilityTest) {
  // Testing-1: No newliner in the middle nor at the end.
  {
    constexpr std::string_view kContent = "hello";
    const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

    StreamRedirectionOption logging_option{};
    logging_option.file_path = test_file_path;
    logging_option.tee_to_stdout = true;

    testing::internal::CaptureStdout();
    auto stream_redirection_handle = CreateRedirectionFileHandle(logging_option);
    stream_redirection_handle.CompleteWrite(kContent.data(), kContent.length());
    stream_redirection_handle.Close();

    const std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, kContent);

    // Pipe logger automatically adds a newliner at the end.
    const auto actual_content = ReadEntireFile(test_file_path);
    RAY_ASSERT_OK(actual_content);
    EXPECT_EQ(*actual_content, "hello\n");

    EXPECT_TRUE(std::filesystem::remove(test_file_path));
  }

  // Testing-2: Newliner at the end.
  {
    constexpr std::string_view kContent = "hello\n";
    const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

    StreamRedirectionOption logging_option{};
    logging_option.file_path = test_file_path;
    logging_option.tee_to_stdout = true;

    testing::internal::CaptureStdout();
    auto stream_redirection_handle = CreateRedirectionFileHandle(logging_option);
    stream_redirection_handle.CompleteWrite(kContent.data(), kContent.length());
    stream_redirection_handle.Close();

    const std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, kContent);

    const auto actual_content = ReadEntireFile(test_file_path);
    RAY_ASSERT_OK(actual_content);
    EXPECT_EQ(*actual_content, "hello\n");

    EXPECT_TRUE(std::filesystem::remove(test_file_path));
  }

  // Testing-3: Newliner in the middle.
  {
    constexpr std::string_view kContent = "hello\nworld";
    const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

    StreamRedirectionOption logging_option{};
    logging_option.file_path = test_file_path;
    logging_option.tee_to_stdout = true;

    testing::internal::CaptureStdout();
    auto stream_redirection_handle = CreateRedirectionFileHandle(logging_option);
    stream_redirection_handle.CompleteWrite(kContent.data(), kContent.length());
    stream_redirection_handle.Close();

    const std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, kContent);

    // Pipe logger automatically adds a newliner at the end.
    const auto actual_content = ReadEntireFile(test_file_path);
    RAY_EXPECT_OK(actual_content);
    EXPECT_EQ(*actual_content, "hello\nworld\n");

    EXPECT_TRUE(std::filesystem::remove(test_file_path));
  }

  // Testing-4: Newliner in the middle and the end.
  {
    constexpr std::string_view kContent = "hello\nworld\n";
    const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

    StreamRedirectionOption logging_option{};
    logging_option.file_path = test_file_path;
    logging_option.tee_to_stdout = true;

    testing::internal::CaptureStdout();
    auto stream_redirection_handle = CreateRedirectionFileHandle(logging_option);
    stream_redirection_handle.CompleteWrite(kContent.data(), kContent.length());
    stream_redirection_handle.Close();

    const std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, kContent);

    const auto actual_content = ReadEntireFile(test_file_path);
    RAY_EXPECT_OK(actual_content);
    EXPECT_EQ(*actual_content, "hello\nworld\n");

    EXPECT_TRUE(std::filesystem::remove(test_file_path));
  }

  // Testing-5: Continuous newliner at the end.
  {
    constexpr std::string_view kContent = "helloworld\n\n\n";
    const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

    StreamRedirectionOption logging_option{};
    logging_option.file_path = test_file_path;
    logging_option.tee_to_stdout = true;

    testing::internal::CaptureStdout();
    auto stream_redirection_handle = CreateRedirectionFileHandle(logging_option);
    stream_redirection_handle.CompleteWrite(kContent.data(), kContent.length());
    stream_redirection_handle.Close();

    const std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, kContent);

    const auto actual_content = ReadEntireFile(test_file_path);
    RAY_EXPECT_OK(actual_content);
    EXPECT_EQ(*actual_content, "helloworld\n\n\n");

    EXPECT_TRUE(std::filesystem::remove(test_file_path));
  }

  // Testing-6: Continous newliner in the middle.
  {
    constexpr std::string_view kContent = "hello\n\n\nworld";
    const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

    StreamRedirectionOption logging_option{};
    logging_option.file_path = test_file_path;
    logging_option.tee_to_stdout = true;

    testing::internal::CaptureStdout();
    auto stream_redirection_handle = CreateRedirectionFileHandle(logging_option);
    stream_redirection_handle.CompleteWrite(kContent.data(), kContent.length());
    stream_redirection_handle.Close();

    const std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, kContent);

    // Pipe logger automatically adds a newliner at the end.
    const auto actual_content = ReadEntireFile(test_file_path);
    RAY_EXPECT_OK(actual_content);
    EXPECT_EQ(*actual_content, "hello\n\n\nworld\n");

    EXPECT_TRUE(std::filesystem::remove(test_file_path));
  }

  // Testing-7: Continuous newliner in the middle and at the end.
  {
    constexpr std::string_view kContent = "hello\n\nworld\n\n";
    const std::string test_file_path = absl::StrFormat("%s.out", GenerateUUIDV4());

    StreamRedirectionOption logging_option{};
    logging_option.file_path = test_file_path;
    logging_option.tee_to_stdout = true;

    testing::internal::CaptureStdout();
    auto stream_redirection_handle = CreateRedirectionFileHandle(logging_option);
    stream_redirection_handle.CompleteWrite(kContent.data(), kContent.length());
    stream_redirection_handle.Close();

    const std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, kContent);

    // Pipe logger automatically adds a newliner at the end.
    const auto actual_content = ReadEntireFile(test_file_path);
    RAY_ASSERT_OK(actual_content);
    EXPECT_EQ(*actual_content, "hello\n\nworld\n\n");

    EXPECT_TRUE(std::filesystem::remove(test_file_path));
  }
}

}  // namespace

}  // namespace ray

#endif
