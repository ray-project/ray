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

#include "ray/util/spdlog_newliner_sink.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "ray/common/test/testing.h"
#include "ray/util/compat.h"
#include "ray/util/filesystem.h"
#include "ray/util/spdlog_fd_sink.h"
#include "ray/util/temporary_directory.h"
#include "spdlog/sinks/basic_file_sink.h"

namespace ray {

namespace {

std::shared_ptr<spdlog::logger> CreateLogger() {
  auto fd_formatter = std::make_unique<spdlog::pattern_formatter>(
      "%v", spdlog::pattern_time_type::local, std::string(""));
  auto fd_sink = std::make_shared<non_owned_fd_sink_st>(GetStdoutHandle());
  // We have to manually set the formatter, since it's not managed by logger.
  fd_sink->set_formatter(std::move(fd_formatter));

  auto sink = std::make_shared<spdlog_newliner_sink_st>(std::move(fd_sink));
  auto logger_formatter = std::make_unique<spdlog::pattern_formatter>(
      "%v", spdlog::pattern_time_type::local, std::string(""));
  auto logger = std::make_shared<spdlog::logger>(/*name=*/"logger", std::move(sink));
  logger->set_formatter(std::move(logger_formatter));
  return logger;
}

std::shared_ptr<spdlog::logger> CreateLogger(const std::string &filepath) {
  auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_st>(filepath);
  file_sink->set_level(spdlog::level::info);
  file_sink->set_formatter(std::make_unique<spdlog::pattern_formatter>(
      "%v", spdlog::pattern_time_type::local, std::string("")));

  auto sink = std::make_shared<spdlog_newliner_sink_st>(std::move(file_sink));
  auto logger_formatter = std::make_unique<spdlog::pattern_formatter>(
      "%v", spdlog::pattern_time_type::local, std::string(""));
  auto logger = std::make_shared<spdlog::logger>(/*name=*/"logger", std::move(sink));
  logger->set_formatter(std::move(logger_formatter));
  return logger;
}

// Testing scenario: Keep writing to spdlog after flush, and check whether all written
// content is correctly reflected.
TEST(NewlinerSinkWithStreamSinkTest, WriteAfterFlush) {
  auto logger = CreateLogger();
  constexpr std::string_view kContent = "hello";

  // First time write and flush.
  testing::internal::CaptureStdout();
  logger->log(spdlog::level::info, kContent);
  logger->flush();
  std::string stdout_content = testing::internal::GetCapturedStdout();
  EXPECT_EQ(stdout_content, kContent);

  // Write after flush.
  testing::internal::CaptureStdout();
  logger->log(spdlog::level::info, kContent);
  logger->flush();
  stdout_content = testing::internal::GetCapturedStdout();
  EXPECT_EQ(stdout_content, kContent);
}

TEST(NewlinerSinkWithStreamSinkTest, AppendAndFlushTest) {
  // Case-1: string with newliner at the end.
  {
    auto logger = CreateLogger();
    constexpr std::string_view kContent = "hello\n";

    testing::internal::CaptureStdout();
    logger->log(spdlog::level::info, kContent);
    std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, kContent);

    testing::internal::CaptureStdout();
    logger->flush();
    stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_TRUE(stdout_content.empty());
  }

  // Case-2: string with no newliner at the end.
  {
    auto logger = CreateLogger();
    constexpr std::string_view kContent = "hello";

    // Before flush, nothing's streamed to stdout because no newliner.
    testing::internal::CaptureStdout();
    logger->log(spdlog::level::info, kContent);
    std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_TRUE(stdout_content.empty());

    // Buffered content streamed after flush.
    testing::internal::CaptureStdout();
    logger->flush();
    stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, kContent);
  }

  // Case-3: newliner in the middle, with trailing newliner.
  {
    auto logger = CreateLogger();
    constexpr std::string_view kContent = "hello\nworld\n";

    testing::internal::CaptureStdout();
    logger->log(spdlog::level::info, kContent);
    std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, kContent);

    testing::internal::CaptureStdout();
    logger->flush();
    stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_TRUE(stdout_content.empty());
  }

  // Case-4: newliner in the middle, without trailing newliner.
  {
    auto logger = CreateLogger();
    constexpr std::string_view kContent = "hello\nworld";

    // Before flush, only content before newliner is streamed.
    testing::internal::CaptureStdout();
    logger->log(spdlog::level::info, kContent);
    std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, "hello\n");

    // Buffered content streamed after flush.
    testing::internal::CaptureStdout();
    logger->flush();
    stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, "world");
  }

  // Case-5: multiple writes.
  {
    auto logger = CreateLogger();
    constexpr std::string_view kContent1 = "hello\nworld";
    constexpr std::string_view kContent2 = "hello\nworld\n";

    // Content before newliner is streamed before flush.
    testing::internal::CaptureStdout();
    logger->log(spdlog::level::info, kContent1);
    std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, "hello\n");

    // Stream content with trailing newliner flushes everything.
    testing::internal::CaptureStdout();
    logger->log(spdlog::level::info, kContent2);
    stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, "worldhello\nworld\n");

    // All content streamed and flushed.
    testing::internal::CaptureStdout();
    logger->flush();
    stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_TRUE(stdout_content.empty());
  }
}

TEST(NewlinerSinkWithFileinkTest, AppendAndFlushTest) {
  ScopedTemporaryDirectory dir{};

  // Case-1: string with newliner at the end.
  {
    const auto filepath = (dir.GetDirectory() / GenerateUUIDV4()).string();
    auto logger = CreateLogger(filepath);
    constexpr std::string_view kContent = "hello\n";

    logger->log(spdlog::level::info, kContent);
    auto content = ReadEntireFile(filepath);
    RAY_ASSERT_OK(content);
    EXPECT_EQ(*content, kContent);

    logger->flush();
    content = ReadEntireFile(filepath);
    RAY_ASSERT_OK(content);
    EXPECT_EQ(*content, kContent);
  }

  // Case-2: string with no newliner at the end.
  {
    const auto filepath = (dir.GetDirectory() / GenerateUUIDV4()).string();
    auto logger = CreateLogger(filepath);
    constexpr std::string_view kContent = "hello";

    // Before flush, nothing's streamed to stdout because no newliner.
    logger->log(spdlog::level::info, kContent);
    auto content = ReadEntireFile(filepath);
    RAY_ASSERT_OK(content);
    EXPECT_TRUE(content->empty());

    // Buffered content streamed after flush.
    logger->flush();
    content = ReadEntireFile(filepath);
    RAY_ASSERT_OK(content);
    EXPECT_EQ(*content, kContent);
  }

  // Case-3: newliner in the middle, with trailing newliner.
  {
    const auto filepath = (dir.GetDirectory() / GenerateUUIDV4()).string();
    auto logger = CreateLogger(filepath);
    constexpr std::string_view kContent = "hello\nworld\n";

    logger->log(spdlog::level::info, kContent);
    auto content = ReadEntireFile(filepath);
    RAY_ASSERT_OK(content);
    EXPECT_EQ(*content, kContent);

    logger->flush();
    content = ReadEntireFile(filepath);
    RAY_ASSERT_OK(content);
    EXPECT_EQ(*content, kContent);
  }

  // // Case-4: newliner in the middle, without trailing newliner.
  {
    const auto filepath = (dir.GetDirectory() / GenerateUUIDV4()).string();
    auto logger = CreateLogger(filepath);
    constexpr std::string_view kContent = "hello\nworld";

    // Before flush, nothing's streamed to stdout because no newliner.
    logger->log(spdlog::level::info, kContent);
    auto content = ReadEntireFile(filepath);
    RAY_ASSERT_OK(content);
    EXPECT_EQ(*content, "hello\n");

    // Buffered content streamed after flush.
    logger->flush();
    content = ReadEntireFile(filepath);
    RAY_ASSERT_OK(content);
    EXPECT_EQ(*content, kContent);
  }

  // // Case-5: multiple writes.
  {
    const auto filepath = (dir.GetDirectory() / GenerateUUIDV4()).string();
    auto logger = CreateLogger(filepath);
    constexpr std::string_view kContent1 = "hello\nworld";
    constexpr std::string_view kContent2 = "hello\nworld\n";

    // Content before newliner is streamed before flush.
    logger->log(spdlog::level::info, kContent1);
    auto content = ReadEntireFile(filepath);
    RAY_ASSERT_OK(content);
    EXPECT_EQ(*content, "hello\n");

    // Stream content with trailing newliner flushes everything.
    logger->log(spdlog::level::info, kContent2);
    content = ReadEntireFile(filepath);
    RAY_ASSERT_OK(content);
    EXPECT_EQ(*content, absl::StrFormat("%s%s", kContent1, kContent2));

    // All content streamed and flushed.
    logger->flush();
    content = ReadEntireFile(filepath);
    RAY_ASSERT_OK(content);
    EXPECT_EQ(*content, absl::StrFormat("%s%s", kContent1, kContent2));
  }
}

}  // namespace

}  // namespace ray
