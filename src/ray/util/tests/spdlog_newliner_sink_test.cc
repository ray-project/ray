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

#include <string_view>

#include "ray/util/compat.h"
#include "ray/util/spdlog_fd_sink.h"
#include "spdlog/sinks/basic_file_sink.h"

namespace ray {

namespace {

std::shared_ptr<spdlog::logger> CreateLogger() {
  auto fd_formatter = std::make_unique<spdlog::pattern_formatter>(
      "%v", spdlog::pattern_time_type::local, std::string(""));
  auto fd_sink = std::make_shared<non_owned_fd_sink_st>(GetStdoutFd());
  // We have to manually set the formatter, since it's not managed by logger.
  fd_sink->set_formatter(std::move(fd_formatter));

  auto sink = std::make_shared<spdlog_newliner_sink_st>(std::move(fd_sink));
  auto logger_formatter = std::make_unique<spdlog::pattern_formatter>(
      "%v", spdlog::pattern_time_type::local, std::string(""));
  auto logger = std::make_shared<spdlog::logger>(/*name=*/"logger", std::move(sink));
  logger->set_formatter(std::move(logger_formatter));
  return logger;
}

TEST(NewlinerSinkTest, AppendAndFlushTest) {
  // Case-1: string with newliner at the end.
  {
    auto logger = CreateLogger();
    constexpr std::string_view kContent = "hello\n";

    testing::internal::CaptureStdout();
    logger->log(spdlog::level::info, kContent);
    logger->flush();
    const std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, kContent);
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
    logger->flush();
    const std::string stdout_content = testing::internal::GetCapturedStdout();
    EXPECT_EQ(stdout_content, kContent);
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

}  // namespace

}  // namespace ray
