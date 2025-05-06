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

#include "ray/util/spdlog_fd_sink.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "ray/util/compat.h"

namespace ray {

namespace {

// Logs "helloworld" for whatever given message; here we don't care the what message is
// logged, the only thing matters is whether msg has been written to the given file
// descriptor correctly.
class HelloworldFormatter : public spdlog::formatter {
 public:
  void format(const spdlog::details::log_msg &msg, spdlog::memory_buf_t &dest) override {
    dest.append(std::string{"helloworld"});
  }
  std::unique_ptr<spdlog::formatter> clone() const override {
    return std::make_unique<HelloworldFormatter>();
  }
};

TEST(SpdlogFdSinkTest, SinkWithFd) {
  non_owned_fd_sink_st sink{GetStdoutHandle()};
  sink.set_formatter(std::make_unique<HelloworldFormatter>());
  spdlog::details::log_msg msg_to_log{
      /*logger_name=*/"logger_name", spdlog::level::level_enum::info, /*msg=*/"content"};

  testing::internal::CaptureStdout();
  sink.log(msg_to_log);
  const std::string stdout_content = testing::internal::GetCapturedStdout();

  EXPECT_EQ(stdout_content, "helloworld");
}

}  // namespace

}  // namespace ray
