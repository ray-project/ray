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

namespace spdlog::sinks {

namespace {

#if defined(__APPLE__) || defined(__linux__)
int GetStdoutHandle() { return STDOUT_FILENO; }
#elif defined(_WIN32)
HANDLE GetStdoutHandle() { return GetStdHandle(STD_OUTPUT_HANDLE); }
#endif

// Logs "helloworld" for whatever given message; here we don't care the what message is
// logged, the only thing matters is whether msg has been written to the given file
// descriptor correctly.
class HelloworldFormatter : public formatter {
 public:
  void format(const details::log_msg &msg, memory_buf_t &dest) override {
    dest.append(std::string{"helloworld"});
  }
  std::unique_ptr<formatter> clone() const override {
    return std::make_unique<HelloworldFormatter>();
  }
};

TEST(SpdlogFdSinkTest, SinkWithFd) {
  fd_sink_st sink{GetStdoutHandle()};
  sink.set_formatter(std::make_unique<HelloworldFormatter>());
  details::log_msg msg_to_log{
      /*logger_name=*/"logger_name", level::level_enum::info, /*msg=*/"content"};

  testing::internal::CaptureStdout();
  sink.log(msg_to_log);
  const std::string stdout_content = testing::internal::GetCapturedStdout();

  EXPECT_EQ(stdout_content, "helloworld");
};

}  // namespace

}  // namespace spdlog::sinks
