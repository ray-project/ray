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

#include "ray/util/compat.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "ray/common/status_or.h"
#include "ray/util/filesystem.h"

#if defined(__APPLE__) || defined(__linux__)
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

namespace ray {

namespace {

constexpr std::string_view kContent = "helloworld";

TEST(CompatTest, WriteTest) {
  int fd = GetStdoutFd();

  testing::internal::CaptureStdout();
  RAY_CHECK_OK(CompleteWrite(fd, kContent.data(), kContent.length()));
  RAY_CHECK_OK(Flush(fd));
  const std::string stdout_content = testing::internal::GetCapturedStdout();

  EXPECT_EQ(stdout_content, kContent);
}

}  // namespace

}  // namespace ray
