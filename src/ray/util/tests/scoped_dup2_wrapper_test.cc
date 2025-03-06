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

#include "ray/util/scoped_dup2_wrapper.h"

#include <gtest/gtest.h>

#include <boost/iostreams/device/file_descriptor.hpp>
#include <iostream>
#include <string>
#include <string_view>

#include "ray/common/test/testing.h"
#include "ray/util/compat.h"
#include "ray/util/filesystem.h"
#include "ray/util/temporary_directory.h"

namespace ray {

namespace {

constexpr std::string_view kContent = "helloworld\n";

TEST(ScopedDup2WrapperTest, BasicTest) {
  ScopedTemporaryDirectory temp_dir;
  const auto dir = temp_dir.GetDirectory();
  const auto path = dir / "test_file";
  const std::string path_string = path.string();
  boost::iostreams::file_descriptor_sink fd_sink{path_string, std::ios_base::out};

  {
    auto dup2_wrapper =
        ScopedDup2Wrapper::New(/*oldfd=*/fd_sink.handle(), /*newfd=*/GetStderrHandle());

    // Write to stdout should appear in file.
    std::cerr << kContent << std::flush;
    const auto actual_content = ReadEntireFile(path_string);
    RAY_ASSERT_OK(actual_content);
    EXPECT_EQ(*actual_content, kContent);
  }

  testing::internal::CaptureStderr();
  std::cerr << kContent << std::flush;
  const std::string stderr_content = testing::internal::GetCapturedStderr();
  EXPECT_EQ(stderr_content, kContent);

  // Not changed since last write.
  const auto actual_content = ReadEntireFile(path_string);
  RAY_ASSERT_OK(actual_content);
  EXPECT_EQ(*actual_content, kContent);
}

}  // namespace

}  // namespace ray
