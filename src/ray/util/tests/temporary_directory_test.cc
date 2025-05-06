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

#include "ray/util/temporary_directory.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "absl/strings/str_format.h"

namespace ray {

TEST(TemporaryDirectoryTest, CreationAndDestruction) {
  std::filesystem::path temp_directory;
  {
    ScopedTemporaryDirectory dir{};
    temp_directory = dir.GetDirectory();

    // Create a file under temporary directory.
    std::filesystem::path empty_file = temp_directory / "empty_file";
    std::ofstream(empty_file).close();
    ASSERT_TRUE(std::filesystem::exists(empty_file));

    // Create a sub-directory under temporary directory.
    std::filesystem::path internal_dir = temp_directory / "dir";
    ASSERT_TRUE(std::filesystem::create_directory(internal_dir));
    ASSERT_TRUE(std::filesystem::exists(internal_dir));

    // Create a file under internal directory.
    std::filesystem::path internal_file = internal_dir / "empty_file";
    std::ofstream(internal_file).close();
    ASSERT_TRUE(std::filesystem::exists(empty_file));
  }
  ASSERT_FALSE(std::filesystem::exists(temp_directory));
}

}  // namespace ray
