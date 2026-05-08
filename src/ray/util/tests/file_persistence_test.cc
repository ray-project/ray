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

#include "ray/util/file_persistence.h"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

namespace ray {

TEST(FilePersistenceTest, ConcurrentRead) {
  auto test_dir = std::filesystem::temp_directory_path() / "file_persistence_concurrent";
  std::filesystem::create_directories(test_dir);
  std::string file_path = (test_dir / "concurrent.txt").string();
  const std::string expected_content = "hello world";

  // Start multiple readers first (they will wait for the file)
  std::vector<std::thread> readers;
  for (int i = 0; i < 5; i++) {
    readers.emplace_back([&]() {
      auto result = WaitForFile(file_path, /*timeout_ms=*/5000, /*poll_interval_ms=*/10);
      EXPECT_TRUE(result.has_value());
      EXPECT_EQ(result.value(), expected_content);
    });
  }

  // Give readers time to start waiting
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Write file once
  ASSERT_TRUE(WriteFile(file_path, expected_content).ok());

  for (auto &t : readers) {
    t.join();
  }

  std::filesystem::remove_all(test_dir);
}

TEST(FilePersistenceTest, TimeoutOnMissingFile) {
  std::string file_path = "/tmp/file_persistence_nonexistent/missing.txt";

  auto result = WaitForFile(file_path,
                            /*timeout_ms=*/10,
                            /*poll_interval_ms=*/5);

  EXPECT_TRUE(result.has_error());
  EXPECT_TRUE(std::holds_alternative<StatusT::TimedOut>(result.error()));
}

}  // namespace ray
