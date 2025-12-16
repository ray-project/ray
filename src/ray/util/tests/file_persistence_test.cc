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

TEST(FilePersistenceTest, ConcurrentReadWithWrite) {
  auto test_dir = std::filesystem::temp_directory_path() / "file_persistence_concurrent";
  std::filesystem::create_directories(test_dir);
  std::string file_path = (test_dir / "concurrent.txt").string();

  std::atomic<bool> stop{false};
  std::atomic<int> corrupted_reads{0};
  std::atomic<int> read_errors{0};
  std::atomic<int> write_errors{0};

  // Start multiple readers first
  std::vector<std::thread> readers;
  for (int i = 0; i < 5; i++) {
    readers.emplace_back([&]() {
      while (!stop) {
        auto result =
            WaitForFile(file_path, /*timeout_ms=*/1000, /*poll_interval_ms=*/10);
        if (result.ok()) {
          try {
            std::stoi(*result);
          } catch (...) {
            corrupted_reads++;
          }
        } else {
          read_errors++;
        }
      }
    });
  }

  // Start writer that overwrites the file repeatedly
  std::thread writer([&]() {
    for (int i = 1; i <= 100 && !stop; i++) {
      if (!WriteFile(file_path, std::to_string(i)).ok()) {
        write_errors++;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    stop = true;
  });

  writer.join();
  for (auto &t : readers) {
    t.join();
  }

  EXPECT_EQ(write_errors.load(), 0);
  EXPECT_EQ(corrupted_reads.load(), 0);
  EXPECT_EQ(read_errors.load(), 0);

  std::filesystem::remove_all(test_dir);
}

TEST(FilePersistenceTest, TimeoutOnMissingFile) {
  std::string file_path = "/tmp/file_persistence_nonexistent/missing.txt";

  auto result = WaitForFile(file_path,
                            /*timeout_ms=*/10,
                            /*poll_interval_ms=*/5);

  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsTimedOut());
}

}  // namespace ray
