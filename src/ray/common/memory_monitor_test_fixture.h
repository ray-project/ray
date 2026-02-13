// Copyright 2026 The Ray Authors.
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

#pragma once

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/cgroup2/cgroup_test_utils.h"

namespace ray {

/**
 * @brief Test utility base class providing memory usage mocking functionality
 * for memory monitor tests.
 *
 * This class manages the lifecycle of temporary directories and files
 * created during tests, ensuring they are cleaned up when the test ends.
 */
class MemoryMonitorTestFixture : public ::testing::Test {
 public:
  /// Destroys the mock directories in reverse order of creation.
  ~MemoryMonitorTestFixture() override {
    mock_cgroup_files_.clear();
    while (!mock_cgroup_dirs_.empty()) {
      mock_cgroup_dirs_.pop_back();
    }
    mock_proc_files_.clear();
    while (!mock_proc_dirs_.empty()) {
      mock_proc_dirs_.pop_back();
    }
  }

  MemoryMonitorTestFixture() = default;
  MemoryMonitorTestFixture(const MemoryMonitorTestFixture &) = delete;
  MemoryMonitorTestFixture &operator=(const MemoryMonitorTestFixture &) = delete;
  MemoryMonitorTestFixture(MemoryMonitorTestFixture &&) = delete;
  MemoryMonitorTestFixture &operator=(MemoryMonitorTestFixture &&) = delete;

 protected:
  /**
   * @brief Creates a mock /proc-style memory usage file for a given process.
   *
   * Creates a temporary directory with the structure `<dir>/<pid>/smaps_rollup`
   * and writes a mock smaps_rollup file with the given usage value.
   * The temporary directory and file are cleaned up automatically by their
   * destructors when the test ends.
   *
   * @param pid The process ID to create usage info for.
   * @param usage_kb The memory usage value in kB to write.
   * @return The path to the created mock proc directory.
   */
  std::string MockProcMemoryUsage(pid_t pid, const std::string &usage_kb);

  /**
   * @brief Sets up a mock cgroup v2 directory for emulating memory usage and populates
   * the files with the provided mock memory values.
   *
   * @param total_bytes The value to write to memory.max (total memory limit).
   * @param current_bytes The value to write to memory.current (current usage).
   * @param inactive_file_bytes The inactive_file value in memory.stat.
   * @param active_file_bytes The active_file value in memory.stat.
   * @return The path to the created mock cgroup directory.
   */
  std::string MockCgroupMemoryUsage(int64_t total_bytes,
                                    int64_t current_bytes,
                                    int64_t inactive_file_bytes,
                                    int64_t active_file_bytes);

 private:
  std::vector<std::unique_ptr<TempDirectory>> mock_proc_dirs_;
  std::vector<std::unique_ptr<TempFile>> mock_proc_files_;
  std::vector<std::unique_ptr<TempDirectory>> mock_cgroup_dirs_;
  std::vector<std::unique_ptr<TempFile>> mock_cgroup_files_;
};

}  // namespace ray
