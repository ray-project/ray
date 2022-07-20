// Copyright 2020 The Ray Authors.
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

#include <gtest/gtest_prod.h>

#include <filesystem>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"

namespace ray {
/// Monitor the filesystem capacity ray is using.
/// This class is thread safe.
class FileSystemMonitor {
 public:
  /// Constructor.
  ///
  /// \param paths paths of the file system to monitor the usage.
  /// \param capacity_threshold a value between 0-1 indicates the capacity limit.
  /// \param monitor_interval_ms control the frequency to check the disk usage.
  FileSystemMonitor(std::vector<std::string> paths,
                    double capacity_threshold,
                    uint64_t monitor_interval_ms = 1000);

  /// Creates a Noop monitor that never reports out of space.
  FileSystemMonitor();

  ~FileSystemMonitor();

  /// Returns the disk usage of a given path.
  ///
  /// \param path path of the file system to query the disk usage.
  /// \return std::filesystem::space_info if query succeeds; or return empty optional
  /// if error happens. Refer to https://en.cppreference.com/w/cpp/filesystem/space_info
  /// for struct details.
  std::optional<std::filesystem::space_info> Space(const std::string &path) const;

  /// Returns true if ANY path's disk usage is over the capacity threshold.
  bool OverCapacity() const;

 private:
  bool CheckIfAnyPathOverCapacity() const;
  // For testing purpose.
  bool OverCapacityImpl(const std::string &path,
                        const std::optional<std::filesystem::space_info> &info) const;

 private:
  FRIEND_TEST(FileSystemTest, TestOverCapacity);
  const std::vector<std::string> paths_;
  const double capacity_threshold_;
  std::atomic<bool> over_capacity_;
  instrumented_io_context io_context_;
  std::thread monitor_thread_;
  PeriodicalRunner runner_;
};

std::vector<std::string> ParseSpillingPaths(const std::string &spilling_config);
}  // namespace ray
