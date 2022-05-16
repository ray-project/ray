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

#include <filesystem>
#include <optional>
#include <string>
#include <utility>

#include "ray/util/logging.h"

namespace ray {

// Monitor ray local disk usage.
class LocalDiskMonitor {
 public:
  LocalDiskMonitor(const std::string &path, double capacity_limit_threshold)
      : path_{path}, available_threshold_{1.0 - capacity_limit_threshold} {}

  std::optional<std::filesystem::space_info> Space() const {
    std::error_code ec;
    const std::filesystem::space_info si = std::filesystem::space(path_, ec);
    if (ec) {
      RAY_LOG_EVERY_MS(WARNING, 60 * 1000)
          << "Failed to get capacity of " << path_ << " with error: " << ec.message();
      return std::nullopt;
    }
    return si;
  }

  bool OverCapacity() const {
    auto space_info = space();
    if (!space_info.has_value() || space_info->capacity == 0) {
      return false;
    }
    return 1.0 * space_info->available / space_info->capacity < available_threshold_;
  }

 private:
  const std::string path_;
  const double available_threshold_;
};

}  // namespace ray
