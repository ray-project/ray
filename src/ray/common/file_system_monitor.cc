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

#include "ray/common/file_system_monitor.h"

#include "nlohmann/json.hpp"
#include "ray/util/logging.h"

using json = nlohmann::json;

namespace ray {

FileSystemMonitor::FileSystemMonitor(std::vector<std::string> paths,
                                     double capacity_threshold,
                                     uint64_t monitor_interval_ms)
    : paths_(std::move(paths)),
      capacity_threshold_(capacity_threshold),
      over_capacity_(CheckIfAnyPathOverCapacity()),
      io_context_(),
      monitor_thread_([this] {
        /// The asio work to keep io_contex_ alive.
        boost::asio::io_service::work io_service_work_(io_context_);
        io_context_.run();
      }),
      runner_(io_context_) {
  runner_.RunFnPeriodically([this] { over_capacity_ = CheckIfAnyPathOverCapacity(); },
                            monitor_interval_ms,
                            "FileSystemMonitor.CheckIfAnyPathOverCapacity");
}

FileSystemMonitor::FileSystemMonitor()
    : FileSystemMonitor(/*paths*/ {},
                        /*capacity_threshold*/ 1,
                        /*monitor_interval_ms*/ 365ULL * 24 * 60 * 60 * 1000) {}

FileSystemMonitor::~FileSystemMonitor() {
  io_context_.stop();
  if (monitor_thread_.joinable()) {
    monitor_thread_.join();
  }
}

std::optional<std::filesystem::space_info> FileSystemMonitor::Space(
    const std::string &path) const {
  std::error_code ec;
  const std::filesystem::space_info si = std::filesystem::space(path, ec);
  if (ec) {
    RAY_LOG_EVERY_MS(WARNING, 60 * 1000)
        << "Failed to get capacity of " << path << " with error: " << ec.message();
    return std::nullopt;
  }
  return si;
}

bool FileSystemMonitor::OverCapacity() const { return over_capacity_.load(); }

bool FileSystemMonitor::CheckIfAnyPathOverCapacity() const {
  if (paths_.empty()) {
    return false;
  }

  if (capacity_threshold_ == 0) {
    return true;
  }

  if (capacity_threshold_ >= 1) {
    return false;
  }

  for (auto &path : paths_) {
    if (OverCapacityImpl(path, Space(path))) {
      return true;
    }
  }
  return false;
}

bool FileSystemMonitor::OverCapacityImpl(
    const std::string &path,
    const std::optional<std::filesystem::space_info> &space_info) const {
  if (!space_info.has_value()) {
    return false;
  }
  if (space_info->capacity <= 0) {
    RAY_LOG_EVERY_MS(ERROR, 60 * 1000)
        << path << " has no capacity, object creation will fail if spilling is required.";
    return true;
  }

  if ((1 - 1.0f * space_info->available / space_info->capacity) < capacity_threshold_) {
    return false;
  }

  RAY_LOG_EVERY_MS(ERROR, 10 * 1000)
      << path << " is over " << capacity_threshold_ * 100
      << "\% full, available space: " << space_info->available
      << "; capacity: " << space_info->capacity
      << ". Object creation will fail if spilling is required.";
  return true;
}

std::vector<std::string> ParseSpillingPaths(const std::string &spilling_config) {
  std::vector<std::string> spilling_paths;

  try {
    json spill_config = json::parse(spilling_config);
    const auto &directory_path = spill_config.at("params").at("directory_path");
    if (directory_path.is_string()) {
      spilling_paths.push_back(directory_path);
    } else if (directory_path.is_array()) {
      for (auto &entry : directory_path) {
        if (entry.is_string()) {
          spilling_paths.push_back(entry);
        } else {
          RAY_LOG(ERROR) << "Failed to parse spilling path: " << entry
                         << ", expecting a string literal.";
        }
      }
    } else {
      RAY_LOG(ERROR) << "Failed to parse spilling path: " << directory_path
                     << ", expecting string or array.";
    }
  } catch (json::exception &ex) {
    RAY_LOG(ERROR) << "Failed to parse spilling config, error message: " << ex.what()
                   << "The config string is probably invalid json: " << spilling_config;
  }
  return spilling_paths;
}
}  // namespace ray
