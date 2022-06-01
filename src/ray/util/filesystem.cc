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

#include "ray/util/filesystem.h"

#include <cstdlib>

#include "ray/util/logging.h"

#ifdef _WIN32
#include <Windows.h>
#endif

namespace ray {

std::string GetFileName(const std::string &path) {
  return std::filesystem::path(path).filename().string();
}

std::string GetUserTempDir() {
  std::string result;
#if defined(__APPLE__) || defined(__linux__)
  // Prefer the hard-coded path for now, for compatibility.
  result = "/tmp";
#elif defined(_WIN32)
  result.resize(1 << 8);
  DWORD n = GetTempPath(static_cast<DWORD>(result.size()), &*result.begin());
  if (n > result.size()) {
    result.resize(n);
    n = GetTempPath(static_cast<DWORD>(result.size()), &*result.begin());
  }
  result.resize(0 < n && n <= result.size() ? static_cast<size_t>(n) : 0);
#else  // not Darwin, or Windows
  const char *candidates[] = {"TMPDIR", "TMP", "TEMP", "TEMPDIR"};
  const char *found = NULL;
  for (char const *candidate : candidates) {
    found = std::getenv(candidate);
    if (found) {
      break;
    }
  }
  result = found ? found : "/tmp";
#endif
  // Strip trailing separators
  while (!result.empty() && IsDirSep(result.back())) {
    result.pop_back();
  }
  RAY_CHECK(!result.empty());
  return result;
}

FileSystemMonitor::FileSystemMonitor(const std::string &path, double capacity_threshold)
    : ray_file_path_(path), capacity_threshold_(capacity_threshold) {}

std::optional<std::filesystem::space_info> FileSystemMonitor::Space() const {
  std::error_code ec;
  const std::filesystem::space_info si = std::filesystem::space(ray_file_path_, ec);
  if (ec) {
    RAY_LOG_EVERY_MS(WARNING, 60 * 1000) << "Failed to get capacity of " << ray_file_path_
                                         << " with error: " << ec.message();
    return std::nullopt;
  }
  return si;
}

bool FileSystemMonitor::OverCapacity() const {
  auto space_info = Space();
  if (!space_info.has_value()) {
    return false;
  }
  if (space_info->capacity == 0) {
    RAY_LOG_EVERY_MS(ERROR, 60 * 1000) << ray_file_path_ << " has no capacity.";
    return true;
  }

  if ((1 - 1.0f * space_info->available / space_info->capacity) < capacity_threshold_) {
    return false;
  }

  RAY_LOG_EVERY_MS(ERROR, 10 * 1000)
      << ray_file_path_ << " is over capacity, available: " << space_info->available
      << ", capacity: " << space_info->capacity << ", threshold: " << capacity_threshold_;
  return true;
}
}  // namespace ray
