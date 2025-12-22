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

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

namespace ray {

Status WriteFile(const std::string &file_path, const std::string &value) {
  std::string tmp_path = file_path + ".tmp";
  std::ofstream file(tmp_path);
  if (!file.is_open()) {
    return Status::IOError("Failed to open temp file for writing: " + tmp_path);
  }
  file << value;
  file.close();
  if (file.fail()) {
    return Status::IOError("Failed to write to temp file: " + tmp_path);
  }

  std::error_code ec;
  // On Windows, rename doesn't overwrite existing files, so remove first.
  std::filesystem::remove(file_path, ec);
  std::filesystem::rename(tmp_path, file_path, ec);
  if (ec) {
    return Status::IOError("Failed to rename " + tmp_path + " to " + file_path + ": " +
                           ec.message());
  }
  return Status::OK();
}

StatusSetOr<std::string, StatusT::IOError, StatusT::TimedOut> WaitForFile(
    const std::string &file_path, int timeout_ms, int poll_interval_ms) {
  auto start = std::chrono::steady_clock::now();
  while (true) {
    if (std::filesystem::exists(file_path)) {
      std::ifstream file(file_path);
      if (file.is_open()) {
        std::string content((std::istreambuf_iterator<char>(file)),
                            std::istreambuf_iterator<char>());
        file.close();
        if (!file.fail()) {
          return content;
        }
        return StatusT::IOError("Failed to read file: " + file_path);
      }
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    if (elapsed >= timeout_ms) {
      return StatusT::TimedOut("Timed out waiting for file " + file_path);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
  }
}

}  // namespace ray
