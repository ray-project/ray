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

#pragma once

#include <string>

#include "ray/common/status.h"
#include "ray/common/status_or.h"

namespace ray {

/**
 * @brief Write a string value to a file atomically. Overwrites if the file exists.
 *
 * Uses temp file + rename pattern for cross-platform (Linux/Windows) and
 * cross-language (C++/Python) safe file sharing. This guarantees readers
 * will not read partial content.
 *
 * @param file_path The absolute path to the target file.
 * @param value The string value to write.
 * @return Status::OK if the file was written successfully.
 * @return Status::IOError if the temp file could not be opened, written to,
 *         or renamed to the target path.
 */
Status WriteFile(const std::string &file_path, const std::string &value);

/**
 * @brief Wait for a file to appear and return its content as a string.
 *
 * Best suited for write-once, read-many scenarios. No protection against
 * concurrent writes or content changes during/after read.
 *
 * @param file_path The absolute path to the file to wait for.
 * @param timeout_ms Maximum time to wait in milliseconds. Defaults to 15000.
 * @param poll_interval_ms Interval between filesystem checks in milliseconds.
 *        Defaults to 50.
 * @return The file content if successful.
 * @return StatusT::IOError if the file exists but cannot be read.
 * @return StatusT::TimedOut if the file does not appear within the timeout period.
 */
StatusSetOr<std::string, StatusT::IOError, StatusT::TimedOut> WaitForFile(
    const std::string &file_path, int timeout_ms = 15000, int poll_interval_ms = 50);

}  // namespace ray
