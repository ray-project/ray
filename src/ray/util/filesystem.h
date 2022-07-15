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

// Filesystem and path manipulation APIs.
// (NTFS stream & attribute paths are not supported.)

namespace ray {

/// \return The portable directory separator (slash on all OSes).
static inline char GetAltDirSep() { return '/'; }

/// Equivalent to Python's os.path.basename() for file system paths.
std::string GetFileName(const std::string &path);

/// \return The non-volatile temporary directory for the current user (often /tmp).
std::string GetUserTempDir();

/// \return Whether or not the given character is a directory separator on this platform.
static inline bool IsDirSep(char ch) {
  bool result = ch == std::filesystem::path::preferred_separator;
#ifdef _WIN32
  result |= ch == GetAltDirSep();
#endif
  return result;
}

/// \return The result of joining multiple path components.
template <class... Paths>
std::string JoinPaths(std::string base, const Paths &...components) {
  auto join = [](auto &joined_path, const auto &component) {
    // if the components begin with "/" or "////", just get the path name.
    if (!component.empty() &&
        component.front() == std::filesystem::path::preferred_separator) {
      joined_path = std::filesystem::path(joined_path)
                        .append(std::filesystem::path(component).filename().string())
                        .string();
    } else {
      joined_path = std::filesystem::path(joined_path).append(component).string();
    }
  };
  (join(base, std::string_view(components)), ...);
  return base;
}
}  // namespace ray
