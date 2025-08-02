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

#include <filesystem>
#include <string>

namespace ray {

/**
  @return the filepath of the log file from the log_dir and the app_name
  */
std::string GetLogFilepathFromDirectory(const std::string &log_dir,
                                        const std::string &app_name);

/**
  @return the filepath of the err file from the log_dir and the app_name
  */
std::string GetErrLogFilepathFromDirectory(const std::string &log_dir,
                                           const std::string &app_name);

/**
  Cross platform utility for joining paths together with the appropriate separator.
  @return the joined path with the base path and all of the components.
  */
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
