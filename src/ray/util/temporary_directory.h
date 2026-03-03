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

// This class creates a temporary directory, which gets deleted (including all files
// inside of it) at its destruction.

#pragma once

#include <filesystem>
#include <string>

namespace ray {

// A scoped temporary directory, which deletes all files and sub-directories recursively
// at its destruction.
class ScopedTemporaryDirectory {
 public:
  // Create a sub-directory under the given [dir].
  // If unspecified, new directory will be created under system temporary directory.
  //
  // If creation or deletion fails, the program will exit after logging error message.
  explicit ScopedTemporaryDirectory(const std::string &dir = "");
  ~ScopedTemporaryDirectory();

  const std::filesystem::path &GetDirectory() const { return temporary_directory_; }

 private:
  std::filesystem::path temporary_directory_;
};

}  // namespace ray
