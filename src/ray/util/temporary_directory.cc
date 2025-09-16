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

#include "ray/util/temporary_directory.h"

#include <cstdlib>
#include <string>

#include "ray/common/id.h"

namespace ray {

ScopedTemporaryDirectory::ScopedTemporaryDirectory(const std::string &dir) {
  temporary_directory_ =
      dir.empty() ? std::filesystem::temp_directory_path() : std::filesystem::path{dir};
  // Manually generate a directory name by appending UUID.
  temporary_directory_ = temporary_directory_ / UniqueID::FromRandom().Hex();
  RAY_CHECK(std::filesystem::create_directory(temporary_directory_));
}
ScopedTemporaryDirectory::~ScopedTemporaryDirectory() {
  RAY_CHECK(std::filesystem::remove_all(temporary_directory_));
}

}  // namespace ray
