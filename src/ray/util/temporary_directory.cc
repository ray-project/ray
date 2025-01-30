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
#include <filesystem>

#include "absl/strings/str_format.h"
#include "ray/util/util.h"

namespace ray {

ScopedTemporaryDirectory::ScopedTemporaryDirectory() {
  const auto current_path = std::filesystem::current_path();
  temporary_directory_ =
      absl::StrFormat("%s/%s", current_path.native(), GenerateUUIDV4());
  RAY_CHECK(std::filesystem::create_directory(temporary_directory_));
}
ScopedTemporaryDirectory::~ScopedTemporaryDirectory() {
  RAY_CHECK(std::filesystem::remove_all(temporary_directory_));
}

}  // namespace ray
