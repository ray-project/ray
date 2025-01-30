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

#include "ray/util/util.h"

namespace ray {

ScopedTemporaryDirectory::ScopedTemporaryDirectory() {
  std::error_code ec;
  temporary_directory_ = std::filesystem::temp_directory_path(ec);



  RAY_LOG(ERROR) << "temp dir = " << temporary_directory_;


  RAY_CHECK(!ec);
}
ScopedTemporaryDirectory::~ScopedTemporaryDirectory() {
  RAY_CHECK(std::filesystem::remove_all(temporary_directory_));
}

}  // namespace ray
