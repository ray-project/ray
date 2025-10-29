// Copyright 2020-2021 The Ray Authors.
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

#include "util.h"

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>

#include "ray/common/constants.h"
#include "ray/util/logging.h"
#include "ray/util/network_util.h"

namespace ray {
namespace internal {

std::string getLibraryPathEnv() {
  auto path_env_p = std::getenv(kLibraryPathEnvName);
  if (path_env_p != nullptr && strlen(path_env_p) != 0) {
    return std::string(path_env_p);
  }
  return {};
}

}  // namespace internal
}  // namespace ray
