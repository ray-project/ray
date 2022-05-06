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

#pragma once
#include <ray/api/ray_exception.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "boost/optional.hpp"

namespace ray {

class RayConfig {
 public:
  // The address of the Ray cluster to connect to.
  // If not provided, it will be initialized from environment variable "RAY_ADDRESS" by
  // default.
  std::string address = "";

  // Whether or not to run this application in a local mode. This is used for debugging.
  bool local_mode = false;

  // An array of directories or dynamic library files that specify the search path for
  // user code. This parameter is not used when the application runs in local mode.
  // Only searching the top level under a directory.
  std::vector<std::string> code_search_path;

  // The command line args to be appended as parameters of the `ray start` command. It
  // takes effect only if Ray head is started by a driver. Run `ray start --help` for
  // details.
  std::vector<std::string> head_args = {};

  /* The following are unstable parameters and their use is discouraged. */

  // Prevents external clients without the password from connecting to Redis if provided.
  boost::optional<std::string> redis_password_;
};

}  // namespace ray