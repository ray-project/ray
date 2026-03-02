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

#include <cstdint>
#include <string>

namespace ray {
namespace core {

/// Information about an RDT object thats outputted in the memory summary API
struct RDTObjectInfo {
  std::string object_id;  // Binary object ID
  std::string device;     // Device location (e.g., "cuda:0", "cpu")
  int64_t object_size;    // Tensor size in bytes
};

}  // namespace core
}  // namespace ray
