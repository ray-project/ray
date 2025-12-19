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

/// Additional information about an RDT object required for observability since the actual
/// tensor is only stored on the actor.
struct RDTObjectInfo {
  std::string object_id;  // Binary object ID
  std::string device;     // Device location (e.g., "cuda:0", "cpu")
  bool is_primary;        // Whether this is the primary copy
  int64_t object_size;    // Actual tensor data size in bytes
};

}  // namespace core
}  // namespace ray
