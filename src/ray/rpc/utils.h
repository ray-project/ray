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

#include <string>

#include "google/protobuf/util/message_differencer.h"
#include "src/ray/protobuf/common.pb.h"

namespace std {
template <>
struct hash<ray::rpc::Address> {
  size_t operator()(const ray::rpc::Address &addr) const {
    size_t hash_value = std::hash<int32_t>()(addr.port());
    hash_value ^= std::hash<std::string>()(addr.ip_address());
    hash_value ^= std::hash<std::string>()(addr.worker_id());
    hash_value ^= std::hash<std::string>()(addr.node_id());
    return hash_value;
  }
};
}  // namespace std

namespace ray {
namespace rpc {
inline bool operator==(const Address &lhs, const Address &rhs) {
  return google::protobuf::util::MessageDifferencer::Equivalent(lhs, rhs);
}
}  // namespace rpc
}  // namespace ray
