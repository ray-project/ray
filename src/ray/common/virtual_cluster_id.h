// Copyright 2017 The Ray Authors.
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

#include "ray/common/simple_id.h"

namespace ray {

class VirtualClusterID : public SimpleID<VirtualClusterID> {
  using SimpleID::SimpleID;
};

inline std::ostream &operator<<(std::ostream &os, const ray::VirtualClusterID &id) {
  os << id.Binary();
  return os;
}

}  // namespace ray

namespace std {

template <>
struct hash<ray::VirtualClusterID> {
  size_t operator()(const ray::VirtualClusterID &id) const { return id.Hash(); }
};
template <>
struct hash<const ray::VirtualClusterID> {
  size_t operator()(const ray::VirtualClusterID &id) const { return id.Hash(); }
};

}  // namespace std
