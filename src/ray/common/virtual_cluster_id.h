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

const std::string kPrimaryClusterID = "kPrimaryClusterID";
const std::string kJobClusterIDSeperator = "##";

class VirtualClusterID : public SimpleID<VirtualClusterID> {
 public:
  using SimpleID::SimpleID;

  VirtualClusterID BuildJobClusterID(const std::string &job_id) const {
    return VirtualClusterID::FromBinary(id_ + kJobClusterIDSeperator + job_id);
  }

  bool IsJobClusterID() const {
    return id_.find(kJobClusterIDSeperator) != std::string::npos;
  }

  VirtualClusterID ParentID() const {
    auto pos = id_.find(kJobClusterIDSeperator);
    return pos == std::string::npos ? Nil()
                                    : VirtualClusterID::FromBinary(id_.substr(0, pos));
  }

  std::string JobName() const {
    auto pos = id_.find(kJobClusterIDSeperator);
    return pos == std::string::npos ? ""
                                    : id_.substr(pos + kJobClusterIDSeperator.size());
  }
};

inline std::ostream &operator<<(std::ostream &os, const ray::VirtualClusterID &id) {
  os << id.Binary();
  return os;
}

template <>
struct DefaultLogKey<VirtualClusterID> {
  constexpr static std::string_view key = kLogKeyVirtualClusterID;
};

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
