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

#include "ray/common/id.h"

namespace ray {

template <typename T>
struct SimpleID {
  static T FromBinary(const std::string &binary) {
    T id;
    id.id_ = binary;
    return id;
  }

  size_t Hash() const {
    // Note(ashione): hash code lazy calculation(it's invoked every time if hash code is
    // default value 0)
    if (!hash_) {
      hash_ = MurmurHash64A(id_.data(), id_.size(), 0);
    }
    return hash_;
  }

  const std::string &Binary() const { return id_; }

  bool operator==(const T &rhs) const { return id_ == rhs.id_; }
  bool operator!=(const T &rhs) const { return !(*this == rhs); }

 private:
  std::string id_;
  mutable size_t hash_ = 0;
};

}  // namespace ray
