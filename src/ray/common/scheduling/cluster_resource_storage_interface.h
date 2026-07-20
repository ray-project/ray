// Copyright 2026 The Ray Authors.
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

#include <deque>
#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/id.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
class ClusterResourceStorageInterface {
 public:
  virtual ~ClusterResourceStorageInterface() = default;

  virtual void Put(const ray::NodeID node_id, const rpc::ResourcesData &data) = 0;
  virtual void Delete(const ray::NodeID node_id) = 0;
};
}  // namespace ray
