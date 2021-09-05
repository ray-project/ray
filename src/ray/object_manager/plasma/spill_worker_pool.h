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

#include <functional>
#include <string>
#include <vector>

#include "ray/common/id.h"
#include "src/ray/protobuf/common.pb.h"

namespace plasma {
class ISpillWorkerPool {
 public:
  virtual ~ISpillWorkerPool() = default;

  virtual void StartSpillWorker(
      std::vector<ObjectID> objects_to_spill,
      std::vector<ray::rpc::Address> object_owners,
      std::function<void(ray::Status, std::vector<std::string>)> spill_callback) = 0;

  virtual void StartDeleteWorker(std::vector<std::string> files_to_delete,
                                 std::function<void(ray::Status)> delete_callback) = 0;
};
}  // namespace plasma