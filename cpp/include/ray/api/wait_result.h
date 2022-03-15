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

#include <ray/api/object_ref.h>

#include <list>

namespace ray {

/// \param T The type of object.
template <typename T>
class WaitResult {
 public:
  /// The object id list of ready objects
  std::list<ObjectRef<T>> ready;
  /// The object id list of unready objects
  std::list<ObjectRef<T>> unready;
  WaitResult(){};
  WaitResult(std::list<ObjectRef<T>> &&ready_objects,
             std::list<ObjectRef<T>> &&unready_objects)
      : ready(ready_objects), unready(unready_objects){};
};

}  // namespace ray