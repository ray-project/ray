// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "ray/common/buffer.h"
#include "ray/common/status.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/util/visibility.h"
#include "src/ray/protobuf/common.pb.h"

namespace plasma {

class ObjectStoreRunnerInterface : public std::enable_shared_from_this<ObjectStoreRunnerInterface> {
 public:
  virtual ~ObjectStoreRunnerInterface(){};

  /// Start the object store runner. The params are a series of key-value.
  virtual void Start(const std::map<std::string, std::string>& params,
                     ray::SpillObjectsCallback spill_objects_callback,
                     std::function<void()> object_store_full_callback,
                     ray::AddObjectCallback add_object_callback,
                     ray::DeleteObjectCallback delete_object_callback) = 0;

  // Stop the object store
  virtual void Stop() = 0;

  virtual bool IsObjectSpillable(const ObjectID &object_id) = 0;

  virtual int64_t GetConsumedBytes() = 0;

  virtual int64_t GetFallbackAllocated() const = 0;

  virtual void GetAvailableMemoryAsync(std::function<void(size_t)> callback) const = 0;

  // The Current total allocated available memory size
  virtual int64_t GetTotalMemorySize() const = 0;

  // Maximal available memory size. It can be different from the Total memory
  // size if the memory is dynamically expandable.
  virtual int64_t GetMaxMemorySize() const = 0;
};

}  // namespace plasma
