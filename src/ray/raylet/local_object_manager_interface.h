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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "src/ray/protobuf/node_manager.pb.h"

namespace ray {

namespace raylet {

class LocalObjectManagerInterface {
 public:
  virtual ~LocalObjectManagerInterface() = default;

  virtual void PinObjectsAndWaitForFree(const std::vector<ObjectID> &,
                                        std::vector<std::unique_ptr<RayObject>> &&,
                                        const rpc::Address &,
                                        const ObjectID & = ObjectID::Nil()) = 0;

  virtual void SpillObjectUptoMaxThroughput() = 0;

  /// TODO(dayshah): This function is only used for testing, we should remove and just
  /// keep SpillObjectsInternal.
  virtual void SpillObjects(const std::vector<ObjectID> &,
                            std::function<void(const ray::Status &)>) = 0;

  virtual void AsyncRestoreSpilledObject(const ObjectID &,
                                         int64_t,
                                         const std::string &,
                                         std::function<void(const ray::Status &)>) = 0;

  virtual void FlushFreeObjects() = 0;

  virtual bool ObjectPendingDeletion(const ObjectID &) = 0;

  virtual void ProcessSpilledObjectsDeleteQueue(uint32_t) = 0;

  virtual bool IsSpillingInProgress() = 0;

  virtual void FillObjectStoreStats(rpc::GetNodeStatsReply *) const = 0;

  virtual void RecordMetrics() const = 0;

  virtual std::string GetLocalSpilledObjectURL(const ObjectID &) = 0;

  virtual int64_t GetPrimaryBytes() const = 0;

  virtual bool HasLocallySpilledObjects() const = 0;

  virtual std::string DebugString() const = 0;
};

};  // namespace raylet

};  // namespace ray
