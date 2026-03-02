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
#include "ray/common/ray_object.h"
#include "ray/common/status.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

namespace raylet {

/// Interface for object spilling operations. Replaces the IOWorkerPoolInterface
/// for spill/restore/delete with a simpler task-submission model.
class ObjectSpillerInterface {
 public:
  virtual ~ObjectSpillerInterface() = default;

  /// Spill a batch of fused objects. Returns URLs via callback.
  ///
  /// \param object_ids The IDs of the objects to spill.
  /// \param objects Pointers to the objects to spill.
  /// \param owner_addresses The owner addresses for each object.
  /// \param callback Callback invoked with status and spilled object URLs.
  virtual void SpillObjects(
      const std::vector<ObjectID> &object_ids,
      const std::vector<const RayObject *> &objects,
      const std::vector<rpc::Address> &owner_addresses,
      std::function<void(const Status &, std::vector<std::string> urls)> callback) = 0;

  /// Restore a spilled object from external storage.
  ///
  /// \param object_id The ID of the object to restore.
  /// \param object_url The URL where the object is spilled.
  /// \param callback Callback invoked with status and bytes restored.
  virtual void RestoreSpilledObject(
      const ObjectID &object_id,
      const std::string &object_url,
      std::function<void(const Status &, int64_t bytes_restored)> callback) = 0;

  /// Delete spilled object files.
  ///
  /// \param urls The URLs of the spilled objects to delete.
  /// \param callback Callback invoked with status after deletion.
  virtual void DeleteSpilledObjects(const std::vector<std::string> &urls,
                                    std::function<void(const Status &)> callback) = 0;
};

}  // namespace raylet

}  // namespace ray
