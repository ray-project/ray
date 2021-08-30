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

#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/id.h"
#include "ray/object_manager/plasma/common.h"

namespace plasma {

// ISpillManager spills objects to external storage asynchronously.
class ISpillManager {
 public:
  virtual ~ISpillManager() = default;

  /// Spill a list of objects in an asynchronous task.
  ///
  /// \param objects_to_spill List of objects to spill.
  /// \param task_finished_callback The callback once the async spill task finishes.
  ///   - If the spill task succeed it returns Status::OK() and a map from object_id
  ///     to the spilled location.
  ///   - If the spill task failed, it returns failure Status and a map from object_id
  ///     to empty strings.
  /// \return wether the spill task is successfully scheduled.
  ///   - It returns true if the spill task is successfuly scheduled,
  ///     where the callback is guarantee to be called.
  ///   - Otherwise it returns false and the callback will not be called.
  virtual bool SubmitSpillTask(
      std::vector<const LocalObject &> objects_to_spill,
      std::function<void(ray::Status status, absl::flat_hash_map<ObjectID, std::string>)>
          task_finished_callback) = 0;

  /// Wether we can submit another spill task.
  virtual bool CanSubmitSpillTask() const = 0;

  /// Delete a spilled object from external storage.
  virtual void DeleteSpilledObject(const ObjectID &object_id) = 0;
};

// NoOpSpillManager can't spill anything!
class NoOpSpillManager : public ISpillManager {
 public:
  bool SubmitSpillTask(
      std::vector<const LocalObject &> /* unused */,
      std::function<void(ray::Status status, absl::flat_hash_map<ObjectID, std::string>)>
      /* unused */) override {
    return false;
  }

  bool CanSubmitSpillTask() const override { return false; }

  void DeleteSpilledObject(const ObjectID &object_id) override {}
};
};  // namespace plasma
