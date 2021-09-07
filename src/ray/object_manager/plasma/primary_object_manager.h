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

#include "ray/object_manager/plasma/object_lifecycle_manager.h"
#include "ray/object_manager/plasma/object_spill_manager.h"

namespace plasma {

enum class SpillState : int8_t {
  NOT_SPILLED = 0,
  SPILLING = 1,
  SPILLED = 2,
};

struct PrimaryObjectState {
  PrimaryObjectState(const ray::rpc::Address &address)
      : spill_state(SpillState::NOT_SPILLED), owner_address(address) {}
  SpillState spill_state;
  ray::rpc::Address owner_address;
};

// PrimaryObjectManager manages primary objects which can't be evicted
// unless being explicitly deleted or spilled.
// Under the hood, it grabs an additional reference to the object
// to prevent it from being evicted.
class PrimaryObjectManager {
 public:
  PrimaryObjectManager(IObjectLifecycleManager &object_lifecyle_manager,
                       std::unique_ptr<ObjectSpillManager> spill_manager,
                       int64_t min_spill_size);

  bool AddPrimaryObject(const ObjectID &id, const ray::rpc::Address &owner_address);
  void DeletePrimaryObject(const ObjectID &id);

  absl::optional<PrimaryObjectState> GetPrimaryObjectState(const ObjectID &id);
  absl::optional<std::string> GetSpilledURL(const ObjectID &id) const;

  bool SpillObjectUptoMaxThroughput();

 private:
  std::pair<std::vector<ObjectID>, std::vector<ray::rpc::Address>> FindObjectsToSpill();

  void OnSpillTaskFinished(std::vector<ObjectID> objects_to_spill,
                           std::vector<ObjectID> spilled_objects);

 private:
  IObjectLifecycleManager &object_lifecyle_manager_;
  std::unique_ptr<ObjectSpillManager> spill_manager_;
  int64_t kMinSpillSize;

  absl::flat_hash_map<ObjectID, PrimaryObjectState> primary_objects_;
};

}  // namespace plasma