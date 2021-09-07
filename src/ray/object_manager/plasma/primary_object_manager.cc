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

#include "ray/object_manager/plasma/primary_object_manager.h"

namespace plasma {

PrimaryObjectManager::PrimaryObjectManager(
    IObjectLifecycleManager &object_lifecyle_manager,
    std::unique_ptr<ObjectSpillManager> spill_manager, int64_t min_spill_size)
    : object_lifecyle_manager_(object_lifecyle_manager),
      spill_manager_(std::move(spill_manager)),
      kMinSpillSize(min_spill_size),
      primary_objects_() {}

bool PrimaryObjectManager::AddPrimaryObject(const ObjectID &id,
                                            const ray::rpc::Address &owner_address) {
  if (!object_lifecyle_manager_.GetObject(id) || primary_objects_.contains(id)) {
    return false;
  }
  primary_objects_.emplace(id, PrimaryObjectState(owner_address));
  object_lifecyle_manager_.AddReference(id);
  return true;
}

void PrimaryObjectManager::DeletePrimaryObject(const ObjectID &id) {
  auto it = primary_objects_.find(id);
  if (it == primary_objects_.end()) {
    return;
  } else if (it->second.spill_state == SpillState::NOT_SPILLED) {
    primary_objects_.erase(it);
    object_lifecyle_manager_.RemoveReference(id);
    return;
  } else if (it->second.spill_state == SpillState::SPILLED) {
    primary_objects_.erase(it);
    object_lifecyle_manager_.RemoveReference(id);
    spill_manager_->DeleteSpilledObject(id);
    return;
  } else {
    // when spill finished, we will check primary_objects and remove the reference.
    primary_objects_.erase(it);
  }
}

bool PrimaryObjectManager::SpillObjectUptoMaxThroughput() {
  while (spill_manager_->CanSubmitSpillTask()) {
    auto pair = FindObjectsToSpill();
    auto objects_to_spill = std::move(pair.first);
    auto owners = std::move(pair.second);
    if (objects_to_spill.empty()) {
      break;
    }
    RAY_CHECK(spill_manager_->SubmitSpillTask(
        objects_to_spill, std::move(owners),
        [this, objects_to_spill](ray::Status status,
                                 std::vector<ObjectID> spilled_objects) {
          // TODO: thread safety!
          OnSpillTaskFinished(std::move(objects_to_spill), std::move(spilled_objects));
        }));
  }
  return spill_manager->IsSpillingInProgress();
}

absl::optional<PrimaryObjectState> PrimaryObjectManager::GetPrimaryObjectState(
    const ObjectID &id) {
  auto it = primary_objects_.find(id);
  if (it == primary_objects_.end()) {
    return absl::nullopt;
  }
  return it->second;
}

absl::optional<std::string> PrimaryObjectManager::GetSpilledURL(
    const ObjectID &id) const {
  return spill_manager_->GetSpilledUrl(id);
}

std::pair<std::vector<ObjectID>, std::vector<ray::rpc::Address>>
PrimaryObjectManager::FindObjectsToSpill() {
  int64_t total_spill_size = 0;
  std::vector<ObjectID> to_spill;
  std::vector<ray::rpc::Address> owners;
  for (auto pair : primary_objects_) {
    if (pair.second.spill_state != SpillState::NOT_SPILLED) {
      continue;
    }
    auto object_id = pair.first;
    auto entry = object_lifecyle_manager_.GetObject(object_id);
    if (!entry->Sealed() || entry->GetRefCount() != 1) {
      continue;
    }

    to_spill.push_back(object_id);
    owners.push_back(pair.second.owner_address);
    total_spill_size += entry->GetObjectSize();
    if (total_spill_size >= kMinSpillSize) {
      break;
    }
  }

  for (auto id : to_spill) {
    primary_objects_.at(id).spill_state = SpillState::SPILLING;
  }
  return std::make_pair(std::move(to_spill), std::move(owners));
}

void PrimaryObjectManager::OnSpillTaskFinished(std::vector<ObjectID> objects_to_spill,
                                               std::vector<ObjectID> spilled_objects) {
  absl::flat_hash_set<ObjectID> spilled_set(spilled_objects.begin(),
                                            spilled_objects.end());
  for (auto object_id : objects_to_spill) {
    bool spilled = spilled_set.contains(object_id);
    if (primary_objects_.contains(object_id)) {
      if (spilled) {
        primary_objects_.at(object_id).spill_state = SpillState::SPILLED;
        object_lifecyle_manager_.RemoveReference(object_id);
      } else {
        primary_objects_.at(object_id).spill_state = SpillState::NOT_SPILLED;
      }
    } else {
      if (spilled) {
        spill_manager_->DeleteSpilledObject(object_id);
      }
      object_lifecyle_manager_.RemoveReference(object_id);
    }
  }
}

}  // namespace plasma