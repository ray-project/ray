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

#include <gtest/gtest_prod.h>

#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/util/logging.h"

/// Limit the ID range to test for collisions.
#define MAX_ID_TEST 8

namespace ray {
const std::string kCPU_ResourceLabel = "CPU";
const std::string kGPU_ResourceLabel = "GPU";
const std::string kObjectStoreMemory_ResourceLabel = "object_store_memory";
const std::string kMemory_ResourceLabel = "memory";
const std::string kBundle_ResourceLabel = "bundle";

/// List of predefined resources.
enum PredefinedResources { CPU, MEM, GPU, OBJECT_STORE_MEM, PredefinedResources_MAX };

/// Class to map string IDs to unique integer IDs and back.
class ResourceIdMap {
 public:
  static ResourceIdMap &GetResourceIdMap();

  /// Get integer ID associated with an existing string ID.
  ///
  /// \param String ID.
  /// \return The integer ID associated with the given string ID.
  int64_t Get(const std::string &string_id) const LOCKS_EXCLUDED(mutex_);

  /// Get string ID associated with an existing integer ID.
  ///
  /// \param Integre ID.
  /// \return The string ID associated with the given integer ID.
  std::string Get(uint64_t id) const LOCKS_EXCLUDED(mutex_);

  /// Insert a string ID and get the associated integer ID.
  ///
  /// \param String ID to be inserted.
  /// \param max_id The number of unique possible ids. This is used
  ///               to force collisions for testing. If -1, it is not used.
  /// \return The integer ID associated with string ID string_id.
  int64_t Insert(const std::string &string_id, uint8_t num_ids = 0)
      LOCKS_EXCLUDED(mutex_);

  /// Insert string ID and its integer ID in the map.
  /// It will crash the process if either string_id or id exists.
  ResourceIdMap &InsertOrDie(const std::string &string_id, int64_t id)
      LOCKS_EXCLUDED(mutex_);

  /// Removing an ID is unsupported, because it is prone to erroneously
  /// deleting an ID still in use.

  /// Get number of identifiers.
  int64_t Count() const LOCKS_EXCLUDED(mutex_);

 private:
  ResourceIdMap(){};

  absl::flat_hash_map<std::string, int64_t> string_to_int_ GUARDED_BY(mutex_);
  absl::flat_hash_map<int64_t, std::string> int_to_string_ GUARDED_BY(mutex_);
  std::hash<std::string> hasher_;
  mutable absl::Mutex mutex_;

  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingIdTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, SchedulingIdInsertOrDieTest);
  FRIEND_TEST(ClusterResourceSchedulerTest, CustomResourceInstanceTest);
};

}  // namespace ray
