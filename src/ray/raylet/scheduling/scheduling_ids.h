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

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

/// Limit the ID range to test for collisions.
#define MAX_ID_TEST 8

namespace ray {

/// List of predefined resources.
enum PredefinedResources { CPU, MEM, GPU, OBJECT_STORE_MEM, PredefinedResources_MAX };

const std::string kCPU_ResourceLabel = "CPU";
const std::string kGPU_ResourceLabel = "GPU";
const std::string kObjectStoreMemory_ResourceLabel = "object_store_memory";
const std::string kMemory_ResourceLabel = "memory";
const std::string kBundle_ResourceLabel = "bundle";

/// Class to map string IDs to unique integer IDs and back.
class StringIdMap {
  absl::flat_hash_map<std::string, int64_t> string_to_int_;
  absl::flat_hash_map<int64_t, std::string> int_to_string_;
  std::hash<std::string> hasher_;
  mutable absl::Mutex mutex_;

 public:
  StringIdMap(){};
  ~StringIdMap(){};

  /// Get integer ID associated with an existing string ID.
  ///
  /// \param String ID.
  /// \return The integer ID associated with the given string ID.
  int64_t Get(const std::string &string_id) const;

  /// Get string ID associated with an existing integer ID.
  ///
  /// \param Integre ID.
  /// \return The string ID associated with the given integer ID.
  std::string Get(uint64_t id) const;

  /// Insert a string ID and get the associated integer ID.
  ///
  /// \param String ID to be inserted.
  /// \param max_id The number of unique possible ids. This is used
  ///               to force collisions for testing. If -1, it is not used.
  /// \return The integer ID associated with string ID string_id.
  int64_t Insert(const std::string &string_id, uint8_t num_ids = 0);

  /// Insert string ID and its integer ID in the map.
  /// It will crash the process if either string_id or id exists.
  StringIdMap &InsertOrDie(const std::string &string_id, int64_t id);

  /// Removing an ID is unsupported, because it is prone to erroneously
  /// deleting an ID still in use.

  /// Get number of identifiers.
  int64_t Count();
};

enum class SchedulingIDTag { Node, Resource };

/// Represent a string scheduling id. It optimizes the storage by
/// using a singleton StringIdMap, and only store the integer index as
/// its only member.
///
/// Note: this class is not thread safe!
template <SchedulingIDTag T>
class BaseSchedulingID {
 public:
  explicit BaseSchedulingID() = default;

  explicit BaseSchedulingID(const std::string &name) : id_{GetMap().Insert(name)} {}

  explicit BaseSchedulingID(int64_t id) : id_{id} {}

  int64_t ToInt() const { return id_; }

  std::string Binary() const { return GetMap().Get(id_); }

  bool operator==(const BaseSchedulingID &rhs) const { return id_ == rhs.id_; }

  bool operator!=(const BaseSchedulingID &rhs) const { return id_ != rhs.id_; }

  bool operator<(const BaseSchedulingID &rhs) const { return id_ < rhs.id_; }

  bool IsNil() const { return id_ == -1; }

  static BaseSchedulingID Nil() { return BaseSchedulingID(-1); }

 private:
  /// Meyer's singleton to store the StringIdMap.
  static StringIdMap &GetMap() {
    static StringIdMap map;
    return map;
  }
  int64_t id_ = -1;
};

template <ray::SchedulingIDTag T>
std::ostream &operator<<(std::ostream &os, const ray::BaseSchedulingID<T> &id) {
  os << id.ToInt();
  return os;
}

template <>
inline std::ostream &operator<<(
    std::ostream &os, const ray::BaseSchedulingID<SchedulingIDTag::Resource> &id) {
  os << id.Binary();
  return os;
}

/// Specialization for SchedulingIDTag. Specifically, we populate
/// the singleton map with PredefinedResources.
template <>
inline StringIdMap &BaseSchedulingID<SchedulingIDTag::Resource>::GetMap() {
  static std::unique_ptr<StringIdMap> map{[]() {
    std::unique_ptr<StringIdMap> map(new StringIdMap());
    map->InsertOrDie(kCPU_ResourceLabel, CPU)
        .InsertOrDie(kGPU_ResourceLabel, GPU)
        .InsertOrDie(kObjectStoreMemory_ResourceLabel, OBJECT_STORE_MEM)
        .InsertOrDie(kMemory_ResourceLabel, MEM);
    return map;
  }()};
  return *map;
}

namespace scheduling {
/// The actual scheduling id definitions which are used in scheduler.
using ResourceID = BaseSchedulingID<SchedulingIDTag::Resource>;
using NodeID = BaseSchedulingID<SchedulingIDTag::Node>;

const ResourceID kCPUResource{CPU};
const ResourceID kGPUResource{GPU};
}  // namespace scheduling
}  // namespace ray

/// implements hash function for BaseSchedulingID<T>
namespace std {
template <ray::SchedulingIDTag T>
struct hash<ray::BaseSchedulingID<T>> {
  std::size_t operator()(const ray::BaseSchedulingID<T> &id) const {
    return std::hash<int64_t>()(id.ToInt());
  }
};
}  // namespace std
