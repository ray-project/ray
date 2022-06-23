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

#include "ray/raylet/scheduling/scheduling_ids.h"

namespace ray {

int64_t StringIdMap::Get(const std::string &string_id) const {
  absl::ReaderMutexLock lock(&mutex_);
  auto it = string_to_int_.find(string_id);
  if (it == string_to_int_.end()) {
    return -1;
  } else {
    return it->second;
  }
};

std::string StringIdMap::Get(uint64_t id) const {
  absl::ReaderMutexLock lock(&mutex_);
  std::string id_string;
  auto it = int_to_string_.find(id);
  if (it == int_to_string_.end()) {
    id_string = "-1";
  } else {
    id_string = it->second;
  }
  return id_string;
};

int64_t StringIdMap::Insert(const std::string &string_id, uint8_t max_id) {
  absl::WriterMutexLock lock(&mutex_);
  auto sit = string_to_int_.find(string_id);
  if (sit == string_to_int_.end()) {
    int64_t id = hasher_(string_id);
    if (max_id != 0) {
      id = id % MAX_ID_TEST;
    }
    for (size_t i = 0; true; i++) {
      auto it = int_to_string_.find(id);
      if (it == int_to_string_.end()) {
        /// No hash collision, so associate string_id with id.
        string_to_int_.emplace(string_id, id);
        int_to_string_.emplace(id, string_id);
        break;
      }
      id = hasher_(string_id + std::to_string(i));
      if (max_id != 0) {
        id = id % max_id;
      }
    }
    return id;
  } else {
    return sit->second;
  }
};

StringIdMap &StringIdMap::InsertOrDie(const std::string &string_id, int64_t value) {
  absl::WriterMutexLock lock(&mutex_);
  RAY_CHECK(string_to_int_.emplace(string_id, value).second)
      << string_id << " or " << value << " already exist!";
  RAY_CHECK(int_to_string_.emplace(value, string_id).second)
      << string_id << " or " << value << " already exist!";
  return *this;
}

int64_t StringIdMap::Count() {
  absl::ReaderMutexLock lock(&mutex_);
  return string_to_int_.size();
}

namespace scheduling {

absl::flat_hash_set<int64_t> &ResourceID::UnitInstanceResources() {
  static absl::flat_hash_set<int64_t> set{[]() {
    absl::flat_hash_set<int64_t> res;

    std::string predefined_unit_instance_resources =
        RayConfig::instance().predefined_unit_instance_resources();
    if (!predefined_unit_instance_resources.empty()) {
      std::vector<std::string> results;
      boost::split(results, predefined_unit_instance_resources, boost::is_any_of(","));
      for (std::string &result : results) {
        int64_t resource_id = ResourceID(result).ToInt();
        RAY_CHECK(resource_id < PredefinedResourcesEnum_MAX)
            << result << " is not a valid predefined resource.";
        res.insert(resource_id);
      }
    }

    std::string custom_unit_instance_resources =
        RayConfig::instance().custom_unit_instance_resources();
    if (!custom_unit_instance_resources.empty()) {
      std::vector<std::string> results;
      boost::split(results, custom_unit_instance_resources, boost::is_any_of(","));
      for (std::string &result : results) {
        int64_t resource_id = scheduling::ResourceID(result).ToInt();
        res.insert(resource_id);
      }
    }
    return res;
  }()};
  return set;
}

}  // namespace scheduling

}  // namespace ray
