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
  auto id = Get(string_id);
  if (id != -1) {
    return id;
  }

  id = hasher_(string_id);
  if (max_id != 0) {
    id = id % MAX_ID_TEST;
  }
  for (size_t i = 0; true; i++) {
    bool hash_collision = false;
    {
      absl::ReaderMutexLock lock(&mutex_);
      hash_collision = int_to_string_.contains(id);
    }

    if (!hash_collision) {
      absl::WriterMutexLock lock(&mutex_);
      auto pair = string_to_int_.emplace(string_id, id);
      if (pair.second) {
        /// No hash collision, so associate string_id with id.
        RAY_CHECK(int_to_string_.emplace(id, string_id).second);
      } else {
        // The `string_id` is inserted in another thread, just return the one that
        // exists.
        id = pair.first->second;
      }
      break;
    }

    id = hasher_(string_id + std::to_string(i));
    if (max_id != 0) {
      id = id % max_id;
    }
  }
  return id;
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

}  // namespace ray
