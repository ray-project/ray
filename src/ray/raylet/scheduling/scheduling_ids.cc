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

int64_t StringIdMap::Get(const std::string &string_id) {
  auto it = string_to_int_.find(string_id);
  if (it == string_to_int_.end()) {
    return -1;
  } else {
    return it->second;
  }
};

std::string StringIdMap::Get(uint64_t id) {
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

void StringIdMap::Remove(const std::string &string_id) {
  auto sit = string_to_int_.find(string_id);
  if (sit != string_to_int_.end()) {
    int_to_string_.erase(string_to_int_[string_id]);
    string_to_int_.erase(sit);
  }
};

void StringIdMap::Remove(int64_t id) {
  auto it = int_to_string_.find(id);
  if (it != int_to_string_.end()) {
    string_to_int_.erase(int_to_string_[id]);
    int_to_string_.erase(it);
  }
};

int64_t StringIdMap::Count() { return string_to_int_.size(); }
