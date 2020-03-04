// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <memory>
#include <string>

#include "arrow/util/logging.h"

#include "plasma/hash_table_store.h"

namespace plasma {

Status HashTableStore::Connect(const std::string& endpoint) { return Status::OK(); }

Status HashTableStore::Put(const std::vector<ObjectID>& ids,
                           const std::vector<std::shared_ptr<Buffer>>& data) {
  for (size_t i = 0; i < ids.size(); ++i) {
    table_[ids[i]] = data[i]->ToString();
  }
  return Status::OK();
}

Status HashTableStore::Get(const std::vector<ObjectID>& ids,
                           std::vector<std::shared_ptr<Buffer>> buffers) {
  ARROW_CHECK(ids.size() == buffers.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    bool valid;
    HashTable::iterator result;
    {
      result = table_.find(ids[i]);
      valid = result != table_.end();
    }
    if (valid) {
      ARROW_CHECK(buffers[i]->size() == static_cast<int64_t>(result->second.size()));
      std::memcpy(buffers[i]->mutable_data(), result->second.data(),
                  result->second.size());
    }
  }
  return Status::OK();
}

REGISTER_EXTERNAL_STORE("hashtable", HashTableStore);

}  // namespace plasma
