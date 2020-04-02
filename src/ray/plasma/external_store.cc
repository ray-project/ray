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

#include <iostream>
#include <sstream>

#include "arrow/util/memory.h"

#include "plasma/external_store.h"

namespace plasma {

Status ExternalStores::ExtractStoreName(const std::string& endpoint,
                                        std::string* store_name) {
  size_t off = endpoint.find_first_of(':');
  if (off == std::string::npos) {
    return Status::Invalid("Malformed endpoint " + endpoint);
  }
  *store_name = endpoint.substr(0, off);
  return Status::OK();
}

void ExternalStores::RegisterStore(const std::string& store_name,
                                   std::shared_ptr<ExternalStore> store) {
  Stores().insert({store_name, store});
}

void ExternalStores::DeregisterStore(const std::string& store_name) {
  auto it = Stores().find(store_name);
  if (it == Stores().end()) {
    return;
  }
  Stores().erase(it);
}

std::shared_ptr<ExternalStore> ExternalStores::GetStore(const std::string& store_name) {
  auto it = Stores().find(store_name);
  if (it == Stores().end()) {
    return nullptr;
  }
  return it->second;
}

ExternalStores::StoreMap& ExternalStores::Stores() {
  static auto* external_stores = new StoreMap();
  return *external_stores;
}

}  // namespace plasma
