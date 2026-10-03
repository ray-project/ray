// Copyright 2025 The Ray Authors.
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

#include "ray/gcs/store_client/table_name_label.h"

#include <string_view>

#include "src/ray/protobuf/gcs.pb.h"

namespace ray::gcs {

std::string_view NormalizeTableNameLabel(std::string_view table_name) {
  rpc::TablePrefix prefix;
  if (!rpc::TablePrefix_Parse(table_name, &prefix)) {
    return kUnknownTable;
  }
  // Deliberately not `table_name`: that view would dangle once the caller's
  // string goes out of scope, and the latency label is read from a completion
  // callback. TablePrefix_Name returns a reference into the generated
  // descriptor's name storage, which lives for the whole process.
  return rpc::TablePrefix_Name(prefix);
}

}  // namespace ray::gcs
