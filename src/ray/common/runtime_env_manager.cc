// Copyright 2021 The Ray Authors.
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

#include "ray/common/runtime_env_manager.h"
#include "ray/util/logging.h"
namespace ray {

void RuntimeEnvManager::AddURIReference(const std::string &hex_id,
                                        const rpc::RuntimeEnv &runtime_env) {
  const auto &uris = runtime_env.uris();
  for (const auto &uri : uris) {
    AddURIReference(hex_id, uri);
  }
}

void RuntimeEnvManager::AddURIReference(const std::string &hex_id,
                                        const std::string &uri) {
  if (unused_uris_.count(uri)) {
    unused_uris_.erase(uri);
  }
  uri_reference_[uri]++;
  id_to_uris_[hex_id].push_back(uri);
}

const std::vector<std::string> &RuntimeEnvManager::GetReferences(
    const std::string &hex_id) const {
  static const std::vector<std::string> _default;
  auto it = id_to_uris_.find(hex_id);
  return it == id_to_uris_.end() ? _default : it->second;
}

void RuntimeEnvManager::RemoveURIReference(const std::string &hex_id) {
  if (!id_to_uris_.count(hex_id)) {
    return;
  }

  for (const auto &uri : id_to_uris_[hex_id]) {
    --uri_reference_[uri];
    auto ref_count = uri_reference_[uri];
    RAY_CHECK(ref_count >= 0);
    if (ref_count == 0) {
      uri_reference_.erase(uri);
      RAY_LOG(DEBUG) << "Deleting uri: " << uri;
      deleter_(uri, [this, uri](bool success) {
        if (!success) {
          unused_uris_.insert(uri);
        }
      });
    }
  }
  id_to_uris_.erase(hex_id);
}

}  // namespace ray
