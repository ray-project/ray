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
#include "ray/common/id.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

/// The class that manages runtime env. The lifetime of runtime env is managed
/// here. There are two places where runtime env need be managed
///    1) central storage, like GCS or global KV storage
///    2) local node, where runtime env is fetched
/// We only track the job and detached actor for runtime env. In summary,
/// runtime env will be cleaned up when there is no job or detached actor is
/// using it. The resouce is tracked in URI level. User need to provider
/// a delete handler.
class RuntimeEnvManager {
 public:
  using DeleteFunc = std::function<void(const std::string &, std::function<void(bool)>)>;
  explicit RuntimeEnvManager(DeleteFunc deleter) : deleter_(deleter) {}

  /// Increase the reference of URI by job_id and runtime_env.
  ///
  /// \param[in] hex_id The id of the runtime env. It can be an actor or job id.
  /// \param[in] runtime_env The runtime env used by the id.
  void AddURIReference(const std::string &hex_id, const rpc::RuntimeEnv &runtime_env);

  /// Increase the reference of URI by URI and runtime_env.
  ///
  /// \param[in] hex_id The id of the runtime env. It can be an actor or job id.
  /// \param[in] uri The URI referenced by the id.
  void AddURIReference(const std::string &hex_id, const std::string &uri);

  /// Get the reference of URIs by id.
  ///
  /// \param[in] hex_id The id of to look.
  /// \return The URIs referenced by the id.
  const std::vector<std::string> &GetReferences(const std::string &hex_id) const;

  /// Decrease the reference of URI by job_id
  /// \param[in] hex_id The id of the runtime env.
  void RemoveURIReference(const std::string &hex_id);

 private:
  DeleteFunc deleter_;
  /// Reference counting of a URI.
  std::unordered_map<std::string, int64_t> uri_reference_;
  /// A map between hex_id and URI.
  std::unordered_map<std::string, std::vector<std::string>> id_to_uris_;
  /// A set of unused URIs
  std::unordered_set<std::string> unused_uris_;
};
}  // namespace ray
