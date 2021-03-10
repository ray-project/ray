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

#include "ray/common/id.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

/// The class that manages runtime env. The lifetime of runtime env is managed
/// here. There are two places where runtime env need be managed
///    1) central storage, like GCS or global KV storage
///    2) local node, where runtime env is fetched
/// We only track the job and detached actor for runtime env. In summary,
/// runtime env will be cleaned up when there is no job or detached actor is
/// using it. The resouce is tracked in uri level.
class RuntimeEnvManagerBase {
 public:
  RuntimeEnvManagerBase() {}

  /// Increase the reference of uri by job_id and runtime_env.
  ///
  /// \param[in] hex_id The id of the runtime env. It can be an actor or job id.
  /// \param[in] runtime_env The runtime env used by the id.
  void AddUriReference(const std::string &hex_id, const rpc::RuntimeEnv &runtime_env);

  /// Decrease the reference of uri by job_id
  /// \param[in] hex_id The id of the runtime env.
  void RemoveUriReference(const std::string &hex_id);

  virtual ~RuntimeEnvManagerBase() {}

 protected:
  /// The handler of deleting a uri.
  ///
  /// \param[in] uri The uri to be deleted.
  virtual void DeleteURI(const std::string &uri) = 0;

 private:
  /// Reference counting of a uri.
  std::unordered_map<std::string, int64_t> uri_reference_;
  /// A map between hex_id and uri.
  std::unordered_map<std::string, std::vector<std::string>> id_to_uris_;
};
}  // namespace ray
