// Copyright 2026 The Ray Authors.
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

#include "ray/gcs/leader_election/leader_election_client_factory.h"

#include <mutex>

#include "ray/gcs/leader_election/k8s_lease_client.h"
#include "ray/rpc/authentication/k8s_util.h"

namespace ray {
namespace gcs {

std::unique_ptr<LeaderLeaseClientInterface> LeaderLeaseClientFactory::Create(
    const std::string &lease_namespace, const std::string &lease_key) {
  // Ensure the in-cluster Kubernetes client configuration is initialized before any
  // K8sApiGet/Post/Put call. Otherwise those helpers return Status::Invalid until some
  // other subsystem (currently only the auth-token validator) happens to initialize it
  // first. Using the same shared std::once_flag makes this idempotent and independent of
  // call order.
  std::call_once(rpc::k8s::k8s_client_config_flag, rpc::k8s::InitK8sClientConfig);

  return std::make_unique<K8sLeaseClient>(
      lease_namespace,
      lease_key,
      [](const std::string &path, nlohmann::json &resp) {
        return rpc::k8s::K8sApiGet(path, resp);
      },
      [](const std::string &path, const nlohmann::json &body, nlohmann::json &resp) {
        return rpc::k8s::K8sApiPost(path, body, resp);
      },
      [](const std::string &path, const nlohmann::json &body, nlohmann::json &resp) {
        return rpc::k8s::K8sApiPut(path, body, resp);
      });
}

}  // namespace gcs
}  // namespace ray
