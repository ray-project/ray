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

#pragma once

#include <memory>
#include <string>

#include "ray/gcs/leader_election/leader_election_client_interface.h"

namespace ray {
namespace gcs {

class LeaderLeaseClientFactory {
 public:
  /**
   * @brief Creates a client for the Kubernetes Lease the GCS heads compete for.
   *
   * Crashes if the lease name or its namespace can't be determined.
   *
   * @param lease_namespace Namespace of the Lease. Resolved with
   *        ResolveLeaseNamespace().
   * @param lease_key Name of the Lease. Surrounding whitespace is ignored.
   * @return The lease client.
   */
  static std::unique_ptr<LeaderLeaseClientInterface> Create(
      const std::string &lease_namespace, const std::string &lease_key);

  /**
   * @brief Resolves the namespace Create() uses for the Lease.
   *
   * Falls back from `lease_namespace` to $POD_NAMESPACE, then to the pod's service
   * account namespace file. Surrounding whitespace is ignored, and an empty value
   * falls through to the next source.
   *
   * @param lease_namespace The configured namespace. May be empty.
   * @return The resolved namespace, or an empty string if none is found.
   */
  static std::string ResolveLeaseNamespace(const std::string &lease_namespace);
};

}  // namespace gcs
}  // namespace ray
