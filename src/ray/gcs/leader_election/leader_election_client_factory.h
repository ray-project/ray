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
  /// Creates a lease client for `lease_key`. An empty `lease_namespace` falls back to
  /// $POD_NAMESPACE, then the pod's service account namespace file. Crashes if either
  /// the key or the namespace can't be determined.
  static std::unique_ptr<LeaderLeaseClientInterface> Create(
      const std::string &lease_namespace, const std::string &lease_key);

  /// The namespace Create() uses for `lease_namespace`, or empty if none is found.
  static std::string ResolveLeaseNamespace(const std::string &lease_namespace);
};

}  // namespace gcs
}  // namespace ray
