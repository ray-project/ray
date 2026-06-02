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

#include <functional>
#include <memory>
#include <string>

#include "absl/time/time.h"
#include "nlohmann/json.hpp"
#include "ray/common/status_or.h"
#include "ray/gcs/leader_election/leader_election_client_interface.h"

namespace ray {
namespace gcs {

/// Represents a type-safe, parsed view of the Kubernetes Lease object retrieved from the
/// API Server.
struct LeaseMetadata {
  // True if the Lease object exists in the specified namespace in Kubernetes.
  bool exists = false;
  // The identity (pod hostname) of the GCS instance currently holding the lease.
  std::string holder_id;
  // The lease lock TTL duration in seconds, default to 15 seconds.
  int duration_seconds = 15;
  // The timestamp of the last successful lease renewal.
  absl::Time renew_time = absl::UnixEpoch();
  // The Optimistic Concurrency Control (OCC) version token required by K8s for safe
  // updates.
  std::string resource_version;
  // The raw JSON body record of the Lease object returned from the API Server.
  nlohmann::json lease_record;
  // The centralized API Server time ground truth parsed from the response Date header.
  absl::Time server_now = absl::UnixEpoch();
};

/// Concrete implementation of the LeaderLeaseClientInterface using Kubernetes Leases.
class K8sLeaseClient : public LeaderLeaseClientInterface {
 public:
  K8sLeaseClient(
      std::string lease_namespace,
      std::string lease_key,
      std::function<Status(const std::string &, nlohmann::json &)> get_api,
      std::function<Status(const std::string &, const nlohmann::json &, nlohmann::json &)>
          post_api,
      std::function<Status(const std::string &, const nlohmann::json &, nlohmann::json &)>
          put_api);

  /// Attempt to acquire leadership by taking ownership of the Kubernetes Lease.
  /// It first gets the current Lease state. If it does not exist, it attempts to create
  /// it. If it exists but has no holder or is expired, it updates the lease record.
  /// \return Status::OK() if the API call succeeded (check current_leader == holder_id
  /// for ownership).
  Status TryAcquire(const std::string &holder_id,
                    int ttl_seconds,
                    std::string &current_leader) override;

  /// Attempt to renew leadership of an already acquired lease.
  /// This optimizes renewal by using a cached resourceVersion to run a Direct-PUT
  /// fast-path. If the direct PUT fails due to network error, timeout, or version
  /// conflict, it invalidates the cached version and falls back to TryAcquire().
  /// \return Status::OK() if leadership renewal was successful.
  Status Renew(const std::string &holder_id,
               int ttl_seconds,
               std::string &current_leader) override;

  /// Voluntarily release the lease by clearing holderIdentity and setting renewTime to
  /// epoch. Called during graceful GCS shutdown to allow standby promotion in sub-2
  /// seconds.
  void Release(const std::string &holder_id) override;

 private:
  StatusOr<LeaseMetadata> GetLeaseMetadata();

  Status CreateLease(const std::string &holder_id, int ttl_seconds, absl::Time now);

  Status UpdateLease(const LeaseMetadata &metadata,
                     const std::string &holder_id,
                     int ttl_seconds,
                     absl::Time now);

  bool CanAcquireLease(const LeaseMetadata &metadata,
                       const std::string &holder_id,
                       absl::Time now);

  std::string lease_namespace_;
  std::string lease_key_;
  std::function<Status(const std::string &, nlohmann::json &)> get_api_;
  std::function<Status(const std::string &, const nlohmann::json &, nlohmann::json &)>
      post_api_;
  std::function<Status(const std::string &, const nlohmann::json &, nlohmann::json &)>
      put_api_;
  // The cached full JSON record of the Lease object, used to execute the Direct-PUT
  // fast-path optimization. When holding leadership, the client uses this cached record
  // to perform renewals in exactly one HTTP PUT call (skipping the initial GET query)
  // while preserving all metadata (labels, annotations, owner references).
  // This cache is cleared on any API errors (such as conflicts or timeouts) to force
  // a fallback to the standard GET-then-PUT flow.
  nlohmann::json cached_lease_record_;
};

}  // namespace gcs
}  // namespace ray
