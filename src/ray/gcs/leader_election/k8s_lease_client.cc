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

#include "ray/gcs/leader_election/k8s_lease_client.h"

#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

K8sLeaseClient::K8sLeaseClient(
    std::string lease_namespace,
    std::string lease_key,
    std::function<Status(const std::string &, nlohmann::json &)> get_api,
    std::function<Status(const std::string &, const nlohmann::json &, nlohmann::json &)>
        post_api,
    std::function<Status(const std::string &, const nlohmann::json &, nlohmann::json &)>
        put_api)
    : lease_namespace_(std::move(lease_namespace)),
      lease_key_(std::move(lease_key)),
      get_api_(std::move(get_api)),
      post_api_(std::move(post_api)),
      put_api_(std::move(put_api)) {}

StatusOr<LeaseMetadata> K8sLeaseClient::GetLeaseMetadata() {
  std::string get_path = "/apis/coordination.k8s.io/v1/namespaces/" + lease_namespace_ +
                         "/leases/" + lease_key_;
  nlohmann::json response;
  Status status = get_api_(get_path, response);
  if (!status.ok()) {
    return status;
  }

  LeaseMetadata metadata;
  metadata.exists = true;
  metadata.lease_record = response;

  absl::Time server_now = absl::Now();
  if (response.contains("__api_server_date__")) {
    std::string date_str = response["__api_server_date__"].get<std::string>();
    std::string err;
    if (!absl::ParseTime("%a, %d %b %Y %H:%M:%S %Z", date_str, &server_now, &err)) {
      RAY_LOG(WARNING) << "Failed to parse API server date: " << date_str
                       << ", error: " << err << ". Falling back to local pod timestamp.";
    }
  }
  metadata.server_now = server_now;

  if (response.contains("spec")) {
    auto &spec = response["spec"];
    if (spec.contains("holderIdentity")) {
      metadata.holder_id = spec["holderIdentity"].get<std::string>();
    }
    if (spec.contains("leaseDurationSeconds")) {
      metadata.duration_seconds = spec["leaseDurationSeconds"].get<int>();
    }
    if (spec.contains("renewTime")) {
      std::string renew_str = spec["renewTime"].get<std::string>();
      std::string parse_err;
      if (!absl::ParseTime(
              absl::RFC3339_full, renew_str, &metadata.renew_time, &parse_err)) {
        metadata.renew_time = server_now;
      }
    }
  }

  if (response.contains("metadata") && response["metadata"].contains("resourceVersion")) {
    metadata.resource_version =
        response["metadata"]["resourceVersion"].get<std::string>();
  }

  return metadata;
}

Status K8sLeaseClient::CreateLease(const std::string &holder_id,
                                   int ttl_seconds,
                                   absl::Time now) {
  std::string now_str =
      absl::FormatTime("%Y-%m-%dT%H:%M:%E6SZ", now, absl::UTCTimeZone());
  nlohmann::json create_req = {
      {"apiVersion", "coordination.k8s.io/v1"},
      {"kind", "Lease"},
      {"metadata", {{"name", lease_key_}, {"namespace", lease_namespace_}}},
      {"spec",
       {{"holderIdentity", holder_id},
        {"leaseDurationSeconds", ttl_seconds},
        {"renewTime", now_str}}}};

  std::string post_path =
      "/apis/coordination.k8s.io/v1/namespaces/" + lease_namespace_ + "/leases";
  nlohmann::json create_resp;
  Status status = post_api_(post_path, create_req, create_resp);
  if (status.ok()) {
    cached_lease_record_ = create_resp;
  }
  return status;
}

Status K8sLeaseClient::UpdateLease(const LeaseMetadata &metadata,
                                   const std::string &holder_id,
                                   int ttl_seconds,
                                   absl::Time now) {
  std::string now_str =
      absl::FormatTime("%Y-%m-%dT%H:%M:%E6SZ", now, absl::UTCTimeZone());
  nlohmann::json update_req = metadata.lease_record;
  update_req.erase("__api_server_date__");
  update_req["spec"]["holderIdentity"] = holder_id;
  update_req["spec"]["leaseDurationSeconds"] = ttl_seconds;
  update_req["spec"]["renewTime"] = now_str;

  if (!metadata.resource_version.empty()) {
    update_req["metadata"]["resourceVersion"] = metadata.resource_version;
  }

  std::string put_path = "/apis/coordination.k8s.io/v1/namespaces/" + lease_namespace_ +
                         "/leases/" + lease_key_;
  nlohmann::json update_resp;
  Status status = put_api_(put_path, update_req, update_resp);
  if (status.ok()) {
    cached_lease_record_ = update_resp;
  }
  return status;
}

bool K8sLeaseClient::CanAcquireLease(const LeaseMetadata &metadata,
                                     const std::string &holder_id,
                                     absl::Time now) {
  // Scenario A: No one currently holds the lease (it was voluntarily released).
  if (metadata.holder_id.empty()) {
    return true;
  }

  // Scenario B: We currently hold the lease (performing a lease renewal).
  if (metadata.holder_id == holder_id) {
    return true;
  }

  // Scenario C: Another candidate holds the lease. We can only preempt if it has expired.
  absl::Time expiration_time =
      metadata.renew_time + absl::Seconds(metadata.duration_seconds);
  if (now > expiration_time) {
    return true;
  }

  return false;
}

Status K8sLeaseClient::TryAcquire(const std::string &holder_id,
                                  int ttl_seconds,
                                  std::string &current_leader) {
  auto metadata_or = GetLeaseMetadata();
  if (metadata_or.status().IsNotFound()) {
    Status create_status = CreateLease(holder_id, ttl_seconds, absl::Now());
    if (create_status.ok()) {
      RAY_LOG(INFO) << "Successfully created Lease and acquired leadership.";
      current_leader = holder_id;
      return Status::OK();
    }
    current_leader = "";
    return create_status;
  }

  if (!metadata_or.ok()) {
    current_leader = "";
    return metadata_or.status();
  }

  const auto &metadata = metadata_or.value();
  current_leader = metadata.holder_id;

  if (CanAcquireLease(metadata, holder_id, metadata.server_now)) {
    Status update_status =
        UpdateLease(metadata, holder_id, ttl_seconds, metadata.server_now);
    if (update_status.ok()) {
      current_leader = holder_id;
      return Status::OK();
    }
    return update_status;
  }

  return Status::OK();
}

Status K8sLeaseClient::Renew(const std::string &holder_id,
                             int ttl_seconds,
                             std::string &current_leader) {
  if (!cached_lease_record_.empty()) {
    absl::Time now = absl::Now();
    std::string now_str =
        absl::FormatTime("%Y-%m-%dT%H:%M:%E6SZ", now, absl::UTCTimeZone());
    nlohmann::json update_req = cached_lease_record_;
    update_req.erase("__api_server_date__");
    update_req["spec"]["holderIdentity"] = holder_id;
    update_req["spec"]["leaseDurationSeconds"] = ttl_seconds;
    update_req["spec"]["renewTime"] = now_str;

    std::string put_path = "/apis/coordination.k8s.io/v1/namespaces/" + lease_namespace_ +
                           "/leases/" + lease_key_;
    nlohmann::json response;
    Status update_status = put_api_(put_path, update_req, response);
    if (update_status.ok()) {
      cached_lease_record_ = response;
      current_leader = holder_id;
      return Status::OK();
    }
    // If direct PUT fails (due to resourceVersion mismatch/conflict, network error,
    // or request timeout), invalidate the cache and fall back to the slow-path
    // TryAcquire() (which performs a GET to refresh the resource version).
    cached_lease_record_ = nlohmann::json();
  }

  return TryAcquire(holder_id, ttl_seconds, current_leader);
}

void K8sLeaseClient::Release(const std::string &holder_id) {
  std::string get_path = "/apis/coordination.k8s.io/v1/namespaces/" + lease_namespace_ +
                         "/leases/" + lease_key_;
  nlohmann::json response;
  if (!get_api_(get_path, response).ok()) {
    return;
  }

  std::string current_holder = "";
  if (response.contains("spec") && response["spec"].contains("holderIdentity")) {
    current_holder = response["spec"]["holderIdentity"].get<std::string>();
  }

  if (current_holder == holder_id) {
    nlohmann::json update_req = response;
    update_req.erase("__api_server_date__");
    update_req["spec"]["holderIdentity"] = "";
    update_req["spec"]["renewTime"] = "1970-01-01T00:00:00Z";

    std::string resource_version = "";
    if (response.contains("metadata") &&
        response["metadata"].contains("resourceVersion")) {
      resource_version = response["metadata"]["resourceVersion"].get<std::string>();
    }
    if (!resource_version.empty()) {
      update_req["metadata"]["resourceVersion"] = resource_version;
    }

    nlohmann::json update_resp;
    put_api_(get_path, update_req, update_resp);
  }
}

}  // namespace gcs
}  // namespace ray
