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
#include "gtest/gtest.h"

namespace ray {
namespace gcs {

class K8sLeaseClientTest : public ::testing::Test {
 protected:
  std::string lease_namespace_ = "ray-cluster";
  std::string lease_key_ = "gcs-lease";
  std::string my_id_ = "node-1";
};

TEST_F(K8sLeaseClientTest, InitialAcquireSuccess) {
  auto get_api = [](const std::string &, nlohmann::json &) {
    return Status::NotFound("Not found");  // Lease does not exist initially
  };

  auto post_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::OK();  // Creation succeeds
  };

  auto put_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::IOError("Failure");
  };

  K8sLeaseClient client(lease_namespace_, lease_key_, get_api, post_api, put_api);
  std::string leader = "";
  EXPECT_TRUE(client.TryAcquire(my_id_, 10000, leader).ok());
  EXPECT_EQ(leader, my_id_);
}

TEST_F(K8sLeaseClientTest, AcquireFailsWhenHeldActive) {
  auto get_api = [&](const std::string &, nlohmann::json &resp) {
    // Held by another ID, not expired
    resp["spec"]["holderIdentity"] = "node-2";
    resp["spec"]["leaseDurationSeconds"] = 10;
    absl::Time future = absl::Now() + absl::Seconds(100);
    resp["spec"]["renewTime"] =
        absl::FormatTime("%Y-%m-%dT%H:%M:%E6SZ", future, absl::UTCTimeZone());
    return Status::OK();
  };

  auto post_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::IOError("Failure");
  };

  auto put_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::IOError("Failure");
  };

  K8sLeaseClient client(lease_namespace_, lease_key_, get_api, post_api, put_api);
  std::string leader = "";
  EXPECT_TRUE(client.TryAcquire(my_id_, 10000, leader).ok());
  EXPECT_EQ(leader, "node-2");
}

TEST_F(K8sLeaseClientTest, AcquireSuccessWhenExpired) {
  auto get_api = [&](const std::string &, nlohmann::json &resp) {
    // Held by another ID, but expired
    resp["spec"]["holderIdentity"] = "node-2";
    resp["spec"]["leaseDurationSeconds"] = 1;
    absl::Time past = absl::Now() - absl::Seconds(100);
    resp["spec"]["renewTime"] =
        absl::FormatTime("%Y-%m-%dT%H:%M:%E6SZ", past, absl::UTCTimeZone());
    return Status::OK();
  };

  auto post_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::IOError("Failure");
  };

  auto put_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::OK();  // Takeover PUT succeeds
  };

  K8sLeaseClient client(lease_namespace_, lease_key_, get_api, post_api, put_api);
  std::string leader = "";
  EXPECT_TRUE(client.TryAcquire(my_id_, 10000, leader).ok());
  EXPECT_EQ(leader, my_id_);
}

TEST_F(K8sLeaseClientTest, AcquireGetErrorPropagated) {
  auto get_api = [](const std::string &, nlohmann::json &) {
    return Status::IOError("Connection refused");  // GET fails with network error
  };

  auto post_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::OK();
  };

  auto put_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::OK();
  };

  K8sLeaseClient client(lease_namespace_, lease_key_, get_api, post_api, put_api);
  std::string leader = "";
  Status status = client.TryAcquire(my_id_, 10000, leader);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsIOError());
  EXPECT_EQ(status.message(), "Connection refused");
  EXPECT_EQ(leader, "");
}

TEST_F(K8sLeaseClientTest, AcquirePostErrorPropagated) {
  auto get_api = [](const std::string &, nlohmann::json &) {
    return Status::NotFound("Missing lease object");  // GET returns NotFound
  };

  auto post_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::IOError("K8s API Limit exceeded");  // POST fails
  };

  auto put_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::OK();
  };

  K8sLeaseClient client(lease_namespace_, lease_key_, get_api, post_api, put_api);
  std::string leader = "";
  Status status = client.TryAcquire(my_id_, 10000, leader);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsIOError());
  EXPECT_EQ(status.message(), "K8s API Limit exceeded");
  EXPECT_EQ(leader, "");
}

TEST_F(K8sLeaseClientTest, RenewSuccess) {
  auto get_api = [&](const std::string &, nlohmann::json &resp) {
    // Currently held by us
    resp["spec"]["holderIdentity"] = my_id_;
    resp["spec"]["leaseDurationSeconds"] = 10;
    resp["spec"]["renewTime"] =
        absl::FormatTime("%Y-%m-%dT%H:%M:%E6SZ", absl::Now(), absl::UTCTimeZone());
    return Status::OK();
  };

  auto post_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::IOError("Failure");
  };

  auto put_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::OK();  // Renewal PUT succeeds
  };

  K8sLeaseClient client(lease_namespace_, lease_key_, get_api, post_api, put_api);
  std::string leader = "";
  EXPECT_TRUE(client.Renew(my_id_, 10000, leader).ok());
  EXPECT_EQ(leader, my_id_);
}

TEST_F(K8sLeaseClientTest, RenewFallbackPutThenGetThenPut) {
  std::atomic<int> put_calls{0};
  std::atomic<int> get_calls{0};

  auto get_api = [&](const std::string &, nlohmann::json &resp) {
    get_calls++;
    resp["spec"]["holderIdentity"] = my_id_;
    resp["spec"]["leaseDurationSeconds"] = 10;
    resp["spec"]["renewTime"] =
        absl::FormatTime("%Y-%m-%dT%H:%M:%E6SZ", absl::Now(), absl::UTCTimeZone());
    if (get_calls == 1) {
      resp["metadata"]["resourceVersion"] = "version-after-first-get";
    } else {
      resp["metadata"]["resourceVersion"] = "version-after-fallback-get";
    }
    return Status::OK();
  };

  auto post_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::IOError("Failure");
  };

  auto put_api =
      [&](const std::string &, const nlohmann::json &body, nlohmann::json &resp) {
        put_calls++;
        if (put_calls == 1) {
          // First PUT call (TryAcquire setup) succeeds
          EXPECT_EQ(body["metadata"]["resourceVersion"].get<std::string>(),
                    "version-after-first-get");
          resp["metadata"]["resourceVersion"] = "cached-version-1";
          return Status::OK();
        } else if (put_calls == 2) {
          // Second PUT call (Renew direct-PUT fast-path) fails (mismatch/conflict)
          EXPECT_EQ(body["metadata"]["resourceVersion"].get<std::string>(),
                    "cached-version-1");
          return Status::IOError("Conflict / Stale resourceVersion");
        } else {
          // Third PUT call (fallback path after GET) succeeds
          EXPECT_EQ(body["metadata"]["resourceVersion"].get<std::string>(),
                    "version-after-fallback-get");
          resp["metadata"]["resourceVersion"] = "cached-version-2";
          return Status::OK();
        }
      };

  K8sLeaseClient client(lease_namespace_, lease_key_, get_api, post_api, put_api);
  std::string leader = "";

  // 1. Initial acquire to populate the cached resource version
  EXPECT_TRUE(client.TryAcquire(my_id_, 10, leader).ok());
  EXPECT_EQ(leader, my_id_);
  EXPECT_EQ(get_calls.load(), 1);
  EXPECT_EQ(put_calls.load(), 1);

  // 2. Renewal call. This triggers:
  //    a. Direct PUT with 'cached-version-1' -> Fails.
  //    b. GET -> returns 'version-after-fallback-get'.
  //    c. PUT with 'version-after-fallback-get' -> Succeeds.
  EXPECT_TRUE(client.Renew(my_id_, 10, leader).ok());
  EXPECT_EQ(leader, my_id_);
  EXPECT_EQ(get_calls.load(), 2);
  EXPECT_EQ(put_calls.load(), 3);
}

TEST_F(K8sLeaseClientTest, ReleaseSuccess) {
  std::atomic<bool> put_called{false};
  auto get_api = [&](const std::string &, nlohmann::json &resp) {
    // Currently held by us
    resp["spec"]["holderIdentity"] = my_id_;
    resp["spec"]["leaseDurationSeconds"] = 10;
    resp["spec"]["renewTime"] =
        absl::FormatTime("%Y-%m-%dT%H:%M:%E6SZ", absl::Now(), absl::UTCTimeZone());
    resp["metadata"]["resourceVersion"] = "12345";
    return Status::OK();
  };

  auto post_api = [](const std::string &, const nlohmann::json &, nlohmann::json &) {
    return Status::IOError("Failure");
  };

  auto put_api = [&](const std::string &, const nlohmann::json &body, nlohmann::json &) {
    put_called = true;
    // Assert that holderIdentity is cleared to empty string, and resourceVersion is
    // passed correctly
    EXPECT_EQ(body["spec"]["holderIdentity"].get<std::string>(), "");
    EXPECT_EQ(body["metadata"]["resourceVersion"].get<std::string>(), "12345");
    return Status::OK();
  };

  K8sLeaseClient client(lease_namespace_, lease_key_, get_api, post_api, put_api);
  client.Release(my_id_);
  EXPECT_TRUE(put_called.load());
}
}  // namespace gcs
}  // namespace ray
