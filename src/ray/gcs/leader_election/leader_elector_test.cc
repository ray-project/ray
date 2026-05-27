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

#include "ray/gcs/leader_election/leader_elector.h"

#include <unordered_set>

#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gtest/gtest.h"

namespace ray {
namespace gcs {

class MockLeaseClient : public LeaderLeaseClientInterface {
 public:
  MockLeaseClient(
      std::function<Status(const std::string &, int, std::string &)> try_acquire,
      std::function<Status(const std::string &, int, std::string &)> renew,
      std::function<void(const std::string &)> release)
      : try_acquire_(std::move(try_acquire)),
        renew_(std::move(renew)),
        release_(std::move(release)) {}

  Status TryAcquire(const std::string &holder_id,
                    int ttl_ms,
                    std::string &current_leader) override {
    return try_acquire_(holder_id, ttl_ms, current_leader);
  }

  Status Renew(const std::string &holder_id,
               int ttl_ms,
               std::string &current_leader) override {
    return renew_(holder_id, ttl_ms, current_leader);
  }

  void Release(const std::string &holder_id) override { release_(holder_id); }

 private:
  std::function<Status(const std::string &, int, std::string &)> try_acquire_;
  std::function<Status(const std::string &, int, std::string &)> renew_;
  std::function<void(const std::string &)> release_;
};

class SharedMockLeaseClient : public LeaderLeaseClientInterface {
 public:
  void Crash(const std::string &holder_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    crashed_holders_.insert(holder_id);
  }

  Status TryAcquire(const std::string &holder_id,
                    int ttl_seconds,
                    std::string &current_leader) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (crashed_holders_.count(holder_id)) {
      return Status::IOError("Crashed node cannot communicate");
    }
    auto now = absl::Now();
    if (!lease_holder_.empty() &&
        now < lease_renew_time_ + absl::Seconds(lease_duration_seconds_)) {
      current_leader = lease_holder_;
      return Status::OK();
    }
    lease_holder_ = holder_id;
    lease_renew_time_ = now;
    lease_duration_seconds_ = ttl_seconds;
    current_leader = holder_id;
    return Status::OK();
  }

  Status Renew(const std::string &holder_id,
               int ttl_seconds,
               std::string &current_leader) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (crashed_holders_.count(holder_id)) {
      return Status::IOError("Crashed node cannot communicate");
    }
    auto now = absl::Now();
    if (lease_holder_ == holder_id) {
      lease_renew_time_ = now;
      lease_duration_seconds_ = ttl_seconds;
      current_leader = holder_id;
      return Status::OK();
    }
    current_leader = lease_holder_;
    return Status::OK();
  }

  void Release(const std::string &holder_id) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (crashed_holders_.count(holder_id)) {
      return;  // Crashed node fails to execute Release
    }
    if (lease_holder_ == holder_id) {
      lease_holder_ = "";
    }
  }

 private:
  std::mutex mutex_;
  std::string lease_holder_ = "";
  absl::Time lease_renew_time_ = absl::UnixEpoch();
  int lease_duration_seconds_ = 0;
  std::unordered_set<std::string> crashed_holders_;
};

class LeaderElectorTest : public ::testing::Test {
 protected:
  std::string holder_id_ = "node-1";
};

// Group 1: Elector Core Lifecycle & Happy Paths
TEST_F(LeaderElectorTest, SuccessfulAcquisitionAndRenewal) {
  std::atomic<int> acquire_calls{0};
  std::atomic<int> renew_calls{0};
  std::atomic<bool> started_leading{false};

  auto try_acquire = [&](const std::string &holder, int, std::string &current) {
    acquire_calls++;
    current = holder;
    return Status::OK();
  };

  auto renew = [&](const std::string &holder, int, std::string &current) {
    renew_calls++;
    current = holder;
    return Status::OK();
  };

  auto release = [](const std::string &) {};

  auto lease_client = std::make_shared<MockLeaseClient>(try_acquire, renew, release);

  LeaderElectionConfig config;
  config.lease_client = lease_client;
  config.holder_id = holder_id_;
  config.lease_duration_seconds = 3;
  config.renew_deadline_seconds = 2;
  config.retry_period_seconds = 1;
  config.on_started_leading = [&]() { started_leading = true; };

  LeaderElector elector(config);
  elector.Run();

  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  elector.Stop();

  EXPECT_TRUE(started_leading.load());
  EXPECT_GE(acquire_calls.load(), 1);
  EXPECT_GE(renew_calls.load(), 1);
}

TEST_F(LeaderElectorTest, AcquireFailureRetriesAndSucceeds) {
  std::atomic<int> acquire_calls{0};
  std::atomic<bool> started_leading{false};
  std::string lease_holder = "node-2";  // Initially held by node-2

  auto try_acquire = [&](const std::string &holder, int, std::string &current) {
    acquire_calls++;
    if (acquire_calls == 1) {
      current = "node-2";  // First attempt: held by node-2
      return Status::OK();
    } else {
      lease_holder = holder;  // Second attempt: Node A wins it
      current = holder;
      return Status::OK();
    }
  };

  auto renew = [](const std::string &holder, int, std::string &current) {
    current = holder;
    return Status::OK();
  };

  auto release = [](const std::string &) {};

  auto lease_client = std::make_shared<MockLeaseClient>(try_acquire, renew, release);

  LeaderElectionConfig config;
  config.lease_client = lease_client;
  config.holder_id = holder_id_;
  config.lease_duration_seconds = 5;
  config.renew_deadline_seconds = 3;
  config.retry_period_seconds = 1;
  config.on_started_leading = [&]() { started_leading = true; };

  LeaderElector elector(config);
  elector.Run();

  // Sleep for 1.5s (should run first acquire at T=0s which fails, retry at T=1s which
  // succeeds)
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  EXPECT_TRUE(started_leading.load());
  EXPECT_TRUE(elector.IsLeader());
  EXPECT_EQ(acquire_calls.load(), 2);

  elector.Stop();
}

TEST_F(LeaderElectorTest, AcquireFailsOnErrorButRetries) {
  std::atomic<int> acquire_calls{0};
  std::atomic<bool> started_leading{false};

  auto try_acquire = [&](const std::string &holder, int, std::string &current) {
    acquire_calls++;
    if (acquire_calls == 1) {
      return Status::IOError(
          "API server overloaded");  // First try fails with network error
    } else {
      current = holder;
      return Status::OK();  // Second try succeeds
    }
  };

  auto renew = [](const std::string &holder, int, std::string &current) {
    current = holder;
    return Status::OK();
  };

  auto release = [](const std::string &) {};

  auto lease_client = std::make_shared<MockLeaseClient>(try_acquire, renew, release);

  LeaderElectionConfig config;
  config.lease_client = lease_client;
  config.holder_id = holder_id_;
  config.lease_duration_seconds = 5;
  config.renew_deadline_seconds = 3;
  config.retry_period_seconds = 1;
  config.on_started_leading = [&]() { started_leading = true; };

  LeaderElector elector(config);
  elector.Run();

  // Sleep for 1.5s (should run first failed check at T=0s, and second successful check at
  // T=1s)
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  EXPECT_TRUE(started_leading.load());
  EXPECT_TRUE(elector.IsLeader());
  EXPECT_EQ(acquire_calls.load(), 2);

  elector.Stop();
}

TEST_F(LeaderElectorTest, DestructorCleanStopAndRelease) {
  std::atomic<int> acquire_calls{0};
  std::atomic<bool> release_called{false};

  auto try_acquire = [&](const std::string &holder, int, std::string &current) {
    acquire_calls++;
    current = holder;
    return Status::OK();
  };

  auto renew = [](const std::string &holder, int, std::string &current) {
    current = holder;
    return Status::OK();
  };

  auto release = [&](const std::string &) { release_called = true; };

  auto lease_client = std::make_shared<MockLeaseClient>(try_acquire, renew, release);

  LeaderElectionConfig config;
  config.lease_client = lease_client;
  config.holder_id = holder_id_;
  config.lease_duration_seconds = 5;
  config.renew_deadline_seconds = 3;
  config.retry_period_seconds = 1;

  {
    LeaderElector elector(config);
    elector.Run();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // Elector goes out of scope and destructor is triggered
  }

  EXPECT_TRUE(release_called.load());
}

// Group 2: Elector Step-Down & Safety Watchdog Cases
TEST_F(LeaderElectorTest, StepDownOnRenewDeadlineExceeded) {
  std::atomic<int> acquire_calls{0};
  std::atomic<bool> started_leading{false};
  std::atomic<bool> stopped_leading{false};
  std::string lease_holder = "";

  auto try_acquire = [&](const std::string &holder, int, std::string &current) {
    acquire_calls++;
    if (lease_holder.empty() || lease_holder == holder) {
      lease_holder = holder;
      current = holder;
      return Status::OK();
    }
    current = lease_holder;
    return Status::OK();
  };

  auto renew = [&](const std::string &, int, std::string &current) {
    lease_holder = "node-2";  // Fail renewal because someone else grabbed the lease
    current = "node-2";
    return Status::OK();
  };

  auto release = [](const std::string &) {};

  auto lease_client = std::make_shared<MockLeaseClient>(try_acquire, renew, release);

  LeaderElectionConfig config;
  config.lease_client = lease_client;
  config.holder_id = holder_id_;
  config.lease_duration_seconds = 2;
  config.renew_deadline_seconds = 1;
  config.retry_period_seconds = 1;
  config.on_started_leading = [&]() { started_leading = true; };
  config.on_stopped_leading = [&]() { stopped_leading = true; };

  LeaderElector elector(config);
  elector.Run();

  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  elector.Stop();

  EXPECT_TRUE(started_leading.load());
  EXPECT_TRUE(stopped_leading.load());
  EXPECT_FALSE(elector.IsLeader());
}

TEST_F(LeaderElectorTest, StepDownImmediatelyIfLeaseStolen) {
  std::atomic<int> acquire_calls{0};
  std::atomic<int> renew_calls{0};
  std::atomic<bool> started_leading{false};
  std::atomic<bool> stopped_leading{false};
  std::string lease_holder = "";

  auto try_acquire = [&](const std::string &holder, int, std::string &current) {
    acquire_calls++;
    if (lease_holder.empty() || lease_holder == holder) {
      lease_holder = holder;
      current = holder;
      return Status::OK();
    }
    current = lease_holder;
    return Status::OK();
  };

  auto renew = [&](const std::string &, int, std::string &current) {
    renew_calls++;
    // Simulate that another node has successfully acquired the lease
    lease_holder = "node-2";
    current = "node-2";
    return Status::OK();  // API succeeded, but lease stolen!
  };

  auto release = [](const std::string &) {};

  auto lease_client = std::make_shared<MockLeaseClient>(try_acquire, renew, release);

  LeaderElectionConfig config;
  config.lease_client = lease_client;
  config.holder_id = holder_id_;
  config.lease_duration_seconds = 10;  // Large TTL
  config.renew_deadline_seconds = 8;   // Large deadline
  config.retry_period_seconds = 1;
  config.on_started_leading = [&]() { started_leading = true; };
  config.on_stopped_leading = [&]() { stopped_leading = true; };

  LeaderElector elector(config);
  elector.Run();

  // Sleep for 1.5 seconds.
  // GCS acquires at T = 0s.
  // The first renewal loop starts at T = 0.001s, calls Renew, which returns Node-2.
  // GCS should immediately step down, without waiting for the 8s deadline!
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  elector.Stop();

  EXPECT_TRUE(started_leading.load());
  EXPECT_TRUE(stopped_leading.load());  // Should step down immediately!
  EXPECT_FALSE(elector.IsLeader());
}

TEST_F(LeaderElectorTest, StepDownOnPersistentRenewalFailure) {
  std::atomic<int> acquire_calls{0};
  std::atomic<int> renew_calls{0};
  std::atomic<bool> started_leading{false};
  std::atomic<bool> stopped_leading{false};

  auto try_acquire = [&](const std::string &holder, int, std::string &current) {
    acquire_calls++;
    current = holder;
    return Status::OK();
  };

  auto renew = [&](const std::string &, int, std::string &) {
    renew_calls++;
    return Status::IOError("Persistent Connection Timeout");  // Keep failing!
  };

  auto release = [](const std::string &) {};

  auto lease_client = std::make_shared<MockLeaseClient>(try_acquire, renew, release);

  LeaderElectionConfig config;
  config.lease_client = lease_client;
  config.holder_id = holder_id_;
  config.lease_duration_seconds = 2;
  config.renew_deadline_seconds = 1;
  config.retry_period_seconds = 1;
  config.on_started_leading = [&]() { started_leading = true; };
  config.on_stopped_leading = [&]() { stopped_leading = true; };

  LeaderElector elector(config);
  elector.Run();

  // Sleep for 1.5 seconds (watchdog deadline is 1s, so it should trigger step down at
  // ~1.01s)
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  EXPECT_TRUE(started_leading.load());
  EXPECT_TRUE(stopped_leading.load());  // Watchdog should have forced a step-down!
  EXPECT_FALSE(elector.IsLeader());

  elector.Stop();
}

TEST_F(LeaderElectorTest, RenewTransientFailureDoesNotStepDown) {
  std::atomic<int> acquire_calls{0};
  std::atomic<int> renew_calls{0};
  std::atomic<bool> started_leading{false};
  std::atomic<bool> stopped_leading{false};

  auto try_acquire = [&](const std::string &holder, int, std::string &current) {
    acquire_calls++;
    current = holder;
    return Status::OK();
  };

  auto renew = [&](const std::string &holder, int, std::string &current) {
    renew_calls++;
    current = holder;
    if (renew_calls == 1) {
      // First renewal fails temporarily (transient network issue)
      return Status::IOError("Transient Network Drop");
    } else {
      // Subsequent renewals succeed
      return Status::OK();
    }
  };

  auto release = [](const std::string &) {};

  auto lease_client = std::make_shared<MockLeaseClient>(try_acquire, renew, release);

  LeaderElectionConfig config;
  config.lease_client = lease_client;
  config.holder_id = holder_id_;
  // Large enough safety window to survive 1 failure (retry_period = 1s, renew_deadline =
  // 3s)
  config.lease_duration_seconds = 4;
  config.renew_deadline_seconds = 3;
  config.retry_period_seconds = 1;
  config.on_started_leading = [&]() { started_leading = true; };
  config.on_stopped_leading = [&]() { stopped_leading = true; };

  LeaderElector elector(config);
  elector.Run();

  // Sleep for 2.5 seconds (should have run 1 failed renewal at ~1s, and 1 successful
  // retry at ~2s)
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));

  EXPECT_TRUE(started_leading.load());
  EXPECT_FALSE(stopped_leading.load());  // Should not step down!
  EXPECT_TRUE(elector.IsLeader());
  EXPECT_GE(renew_calls.load(), 2);

  elector.Stop();
}

// Group 3: Telemetry & Transition Callbacks
TEST_F(LeaderElectorTest, LeadershipTransitionCallback) {
  std::atomic<int> leader_changes{0};
  std::string observed_leader = "";

  auto try_acquire = [&](const std::string &, int, std::string &current) {
    current = "node-2";  // Lease actively held by node-2
    return Status::OK();
  };

  auto renew = [](const std::string &holder, int, std::string &current) {
    current = holder;
    return Status::OK();
  };

  auto release = [](const std::string &) {};

  auto lease_client = std::make_shared<MockLeaseClient>(try_acquire, renew, release);

  LeaderElectionConfig config;
  config.lease_client = lease_client;
  config.holder_id = holder_id_;
  config.lease_duration_seconds = 2;
  config.renew_deadline_seconds = 1;
  config.retry_period_seconds = 1;
  config.on_new_leader = [&](const std::string &leader) {
    leader_changes++;
    observed_leader = leader;
  };

  LeaderElector elector(config);
  elector.Run();

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  elector.Stop();

  EXPECT_GE(leader_changes.load(), 1);
  EXPECT_EQ(observed_leader, "node-2");
}

TEST_F(LeaderElectorTest, NewLeaderCallbackTriggersOnce) {
  std::atomic<int> leader_changes{0};
  std::string observed_leader = "";
  std::atomic<int> acquire_calls{0};

  auto try_acquire = [&](const std::string &, int, std::string &current) {
    acquire_calls++;
    if (acquire_calls <= 2) {
      current = "node-2";  // First and second attempts return "node-2"
    } else {
      current = "node-3";  // Third attempt returns "node-3"
    }
    return Status::OK();
  };

  auto renew = [](const std::string &holder, int, std::string &current) {
    current = holder;
    return Status::OK();
  };

  auto release = [](const std::string &) {};

  auto lease_client = std::make_shared<MockLeaseClient>(try_acquire, renew, release);

  LeaderElectionConfig config;
  config.lease_client = lease_client;
  config.holder_id = holder_id_;
  config.lease_duration_seconds = 10;
  config.renew_deadline_seconds = 8;
  config.retry_period_seconds = 1;
  config.on_new_leader = [&](const std::string &leader) {
    leader_changes++;
    observed_leader = leader;
  };

  LeaderElector elector(config);
  elector.Run();

  // Sleep for 2.5s (runs 3 acquire checks: T=0s [node-2], T=1s [node-2], T=2s [node-3])
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  elector.Stop();

  // Total callbacks should be exactly 2 (once for node-2, once for node-3)
  EXPECT_EQ(leader_changes.load(), 2);
  EXPECT_EQ(observed_leader, "node-3");
}

// Group 4: Multi-Replica Election & Failover
TEST_F(LeaderElectorTest, TwoReplicasElectionWithGracefulExit) {
  // 1. Setup a shared coordinate client
  auto shared_client = std::make_shared<SharedMockLeaseClient>();

  // 2. Setup Elector A
  LeaderElectionConfig config_a;
  config_a.lease_client = shared_client;
  config_a.holder_id = "node-a";
  config_a.lease_duration_seconds = 4;
  config_a.renew_deadline_seconds = 3;
  config_a.retry_period_seconds = 1;
  std::atomic<bool> leading_a{false};
  std::atomic<bool> stopped_leading_a{false};
  config_a.on_started_leading = [&]() { leading_a = true; };
  config_a.on_stopped_leading = [&]() { stopped_leading_a = true; };
  LeaderElector elector_a(config_a);

  // 3. Setup Elector B
  LeaderElectionConfig config_b;
  config_b.lease_client = shared_client;
  config_b.holder_id = "node-b";
  config_b.lease_duration_seconds = 4;
  config_b.renew_deadline_seconds = 3;
  config_b.retry_period_seconds = 1;
  std::atomic<bool> leading_b{false};
  std::atomic<bool> stopped_leading_b{false};
  config_b.on_started_leading = [&]() { leading_b = true; };
  config_b.on_stopped_leading = [&]() { stopped_leading_b = true; };
  LeaderElector elector_b(config_b);

  // 4. Start both concurrently
  elector_a.Run();
  elector_b.Run();

  // Sleep for 1.5s to let both loops run initial acquire checks
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  // Assert only exactly one replica successfully became the leader
  EXPECT_TRUE(elector_a.IsLeader() != elector_b.IsLeader());
  EXPECT_TRUE(leading_a.load() != leading_b.load());

  // Determine who won and who is standby
  LeaderElector *winner = elector_a.IsLeader() ? &elector_a : &elector_b;
  LeaderElector *loser = elector_a.IsLeader() ? &elector_b : &elector_a;
  std::atomic<bool> *winner_stopped =
      elector_a.IsLeader() ? &stopped_leading_a : &stopped_leading_b;
  std::atomic<bool> *loser_leading = elector_a.IsLeader() ? &leading_b : &leading_a;

  // 5. Simulate leader crash/graceful exit by stopping the winner
  winner->Stop();

  // Sleep for 1.5s (loser should wake up, detect vacant lease, and promote itself)
  // The loser does not have to wait for 4s to acquire the lease.
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  // Assert failover took place: standby promoted to leader
  EXPECT_TRUE(loser->IsLeader());
  EXPECT_TRUE(loser_leading->load());
  EXPECT_FALSE(
      winner_stopped->load());  // Graceful stop shouldn't trigger watchdog step-down logs

  loser->Stop();
}

TEST_F(LeaderElectorTest, TwoReplicasElectionWithNonGracefulExit) {
  auto shared_client = std::make_shared<SharedMockLeaseClient>();

  // 1. Setup elector A
  LeaderElectionConfig config_a;
  config_a.lease_client = shared_client;
  config_a.holder_id = "node-a";
  config_a.lease_duration_seconds = 3;  // TTL of 3 seconds
  config_a.renew_deadline_seconds = 2;  // Watchdog deadline of 2 seconds
  config_a.retry_period_seconds = 1;    // Retry heartbeat every 1 second
  std::atomic<bool> leading_a{false};
  std::atomic<bool> stopped_leading_a{false};
  config_a.on_started_leading = [&]() { leading_a = true; };
  config_a.on_stopped_leading = [&]() { stopped_leading_a = true; };
  LeaderElector elector_a(config_a);

  // 2. Setup elector B
  LeaderElectionConfig config_b;
  config_b.lease_client = shared_client;
  config_b.holder_id = "node-b";
  config_b.lease_duration_seconds = 3;  // TTL of 3 seconds
  config_b.renew_deadline_seconds = 2;  // Watchdog deadline of 2 seconds
  config_b.retry_period_seconds = 1;    // Retry heartbeat every 1 second
  std::atomic<bool> leading_b{false};
  std::atomic<bool> stopped_leading_b{false};
  config_b.on_started_leading = [&]() { leading_b = true; };
  config_b.on_stopped_leading = [&]() { stopped_leading_b = true; };
  LeaderElector elector_b(config_b);

  // Start both concurrently
  elector_a.Run();
  elector_b.Run();

  // Sleep to resolve initial race
  std::this_thread::sleep_for(std::chrono::milliseconds(1200));

  // Determine who won
  LeaderElector *winner = elector_a.IsLeader() ? &elector_a : &elector_b;
  LeaderElector *loser = elector_a.IsLeader() ? &elector_b : &elector_a;
  std::string winner_id = elector_a.IsLeader() ? "node-a" : "node-b";
  std::atomic<bool> *loser_leading = elector_a.IsLeader() ? &leading_b : &leading_a;

  // 3. Simulate Node A crashes abruptly (we call Crash on client to block further
  // renewals/releases)
  shared_client->Crash(winner_id);
  winner->Stop();  // Calls Stop which triggers release internally, but client ignores it!

  // 4. Sleep for 1.2s (TTL is 2s, so Node B shouldn't be promoted yet because lease
  // hasn't expired)
  std::this_thread::sleep_for(std::chrono::milliseconds(1200));
  EXPECT_FALSE(loser->IsLeader());
  EXPECT_FALSE(loser_leading->load());

  // 5. Sleep for another 2.0s (Total 3.2s since crash. Lock duration of 3s has expired!)
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // Assert Node B successfully took over the expired lease!
  EXPECT_TRUE(loser->IsLeader());
  EXPECT_TRUE(loser_leading->load());

  loser->Stop();
}

}  // namespace gcs
}  // namespace ray
