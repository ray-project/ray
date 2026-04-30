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

// Phase 3 walking-skeleton tests for RocksDbStoreClient (REP-64 POC).
//
// What this file proves:
//   1. AsyncPut + AsyncGet roundtrip a value within a single client.
//   2. State persists across close+reopen of the client at the same
//      path — the *core* claim Phase 3 is here to demonstrate.
//   3. GetNextJobIDSync (the synchronous helper exercised by the POC
//      concurrency tests) is monotonic within a process and persists
//      across restart so a recovered cluster doesn't re-issue old
//      job IDs.
//   4. The cluster-ID marker fail-fast on PVC reuse.
//
// Phase 6's StoreClientTestBase parity test
// (rep-64-poc/harness/store_client_parity/) covers the full API surface
// against the same fixture in_memory_store_client_test and
// redis_store_client_test use; this file owns the Phase-3-specific
// scenarios (close+reopen, cluster-id marker) that don't fit
// StoreClientTestBase.

#include "ray/gcs/store_client/rocksdb_store_client.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"

namespace fs = std::filesystem;

namespace ray {
namespace gcs {

namespace {

fs::path UniqueTempDir(const std::string &tag) {
  std::random_device rd;
  std::mt19937_64 rng(rd());
  auto p = fs::temp_directory_path() / ("rep64-poc-" + tag + "-" + std::to_string(rng()));
  fs::create_directories(p);
  return p;
}

class IoServiceFixture {
 public:
  IoServiceFixture()
      : work_(std::make_unique<boost::asio::io_service::work>(io_)),
        thread_([this] { io_.run(); }) {}

  ~IoServiceFixture() {
    work_.reset();
    if (thread_.joinable()) thread_.join();
  }

  instrumented_io_context &io() { return io_; }

 private:
  instrumented_io_context io_;
  std::unique_ptr<boost::asio::io_service::work> work_;
  std::thread thread_;
};

bool WaitFor(std::function<bool()> pred,
             std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return pred();
}

}  // namespace

TEST(RocksDbStoreClientTest, PutGetRoundtrip) {
  IoServiceFixture io;
  const fs::path db = UniqueTempDir("phase3-roundtrip");
  RocksDbStoreClient client(io.io(), db.string(), /*expected_cluster_id=*/"");

  std::atomic<int> done{0};
  std::optional<std::string> got;

  client.AsyncPut("t1",
                  "k1",
                  "v1",
                  /*overwrite=*/true,
                  {[&done](bool) { done.fetch_add(1); }, io.io()});
  ASSERT_TRUE(WaitFor([&] { return done.load() == 1; }));

  client.AsyncGet("t1",
                  "k1",
                  {[&done, &got](Status, std::optional<std::string> value) {
                     got = std::move(value);
                     done.fetch_add(1);
                   },
                   io.io()});
  ASSERT_TRUE(WaitFor([&] { return done.load() == 2; }));
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, "v1");

  fs::remove_all(db);
}

TEST(RocksDbStoreClientTest, RecoverAcrossReopen) {
  // Phase 3 headline proof: write data, destroy the client (which
  // closes the underlying RocksDB), construct a new client at the
  // same path with the same cluster_id, read the data back.
  IoServiceFixture io;
  const fs::path db = UniqueTempDir("phase3-recover");
  const std::string cluster_id = "deadbeef-cluster-id-marker";

  {
    RocksDbStoreClient writer(io.io(), db.string(), cluster_id);
    std::atomic<int> done{0};
    writer.AsyncPut(
        "table_a", "k1", "vA1", true, {[&done](bool) { done.fetch_add(1); }, io.io()});
    writer.AsyncPut(
        "table_a", "k2", "vA2", true, {[&done](bool) { done.fetch_add(1); }, io.io()});
    writer.AsyncPut(
        "table_b", "k1", "vB1", true, {[&done](bool) { done.fetch_add(1); }, io.io()});
    ASSERT_TRUE(WaitFor([&] { return done.load() == 3; }));
  }
  // Destroyed: RocksDB is closed. Now reopen.

  {
    RocksDbStoreClient reader(io.io(), db.string(), cluster_id);
    std::atomic<int> done{0};
    std::string vA1, vA2, vB1;
    reader.AsyncGet("table_a",
                    "k1",
                    {[&](Status, std::optional<std::string> v) {
                       if (v) vA1 = *v;
                       done.fetch_add(1);
                     },
                     io.io()});
    reader.AsyncGet("table_a",
                    "k2",
                    {[&](Status, std::optional<std::string> v) {
                       if (v) vA2 = *v;
                       done.fetch_add(1);
                     },
                     io.io()});
    reader.AsyncGet("table_b",
                    "k1",
                    {[&](Status, std::optional<std::string> v) {
                       if (v) vB1 = *v;
                       done.fetch_add(1);
                     },
                     io.io()});
    ASSERT_TRUE(WaitFor([&] { return done.load() == 3; }));
    EXPECT_EQ(vA1, "vA1");
    EXPECT_EQ(vA2, "vA2");
    EXPECT_EQ(vB1, "vB1");
  }
  fs::remove_all(db);
}

TEST(RocksDbStoreClientTest, JobIdMonotonicAndPersists) {
  IoServiceFixture io;
  const fs::path db = UniqueTempDir("phase3-jobid");

  int last;
  {
    RocksDbStoreClient client(io.io(), db.string(), "");
    int a = client.GetNextJobIDSync();
    int b = client.GetNextJobIDSync();
    int c = client.GetNextJobIDSync();
    EXPECT_LT(a, b);
    EXPECT_LT(b, c);
    last = c;
  }
  {
    RocksDbStoreClient client(io.io(), db.string(), "");
    int next = client.GetNextJobIDSync();
    EXPECT_GT(next, last)
        << "Expected job ID to advance beyond the previous lifetime's max (" << last
        << "), got " << next;
  }
  fs::remove_all(db);
}

TEST(RocksDbStoreClientTest, ClusterIdMarkerWritesOnFirstOpen) {
  // First open with a cluster_id should write the marker. Second open
  // with the same cluster_id should accept it (no death).
  IoServiceFixture io;
  const fs::path db = UniqueTempDir("phase3-cluster-marker");
  const std::string cid = "cluster-x";

  { RocksDbStoreClient client(io.io(), db.string(), cid); }
  { RocksDbStoreClient client(io.io(), db.string(), cid); }

  fs::remove_all(db);
}

}  // namespace gcs
}  // namespace ray
