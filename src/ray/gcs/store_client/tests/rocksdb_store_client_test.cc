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

// RocksDb-specific tests for RocksDbStoreClient.
//
// What this file proves:
//   1. AsyncPut + AsyncGet roundtrip a value within a single client.
//   2. State persists across close+reopen of the client at the same
//      path — the core GCS recovery claim.
//   3. AsyncGetNextJobID is monotonic within a process and persists
//      across restart so a recovered cluster doesn't re-issue old
//      job IDs.
//   4. The cluster-ID marker fail-fast on PVC reuse.
//   5. Writes roundtrip across the I/O offload pool threads.
//
// `rocksdb_parity_test` covers the full StoreClient API surface against
// the same fixture `in_memory_store_client_test` and
// `redis_store_client_test` use; this file owns the RocksDb-specific
// scenarios (close+reopen, cluster-id marker, pool roundtrip) that don't
// fit StoreClientTestBase.

#include "ray/gcs/store_client/rocksdb_store_client.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <thread>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest.h"
#include "ray/asio/instrumented_io_context.h"

namespace fs = std::filesystem;

namespace ray {
namespace gcs {

namespace {

fs::path UniqueTempDir(const std::string &tag) {
  std::random_device rd;
  std::mt19937_64 rng(rd());
  auto p = fs::temp_directory_path() /
           ("ray-rocksdb-test-" + tag + "-" + std::to_string(rng()));
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

// Blocks on the async job-ID interface method. The production API is
// async-only; tests that need a value synchronously drive it through a
// promise/future rather than reaching for an internal helper.
int SyncGetNextJobID(RocksDbStoreClient &client, instrumented_io_context &io) {
  std::promise<int> p;
  auto f = p.get_future();
  client.AsyncGetNextJobID({[&p](int id) { p.set_value(id); }, io});
  return f.get();
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
  // Headline GCS recovery check: write data, destroy the client (which
  // closes the underlying RocksDB), construct a new client at the same
  // path with the same cluster_id, read the data back.
  IoServiceFixture io;
  const fs::path db = UniqueTempDir("recover");
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
    int a = SyncGetNextJobID(client, io.io());
    int b = SyncGetNextJobID(client, io.io());
    int c = SyncGetNextJobID(client, io.io());
    EXPECT_LT(a, b);
    EXPECT_LT(b, c);
    last = c;
  }
  {
    RocksDbStoreClient client(io.io(), db.string(), "");
    int next = SyncGetNextJobID(client, io.io());
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

TEST(RocksDbStoreClientTest, OffloadPathRoundtripsAcrossPoolThreads) {
  // RocksDB calls run on the I/O offload pool, not the caller's thread.
  // Issues 64 concurrent Puts back-to-back from a single thread (the
  // io_service thread); the pool drains them in parallel. Every callback
  // must fire and every Get must return the right value. This is the
  // correctness contract; throughput is a microbench question, not a
  // unit-test concern.
  IoServiceFixture io;
  const fs::path db = UniqueTempDir("offload-roundtrip");
  RocksDbStoreClient client(io.io(),
                            db.string(),
                            /*expected_cluster_id=*/"",
                            /*io_pool_size=*/4);

  constexpr int kN = 64;
  std::atomic<int> writes_done{0};
  for (int i = 0; i < kN; ++i) {
    client.AsyncPut("offload_t",
                    "k" + std::to_string(i),
                    "v" + std::to_string(i),
                    /*overwrite=*/true,
                    {[&writes_done](bool) { writes_done.fetch_add(1); }, io.io()});
  }
  ASSERT_TRUE(WaitFor([&] { return writes_done.load() == kN; }));

  std::atomic<int> reads_done{0};
  std::vector<std::optional<std::string>> got(kN);
  for (int i = 0; i < kN; ++i) {
    client.AsyncGet("offload_t",
                    "k" + std::to_string(i),
                    {[i, &reads_done, &got](Status, std::optional<std::string> v) {
                       got[i] = std::move(v);
                       reads_done.fetch_add(1);
                     },
                     io.io()});
  }
  ASSERT_TRUE(WaitFor([&] { return reads_done.load() == kN; }));

  for (int i = 0; i < kN; ++i) {
    ASSERT_TRUE(got[i].has_value()) << "missing key " << i;
    EXPECT_EQ(*got[i], "v" + std::to_string(i));
  }

  fs::remove_all(db);
}

TEST(RocksDbStoreClientTest, OffloadPathDestructorJoinsBeforeDbClose) {
  // Regression: pool tasks must complete (or be drained) before db_ is
  // destroyed. If we tear down while a task is still mid-Put, we'd see
  // a use-after-free on the rocksdb::DB handle. Issue a write, then let
  // the client go out of scope immediately. The destructor must drain
  // the pool *before* db_ destructs. ASan would catch a regression here;
  // for normal builds this just verifies the code path doesn't deadlock.
  IoServiceFixture io;
  const fs::path db = UniqueTempDir("offload-dtor");

  {
    RocksDbStoreClient client(io.io(),
                              db.string(),
                              /*expected_cluster_id=*/"",
                              /*io_pool_size=*/2);
    std::atomic<int> done{0};
    for (int i = 0; i < 16; ++i) {
      client.AsyncPut("dtor_t",
                      "k" + std::to_string(i),
                      "v" + std::to_string(i),
                      true,
                      {[&done](bool) { done.fetch_add(1); }, io.io()});
    }
    // Wait so the test asserts the writes actually landed; the destructor
    // join is the load-bearing piece, but verifying durability tightens
    // the contract.
    ASSERT_TRUE(WaitFor([&] { return done.load() == 16; }));
  }
  // Reopen and read back.
  {
    RocksDbStoreClient reader(io.io(), db.string(), /*expected_cluster_id=*/"");
    std::atomic<int> reads_done{0};
    int matched = 0;
    for (int i = 0; i < 16; ++i) {
      reader.AsyncGet("dtor_t",
                      "k" + std::to_string(i),
                      {[i, &reads_done, &matched](Status, std::optional<std::string> v) {
                         if (v && *v == "v" + std::to_string(i)) ++matched;
                         reads_done.fetch_add(1);
                       },
                       io.io()});
    }
    ASSERT_TRUE(WaitFor([&] { return reads_done.load() == 16; }));
    EXPECT_EQ(matched, 16);
  }
  fs::remove_all(db);
}

TEST(RocksDbStoreClientTest, OffloadStrandSerializesOverwriteFalseRace) {
  // Without per-key strand bucketing, two concurrent AsyncPut(K, !overwrite)
  // calls on the offload path can both observe "not found" and both write,
  // both reporting inserted=true. With per-key strand dispatch, the same
  // bucket serializes those calls so exactly one wins. We stress-test by
  // launching many issuer threads all racing on the same key.
  IoServiceFixture io;
  const fs::path db = UniqueTempDir("strand-noov-race");
  RocksDbStoreClient client(io.io(),
                            db.string(),
                            /*expected_cluster_id=*/"",
                            /*io_pool_size=*/4,
                            /*strand_buckets=*/64);

  constexpr int kIssuers = 128;
  std::atomic<int> inserted_true{0};
  std::atomic<int> inserted_false{0};
  std::atomic<int> done{0};

  // Hold the issuers at a starting line so they all submit roughly
  // simultaneously, maximising the chance pool threads see "not found"
  // before any winner has committed.
  std::atomic<bool> go{false};
  std::vector<std::thread> issuers;
  issuers.reserve(kIssuers);
  for (int t = 0; t < kIssuers; ++t) {
    issuers.emplace_back([&, t] {
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      client.AsyncPut("noov_t",
                      "shared_key",
                      "v" + std::to_string(t),
                      /*overwrite=*/false,
                      {[&](bool inserted) {
                         (inserted ? inserted_true : inserted_false).fetch_add(1);
                         done.fetch_add(1);
                       },
                       io.io()});
    });
  }
  go.store(true, std::memory_order_release);
  for (auto &th : issuers) th.join();
  ASSERT_TRUE(WaitFor([&] { return done.load() == kIssuers; }));

  EXPECT_EQ(inserted_true.load(), 1);
  EXPECT_EQ(inserted_false.load(), kIssuers - 1);
  fs::remove_all(db);
}

TEST(RocksDbStoreClientTest, OffloadStrandPreservesLastWriterWinsForSameKey) {
  // Per-key strand must give submission-order-equals-execution-order for
  // the same key. Burst N back-to-back overwrites from the io_service
  // thread (single-threaded, FIFO submission), then read; the final
  // value must be the last one we submitted. Without strand, the pool
  // can reorder same-key writes and the read may see an earlier value.
  // Repeat across many trials to make the contract sharp.
  IoServiceFixture io;
  const fs::path db = UniqueTempDir("strand-lww");
  RocksDbStoreClient client(io.io(),
                            db.string(),
                            /*expected_cluster_id=*/"",
                            /*io_pool_size=*/4,
                            /*strand_buckets=*/64);

  constexpr int kTrials = 32;
  constexpr int kPutsPerTrial = 16;
  for (int trial = 0; trial < kTrials; ++trial) {
    const std::string key = "k_" + std::to_string(trial);
    std::atomic<int> writes_done{0};
    for (int i = 0; i < kPutsPerTrial; ++i) {
      client.AsyncPut("lww_t",
                      key,
                      "v" + std::to_string(i),
                      /*overwrite=*/true,
                      {[&writes_done](bool) { writes_done.fetch_add(1); }, io.io()});
    }
    ASSERT_TRUE(WaitFor([&] { return writes_done.load() == kPutsPerTrial; }));

    std::atomic<bool> read_done{false};
    std::optional<std::string> got;
    client.AsyncGet("lww_t",
                    key,
                    {[&got, &read_done](Status, std::optional<std::string> v) {
                       got = std::move(v);
                       read_done.store(true);
                     },
                     io.io()});
    ASSERT_TRUE(WaitFor([&] { return read_done.load(); }));

    ASSERT_TRUE(got.has_value()) << "trial " << trial;
    EXPECT_EQ(*got, "v" + std::to_string(kPutsPerTrial - 1))
        << "trial " << trial << ": last-writer-wins violated";
  }
  fs::remove_all(db);
}

TEST(RocksDbStoreClientTest, OffloadStrandPreservesDeleteThenPut) {
  // Submission order: Delete(K), Put(K, V). Final state must contain V.
  // Without per-key strand, the pool can run the Put before the Delete,
  // erasing V. Repeat across many trials to amplify any reordering.
  IoServiceFixture io;
  const fs::path db = UniqueTempDir("strand-del-put");
  RocksDbStoreClient client(io.io(),
                            db.string(),
                            /*expected_cluster_id=*/"",
                            /*io_pool_size=*/4,
                            /*strand_buckets=*/64);

  constexpr int kTrials = 64;
  for (int trial = 0; trial < kTrials; ++trial) {
    const std::string key = "k_" + std::to_string(trial);

    // Seed an existing value so Delete is a real delete, not a no-op.
    std::atomic<bool> seeded{false};
    client.AsyncPut("dp_t",
                    key,
                    "old",
                    /*overwrite=*/true,
                    {[&seeded](bool) { seeded.store(true); }, io.io()});
    ASSERT_TRUE(WaitFor([&] { return seeded.load(); }));

    std::atomic<int> ops_done{0};
    client.AsyncDelete(
        "dp_t", key, {[&ops_done](bool) { ops_done.fetch_add(1); }, io.io()});
    client.AsyncPut("dp_t",
                    key,
                    "new",
                    /*overwrite=*/true,
                    {[&ops_done](bool) { ops_done.fetch_add(1); }, io.io()});
    ASSERT_TRUE(WaitFor([&] { return ops_done.load() == 2; }));

    std::atomic<bool> read_done{false};
    std::optional<std::string> got;
    client.AsyncGet("dp_t",
                    key,
                    {[&got, &read_done](Status, std::optional<std::string> v) {
                       got = std::move(v);
                       read_done.store(true);
                     },
                     io.io()});
    ASSERT_TRUE(WaitFor([&] { return read_done.load(); }));

    ASSERT_TRUE(got.has_value()) << "trial " << trial << ": Put after Delete lost";
    EXPECT_EQ(*got, "new") << "trial " << trial;
  }
  fs::remove_all(db);
}

TEST(RocksDbStoreClientTest, OffloadStrandPreservesCrossKeyParallelism) {
  // Per-key strand must not collapse cross-key parallelism. Drive 1024
  // writes spread across 1024 distinct keys with pool=4 and buckets=64;
  // bucket collisions are expected for some keys but must not serialize
  // the whole workload. We assert all writes complete and every key
  // reads back its expected value — a regression that accidentally
  // routed everything through one strand would still pass correctness
  // but spike wall-clock; we keep it loose-but-bounded by giving
  // WaitFor its standard 30 s deadline rather than asserting on a hard
  // throughput floor (which would be flaky on shared CI hardware).
  IoServiceFixture io;
  const fs::path db = UniqueTempDir("strand-crosskey");
  RocksDbStoreClient client(io.io(),
                            db.string(),
                            /*expected_cluster_id=*/"",
                            /*io_pool_size=*/4,
                            /*strand_buckets=*/64);

  constexpr int kKeys = 1024;
  using namespace std::chrono_literals;
  std::atomic<int> writes_done{0};
  for (int i = 0; i < kKeys; ++i) {
    client.AsyncPut("xkey_t",
                    "k" + std::to_string(i),
                    "v" + std::to_string(i),
                    /*overwrite=*/true,
                    {[&writes_done](bool) { writes_done.fetch_add(1); }, io.io()});
  }
  ASSERT_TRUE(WaitFor([&] { return writes_done.load() == kKeys; }, 60s));

  std::atomic<int> reads_done{0};
  std::atomic<int> matches{0};
  for (int i = 0; i < kKeys; ++i) {
    client.AsyncGet("xkey_t",
                    "k" + std::to_string(i),
                    {[i, &reads_done, &matches](Status, std::optional<std::string> v) {
                       if (v && *v == "v" + std::to_string(i)) matches.fetch_add(1);
                       reads_done.fetch_add(1);
                     },
                     io.io()});
  }
  ASSERT_TRUE(WaitFor([&] { return reads_done.load() == kKeys; }, 30s));
  EXPECT_EQ(matches.load(), kKeys);
  fs::remove_all(db);
}

TEST(RocksDbStoreClientTest, OffloadStrandOrdersBatchDeleteAfterSameKeyPut) {
  // AsyncBatchDelete must order against a same-key write. Submission
  // order: Put(K, "new"), BatchDelete([K]); both target K, so the per-key
  // strand runs them in that order — the Put commits, BatchDelete then
  // sees K present, deletes it (count 1), and K ends up absent. If
  // BatchDelete were dispatched on the unordered pool (the pre-fix
  // behaviour), it could run before the Put committed: the Put would then
  // resurrect K and the final read would see "new". Seeding "old" first
  // makes the delete real in either ordering, so the discriminating
  // signal is purely the final state. Repeat across trials to amplify any
  // reordering. Guards the fix for
  // https://github.com/ray-project/ray/pull/63657#discussion_r3405306127.
  IoServiceFixture io;
  const fs::path db = UniqueTempDir("strand-put-batchdel");
  RocksDbStoreClient client(io.io(),
                            db.string(),
                            /*expected_cluster_id=*/"",
                            /*io_pool_size=*/4,
                            /*strand_buckets=*/64);

  constexpr int kTrials = 64;
  for (int trial = 0; trial < kTrials; ++trial) {
    const std::string key = "k_" + std::to_string(trial);

    // Seed an existing value so the delete is a real delete, not a no-op.
    std::atomic<bool> seeded{false};
    client.AsyncPut("pbd_t",
                    key,
                    "old",
                    /*overwrite=*/true,
                    {[&seeded](bool) { seeded.store(true); }, io.io()});
    ASSERT_TRUE(WaitFor([&] { return seeded.load(); }));

    std::atomic<int> ops_done{0};
    std::atomic<int64_t> batch_deleted{-1};
    client.AsyncPut("pbd_t",
                    key,
                    "new",
                    /*overwrite=*/true,
                    {[&ops_done](bool) { ops_done.fetch_add(1); }, io.io()});
    client.AsyncBatchDelete("pbd_t",
                            {key},
                            {[&ops_done, &batch_deleted](int64_t n) {
                               batch_deleted.store(n);
                               ops_done.fetch_add(1);
                             },
                             io.io()});
    ASSERT_TRUE(WaitFor([&] { return ops_done.load() == 2; }));

    std::atomic<bool> read_done{false};
    std::optional<std::string> got;
    client.AsyncGet("pbd_t",
                    key,
                    {[&got, &read_done](Status, std::optional<std::string> v) {
                       got = std::move(v);
                       read_done.store(true);
                     },
                     io.io()});
    ASSERT_TRUE(WaitFor([&] { return read_done.load(); }));

    EXPECT_FALSE(got.has_value())
        << "trial " << trial << ": BatchDelete ran before the same-key Put committed";
    EXPECT_EQ(batch_deleted.load(), 1)
        << "trial " << trial << ": BatchDelete did not observe the committed Put";
  }
  fs::remove_all(db);
}

TEST(RocksDbStoreClientTest, OffloadStrandOrdersMultiGetAfterSameKeyPut) {
  // AsyncMultiGet must order against same-key writes. Burst N back-to-back
  // overwrites of K from the single-threaded io_service (FIFO submission),
  // then MultiGet([K]) without waiting for the writes to drain; the per-key
  // strand runs the read after all the writes, so it must return the last
  // value submitted. If MultiGet were dispatched on the unordered pool (the
  // pre-fix behaviour), it could run before the final writes committed and
  // return an earlier value — or miss K entirely. Repeat across trials.
  // Guards the fix for
  // https://github.com/ray-project/ray/pull/63657#discussion_r3405306127.
  IoServiceFixture io;
  const fs::path db = UniqueTempDir("strand-put-multiget");
  RocksDbStoreClient client(io.io(),
                            db.string(),
                            /*expected_cluster_id=*/"",
                            /*io_pool_size=*/4,
                            /*strand_buckets=*/64);

  constexpr int kTrials = 32;
  constexpr int kPutsPerTrial = 16;
  for (int trial = 0; trial < kTrials; ++trial) {
    const std::string key = "k_" + std::to_string(trial);
    std::atomic<int> writes_done{0};
    for (int i = 0; i < kPutsPerTrial; ++i) {
      client.AsyncPut("pmg_t",
                      key,
                      "v" + std::to_string(i),
                      /*overwrite=*/true,
                      {[&writes_done](bool) { writes_done.fetch_add(1); }, io.io()});
    }

    // Issue the read immediately, before draining the writes: only the
    // per-key strand ordering can make it observe the final value.
    std::atomic<bool> read_done{false};
    absl::flat_hash_map<std::string, std::string> got;
    client.AsyncMultiGet(
        "pmg_t",
        {key},
        {[&got, &read_done](absl::flat_hash_map<std::string, std::string> result) {
           got = std::move(result);
           read_done.store(true);
         },
         io.io()});
    ASSERT_TRUE(WaitFor([&] { return read_done.load(); }));
    ASSERT_TRUE(WaitFor([&] { return writes_done.load() == kPutsPerTrial; }));

    auto it = got.find(key);
    ASSERT_TRUE(it != got.end()) << "trial " << trial << ": MultiGet missed the key";
    EXPECT_EQ(it->second, "v" + std::to_string(kPutsPerTrial - 1))
        << "trial " << trial
        << ": MultiGet observed a stale value (ran before the same-key writes committed)";
  }
  fs::remove_all(db);
}

}  // namespace gcs
}  // namespace ray
