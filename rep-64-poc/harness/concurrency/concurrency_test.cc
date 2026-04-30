// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

// Phase 5 concurrency tests for RocksDbStoreClient (REP-64 POC).
//
// Tests two REP claims under contention:
//   1. The mutex-RMW JobID counter is atomic and produces no
//      duplicates / no skips under N-thread contention. Exercised via
//      `GetNextJobIDSync()` (the synchronous helper retained for
//      test purposes; production code goes through `AsyncGetNextJobID`).
//   2. AsyncPut from multiple producer threads against a single
//      io_context yields fully-recoverable, value-correct state.
//
// The PLAN's "RocksDB Merge operator with uint64 addition" alternative
// is left as an open follow-on; see phase-5-concurrency.md for the
// reasoning to defer it.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/store_client/rocksdb_store_client.h"

namespace fs = std::filesystem;

namespace ray {
namespace gcs {
namespace {

fs::path UniqueTempDir(const std::string &tag) {
  std::random_device rd;
  std::mt19937_64 rng(rd());
  auto p = fs::temp_directory_path() /
           ("rep64-phase5-" + tag + "-" + std::to_string(rng()));
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
             std::chrono::milliseconds timeout = std::chrono::seconds(60)) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return pred();
}

class RocksDbConcurrencyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_path_ = UniqueTempDir("rocksdb");
    io_fixture_ = std::make_unique<IoServiceFixture>();
    client_ = std::make_unique<RocksDbStoreClient>(io_fixture_->io(),
                                                    db_path_.string(), "");
  }

  void TearDown() override {
    client_.reset();
    io_fixture_.reset();
    fs::remove_all(db_path_);
  }

  fs::path db_path_;
  std::unique_ptr<IoServiceFixture> io_fixture_;
  std::unique_ptr<RocksDbStoreClient> client_;
};

// --- 1. JobID counter: no duplicates across threads. -----------------------
TEST_F(RocksDbConcurrencyTest, JobIdNoDuplicatesAcrossThreads) {
  constexpr int kThreads = 16;
  constexpr int kPerThread = 1000;
  constexpr int kTotal = kThreads * kPerThread;

  std::vector<std::thread> workers;
  std::mutex collector_mutex;
  std::vector<int> all_ids;
  all_ids.reserve(kTotal);

  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([this, &collector_mutex, &all_ids] {
      std::vector<int> local;
      local.reserve(kPerThread);
      for (int i = 0; i < kPerThread; ++i) {
        local.push_back(client_->GetNextJobIDSync());
      }
      std::lock_guard<std::mutex> lock(collector_mutex);
      for (int id : local) all_ids.push_back(id);
    });
  }
  for (auto &w : workers) w.join();

  ASSERT_EQ(all_ids.size(), kTotal);
  std::set<int> unique(all_ids.begin(), all_ids.end());
  ASSERT_EQ(unique.size(), all_ids.size())
      << "duplicate IDs detected — atomicity bug";
  EXPECT_EQ(*unique.begin(), 1);
  EXPECT_EQ(*unique.rbegin(), kTotal)
      << "expected the contiguous range [1, " << kTotal
      << "]; missing IDs would mean lost increments";
}

// --- 2. Per-thread monotonicity. ------------------------------------------
TEST_F(RocksDbConcurrencyTest, JobIdMonotonicPerThread) {
  constexpr int kThreads = 8;
  constexpr int kPerThread = 500;

  std::vector<std::thread> workers;
  std::vector<std::vector<int>> per_thread(kThreads);

  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([this, t, &per_thread] {
      auto &v = per_thread[t];
      v.reserve(kPerThread);
      for (int i = 0; i < kPerThread; ++i) {
        v.push_back(client_->GetNextJobIDSync());
      }
    });
  }
  for (auto &w : workers) w.join();

  for (int t = 0; t < kThreads; ++t) {
    const auto &v = per_thread[t];
    for (size_t i = 1; i < v.size(); ++i) {
      ASSERT_GT(v[i], v[i - 1])
          << "thread " << t << " saw non-monotonic ID at index " << i;
    }
  }
}

// --- 3. Parallel AsyncPut from many producer threads. ---------------------
TEST_F(RocksDbConcurrencyTest, ParallelAsyncPutAllSurviveWithCorrectValues) {
  constexpr int kThreads = 8;
  constexpr int kPerThread = 500;
  constexpr int kTotal = kThreads * kPerThread;
  const std::string kTable = "phase5_table";

  std::atomic<int> put_acks{0};
  std::vector<std::thread> producers;
  for (int t = 0; t < kThreads; ++t) {
    producers.emplace_back([this, t, kTable, &put_acks] {
      for (int i = 0; i < kPerThread; ++i) {
        std::string key = "k" + std::to_string(t) + "_" + std::to_string(i);
        std::string value = "v" + std::to_string(t) + "_" + std::to_string(i);
        client_->AsyncPut(kTable, key, value, /*overwrite=*/true,
                          {[&put_acks](bool) { put_acks.fetch_add(1); },
                           io_fixture_->io()});
      }
    });
  }
  for (auto &p : producers) p.join();

  ASSERT_TRUE(WaitFor([&] { return put_acks.load() == kTotal; }))
      << "Only " << put_acks.load() << " of " << kTotal << " AsyncPut callbacks fired";

  std::atomic<int> get_acks{0};
  std::atomic<int> mismatches{0};
  std::atomic<int> missing{0};
  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kPerThread; ++i) {
      std::string key = "k" + std::to_string(t) + "_" + std::to_string(i);
      std::string expected = "v" + std::to_string(t) + "_" + std::to_string(i);
      client_->AsyncGet(
          kTable, key,
          {[expected, &get_acks, &mismatches, &missing](
               Status status, std::optional<std::string> value) {
             if (!status.ok() || !value) {
               missing.fetch_add(1);
             } else if (*value != expected) {
               mismatches.fetch_add(1);
             }
             get_acks.fetch_add(1);
           },
           io_fixture_->io()});
    }
  }
  ASSERT_TRUE(WaitFor([&] { return get_acks.load() == kTotal; }))
      << "Only " << get_acks.load() << " of " << kTotal << " AsyncGet callbacks fired";

  EXPECT_EQ(missing.load(), 0) << "Some keys did not survive the parallel AsyncPut";
  EXPECT_EQ(mismatches.load(), 0)
      << "Some keys read back the wrong value — possible cross-key write interference";
}

// --- 4. JobID counter durability under load + restart. -------------------
TEST_F(RocksDbConcurrencyTest, JobIdSurvivesRestartAfterConcurrentLoad) {
  constexpr int kThreads = 8;
  constexpr int kPerThread = 200;
  constexpr int kTotalFirstLifetime = kThreads * kPerThread;

  std::vector<int> ids_first_lifetime;
  ids_first_lifetime.reserve(kTotalFirstLifetime);
  std::mutex m;

  std::vector<std::thread> workers;
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([this, &m, &ids_first_lifetime] {
      std::vector<int> local;
      for (int i = 0; i < kPerThread; ++i) {
        local.push_back(client_->GetNextJobIDSync());
      }
      std::lock_guard<std::mutex> lock(m);
      for (int id : local) ids_first_lifetime.push_back(id);
    });
  }
  for (auto &w : workers) w.join();
  ASSERT_EQ(ids_first_lifetime.size(), kTotalFirstLifetime);
  int max_first = *std::max_element(ids_first_lifetime.begin(),
                                     ids_first_lifetime.end());
  ASSERT_EQ(max_first, kTotalFirstLifetime);

  // Close + reopen.
  client_.reset();
  client_ = std::make_unique<RocksDbStoreClient>(io_fixture_->io(),
                                                  db_path_.string(), "");

  int next = client_->GetNextJobIDSync();
  EXPECT_GT(next, max_first)
      << "Job ID rolled back across restart; this would let a recovered "
         "cluster re-issue an old job ID — silent corruption.";
}

}  // namespace
}  // namespace gcs
}  // namespace ray
