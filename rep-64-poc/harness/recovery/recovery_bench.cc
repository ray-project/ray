// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

// Phase 8 storage-layer recovery-time benchmark.
//
// Measures cold-open latency for a RocksDbStoreClient at varying state
// sizes. This is the storage-layer half of the REP's "recovery time
// equal to or better than Redis-based FT" claim: how long does it
// take to construct a fresh RocksDbStoreClient against a populated
// database?
//
// (Full GCS-process recovery + actor-survival measurement involves
// killing gcs_server and re-attaching workers, which needs a Ray
// Python wheel built from this branch -- a much larger lift. The
// scaffold for that test lives at
// rep-64-poc/harness/integration/test_rocksdb_recovery.py and is
// gated on RAY_REP64_RUN_E2E=1. Phase 8's storage-layer numbers here
// constrain the lower bound on full GCS recovery time.)
//
// Workflow per iteration:
//   1. Open RocksDbStoreClient at a given path (cold open, fresh
//      process via run-script wrapper).
//   2. AsyncGetAll() to read every persisted entry back.
//   3. Time both phases.
//
// Two modes:
//   --mode populate  -- write N entries, exit. Used to seed the DB.
//   --mode recover   -- open the existing DB, GetAll, report timing.
//
// Usage from Python orchestrator:
//   recovery_bench --mode populate --db-dir /path --num-keys 10000
//   recovery_bench --mode recover  --db-dir /path

#include <atomic>
#include <boost/asio.hpp>
#include <boost/optional.hpp>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <thread>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/store_client/rocksdb_store_client.h"

namespace {

using ray::Status;
using ray::gcs::RocksDbStoreClient;

const std::string kTable = "phase8_recovery";

class IoFixture {
 public:
  IoFixture()
      : work_(std::make_unique<boost::asio::io_service::work>(io_)),
        thread_([this] { io_.run(); }) {}
  ~IoFixture() {
    work_.reset();
    if (thread_.joinable()) thread_.join();
  }
  instrumented_io_context &io() { return io_; }

 private:
  instrumented_io_context io_;
  std::unique_ptr<boost::asio::io_service::work> work_;
  std::thread thread_;
};

double Elapsed(std::chrono::steady_clock::time_point a,
               std::chrono::steady_clock::time_point b) {
  return std::chrono::duration<double>(b - a).count();
}

int Populate(const std::string &db_dir, int num_keys) {
  IoFixture io;
  RocksDbStoreClient client(io.io(), db_dir, /*expected_cluster_id=*/"");

  std::atomic<int> acked{0};
  for (int i = 0; i < num_keys; ++i) {
    // Mimic an actor-table-style payload: ~256 bytes of mostly random data.
    std::string key = "actor_" + std::to_string(i);
    std::string value(256, 'x');
    for (size_t j = 0; j < value.size(); ++j) {
      value[j] = static_cast<char>('A' + ((i + j) % 26));
    }
    auto s =
        client.AsyncPut(kTable, key, value, true, [&acked](bool) { acked.fetch_add(1); });
    if (!s.ok()) {
      std::cerr << "AsyncPut failed: " << s.ToString() << std::endl;
      return 1;
    }
  }
  while (acked.load() < num_keys) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  std::cout << "{\"populated\": " << num_keys << "}\n";
  return 0;
}

int Recover(const std::string &db_dir) {
  // Time three phases:
  //  - "open": construct the StoreClient (RocksDB DB::Open + CF list).
  //  - "scan": single AsyncGetAll over every persisted key.
  //  - "lookup": one AsyncGet of a known-existing key (tail-latency
  //    proxy for "first read after a cold open").
  auto t_open_start = std::chrono::steady_clock::now();
  IoFixture io;
  RocksDbStoreClient client(io.io(), db_dir, /*expected_cluster_id=*/"");
  auto t_open_end = std::chrono::steady_clock::now();

  std::atomic<int> done{0};
  size_t scan_count = 0;
  auto t_scan_start = std::chrono::steady_clock::now();
  auto s = client.AsyncGetAll(
      kTable,
      [&done, &scan_count](absl::flat_hash_map<std::string, std::string> &&result) {
        scan_count = result.size();
        done.fetch_add(1);
      });
  if (!s.ok()) {
    std::cerr << "AsyncGetAll failed: " << s.ToString() << std::endl;
    return 1;
  }
  while (done.load() == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  auto t_scan_end = std::chrono::steady_clock::now();

  // Lookup of a known key for first-read latency.
  std::atomic<int> lookup_done{0};
  bool lookup_present = false;
  auto t_lookup_start = std::chrono::steady_clock::now();
  s = client.AsyncGet(
      kTable,
      "actor_0",
      [&lookup_done, &lookup_present](Status, const boost::optional<std::string> &v) {
        lookup_present = v.has_value();
        lookup_done.fetch_add(1);
      });
  if (!s.ok()) {
    std::cerr << "AsyncGet failed: " << s.ToString() << std::endl;
    return 1;
  }
  while (lookup_done.load() == 0)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  auto t_lookup_end = std::chrono::steady_clock::now();

  std::cout << std::fixed << std::setprecision(6) << "{"
            << "\"open_seconds\": " << Elapsed(t_open_start, t_open_end) << ","
            << "\"scan_seconds\": " << Elapsed(t_scan_start, t_scan_end) << ","
            << "\"lookup_seconds\": " << Elapsed(t_lookup_start, t_lookup_end) << ","
            << "\"scan_count\": " << scan_count << ","
            << "\"lookup_present\": " << (lookup_present ? "true" : "false") << "}\n";
  return 0;
}

}  // namespace

int main(int argc, char **argv) {
  std::string mode;
  std::string db_dir;
  int num_keys = 10000;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--mode" && i + 1 < argc)
      mode = argv[++i];
    else if (a == "--db-dir" && i + 1 < argc)
      db_dir = argv[++i];
    else if (a == "--num-keys" && i + 1 < argc)
      num_keys = std::atoi(argv[++i]);
  }
  if (mode.empty() || db_dir.empty()) {
    std::cerr << "usage: recovery_bench --mode {populate|recover} --db-dir <path> "
                 "[--num-keys N]"
              << std::endl;
    return 2;
  }
  if (mode == "populate") return Populate(db_dir, num_keys);
  if (mode == "recover") return Recover(db_dir);
  std::cerr << "unknown mode: " << mode << std::endl;
  return 2;
}
