// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

// Phase 7 microbenchmark — RocksDbStoreClient (inline + offload) vs
// InMemoryStoreClient.
//
// Compares the two backends on the operations the GCS uses on the hot
// path: AsyncPut, AsyncGet, AsyncGetAll, AsyncGetKeys (prefix scan).
// Measures per-op end-to-end latency (issue → callback) and wall-clock
// throughput, computes p50/p95/p99 distributions, and emits a
// structured JSON report.
//
// **End-to-end latency.** Per-op samples capture time from the AsyncPut
// call site to the user-supplied callback firing. This is the metric
// the caller actually observes, and is the only fair comparison
// between the inline and offload paths (issue-only timing makes
// offload look ~5000x faster than it really is — that number is just
// the enqueue cost, not the work).
//
// **Three backends in one run** when --include-offload is passed:
//   in_memory, rocksdb_inline, rocksdb_offload.
//
// We deliberately skip Redis here. A like-for-like comparison against
// Redis would need a Redis container plus careful network-vs-local
// latency accounting, which the PLAN routes through the Docker
// Compose harness in Phase 7's "release-test tier". The cc_test layer
// here exists to give per-commit fast feedback on whether RocksDB's
// own performance characteristics match the REP's stated ranges.
//
// Usage (no line continuations to keep -Werror=comment happy):
//   bazel run --config=ci -c opt //rep-64-poc/harness/microbench:storage_microbench --
//   --include-offload
//   --io-pool-size 4
//   --output rep-64-poc/harness/microbench/results/<name>.json
//
// Build with -c opt to avoid debug-build noise; the numbers below are
// only meaningful from an opt-mode binary.

#include <algorithm>
#include <atomic>
#include <boost/asio.hpp>
#include <boost/optional.hpp>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/store_client/in_memory_store_client.h"
#include "ray/gcs/store_client/rocksdb_store_client.h"

namespace fs = std::filesystem;

namespace {

using ray::Status;
using ray::gcs::InMemoryStoreClient;
using ray::gcs::RocksDbStoreClient;
using ray::gcs::StoreClient;

constexpr int kKeyCount = 10000;    // total keys per backend
constexpr int kSampledOps = 10000;  // measure first N ops of each kind
const std::string kTable = "phase7";

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

struct LatencyStats {
  size_t count = 0;
  double mean_us = 0.0;
  double p50_us = 0.0;
  double p95_us = 0.0;
  double p99_us = 0.0;
  double min_us = 0.0;
  double max_us = 0.0;
};

LatencyStats Summarise(std::vector<double> samples_us) {
  LatencyStats s;
  s.count = samples_us.size();
  if (samples_us.empty()) return s;
  std::sort(samples_us.begin(), samples_us.end());
  s.min_us = samples_us.front();
  s.max_us = samples_us.back();
  double sum = 0;
  for (double v : samples_us) sum += v;
  s.mean_us = sum / static_cast<double>(samples_us.size());
  auto pct = [&](double p) {
    size_t idx = static_cast<size_t>(p * (samples_us.size() - 1) + 0.5);
    return samples_us[std::min(idx, samples_us.size() - 1)];
  };
  s.p50_us = pct(0.50);
  s.p95_us = pct(0.95);
  s.p99_us = pct(0.99);
  return s;
}

std::string StatsJson(const std::string &label,
                      const LatencyStats &s,
                      double aggregate_seconds,
                      int op_count) {
  std::ostringstream os;
  double ops_per_sec = aggregate_seconds > 0 ? op_count / aggregate_seconds : 0.0;
  os << "    \"" << label << "\": {\n"
     << "      \"count\": " << s.count << ",\n"
     << "      \"aggregate_seconds\": " << std::fixed << std::setprecision(4)
     << aggregate_seconds << ",\n"
     << "      \"ops_per_sec\": " << std::fixed << std::setprecision(1) << ops_per_sec
     << ",\n"
     << "      \"mean_us\": " << std::fixed << std::setprecision(2) << s.mean_us << ",\n"
     << "      \"p50_us\": " << s.p50_us << ",\n"
     << "      \"p95_us\": " << s.p95_us << ",\n"
     << "      \"p99_us\": " << s.p99_us << ",\n"
     << "      \"min_us\": " << s.min_us << ",\n"
     << "      \"max_us\": " << s.max_us << "\n"
     << "    }";
  return os.str();
}

double Elapsed(std::chrono::steady_clock::time_point a,
               std::chrono::steady_clock::time_point b) {
  return std::chrono::duration<double>(b - a).count();
}

// When true, the put/get loop blocks for each op's callback before
// issuing the next. Set via --sequential. Pipelined (false) saturates
// the offload pool's queue, which inflates tail latencies but maximises
// aggregate throughput. Sequential serialises in-flight ops to one,
// giving honest per-op e2e numbers for the offload path (which would
// otherwise pay queue-depth time).
bool g_sequential = false;

// Issue kKeyCount AsyncPut calls and measure per-op end-to-end latency
// (issue → callback) for the first kSampledOps. Returns: (samples,
// total wall-clock seconds for the whole run).
std::pair<std::vector<double>, double> RunPutBench(StoreClient &client,
                                                   IoFixture &io_fixture) {
  std::atomic<int> acked{0};
  // Per-op samples are written from inside the callbacks (which may run
  // on a pool thread for the offload backend), so the slot vector must
  // be sized up front and accessed by index.
  std::vector<double> samples(kSampledOps, 0.0);

  // Pre-warm the column family so the first Put doesn't include the
  // cf-create cost in its sample.
  std::atomic<int> warm_acked{0};
  client.AsyncPut(kTable,
                  "warmup",
                  "warmup",
                  true,
                  {[&warm_acked](bool) { warm_acked.fetch_add(1); }, io_fixture.io()});
  while (warm_acked.load() == 0)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

  auto wall_start = std::chrono::steady_clock::now();
  for (int i = 0; i < kKeyCount; ++i) {
    std::string key = "k" + std::to_string(i);
    std::string value = "v" + std::to_string(i) + "-payload-padding-data";
    int target = i + 1;
    auto op_start = std::chrono::steady_clock::now();
    client.AsyncPut(
        kTable,
        key,
        value,
        true,
        {[op_start, i, &samples, &acked](bool) {
           if (i < kSampledOps) {
             auto cb_end = std::chrono::steady_clock::now();
             samples[i] =
                 std::chrono::duration_cast<std::chrono::nanoseconds>(cb_end - op_start)
                     .count() /
                 1000.0;
           }
           acked.fetch_add(1);
         },
         io_fixture.io()});
    if (g_sequential) {
      while (acked.load() < target) {
        std::this_thread::sleep_for(std::chrono::microseconds(50));
      }
    }
  }
  while (acked.load() < kKeyCount) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  auto wall_end = std::chrono::steady_clock::now();
  return {std::move(samples), Elapsed(wall_start, wall_end)};
}

std::pair<std::vector<double>, double> RunGetBench(StoreClient &client,
                                                   IoFixture &io_fixture) {
  std::atomic<int> acked{0};
  std::vector<double> samples(kSampledOps, 0.0);

  // Random read pattern over the kKeyCount keys we just inserted.
  std::mt19937 rng(0xCAFEBABE);
  std::uniform_int_distribution<int> dist(0, kKeyCount - 1);

  auto wall_start = std::chrono::steady_clock::now();
  for (int i = 0; i < kKeyCount; ++i) {
    std::string key = "k" + std::to_string(dist(rng));
    auto op_start = std::chrono::steady_clock::now();
    client.AsyncGet(
        kTable,
        key,
        {[op_start, i, &acked, &samples](Status, std::optional<std::string>) {
           if (i < kSampledOps) {
             auto cb_end = std::chrono::steady_clock::now();
             samples[i] =
                 std::chrono::duration_cast<std::chrono::nanoseconds>(cb_end - op_start)
                     .count() /
                 1000.0;
           }
           acked.fetch_add(1);
         },
         io_fixture.io()});
  }
  while (acked.load() < kKeyCount) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  auto wall_end = std::chrono::steady_clock::now();
  return {std::move(samples), Elapsed(wall_start, wall_end)};
}

double RunGetAllBench(StoreClient &client, IoFixture &io_fixture, size_t *result_size) {
  std::atomic<int> done{0};
  size_t got = 0;
  auto t0 = std::chrono::steady_clock::now();
  client.AsyncGetAll(
      kTable,
      {[&done, &got](absl::flat_hash_map<std::string, std::string> result) {
         got = result.size();
         done.fetch_add(1);
       },
       io_fixture.io()});
  while (done.load() == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  auto t1 = std::chrono::steady_clock::now();
  *result_size = got;
  return Elapsed(t0, t1);
}

double RunGetKeysBench(StoreClient &client,
                       IoFixture &io_fixture,
                       const std::string &prefix,
                       size_t *result_size) {
  std::atomic<int> done{0};
  size_t got = 0;
  auto t0 = std::chrono::steady_clock::now();
  client.AsyncGetKeys(kTable,
                      prefix,
                      {[&done, &got](std::vector<std::string> result) {
                         got = result.size();
                         done.fetch_add(1);
                       },
                       io_fixture.io()});
  while (done.load() == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  auto t1 = std::chrono::steady_clock::now();
  *result_size = got;
  return Elapsed(t0, t1);
}

struct BackendResult {
  std::string name;
  LatencyStats put;
  double put_total_seconds;
  LatencyStats get;
  double get_total_seconds;
  double get_all_seconds;
  size_t get_all_count;
  double get_keys_seconds;
  size_t get_keys_count;
};

BackendResult BenchInMemory(IoFixture &io_fixture) {
  std::cerr << "==> InMemoryStoreClient" << std::endl;
  InMemoryStoreClient client;
  BackendResult br;
  br.name = "in_memory";
  auto [put_samples, put_total] = RunPutBench(client, io_fixture);
  br.put = Summarise(std::move(put_samples));
  br.put_total_seconds = put_total;
  std::cerr << "    Put done in " << put_total << "s" << std::endl;

  auto [get_samples, get_total] = RunGetBench(client, io_fixture);
  br.get = Summarise(std::move(get_samples));
  br.get_total_seconds = get_total;
  std::cerr << "    Get done in " << get_total << "s" << std::endl;

  br.get_all_seconds = RunGetAllBench(client, io_fixture, &br.get_all_count);
  std::cerr << "    GetAll done in " << br.get_all_seconds << "s (" << br.get_all_count
            << " entries)" << std::endl;

  br.get_keys_seconds = RunGetKeysBench(client, io_fixture, "k1", &br.get_keys_count);
  std::cerr << "    GetKeys('k1') done in " << br.get_keys_seconds << "s ("
            << br.get_keys_count << " keys)" << std::endl;
  return br;
}

BackendResult BenchRocksDb(IoFixture &io_fixture,
                           const std::string &db_path,
                           bool offload_io,
                           std::size_t io_pool_size,
                           std::size_t strand_buckets) {
  std::cerr << "==> RocksDbStoreClient (" << (offload_io ? "offload" : "inline")
            << ") at " << db_path << std::endl;
  RocksDbStoreClient client(io_fixture.io(),
                            db_path,
                            /*expected_cluster_id=*/"",
                            offload_io,
                            io_pool_size,
                            strand_buckets);
  BackendResult br;
  br.name = offload_io ? "rocksdb_offload" : "rocksdb_inline";
  auto [put_samples, put_total] = RunPutBench(client, io_fixture);
  br.put = Summarise(std::move(put_samples));
  br.put_total_seconds = put_total;
  std::cerr << "    Put done in " << put_total << "s" << std::endl;

  auto [get_samples, get_total] = RunGetBench(client, io_fixture);
  br.get = Summarise(std::move(get_samples));
  br.get_total_seconds = get_total;
  std::cerr << "    Get done in " << get_total << "s" << std::endl;

  br.get_all_seconds = RunGetAllBench(client, io_fixture, &br.get_all_count);
  std::cerr << "    GetAll done in " << br.get_all_seconds << "s (" << br.get_all_count
            << " entries)" << std::endl;

  br.get_keys_seconds = RunGetKeysBench(client, io_fixture, "k1", &br.get_keys_count);
  std::cerr << "    GetKeys('k1') done in " << br.get_keys_seconds << "s ("
            << br.get_keys_count << " keys)" << std::endl;
  return br;
}

void EmitJson(std::ostream &os, const std::vector<BackendResult> &results) {
  os << "{\n";
  os << "  \"schema_version\": 1,\n";
  os << "  \"key_count\": " << kKeyCount << ",\n";
  os << "  \"sampled_ops_per_kind\": " << kSampledOps << ",\n";
  os << "  \"backends\": {\n";
  for (size_t i = 0; i < results.size(); ++i) {
    const auto &r = results[i];
    os << "    \"" << r.name << "\": {\n";
    os << StatsJson("put", r.put, r.put_total_seconds, kKeyCount) << ",\n";
    os << StatsJson("get", r.get, r.get_total_seconds, kKeyCount) << ",\n";
    os << "      \"get_all_seconds\": " << std::fixed << std::setprecision(6)
       << r.get_all_seconds << ",\n";
    os << "      \"get_all_count\": " << r.get_all_count << ",\n";
    os << "      \"get_keys_seconds\": " << r.get_keys_seconds << ",\n";
    os << "      \"get_keys_count\": " << r.get_keys_count << "\n";
    os << "    }";
    if (i + 1 < results.size()) os << ",";
    os << "\n";
  }
  os << "  }\n";
  os << "}\n";
}

}  // namespace

int main(int argc, char **argv) {
  std::string output_path;
  // IMPORTANT: --db-dir defaults to a fresh subdir under $HOME, NOT
  // /tmp. On many Linux systems /tmp is tmpfs (RAM-backed); a tmpfs
  // returns immediately from fsync, which makes RocksDB's sync=true
  // numbers look ~100x faster than they would on real ext4. Phase 1's
  // probe verified this VM's /home is on honest ext4 at fsync p50
  // = 3.6 ms, so that's the production-realistic substrate.
  std::string db_dir =
      std::string(std::getenv("HOME") ? std::getenv("HOME") : "/var/tmp") +
      "/.cache/rep64-microbench";
  bool include_offload = false;
  std::size_t io_pool_size = 4;
  std::size_t strand_buckets = 64;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--output" && i + 1 < argc) {
      output_path = argv[++i];
    } else if (a == "--db-dir" && i + 1 < argc) {
      db_dir = argv[++i];
    } else if (a == "--include-offload") {
      include_offload = true;
    } else if (a == "--io-pool-size" && i + 1 < argc) {
      io_pool_size = static_cast<std::size_t>(std::stoul(argv[++i]));
    } else if (a == "--strand-buckets" && i + 1 < argc) {
      strand_buckets = static_cast<std::size_t>(std::stoul(argv[++i]));
    } else if (a == "--sequential") {
      g_sequential = true;
    }
  }

  IoFixture io_fixture;
  std::vector<BackendResult> results;

  results.push_back(BenchInMemory(io_fixture));

  // Use a fresh subdir under db_dir for each RocksDB run so prior data
  // does not bias memtable warmth between runs. db_dir defaults to a
  // honest-fsync substrate (see the comment near argv parsing).
  std::random_device rd;
  std::mt19937_64 rng(rd());

  fs::path inline_db = fs::path(db_dir) / ("phase7-inline-" + std::to_string(rng()));
  fs::create_directories(inline_db);
  std::cerr << "RocksDB inline working under: " << inline_db << std::endl;
  results.push_back(BenchRocksDb(io_fixture,
                                 inline_db.string(),
                                 /*offload_io=*/false,
                                 io_pool_size,
                                 strand_buckets));
  fs::remove_all(inline_db);

  if (include_offload) {
    fs::path offload_db = fs::path(db_dir) / ("phase7-offload-" + std::to_string(rng()));
    fs::create_directories(offload_db);
    std::cerr << "RocksDB offload working under: " << offload_db
              << " (pool=" << io_pool_size << ", strand_buckets=" << strand_buckets << ")"
              << std::endl;
    results.push_back(BenchRocksDb(io_fixture,
                                   offload_db.string(),
                                   /*offload_io=*/true,
                                   io_pool_size,
                                   strand_buckets));
    fs::remove_all(offload_db);
  }

  EmitJson(std::cout, results);
  if (!output_path.empty()) {
    fs::create_directories(fs::path(output_path).parent_path());
    std::ofstream f(output_path);
    EmitJson(f, results);
    std::cerr << "wrote " << output_path << std::endl;
  }
  return 0;
}
