// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

// Phase 7 microbenchmark — RocksDbStoreClient vs InMemoryStoreClient.
//
// Compares the two backends on the operations the GCS uses on the hot
// path: AsyncPut, AsyncGet, AsyncGetAll, AsyncGetKeys (prefix scan).
// Measures per-op wall-clock, computes p50/p95/p99 distributions, and
// emits a structured JSON report.
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

// Issue kKeyCount AsyncPut calls and measure per-op wall-clock for the
// first kSampledOps. Returns: (samples, total wall-clock seconds).
std::pair<std::vector<double>, double> RunPutBench(StoreClient &client) {
  std::atomic<int> acked{0};
  std::vector<double> samples;
  samples.reserve(kSampledOps);

  // Pre-warm the column family so the first Put doesn't include the
  // cf-create cost in its sample.
  std::atomic<int> warm_acked{0};
  Status s = client.AsyncPut(
      kTable, "warmup", "warmup", true, [&warm_acked](bool) { warm_acked.fetch_add(1); });
  while (warm_acked.load() == 0)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  (void)s;

  auto wall_start = std::chrono::steady_clock::now();
  for (int i = 0; i < kKeyCount; ++i) {
    std::string key = "k" + std::to_string(i);
    std::string value = "v" + std::to_string(i) + "-payload-padding-data";
    auto op_start = std::chrono::steady_clock::now();
    Status ps =
        client.AsyncPut(kTable, key, value, true, [&acked](bool) { acked.fetch_add(1); });
    auto op_end = std::chrono::steady_clock::now();
    if (i < kSampledOps) {
      samples.push_back(
          std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start)
              .count() /
          1000.0);
    }
    if (!ps.ok()) {
      std::cerr << "AsyncPut failed at i=" << i << ": " << ps.ToString() << std::endl;
      std::exit(1);
    }
  }
  while (acked.load() < kKeyCount) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  auto wall_end = std::chrono::steady_clock::now();
  return {std::move(samples), Elapsed(wall_start, wall_end)};
}

std::pair<std::vector<double>, double> RunGetBench(StoreClient &client) {
  std::atomic<int> acked{0};
  std::vector<double> samples;
  samples.reserve(kSampledOps);

  // Random read pattern over the kKeyCount keys we just inserted.
  std::mt19937 rng(0xCAFEBABE);
  std::uniform_int_distribution<int> dist(0, kKeyCount - 1);

  auto wall_start = std::chrono::steady_clock::now();
  for (int i = 0; i < kKeyCount; ++i) {
    std::string key = "k" + std::to_string(dist(rng));
    auto op_start = std::chrono::steady_clock::now();
    Status gs = client.AsyncGet(
        kTable, key, [&acked](Status, const boost::optional<std::string> &) {
          acked.fetch_add(1);
        });
    auto op_end = std::chrono::steady_clock::now();
    if (i < kSampledOps) {
      samples.push_back(
          std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start)
              .count() /
          1000.0);
    }
    if (!gs.ok()) {
      std::cerr << "AsyncGet failed at i=" << i << ": " << gs.ToString() << std::endl;
      std::exit(1);
    }
  }
  while (acked.load() < kKeyCount) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  auto wall_end = std::chrono::steady_clock::now();
  return {std::move(samples), Elapsed(wall_start, wall_end)};
}

double RunGetAllBench(StoreClient &client, size_t *result_size) {
  std::atomic<int> done{0};
  size_t got = 0;
  auto t0 = std::chrono::steady_clock::now();
  Status s = client.AsyncGetAll(
      kTable, [&done, &got](absl::flat_hash_map<std::string, std::string> &&result) {
        got = result.size();
        done.fetch_add(1);
      });
  if (!s.ok()) {
    std::cerr << "AsyncGetAll failed: " << s.ToString() << std::endl;
    std::exit(1);
  }
  while (done.load() == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  auto t1 = std::chrono::steady_clock::now();
  *result_size = got;
  return Elapsed(t0, t1);
}

double RunGetKeysBench(StoreClient &client,
                       const std::string &prefix,
                       size_t *result_size) {
  std::atomic<int> done{0};
  size_t got = 0;
  auto t0 = std::chrono::steady_clock::now();
  Status s =
      client.AsyncGetKeys(kTable, prefix, [&done, &got](std::vector<std::string> result) {
        got = result.size();
        done.fetch_add(1);
      });
  if (!s.ok()) {
    std::cerr << "AsyncGetKeys failed: " << s.ToString() << std::endl;
    std::exit(1);
  }
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
  InMemoryStoreClient client(io_fixture.io());
  BackendResult br;
  br.name = "in_memory";
  auto [put_samples, put_total] = RunPutBench(client);
  br.put = Summarise(std::move(put_samples));
  br.put_total_seconds = put_total;
  std::cerr << "    Put done in " << put_total << "s" << std::endl;

  auto [get_samples, get_total] = RunGetBench(client);
  br.get = Summarise(std::move(get_samples));
  br.get_total_seconds = get_total;
  std::cerr << "    Get done in " << get_total << "s" << std::endl;

  br.get_all_seconds = RunGetAllBench(client, &br.get_all_count);
  std::cerr << "    GetAll done in " << br.get_all_seconds << "s (" << br.get_all_count
            << " entries)" << std::endl;

  br.get_keys_seconds = RunGetKeysBench(client, "k1", &br.get_keys_count);
  std::cerr << "    GetKeys('k1') done in " << br.get_keys_seconds << "s ("
            << br.get_keys_count << " keys)" << std::endl;
  return br;
}

BackendResult BenchRocksDb(IoFixture &io_fixture, const std::string &db_path) {
  std::cerr << "==> RocksDbStoreClient at " << db_path << std::endl;
  RocksDbStoreClient client(io_fixture.io(), db_path, /*expected_cluster_id=*/"");
  BackendResult br;
  br.name = "rocksdb";
  auto [put_samples, put_total] = RunPutBench(client);
  br.put = Summarise(std::move(put_samples));
  br.put_total_seconds = put_total;
  std::cerr << "    Put done in " << put_total << "s" << std::endl;

  auto [get_samples, get_total] = RunGetBench(client);
  br.get = Summarise(std::move(get_samples));
  br.get_total_seconds = get_total;
  std::cerr << "    Get done in " << get_total << "s" << std::endl;

  br.get_all_seconds = RunGetAllBench(client, &br.get_all_count);
  std::cerr << "    GetAll done in " << br.get_all_seconds << "s (" << br.get_all_count
            << " entries)" << std::endl;

  br.get_keys_seconds = RunGetKeysBench(client, "k1", &br.get_keys_count);
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
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--output" && i + 1 < argc) {
      output_path = argv[++i];
    } else if (a == "--db-dir" && i + 1 < argc) {
      db_dir = argv[++i];
    }
  }

  IoFixture io_fixture;
  std::vector<BackendResult> results;

  results.push_back(BenchInMemory(io_fixture));

  // Use a fresh subdir under db_dir for the RocksDB run so prior data
  // does not bias memtable warmth between runs. db_dir defaults to a
  // honest-fsync substrate (see the comment near argv parsing).
  std::random_device rd;
  std::mt19937_64 rng(rd());
  fs::path db_path = fs::path(db_dir) / ("phase7-" + std::to_string(rng()));
  fs::create_directories(db_path);
  std::cerr << "RocksDB working under: " << db_path << std::endl;
  results.push_back(BenchRocksDb(io_fixture, db_path.string()));
  fs::remove_all(db_path);

  EmitJson(std::cout, results);
  if (!output_path.empty()) {
    fs::create_directories(fs::path(output_path).parent_path());
    std::ofstream f(output_path);
    EmitJson(f, results);
    std::cerr << "wrote " << output_path << std::endl;
  }
  return 0;
}
