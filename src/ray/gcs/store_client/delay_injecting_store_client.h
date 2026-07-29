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

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <system_error>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "boost/asio/executor_work_guard.hpp"
#include "boost/asio/io_context.hpp"
#include "boost/asio/post.hpp"
#include "boost/asio/steady_timer.hpp"
#include "boost/asio/thread_pool.hpp"
#include "ray/gcs/store_client/store_client.h"

namespace ray {
namespace gcs {

/// TEST-ONLY (REP-64 provenance). Wraps a StoreClient and injects latency at the
/// storage layer -- i.e. exactly where the RocksDB backend introduces it.
///
/// Why this exists: the original REP-64 harness injected delay into one *logical
/// path* (e.g. only the node-death publish) while the rest of the GCS stayed
/// fast. Real RocksDB does not behave that way, so that design structurally
/// could not observe two of its four delay mechanisms. This decorator models all
/// four from a single place:
///
///   1. per-write WAL fsync latency, on *every* table   -> WRITE_DELAY_MS
///   2. read latency                                     -> READ_DELAY_MS
///   3. bounded shared I/O pool contention (reads queue
///      behind fsyncing writes; default pool is 4)       -> IO_CONCURRENCY
///   4. scoping any of the above to specific tables      -> DELAY_TABLES
///
/// Env knobs (all inert unless set, so default behavior is identical to
/// upstream and this decorator is not even installed):
///
///   RAY_TESTING_GCS_STORE_WRITE_DELAY_MS  delay before each Put/Delete/BatchDelete
///   RAY_TESTING_GCS_STORE_READ_DELAY_MS   delay before each Get/GetAll/MultiGet/
///                                         GetKeys/Exists
///   RAY_TESTING_GCS_STORE_DELAY_TABLES    comma-separated table filter; unset =>
///                                         every table
///   RAY_TESTING_GCS_STORE_DELAY_TRIGGER_FILE  injection active only while this
///                                         path exists (armed by the harness right
///                                         before the node kill), so cluster startup
///                                         is never slowed
///   RAY_TESTING_GCS_STORE_IO_CONCURRENCY  >0 => route every operation through a
///                                         bounded pool of N threads, each op
///                                         *occupying* its thread for the delay
///                                         (models fsync holding a pool slot).
///                                         Unset/0 => timer-based delay with
///                                         unbounded concurrency, which isolates
///                                         pure latency from contention.
class DelayInjectingStoreClient : public StoreClient {
 public:
  struct Config {
    int64_t write_delay_ms = 0;
    int64_t read_delay_ms = 0;
    int64_t io_concurrency = 0;
    // Empty => apply to every table.
    absl::flat_hash_set<std::string> tables;
    // When non-empty, injection is active only while this file exists. The
    // harness creates it immediately before the node kill, so cluster startup
    // (node registration, actor creation) runs at full speed and only the
    // death -> notification -> reconstruction phase under study is slowed.
    // Without this, a "stalled forever" arm would merely prevent the cluster
    // from booting and prove nothing.
    std::string trigger_file;

    bool Enabled() const { return write_delay_ms > 0 || read_delay_ms > 0; }
  };

  /// Parse the env knobs once. Returns the same Config for the process lifetime.
  static const Config &EnvConfig();

  /// True when the env asks for any injection, i.e. when the decorator should be
  /// installed at all.
  static bool EnabledFromEnv() { return EnvConfig().Enabled(); }

  DelayInjectingStoreClient(std::shared_ptr<StoreClient> delegate, Config config);

  ~DelayInjectingStoreClient() override;

  void AsyncPut(const std::string &table_name,
                const std::string &key,
                std::string data,
                bool overwrite,
                Postable<void(bool)> callback) override;

  void AsyncGet(const std::string &table_name,
                const std::string &key,
                ToPostable<rpc::OptionalItemCallback<std::string>> callback) override;

  void AsyncGetAll(
      const std::string &table_name,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  void AsyncMultiGet(
      const std::string &table_name,
      const std::vector<std::string> &keys,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  void AsyncDelete(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

  void AsyncBatchDelete(const std::string &table_name,
                        const std::vector<std::string> &keys,
                        Postable<void(int64_t)> callback) override;

  void AsyncGetNextJobID(Postable<void(int)> callback) override;

  void AsyncGetKeys(const std::string &table_name,
                    const std::string &prefix,
                    Postable<void(std::vector<std::string>)> callback) override;

  void AsyncExists(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

 private:
  bool AppliesTo(const std::string &table_name) const {
    return config_.tables.empty() || config_.tables.contains(table_name);
  }

  bool InjectionArmed() const {
    if (config_.trigger_file.empty()) {
      return true;
    }
    std::error_code ec;
    return std::filesystem::exists(config_.trigger_file, ec);
  }

  int64_t DelayForMs(const std::string &table_name, bool is_write) const {
    if (!AppliesTo(table_name) || !InjectionArmed()) {
      return 0;
    }
    return is_write ? config_.write_delay_ms : config_.read_delay_ms;
  }

  /// Run `work` after the configured delay for this (table, op-kind).
  ///
  /// Bounded mode routes *every* operation through the pool -- including
  /// zero-delay ones -- because in real RocksDB every operation contends for the
  /// same threads; that is the whole point of mechanism 3.
  template <typename Work>
  void Schedule(const std::string &table_name, bool is_write, Work &&work) {
    const int64_t delay_ms = DelayForMs(table_name, is_write);

    if (config_.io_concurrency > 0) {
      boost::asio::post(*pool_,
                        [delay_ms, work = std::forward<Work>(work)]() mutable {
                          if (delay_ms > 0) {
                            // Occupy this pool thread for the delay, exactly as a
                            // blocking fsync occupies a RocksDB I/O thread.
                            std::this_thread::sleep_for(
                                std::chrono::milliseconds(delay_ms));
                          }
                          work();
                        });
      return;
    }

    if (delay_ms <= 0) {
      work();
      return;
    }

    // Unbounded mode: a timer parks the operation without holding a thread, so
    // arbitrarily many operations can be "slow" at once. This isolates latency
    // (mechanism 1/2) from pool contention (mechanism 3).
    auto timer = std::make_shared<boost::asio::steady_timer>(
        timer_io_context_, std::chrono::milliseconds(delay_ms));
    timer->async_wait([timer, work = std::forward<Work>(work)](
                          const boost::system::error_code &) mutable { work(); });
  }

  std::shared_ptr<StoreClient> delegate_;
  Config config_;

  // Bounded mode: models the RocksDB shared I/O pool.
  std::unique_ptr<boost::asio::thread_pool> pool_;

  // Unbounded mode: drives the delay timers.
  boost::asio::io_context timer_io_context_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_;
  std::thread timer_thread_;
};

}  // namespace gcs
}  // namespace ray
