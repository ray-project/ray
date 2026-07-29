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

#include "ray/gcs/store_client/delay_injecting_store_client.h"

#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/util/logging.h"

namespace ray {
namespace gcs {

namespace {

int64_t EnvInt64(const char *name) {
  const char *value = std::getenv(name);
  if (value == nullptr) {
    return 0;
  }
  return std::atoll(value);
}

absl::flat_hash_set<std::string> EnvTableSet(const char *name) {
  absl::flat_hash_set<std::string> tables;
  const char *value = std::getenv(name);
  if (value == nullptr) {
    return tables;
  }
  std::string spec(value);
  std::string::size_type start = 0;
  while (start <= spec.size()) {
    const auto comma = spec.find(',', start);
    const auto end = comma == std::string::npos ? spec.size() : comma;
    auto token = spec.substr(start, end - start);
    const auto first = token.find_first_not_of(" \t");
    if (first != std::string::npos) {
      const auto last = token.find_last_not_of(" \t");
      tables.insert(token.substr(first, last - first + 1));
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  return tables;
}

}  // namespace

const DelayInjectingStoreClient::Config &DelayInjectingStoreClient::EnvConfig() {
  static const Config *const kConfig = [] {
    auto *config = new Config();
    config->write_delay_ms = EnvInt64("RAY_TESTING_GCS_STORE_WRITE_DELAY_MS");
    config->read_delay_ms = EnvInt64("RAY_TESTING_GCS_STORE_READ_DELAY_MS");
    config->io_concurrency = EnvInt64("RAY_TESTING_GCS_STORE_IO_CONCURRENCY");
    config->tables = EnvTableSet("RAY_TESTING_GCS_STORE_DELAY_TABLES");
    const char *trigger = std::getenv("RAY_TESTING_GCS_STORE_DELAY_TRIGGER_FILE");
    config->trigger_file = trigger != nullptr ? std::string(trigger) : std::string();
    return config;
  }();
  return *kConfig;
}

DelayInjectingStoreClient::DelayInjectingStoreClient(
    std::shared_ptr<StoreClient> delegate, Config config)
    : delegate_(std::move(delegate)),
      config_(std::move(config)),
      work_guard_(boost::asio::make_work_guard(timer_io_context_)) {
  if (config_.io_concurrency > 0) {
    pool_ = std::make_unique<boost::asio::thread_pool>(
        static_cast<std::size_t>(config_.io_concurrency));
  }
  timer_thread_ = std::thread([this]() { timer_io_context_.run(); });

  RAY_LOG(WARNING) << "TEST-ONLY DelayInjectingStoreClient installed: write_delay_ms="
                   << config_.write_delay_ms << " read_delay_ms=" << config_.read_delay_ms
                   << " io_concurrency=" << config_.io_concurrency
                   << " tables=" << (config_.tables.empty() ? "<all>" : "<filtered>")
                   << " trigger_file="
                   << (config_.trigger_file.empty() ? "<always-on>"
                                                    : config_.trigger_file);
}

DelayInjectingStoreClient::~DelayInjectingStoreClient() {
  if (pool_ != nullptr) {
    pool_->join();
  }
  work_guard_.reset();
  timer_io_context_.stop();
  if (timer_thread_.joinable()) {
    timer_thread_.join();
  }
}

void DelayInjectingStoreClient::AsyncPut(const std::string &table_name,
                                         const std::string &key,
                                         std::string data,
                                         bool overwrite,
                                         Postable<void(bool)> callback) {
  Schedule(table_name,
           /*is_write=*/true,
           [this,
            table_name,
            key,
            data = std::move(data),
            overwrite,
            callback = std::move(callback)]() mutable {
             delegate_->AsyncPut(
                 table_name, key, std::move(data), overwrite, std::move(callback));
           });
}

void DelayInjectingStoreClient::AsyncGet(
    const std::string &table_name,
    const std::string &key,
    ToPostable<rpc::OptionalItemCallback<std::string>> callback) {
  Schedule(table_name,
           /*is_write=*/false,
           [this, table_name, key, callback = std::move(callback)]() mutable {
             delegate_->AsyncGet(table_name, key, std::move(callback));
           });
}

void DelayInjectingStoreClient::AsyncGetAll(
    const std::string &table_name,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  Schedule(table_name,
           /*is_write=*/false,
           [this, table_name, callback = std::move(callback)]() mutable {
             delegate_->AsyncGetAll(table_name, std::move(callback));
           });
}

void DelayInjectingStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  Schedule(table_name,
           /*is_write=*/false,
           [this, table_name, keys, callback = std::move(callback)]() mutable {
             delegate_->AsyncMultiGet(table_name, keys, std::move(callback));
           });
}

void DelayInjectingStoreClient::AsyncDelete(const std::string &table_name,
                                            const std::string &key,
                                            Postable<void(bool)> callback) {
  Schedule(table_name,
           /*is_write=*/true,
           [this, table_name, key, callback = std::move(callback)]() mutable {
             delegate_->AsyncDelete(table_name, key, std::move(callback));
           });
}

void DelayInjectingStoreClient::AsyncBatchDelete(const std::string &table_name,
                                                 const std::vector<std::string> &keys,
                                                 Postable<void(int64_t)> callback) {
  Schedule(table_name,
           /*is_write=*/true,
           [this, table_name, keys, callback = std::move(callback)]() mutable {
             delegate_->AsyncBatchDelete(table_name, keys, std::move(callback));
           });
}

void DelayInjectingStoreClient::AsyncGetNextJobID(Postable<void(int)> callback) {
  // Counter increment-and-persist: treated as a write, with no table name to
  // filter on (so a table filter never selects it).
  Schedule("JobCounter",
           /*is_write=*/true,
           [this, callback = std::move(callback)]() mutable {
             delegate_->AsyncGetNextJobID(std::move(callback));
           });
}

void DelayInjectingStoreClient::AsyncGetKeys(
    const std::string &table_name,
    const std::string &prefix,
    Postable<void(std::vector<std::string>)> callback) {
  Schedule(table_name,
           /*is_write=*/false,
           [this, table_name, prefix, callback = std::move(callback)]() mutable {
             delegate_->AsyncGetKeys(table_name, prefix, std::move(callback));
           });
}

void DelayInjectingStoreClient::AsyncExists(const std::string &table_name,
                                            const std::string &key,
                                            Postable<void(bool)> callback) {
  Schedule(table_name,
           /*is_write=*/false,
           [this, table_name, key, callback = std::move(callback)]() mutable {
             delegate_->AsyncExists(table_name, key, std::move(callback));
           });
}

}  // namespace gcs
}  // namespace ray
