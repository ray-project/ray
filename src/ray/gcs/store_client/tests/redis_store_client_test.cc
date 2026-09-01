// Copyright 2017 The Ray Authors.
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

#include "ray/gcs/store_client/redis_store_client.h"

#include <boost/optional/optional_io.hpp>
#include <chrono>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/store_client/tests/store_client_test_base.h"
#include "ray/util/clock.h"
#include "ray/util/network_util.h"
#include "ray/util/path_utils.h"
#include "ray/util/raii.h"

using namespace std::chrono_literals;  // NOLINT
namespace ray {

namespace gcs {

class RedisStoreClientTest : public StoreClientTestBase {
 public:
  RedisStoreClientTest() {
    if (std::getenv("REDIS_CHAOS") != nullptr) {
      ::RayConfig::instance().num_redis_request_retries() = 1000;
      ::RayConfig::instance().redis_retry_base_ms() = 10;
      ::RayConfig::instance().redis_retry_max_ms() = 100;
    }
  }

  virtual ~RedisStoreClientTest() {}

  static void SetUpTestCase() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  static void TearDownTestCase() { TestSetupUtil::ShutDownRedisServers(); }

  void SetUp() override {
    auto port = TEST_REDIS_SERVER_PORTS.front();
    TestSetupUtil::FlushRedisServer(port);
    StoreClientTestBase::SetUp();
    if (std::getenv("REDIS_CHAOS") != nullptr) {
      t_ = std::make_unique<std::thread>([this, port]() {
        while (!stopped_) {
          TestSetupUtil::ExecuteRedisCmd(port, {"REPLICAOF", "localhost", "1234"});
          std::this_thread::sleep_for(50ms);
          TestSetupUtil::ExecuteRedisCmd(port, {"REPLICAOF", "NO", "ONE"});
          std::this_thread::sleep_for(200ms);
        }
      });
    }
  }

  void TearDown() override {
    stopped_ = true;
    if (t_) {
      t_->join();
    }
    StoreClientTestBase::TearDown();
  }

  void InitStoreClient() override {
    auto &io_context = *io_service_pool_->Get();
    RedisClientOptions options{"127.0.0.1", TEST_REDIS_SERVER_PORTS.front()};
    store_client_ = std::make_shared<RedisStoreClient>(io_context, options, clock_);
  }

 protected:
  ray::Clock clock_;
  std::unique_ptr<std::thread> t_;
  std::atomic<bool> stopped_ = false;
};

TEST_F(RedisStoreClientTest, AsyncPutAndAsyncGetTest) { TestAsyncPutAndAsyncGet(); }

TEST_F(RedisStoreClientTest, AsyncGetAllAndBatchDeleteTest) {
  TestAsyncGetAllAndBatchDelete();
}

TEST_F(RedisStoreClientTest, BasicSimple) {
  // Send 100 times write and then read
  auto cnt = std::make_shared<std::atomic<size_t>>(0);
  for (size_t i = 0; i < 100; ++i) {
    for (size_t j = 0; j < 20; ++j) {
      ++*cnt;
      store_client_->AsyncPut("T",
                              absl::StrCat("A", std::to_string(j)),
                              std::to_string(i),
                              true,
                              {[i, cnt](auto r) {
                                 --*cnt;
                                 ASSERT_TRUE((i == 0 && r) || (i != 0 && !r));
                               },
                               *io_service_pool_->Get()});
    }
  }
  for (size_t j = 0; j < 20; ++j) {
    ++*cnt;
    store_client_->AsyncGet("T",
                            absl::StrCat("A", std::to_string(j)),
                            {[cnt](auto s, auto r) {
                               --*cnt;
                               ASSERT_TRUE(r.has_value());
                               ASSERT_EQ(*r, "99");
                             },
                             *io_service_pool_->Get()});
  }
  ASSERT_TRUE(WaitForCondition([cnt]() { return *cnt == 0; }, 5000));
}

TEST_F(RedisStoreClientTest, Complicated) {
  int window = 10;
  std::atomic<size_t> finished{0};
  std::atomic<size_t> sent{0};

  for (int i = 0; i < 1000; i += window) {
    std::vector<std::string> keys;
    for (int j = i; j < i + window; ++j) {
      ++sent;
      RAY_LOG(INFO) << "S AsyncPut: " << ("P_" + std::to_string(j));
      store_client_->AsyncPut("N",
                              "P_" + std::to_string(j),
                              std::to_string(j),
                              true,
                              {[&finished, j](auto r) mutable {
                                 RAY_LOG(INFO)
                                     << "F AsyncPut: " << ("P_" + std::to_string(j));
                                 ++finished;
                                 ASSERT_TRUE(r);
                               },
                               *io_service_pool_->Get()});
      keys.push_back(std::to_string(j));
    }

    std::vector<std::string> p_keys;
    for (auto &key : keys) {
      p_keys.push_back("P_" + key);
    }

    std::vector<std::string> n_keys;
    for (auto &key : keys) {
      n_keys.push_back("N_" + key);
    }

    ++sent;
    RAY_LOG(INFO) << "S AsyncMultiGet: " << absl::StrJoin(p_keys, ",");
    store_client_->AsyncMultiGet(
        "N",
        p_keys,
        {[&finished, i, keys, window, &sent, p_keys, n_keys, this](
             absl::flat_hash_map<std::string, std::string> m) mutable -> void {
           RAY_LOG(INFO) << "F SendAsyncMultiGet: " << absl::StrJoin(p_keys, ",");
           ++finished;
           ASSERT_EQ(keys.size(), m.size());
           for (auto &key : keys) {
             ASSERT_EQ(m["P_" + key], key);
           }

           if ((i / window) % 2 == 0) {
             // Delete non exist keys
             for (size_t jj = 0; jj < keys.size(); ++jj) {
               ++sent;
               RAY_LOG(INFO) << "S AsyncDelete: " << n_keys[jj];
               store_client_->AsyncDelete("N",
                                          n_keys[jj],
                                          {[&finished, n_keys, jj](auto b) mutable {
                                             RAY_LOG(INFO)
                                                 << "F AsyncDelete: " << n_keys[jj];
                                             ++finished;
                                             ASSERT_FALSE(b);
                                           },
                                           *this->io_service_pool_->Get()});

               ++sent;
               RAY_LOG(INFO) << "S AsyncExists: " << p_keys[jj];
               store_client_->AsyncExists("N",
                                          p_keys[jj],
                                          {[&finished, p_keys, jj](auto b) mutable {
                                             RAY_LOG(INFO)
                                                 << "F AsyncExists: " << p_keys[jj];
                                             ++finished;
                                             ASSERT_TRUE(b);
                                           },
                                           *this->io_service_pool_->Get()});
             }
           } else {
             ++sent;
             RAY_LOG(INFO) << "S AsyncBatchDelete: " << absl::StrJoin(p_keys, ",");
             store_client_->AsyncBatchDelete(
                 "N",
                 p_keys,
                 {[&finished, p_keys, keys](auto n) mutable {
                    RAY_LOG(INFO) << "F AsyncBatchDelete: " << absl::StrJoin(p_keys, ",");
                    ++finished;
                    ASSERT_EQ(n, keys.size());
                  },
                  *this->io_service_pool_->Get()});

             for (auto p_key : p_keys) {
               ++sent;
               RAY_LOG(INFO) << "S AsyncExists: " << p_key;
               store_client_->AsyncExists("N",
                                          p_key,
                                          {[&finished, p_key](auto b) mutable {
                                             RAY_LOG(INFO) << "F AsyncExists: " << p_key;
                                             ++finished;
                                             ASSERT_FALSE(false);
                                           },
                                           *this->io_service_pool_->Get()});
             }
           }
         },
         *io_service_pool_->Get()});
  }
  ASSERT_TRUE(WaitForCondition(
      [&finished, &sent]() {
        RAY_LOG(INFO) << finished << "/" << sent;
        return finished == sent;
      },
      5000));
}

TEST_F(RedisStoreClientTest, Random) {
  std::map<std::string, std::string> dict;
  auto counter = std::make_shared<std::atomic<size_t>>(0);
  auto m_gen_keys = []() {
    auto num_keys = static_cast<size_t>(std::rand() % 10);
    std::unordered_set<std::string> keys;
    while (keys.size() < num_keys) {
      auto k = std::to_string(std::rand() % 1000);
      keys.insert(k);
    }
    return std::vector<std::string>(keys.begin(), keys.end());
  };

  auto m_multi_get = [&, counter, this](size_t idx) {
    auto keys = m_gen_keys();
    absl::flat_hash_map<std::string, std::string> result;
    for (auto key : keys) {
      auto iter = dict.find(key);
      if (iter != dict.end()) {
        result[key] = iter->second;
      }
    }
    RAY_LOG(INFO) << "m_multi_get Sending: " << idx;
    *counter += 1;
    store_client_->AsyncMultiGet("N",
                                 keys,
                                 {[result, idx, counter](auto m) mutable {
                                    RAY_LOG(INFO) << "m_multi_get Finished: " << idx
                                                  << " " << m.size();
                                    *counter -= 1;
                                    ASSERT_TRUE(m == result);
                                  },
                                  *io_service_pool_->Get()});
  };

  auto m_batch_delete = [&, counter, this](size_t idx) mutable {
    auto keys = m_gen_keys();
    size_t deleted_num = 0;
    for (auto key : keys) {
      deleted_num += dict.erase(key);
    }
    RAY_LOG(INFO) << "m_batch_delete Sending: " << idx;
    *counter += 1;
    store_client_->AsyncBatchDelete("N",
                                    keys,
                                    {[&counter, deleted_num, idx](auto v) mutable {
                                       RAY_LOG(INFO) << "m_batch_delete Finished: " << idx
                                                     << " " << v;
                                       *counter -= 1;
                                       ASSERT_EQ(v, deleted_num);
                                     },
                                     *io_service_pool_->Get()});
  };

  auto m_delete = [&, this](size_t idx) mutable {
    auto k = std::to_string(std::rand() % 1000);
    bool deleted = dict.erase(k) > 0;
    RAY_LOG(INFO) << "m_delete Sending: " << idx << " " << k;
    *counter += 1;
    store_client_->AsyncDelete("N",
                               k,
                               {[counter, k, idx, deleted](auto r) {
                                  RAY_LOG(INFO) << "m_delete Finished: " << idx << " "
                                                << k << " " << deleted;
                                  *counter -= 1;
                                  ASSERT_EQ(deleted, r);
                                },
                                *io_service_pool_->Get()});
  };

  auto m_get = [&, counter, this](size_t idx) {
    auto k = std::to_string(std::rand() % 1000);
    std::optional<std::string> v;
    if (dict.count(k)) {
      v = dict[k];
    }
    RAY_LOG(INFO) << "m_get Sending: " << idx;
    *counter += 1;
    store_client_->AsyncGet("N",
                            k,
                            {[counter, idx, v](auto, auto r) {
                               RAY_LOG(INFO) << "m_get Finished: " << idx << " "
                                             << (r ? *r : std::string("-"));
                               *counter -= 1;
                               ASSERT_EQ(v, r);
                             },
                             *io_service_pool_->Get()});
  };

  auto m_exists = [&, counter, this](size_t idx) {
    auto k = std::to_string(std::rand() % 1000);
    bool existed = dict.count(k);
    RAY_LOG(INFO) << "m_exists Sending: " << idx;
    *counter += 1;
    store_client_->AsyncExists("N",
                               k,
                               {[k, existed, counter, idx](auto r) mutable {
                                  RAY_LOG(INFO) << "m_exists Finished: " << idx << " "
                                                << k << " " << r;
                                  *counter -= 1;
                                  ASSERT_EQ(existed, r) << " exists check " << k;
                                },
                                *io_service_pool_->Get()});
  };

  auto m_puts = [&, counter, this](size_t idx) mutable {
    auto k = std::to_string(std::rand() % 1000);
    auto v = std::to_string(std::rand() % 1000);
    bool added = false;
    if (!dict.count(k)) {
      added = true;
    }
    dict[k] = v;
    RAY_LOG(INFO) << "m_put Sending: " << idx << " " << k << " " << v;
    *counter += 1;
    store_client_->AsyncPut("N",
                            k,
                            v,
                            true,
                            {[idx, added, k, counter](bool r) mutable {
                               RAY_LOG(INFO)
                                   << "m_put Finished: " << idx << " " << k << " " << r;
                               *counter -= 1;
                               ASSERT_EQ(r, added);
                             },
                             *io_service_pool_->Get()});
  };

  std::vector<std::function<void(size_t idx)>> ops{
      m_batch_delete, m_delete, m_get, m_exists, m_multi_get, m_puts};

  for (size_t i = 0; i < 10000; ++i) {
    auto idx = std::rand() % ops.size();
    ops[idx](i);
  }
  EXPECT_TRUE(WaitForCondition([&counter]() { return *counter == 0; }, 10000));
  auto redis_store_client_raw_ptr =
      reinterpret_cast<RedisStoreClient *>(store_client_.get());
  absl::MutexLock lock(&redis_store_client_raw_ptr->mu_);
  ASSERT_TRUE(redis_store_client_raw_ptr->pending_redis_request_by_key_.empty());
}

// Tests for the Redis payload byte metrics. These assert exact deltas against
// the definition published with the metrics, so a change here is a change to
// what operators' dashboards mean -- see
// GetGcsRedisRequestPayloadBytesSumMetric in src/ray/gcs/metrics.h.
//
// The fixture drives a real RedisStoreClient with FakeCounters injected, rather
// than inspecting the recording sites, because the contract under test is the
// exported label/value pair and not the code path that produces it.
class RedisStoreClientMetricsTest : public ::testing::Test {
 public:
  static void SetUpTestCase() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  static void TearDownTestCase() { TestSetupUtil::ShutDownRedisServers(); }

  void SetUp() override {
    if (std::getenv("REDIS_CHAOS") != nullptr) {
      GTEST_SKIP() << "Exact byte assertions are incompatible with REPLICAOF flapping.";
    }
    port_ = TEST_REDIS_SERVER_PORTS.front();
    TestSetupUtil::FlushRedisServer(port_);
    // The kill switch is read once in the RedisStoreClient constructor, so it
    // has to be set before the client is built -- which is also why it is a
    // fixture-level knob and not something a test flips mid-run. Rebuilding the
    // client while the io thread is polling would destroy a RedisAsyncContext
    // out from under a pending socket wait.
    RayConfig::instance().gcs_redis_payload_metrics_enabled() = PayloadMetricsEnabled();
    io_service_ = std::make_unique<instrumented_io_context>(
        /*enable_lag_probe=*/false, /*running_on_single_thread=*/true);
    MakeStoreClient();
    thread_ = std::make_unique<std::thread>([this]() {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          io_service_->get_executor());
      io_service_->run();
    });
  }

  void TearDown() override {
    if (io_service_ != nullptr) {
      io_service_->stop();
      thread_->join();
      store_client_.reset();
      io_service_.reset();
    }
    RayConfig::instance().gcs_redis_payload_metrics_enabled() = true;
  }

 protected:
  // Whether the client under test is built with the payload metrics enabled.
  // Overridden by RedisStoreClientMetricsDisabledTest below.
  virtual bool PayloadMetricsEnabled() const { return true; }

  void MakeStoreClient() {
    RedisClientOptions options{"127.0.0.1", port_};
    store_client_ = std::make_unique<RedisStoreClient>(
        *io_service_,
        options,
        clock_,
        RedisMetrics{request_bytes_, response_bytes_, command_count_});
  }

  static double ValueFor(const observability::FakeCounter &metric,
                         const std::string &command,
                         const std::string &table) {
    const absl::flat_hash_map<std::string, std::string> tags{{"Command", command},
                                                             {"TableName", table}};
    auto all = metric.GetTagToValue();
    auto it = all.find(tags);
    return it == all.end() ? 0.0 : it->second;
  }

  double RequestBytes(const std::string &command, const std::string &table) const {
    return ValueFor(request_bytes_, command, table);
  }
  double ResponseBytes(const std::string &command, const std::string &table) const {
    return ValueFor(response_bytes_, command, table);
  }
  double CommandCount(const std::string &command, const std::string &table) const {
    return ValueFor(command_count_, command, table);
  }

  // The Redis "key" a GCS table maps to, whose bytes are part of every request.
  static std::string RedisKeyFor(const std::string &table) {
    return RedisKey{RayConfig::instance().external_storage_namespace(), table}.ToString();
  }

  void PutSync(const std::string &table, const std::string &key, std::string value) {
    std::promise<bool> promise;
    store_client_->AsyncPut(
        table,
        key,
        std::move(value),
        /*overwrite=*/true,
        {[&promise](bool added) { promise.set_value(added); }, *io_service_});
    promise.get_future().get();
  }

  std::optional<std::string> GetSync(const std::string &table, const std::string &key) {
    std::promise<std::optional<std::string>> promise;
    store_client_->AsyncGet(
        table,
        key,
        {[&promise](const Status &status, std::optional<std::string> result) {
           RAY_CHECK_OK(status);
           promise.set_value(std::move(result));
         },
         *io_service_});
    return promise.get_future().get();
  }

  absl::flat_hash_map<std::string, std::string> MultiGetSync(
      const std::string &table, const std::vector<std::string> &keys) {
    std::promise<absl::flat_hash_map<std::string, std::string>> promise;
    store_client_->AsyncMultiGet(
        table,
        keys,
        {[&promise](absl::flat_hash_map<std::string, std::string> result) {
           promise.set_value(std::move(result));
         },
         *io_service_});
    return promise.get_future().get();
  }

  absl::flat_hash_map<std::string, std::string> GetAllSync(const std::string &table) {
    std::promise<absl::flat_hash_map<std::string, std::string>> promise;
    store_client_->AsyncGetAll(
        table,
        {[&promise](absl::flat_hash_map<std::string, std::string> result) {
           promise.set_value(std::move(result));
         },
         *io_service_});
    return promise.get_future().get();
  }

  int64_t BatchDeleteSync(const std::string &table,
                          const std::vector<std::string> &keys) {
    std::promise<int64_t> promise;
    store_client_->AsyncBatchDelete(
        table, keys, {[&promise](int64_t n) { promise.set_value(n); }, *io_service_});
    return promise.get_future().get();
  }

  bool ExistsSync(const std::string &table, const std::string &key) {
    std::promise<bool> promise;
    store_client_->AsyncExists(
        table, key, {[&promise](bool e) { promise.set_value(e); }, *io_service_});
    return promise.get_future().get();
  }

  int port_ = 0;
  ray::Clock clock_;
  std::unique_ptr<instrumented_io_context> io_service_;
  std::unique_ptr<std::thread> thread_;
  std::unique_ptr<RedisStoreClient> store_client_;
  observability::FakeCounter request_bytes_;
  observability::FakeCounter response_bytes_;
  observability::FakeCounter command_count_;
};

// The published definition, pinned exactly: request bytes are the sum of the
// RESP argument lengths -- verb, Redis key, field name and value -- while the
// HSET integer reply contributes its decimal text length.
TEST_F(RedisStoreClientMetricsTest, PutMatchesDocumentedDefinition) {
  const std::string table = "NODE";
  const std::string key = "node-id-0123456789";
  const std::string value(4096, 'v');

  PutSync(table, key, value);

  const double expected =
      std::string("HSET").size() + RedisKeyFor(table).size() + key.size() + value.size();
  EXPECT_EQ(RequestBytes("HSET", table), expected);
  EXPECT_EQ(ResponseBytes("HSET", table), 1.0);
  EXPECT_EQ(CommandCount("HSET", table), 1.0);
}

// The issue's size ladder. Each step must move the request counter by exactly
// the value delta, and 8 MiB is far below the 2^53 bound where a double stops
// representing byte counts exactly.
TEST_F(RedisStoreClientMetricsTest, RequestBytesTrackValueSize) {
  const std::string table = "ACTOR";
  const std::string key = "k";
  const size_t overhead =
      std::string("HSET").size() + RedisKeyFor(table).size() + key.size();

  double previous = 0;
  for (size_t size : {size_t{1} << 10, size_t{1} << 20, size_t{8} << 20}) {
    PutSync(table, key, std::string(size, 'x'));
    const double now = RequestBytes("HSET", table);
    EXPECT_EQ(now - previous, static_cast<double>(overhead + size)) << "size " << size;
    previous = now;
  }
}

TEST_F(RedisStoreClientMetricsTest, GetCountsReturnedValueOnly) {
  const std::string table = "WORKERS";
  const std::string key = "worker-0";
  const std::string value(2048, 'w');
  PutSync(table, key, value);

  ASSERT_TRUE(GetSync(table, key).has_value());
  EXPECT_EQ(ResponseBytes("HGET", table), static_cast<double>(value.size()));
  EXPECT_EQ(RequestBytes("HGET", table),
            static_cast<double>(std::string("HGET").size() + RedisKeyFor(table).size() +
                                key.size()));
  EXPECT_EQ(CommandCount("HGET", table), 1.0);

  // A miss still costs a round trip and still sends the key, but returns
  // nothing: the counter measures data returned, not data requested.
  ASSERT_FALSE(GetSync(table, "absent").has_value());
  EXPECT_EQ(ResponseBytes("HGET", table), static_cast<double>(value.size()));
  EXPECT_EQ(CommandCount("HGET", table), 2.0);
}

// The "bytes sent" half of the issue. An HMGET's field-name vector is the
// payload that grows with a large read, and a values-only definition would
// report zero here.
TEST_F(RedisStoreClientMetricsTest, MultiGetCountsRequestFieldNames) {
  const std::string table = "KV";
  std::vector<std::string> keys;
  size_t key_bytes = 0;
  for (int i = 0; i < 50; ++i) {
    keys.push_back(absl::StrCat("a-fairly-long-internal-kv-field-name-", i));
    key_bytes += keys.back().size();
  }

  // Nothing was written, so every field misses and the reply is all nils.
  ASSERT_TRUE(MultiGetSync(table, keys).empty());

  EXPECT_EQ(RequestBytes("HMGET", table),
            static_cast<double>(std::string("HMGET").size() + RedisKeyFor(table).size() +
                                key_bytes));
  EXPECT_EQ(ResponseBytes("HMGET", table), 0.0);
  EXPECT_EQ(CommandCount("HMGET", table), 1.0);
}

// The issue's explicit expectation: HSCAN must report non-zero response bytes.
// This is also the regression test for reading a length off a string that has
// already been moved into the result map -- that mistake reports ~0 here.
TEST_F(RedisStoreClientMetricsTest, GetAllReportsScanResponseBytes) {
  const std::string table = "PLACEMENT_GROUP";
  const size_t num_entries = 200;
  size_t field_and_value_bytes = 0;
  for (size_t i = 0; i < num_entries; ++i) {
    std::string key = absl::StrCat("placement-group-", i);
    std::string value(512, 'p');
    field_and_value_bytes += key.size() + value.size();
    PutSync(table, key, value);
  }

  ASSERT_EQ(GetAllSync(table).size(), num_entries);

  // Strictly greater than the fields and values, because every HSCAN round also
  // returns a cursor bulk string. Any zero, or any values-only accounting,
  // fails here.
  EXPECT_GT(ResponseBytes("HSCAN", table), static_cast<double>(field_and_value_bytes));
  EXPECT_GE(CommandCount("HSCAN", table), 1.0);
  EXPECT_GT(RequestBytes("HSCAN", table), 0.0);
}

// Batched operations are counted per chunk, so the count is Redis round trips
// rather than StoreClient calls. Pin that, and pin that the byte totals
// aggregate across chunks.
TEST_F(RedisStoreClientMetricsTest, BatchedCommandsCountPerChunk) {
  const std::string table = "JOB";
  const size_t batch = 4;
  RayConfig::instance().maximum_gcs_storage_operation_batch_size() = batch;
  auto restore = absl::MakeCleanup(
      []() { RayConfig::instance().maximum_gcs_storage_operation_batch_size() = 1000; });

  std::vector<std::string> keys;
  size_t key_bytes = 0;
  for (size_t i = 0; i < 10; ++i) {
    keys.push_back(absl::StrCat("job-", i));
    key_bytes += keys.back().size();
  }

  MultiGetSync(table, keys);
  const double expected_chunks = 3;  // ceil(10 / 4)
  EXPECT_EQ(CommandCount("HMGET", table), expected_chunks);
  EXPECT_EQ(RequestBytes("HMGET", table),
            static_cast<double>(expected_chunks * (std::string("HMGET").size() +
                                                   RedisKeyFor(table).size()) +
                                key_bytes));

  BatchDeleteSync(table, keys);
  EXPECT_EQ(CommandCount("HDEL", table), expected_chunks);
}

// Every public method should use the actual Redis verb and the logical table,
// never a rendered Redis key containing the per-cluster namespace.
TEST_F(RedisStoreClientMetricsTest, LabelsMatchCommandsAndTables) {
  const std::string table = "ACTOR_TASK_SPEC";
  PutSync(table, "k1", "v1");
  GetSync(table, "k1");
  MultiGetSync(table, {"k1", "k2"});
  GetAllSync(table);
  ExistsSync(table, "k1");
  BatchDeleteSync(table, {"k1"});
  {
    std::promise<int> promise;
    store_client_->AsyncGetNextJobID(
        {[&promise](int id) { promise.set_value(id); }, *io_service_});
    promise.get_future().get();
  }
  {
    std::promise<Status> promise;
    store_client_->AsyncCheckHealth(
        {[&promise](Status s) { promise.set_value(s); }, *io_service_});
    ASSERT_TRUE(promise.get_future().get().ok());
  }

  const absl::flat_hash_set<std::string> allowed_commands{"HSET",
                                                          "HSETNX",
                                                          "HGET",
                                                          "HMGET",
                                                          "HDEL",
                                                          "HEXISTS",
                                                          "HSCAN",
                                                          "INCRBY",
                                                          "PING",
                                                          "SCAN",
                                                          "DEL",
                                                          "UNLINK",
                                                          "INFO"};
  const absl::flat_hash_set<std::string> allowed_tables{
      table, "JobCounter", std::string(kNoTable), std::string(kAllTables)};

  size_t observed = 0;
  for (const auto *metric : {&request_bytes_, &response_bytes_, &command_count_}) {
    for (const auto &[tags, _] : metric->GetTagToValue()) {
      ASSERT_EQ(tags.size(), 2u);
      const std::string &command = tags.at("Command");
      const std::string &table_name = tags.at("TableName");
      EXPECT_TRUE(allowed_commands.contains(command)) << "unexpected Command " << command;
      EXPECT_TRUE(allowed_tables.contains(table_name))
          << "unexpected TableName " << table_name;
      // The rendered Redis key carries the storage namespace, which is a
      // per-cluster identifier. If it ever leaks into a label the cardinality
      // bound is gone.
      EXPECT_EQ(table_name.find(RayConfig::instance().external_storage_namespace()),
                std::string::npos);
      ++observed;
    }
  }
  EXPECT_GT(observed, 0u);
}

// Same fixture, but the client is constructed with the kill switch already off,
// so nothing has to be torn down mid-test to observe the disabled behavior.
class RedisStoreClientMetricsDisabledTest : public RedisStoreClientMetricsTest {
 protected:
  bool PayloadMetricsEnabled() const override { return false; }
};

TEST_F(RedisStoreClientMetricsDisabledTest, KillSwitchStopsRecording) {
  const std::string table = "NODE";
  PutSync(table, "k", std::string(1024, 'v'));
  ASSERT_TRUE(GetSync(table, "k").has_value());

  EXPECT_TRUE(request_bytes_.GetTagToValue().empty());
  EXPECT_TRUE(response_bytes_.GetTagToValue().empty());
  EXPECT_TRUE(command_count_.GetTagToValue().empty());
}

// Tests for RedisDelKeyPrefixSync (namespace cleanup). These assert exact
// command-count deltas from INFO commandstats: RedisDelKeyPrefixSync runs its
// own Connect(), and every non-Sentinel connect issues one DEL DUMMY, so
// cmdstat_del is never absent and only exact deltas prove which verb the
// delete loop used.
//
// CAUTION for new test cases: the fixture helpers go through RunArgvAsync,
// which treats every Redis *error reply* as transient -- it retries
// num_redis_request_retries times and then RAY_LOG(FATAL)s. A command the
// server rejects (unknown command, bad arguments, ACL denial) therefore aborts
// the test process after ~2s of retries instead of failing an assertion.
class RedisDelKeyPrefixSyncTest : public ::testing::Test {
 public:
  static void SetUpTestCase() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  static void TearDownTestCase() { TestSetupUtil::ShutDownRedisServers(); }

  void SetUp() override {
    original_cleanup_use_unlink_ =
        RayConfig::instance().redis_namespace_cleanup_use_unlink();
    if (std::getenv("REDIS_CHAOS") != nullptr) {
      GTEST_SKIP() << "Exact command-count assertions are incompatible with "
                      "REPLICAOF flapping.";
    }
    port_ = TEST_REDIS_SERVER_PORTS.front();
    TestSetupUtil::FlushRedisServer(port_);
    io_service_ = std::make_unique<instrumented_io_context>(
        /*emit_metrics=*/false, /*running_on_single_thread=*/true);
    context_ = std::make_unique<RedisContext>(*io_service_, clock_);
    RAY_CHECK_OK(context_->Connect("127.0.0.1",
                                   port_,
                                   /*username=*/"",
                                   /*password=*/"",
                                   /*enable_ssl=*/false));
    thread_ = std::make_unique<std::thread>([this]() {
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work(
          io_service_->get_executor());
      io_service_->run();
    });
  }

  void TearDown() override {
    RayConfig::instance().redis_namespace_cleanup_use_unlink() =
        original_cleanup_use_unlink_;
    if (io_service_ != nullptr) {
      io_service_->stop();
      thread_->join();
      context_.reset();
      io_service_.reset();
    }
  }

 protected:
  // Runs one command on the admin connection and returns its reply.
  std::shared_ptr<CallbackReply> RunCmd(std::vector<std::string> cmd) {
    std::promise<std::shared_ptr<CallbackReply>> promise;
    context_->RunArgvAsync(
        std::move(cmd),
        [&promise](const std::shared_ptr<CallbackReply> &reply) {
          promise.set_value(reply);
        },
        kAllTables);
    return promise.get_future().get();
  }

  // Creates one GCS-shaped hash per table under the namespace prefix.
  void SeedNamespace(const std::string &ns, const std::vector<std::string> &tables) {
    for (const auto &table : tables) {
      auto reply = RunCmd({"HSET", RedisKey{ns, table}.ToString(), "field", "value"});
      ASSERT_EQ(reply->ReadAsInteger(), 1);
    }
  }

  void ResetCommandStats() { RunCmd({"CONFIG", "RESETSTAT"}); }

  // Returns cmdstat_<command> calls since the last RESETSTAT, 0 if absent.
  int64_t CommandCalls(const std::string &command) {
    auto reply = RunCmd({"INFO", "commandstats"});
    const std::string &info = reply->ReadAsString();
    // Lines look like: cmdstat_del:calls=3,usec=...
    const std::string needle = absl::StrCat("cmdstat_", command, ":calls=");
    auto pos = info.find(needle);
    if (pos == std::string::npos) {
      return 0;
    }
    return std::stoll(info.substr(pos + needle.size()));
  }

  int64_t NumKeysWithPrefix(const std::string &ns) {
    auto reply = RunCmd({"KEYS", absl::StrCat("RAY", ns, "@*")});
    return static_cast<int64_t>(reply->ReadAsStringArray().size());
  }

  bool RunCleanup(const std::string &ns) {
    return RedisDelKeyPrefixSync(
        "127.0.0.1", port_, /*username=*/"", /*password=*/"", /*use_ssl=*/false, ns);
  }

  ray::Clock clock_;
  bool original_cleanup_use_unlink_ = false;
  int port_ = 0;
  std::unique_ptr<instrumented_io_context> io_service_;
  std::unique_ptr<RedisContext> context_;
  std::unique_ptr<std::thread> thread_;
};

TEST_F(RedisDelKeyPrefixSyncTest, DelIsUsedByDefault) {
  ASSERT_FALSE(RayConfig::instance().redis_namespace_cleanup_use_unlink());
  const std::vector<std::string> tables = {"KV", "NODE", "WORKERS"};
  SeedNamespace("del_ns", tables);
  ResetCommandStats();
  ASSERT_TRUE(RunCleanup("del_ns"));
  ASSERT_EQ(NumKeysWithPrefix("del_ns"), 0);
  // DEL DUMMY from Connect() plus one DEL per key.
  ASSERT_EQ(CommandCalls("del"), static_cast<int64_t>(tables.size()) + 1);
  ASSERT_EQ(CommandCalls("unlink"), 0);
}

TEST_F(RedisDelKeyPrefixSyncTest, UnlinkIsUsedWhenEnabled) {
  const std::vector<std::string> tables = {"KV", "NODE", "WORKERS"};
  SeedNamespace("unlink_ns", tables);
  RayConfig::instance().redis_namespace_cleanup_use_unlink() = true;
  ResetCommandStats();
  ASSERT_TRUE(RunCleanup("unlink_ns"));
  ASSERT_EQ(NumKeysWithPrefix("unlink_ns"), 0);
  // The cleanup's own Connect() issues exactly one DEL (DEL DUMMY); the delete
  // loop must not add to it.
  ASSERT_EQ(CommandCalls("del"), 1);
  ASSERT_EQ(CommandCalls("unlink"), static_cast<int64_t>(tables.size()));
}

TEST_F(RedisDelKeyPrefixSyncTest, CleanupIsIdempotent) {
  SeedNamespace("idem_ns", {"KV", "NODE"});
  ASSERT_TRUE(RunCleanup("idem_ns"));
  ASSERT_EQ(NumKeysWithPrefix("idem_ns"), 0);
  // The second run finds nothing and must still report success.
  ASSERT_TRUE(RunCleanup("idem_ns"));
}

TEST_F(RedisDelKeyPrefixSyncTest, OtherNamespacesSurvive) {
  SeedNamespace("ns1", {"KV", "NODE"});
  SeedNamespace("ns2", {"KV", "NODE"});
  ASSERT_TRUE(RunCleanup("ns1"));
  ASSERT_EQ(NumKeysWithPrefix("ns1"), 0);
  // Guards the SCAN match pattern.
  ASSERT_EQ(NumKeysWithPrefix("ns2"), 2);
}

}  // namespace gcs

}  // namespace ray
