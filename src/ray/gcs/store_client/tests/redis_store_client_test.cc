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
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "absl/cleanup/cleanup.h"
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
// the test process after ~2s of retries instead of failing an assertion. Gate
// on server capabilities (e.g. RedisMajorVersion below) before issuing any
// command the server might reject.
class RedisDelKeyPrefixSyncTest : public ::testing::Test {
 public:
  static void SetUpTestCase() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  static void TearDownTestCase() { TestSetupUtil::ShutDownRedisServers(); }

  void SetUp() override {
    if (std::getenv("REDIS_CHAOS") != nullptr) {
      GTEST_SKIP() << "Exact command-count assertions are incompatible with "
                      "REPLICAOF flapping.";
    }
    port_ = TEST_REDIS_SERVER_PORTS.front();
    TestSetupUtil::FlushRedisServer(port_);
    // Tests mutate this config; pin the default so no test leaks state.
    RayConfig::instance().redis_namespace_cleanup_use_unlink() = true;
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
    context_->RunArgvAsync(std::move(cmd),
                           [&promise](const std::shared_ptr<CallbackReply> &reply) {
                             promise.set_value(reply);
                           });
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

  // Parses the server's major version from INFO server. Capability gates must
  // use this instead of probing with the command in question: an error reply
  // on this connection aborts the process (see the fixture comment).
  int RedisMajorVersion() {
    auto reply = RunCmd({"INFO", "server"});
    const std::string &info = reply->ReadAsString();
    const std::string needle = "redis_version:";
    auto pos = info.find(needle);
    RAY_CHECK(pos != std::string::npos) << "No redis_version in INFO server: " << info;
    return std::stoi(info.substr(pos + needle.size()));
  }

  bool RunCleanup(const std::string &ns,
                  const std::string &username = "",
                  const std::string &password = "") {
    return RedisDelKeyPrefixSync(
        "127.0.0.1", port_, username, password, /*use_ssl=*/false, ns);
  }

  ray::Clock clock_;
  int port_ = 0;
  std::unique_ptr<instrumented_io_context> io_service_;
  std::unique_ptr<RedisContext> context_;
  std::unique_ptr<std::thread> thread_;
};

TEST_F(RedisDelKeyPrefixSyncTest, UnlinkIsUsedByDefault) {
  const std::vector<std::string> tables = {"KV", "NODE", "WORKERS"};
  SeedNamespace("unlink_ns", tables);
  ResetCommandStats();
  ASSERT_TRUE(RunCleanup("unlink_ns"));
  ASSERT_EQ(NumKeysWithPrefix("unlink_ns"), 0);
  // The cleanup's own Connect() issues exactly one DEL (DEL DUMMY); the delete
  // loop must not add to it.
  ASSERT_EQ(CommandCalls("del"), 1);
  // The capability probe + one UNLINK per key.
  ASSERT_EQ(CommandCalls("unlink"), static_cast<int64_t>(tables.size()) + 1);
}

TEST_F(RedisDelKeyPrefixSyncTest, FallsBackToDelWhenDisabled) {
  const std::vector<std::string> tables = {"KV", "NODE", "WORKERS"};
  SeedNamespace("nounlink_ns", tables);
  RayConfig::instance().redis_namespace_cleanup_use_unlink() = false;
  ResetCommandStats();
  ASSERT_TRUE(RunCleanup("nounlink_ns"));
  ASSERT_EQ(NumKeysWithPrefix("nounlink_ns"), 0);
  // DEL DUMMY + one DEL per key.
  ASSERT_EQ(CommandCalls("del"), static_cast<int64_t>(tables.size()) + 1);
  // The probe is skipped entirely.
  ASSERT_EQ(CommandCalls("unlink"), 0);
}

TEST_F(RedisDelKeyPrefixSyncTest, NopermFallsBackToDel) {
  // ACL exists since Redis 6.0. The Windows test server is tporadowski Redis
  // 5.0.9 (//:redis-server select), where ACL SETUSER would be an unknown
  // command -- which on this connection is a process abort, not a failed
  // assertion (see the fixture comment). Gate on the version, not on a trial
  // ACL command, for the same reason.
  if (RedisMajorVersion() < 6) {
    GTEST_SKIP() << "ACL requires Redis >= 6.";
  }
  const std::vector<std::string> tables = {"KV", "NODE", "WORKERS"};
  SeedNamespace("noperm_ns", tables);
  // A user that can run everything except UNLINK: the probe receives NOPERM
  // and cleanup must degrade to DEL automatically.
  RunCmd({"ACL", "SETUSER", "nounlink", "on", ">pw", "~*", "+@all", "-unlink"});
  // FLUSHALL does not remove ACL users, so clean up even on early assertion
  // failure.
  auto delete_acl_user = absl::MakeCleanup([this]() {
    RunCmd({"ACL", "DELUSER", "nounlink"});
  });
  ResetCommandStats();
  ASSERT_TRUE(RunCleanup("noperm_ns", "nounlink", "pw"));
  ASSERT_EQ(NumKeysWithPrefix("noperm_ns"), 0);
  // DEL DUMMY + one DEL per key. No assertion on unlink counts: whether an
  // ACL-rejected command is counted in commandstats is a server detail.
  ASSERT_EQ(CommandCalls("del"), static_cast<int64_t>(tables.size()) + 1);
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
  // Guards both the SCAN match pattern and the probe-key prefix.
  ASSERT_EQ(NumKeysWithPrefix("ns2"), 2);
}

}  // namespace gcs

}  // namespace ray
