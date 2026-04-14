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

#include "ray/gcs/store_client_kv.h"

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "ray/common/asio/asio_util.h"
#include "ray/gcs/store_client/in_memory_store_client.h"

namespace ray {
namespace gcs {

class StoreClientKVTest : public ::testing::Test {
 protected:
  void SetUp() override {
    io_context_ =
        std::make_unique<InstrumentedIOContextWithThread>("StoreClientKVTest");
    kv_ = std::make_unique<StoreClientInternalKV>(
        std::make_unique<InMemoryStoreClient>());
  }

  void TearDown() override {
    io_context_.reset();
  }

  // Synchronous wrappers for async KV operations.
  bool Put(const std::string &ns,
           const std::string &key,
           const std::string &value,
           bool overwrite) {
    std::promise<bool> p;
    kv_->Put(ns, key, std::string(value), overwrite,
             {[&p](bool added) { p.set_value(added); }, io_context_->GetIoService()});
    return p.get_future().get();
  }

  std::optional<std::string> Get(const std::string &ns, const std::string &key) {
    std::promise<std::optional<std::string>> p;
    kv_->Get(ns, key,
             {[&p](std::optional<std::string> val) { p.set_value(val); },
              io_context_->GetIoService()});
    return p.get_future().get();
  }

  int64_t Del(const std::string &ns, const std::string &key, bool del_by_prefix) {
    std::promise<int64_t> p;
    kv_->Del(ns, key, del_by_prefix,
             {[&p](int64_t count) { p.set_value(count); }, io_context_->GetIoService()});
    return p.get_future().get();
  }

  bool Exists(const std::string &ns, const std::string &key) {
    std::promise<bool> p;
    kv_->Exists(ns, key,
                {[&p](bool exists) { p.set_value(exists); },
                 io_context_->GetIoService()});
    return p.get_future().get();
  }

  std::vector<std::string> Keys(const std::string &ns, const std::string &prefix) {
    std::promise<std::vector<std::string>> p;
    kv_->Keys(ns, prefix,
              {[&p](std::vector<std::string> keys) { p.set_value(std::move(keys)); },
               io_context_->GetIoService()});
    return p.get_future().get();
  }

  absl::flat_hash_map<std::string, std::string> MultiGet(
      const std::string &ns, const std::vector<std::string> &keys) {
    std::promise<absl::flat_hash_map<std::string, std::string>> p;
    kv_->MultiGet(
        ns, keys,
        {[&p](absl::flat_hash_map<std::string, std::string> result) {
           p.set_value(std::move(result));
         },
         io_context_->GetIoService()});
    return p.get_future().get();
  }

  std::unique_ptr<InstrumentedIOContextWithThread> io_context_;
  std::unique_ptr<StoreClientInternalKV> kv_;
};

TEST_F(StoreClientKVTest, TestPutAndGet) {
  Put("ns", "key1", "value1", /*overwrite=*/false);
  auto result = Get("ns", "key1");
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), "value1");
}

TEST_F(StoreClientKVTest, TestGetNonExistent) {
  auto result = Get("ns", "no_such_key");
  ASSERT_FALSE(result.has_value());
}

TEST_F(StoreClientKVTest, TestPutOverwrite) {
  Put("ns", "key1", "v1", /*overwrite=*/false);
  Put("ns", "key1", "v2", /*overwrite=*/true);
  auto result = Get("ns", "key1");
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), "v2");
}

TEST_F(StoreClientKVTest, TestPutNoOverwrite) {
  Put("ns", "key1", "v1", /*overwrite=*/false);
  // Should not overwrite since overwrite=false.
  Put("ns", "key1", "v2", /*overwrite=*/false);
  auto result = Get("ns", "key1");
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), "v1");
}

TEST_F(StoreClientKVTest, TestDelete) {
  Put("ns", "key1", "value1", /*overwrite=*/false);
  ASSERT_TRUE(Exists("ns", "key1"));

  auto deleted = Del("ns", "key1", /*del_by_prefix=*/false);
  ASSERT_EQ(deleted, 1);

  ASSERT_FALSE(Exists("ns", "key1"));
  auto result = Get("ns", "key1");
  ASSERT_FALSE(result.has_value());
}

TEST_F(StoreClientKVTest, TestDeleteNonExistent) {
  auto deleted = Del("ns", "no_such_key", /*del_by_prefix=*/false);
  ASSERT_EQ(deleted, 0);
}

TEST_F(StoreClientKVTest, TestDeleteByPrefix) {
  Put("ns", "prefix/a", "1", false);
  Put("ns", "prefix/b", "2", false);
  Put("ns", "other/c", "3", false);

  auto deleted = Del("ns", "prefix/", /*del_by_prefix=*/true);
  ASSERT_EQ(deleted, 2);

  ASSERT_FALSE(Exists("ns", "prefix/a"));
  ASSERT_FALSE(Exists("ns", "prefix/b"));
  ASSERT_TRUE(Exists("ns", "other/c"));
}

TEST_F(StoreClientKVTest, TestDeleteByPrefixEmpty) {
  // Delete by prefix when no keys match.
  auto deleted = Del("ns", "no_match/", /*del_by_prefix=*/true);
  ASSERT_EQ(deleted, 0);
}

TEST_F(StoreClientKVTest, TestExists) {
  ASSERT_FALSE(Exists("ns", "key1"));
  Put("ns", "key1", "value1", false);
  ASSERT_TRUE(Exists("ns", "key1"));
  Del("ns", "key1", false);
  ASSERT_FALSE(Exists("ns", "key1"));
}

TEST_F(StoreClientKVTest, TestKeys) {
  Put("ns", "a/1", "v1", false);
  Put("ns", "a/2", "v2", false);
  Put("ns", "b/1", "v3", false);

  auto keys_a = Keys("ns", "a/");
  ASSERT_EQ(keys_a.size(), 2);

  auto keys_b = Keys("ns", "b/");
  ASSERT_EQ(keys_b.size(), 1);

  auto keys_all = Keys("ns", "");
  ASSERT_EQ(keys_all.size(), 3);
}

TEST_F(StoreClientKVTest, TestMultiGet) {
  Put("ns", "k1", "v1", false);
  Put("ns", "k2", "v2", false);
  Put("ns", "k3", "v3", false);

  auto result = MultiGet("ns", {"k1", "k3", "k_missing"});
  ASSERT_EQ(result.size(), 2);
  ASSERT_EQ(result["k1"], "v1");
  ASSERT_EQ(result["k3"], "v3");
  ASSERT_EQ(result.count("k_missing"), 0);
}

TEST_F(StoreClientKVTest, TestNamespaceIsolation) {
  Put("ns1", "key", "value_ns1", false);
  Put("ns2", "key", "value_ns2", false);

  auto r1 = Get("ns1", "key");
  auto r2 = Get("ns2", "key");
  ASSERT_TRUE(r1.has_value());
  ASSERT_TRUE(r2.has_value());
  ASSERT_EQ(r1.value(), "value_ns1");
  ASSERT_EQ(r2.value(), "value_ns2");
}

TEST_F(StoreClientKVTest, TestEmptyNamespace) {
  Put("", "key1", "value1", false);
  auto result = Get("", "key1");
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), "value1");
}

}  // namespace gcs
}  // namespace ray
