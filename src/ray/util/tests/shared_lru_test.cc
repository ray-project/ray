// Copyright 2024 The Ray Authors.
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

#include "ray/util/shared_lru.h"

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace ray::utils::container {

namespace {
constexpr size_t kTestCacheSz = 1;

class TestClassWithHashAndEq {
 public:
  explicit TestClassWithHashAndEq(std::string data) : data_(std::move(data)) {}
  bool operator==(const TestClassWithHashAndEq &rhs) const { return data_ == rhs.data_; }
  template <typename H>
  friend H AbslHashValue(H h, const TestClassWithHashAndEq &obj) {
    return H::combine(std::move(h), obj.data_);
  }

 private:
  std::string data_;
};
}  // namespace

TEST(SharedLruCache, PutAndGet) {
  ThreadSafeSharedLruCache<std::string, std::string> cache{kTestCacheSz};

  // No value initially.
  auto val = cache.Get("1");
  EXPECT_EQ(val, nullptr);

  // Check put and get.
  cache.Put("1", std::make_shared<std::string>("1"));
  val = cache.Get(std::string_view{"1"});
  EXPECT_NE(val, nullptr);
  EXPECT_EQ(*val, "1");

  // Check key eviction.
  cache.Put("2", std::make_shared<std::string>("2"));
  val = cache.Get(std::string_view{"1"});
  EXPECT_EQ(val, nullptr);
  val = cache.Get(std::string_view{"2"});
  EXPECT_NE(val, nullptr);
  EXPECT_EQ(*val, "2");

  // Check deletion.
  EXPECT_FALSE(cache.Delete(std::string_view{"1"}));
  val = cache.Get(std::string_view{"1"});
  EXPECT_EQ(val, nullptr);
}

// Testing senario: push multiple same keys into the cache.
TEST(SharedLruCache, SameKeyTest) {
  ThreadSafeSharedLruCache<int, int> cache{2};

  cache.Put(1, std::make_shared<int>(1));
  auto val = cache.Get(1);
  EXPECT_NE(val, nullptr);
  EXPECT_EQ(1, *val);

  cache.Put(1, std::make_shared<int>(2));
  val = cache.Get(1);
  EXPECT_NE(val, nullptr);
  EXPECT_EQ(2, *val);
}

TEST(SharedLruCache, FactoryTest) {
  using CacheType = ThreadSafeSharedLruCache<std::string, std::string>;

  std::atomic<bool> invoked = {false};  // Used to check only invoke once.
  auto factory = [&invoked](const std::string &key) -> std::shared_ptr<std::string> {
    EXPECT_FALSE(invoked.exchange(true));
    // Sleep for a while so multiple threads could kick in and get blocked.
    std::this_thread::sleep_for(std::chrono::seconds(3));
    return std::make_shared<std::string>(key);
  };

  CacheType cache{1};

  constexpr size_t kFutureNum = 100;
  std::vector<std::future<std::shared_ptr<std::string>>> futures;
  futures.reserve(kFutureNum);

  const std::string key = "key";
  for (size_t idx = 0; idx < kFutureNum; ++idx) {
    futures.emplace_back(std::async(std::launch::async, [&cache, &key, &factory]() {
      return cache.GetOrCreate(key, factory);
    }));
  }
  for (auto &fut : futures) {
    auto val = fut.get();
    ASSERT_NE(val, nullptr);
    ASSERT_EQ(*val, key);
  }

  // After we're sure key-value pair exists in cache, make one more call.
  auto cached_val = cache.GetOrCreate(key, factory);
  ASSERT_NE(cached_val, nullptr);
  ASSERT_EQ(*cached_val, key);
}

TEST(SharedLruConstCache, TypeAliasAssertion) {
  static_assert(
      std::is_same_v<SharedLruConstCache<int, int>, SharedLruCache<int, const int>>);
}

TEST(SharedLruConstCache, CustomizedKey) {
  TestClassWithHashAndEq obj1{"hello"};
  TestClassWithHashAndEq obj2{"hello"};
  SharedLruCache<TestClassWithHashAndEq, std::string> cache{2};
  cache.Put(obj1, std::make_shared<std::string>("val"));
  auto val = cache.Get(obj2);
  EXPECT_EQ(*val, "val");
}

}  // namespace ray::utils::container
