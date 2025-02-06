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

#include <string>
#include <type_traits>

namespace ray::utils::container {

namespace {
constexpr size_t kTestCacheSz = 1;

class TestClassWithHashAndEq {
 public:
  TestClassWithHashAndEq(std::string data) : data_(std::move(data)) {}
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
