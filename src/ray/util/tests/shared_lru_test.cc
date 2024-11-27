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

#include "src/ray/util/shared_lru.h"

#include <gtest/gtest.h>

#include <string>

namespace ray::utils::container {

namespace {
constexpr size_t kTestCacheSz = 1;
}  // namespace

TEST(SharedLruCacheTest, PutAndGet) {
  ThreadSafeSharedLruCache<std::string, std::string> cache{kTestCacheSz};

  // No value initially.
  auto val = cache.Get(std::string_view{"1"});
  EXPECT_FALSE(val.has_value());

  // Check put and get.
  cache.Put("1", "1");
  val = cache.Get(std::string_view{"1"});
  EXPECT_TRUE(val.has_value());
  EXPECT_EQ(*val, std::string_view{"1"});

  // Check key eviction.
  cache.Put("2", "2");
  val = cache.Get(std::string_view{"1"});
  EXPECT_FALSE(val.has_value());
  val = cache.Get(std::string_view{"2"});
  EXPECT_TRUE(val.has_value());
  EXPECT_EQ(*val, std::string_view{"2"});

  // Check deletion.
  EXPECT_FALSE(cache.Delete(std::string_view{"1"}));
  EXPECT_TRUE(cache.Delete(std::string_view{"2"}));
  val = cache.Get(std::string_view{"2"});
  EXPECT_FALSE(val.has_value());
}

}  // namespace ray::utils::container
