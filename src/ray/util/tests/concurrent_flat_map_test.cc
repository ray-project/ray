// Copyright 2025 The Ray Authors.
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

#include "ray/util/concurrent_flat_map.h"

#include <gtest/gtest.h>

#include <string>

namespace ray {

TEST(ConcurrentFlatMap, TestConcurrentFlatMapBasic) {
  ConcurrentFlatMap<std::string, std::string> map;
  map.Reserve(1);

  // simple emplace and get
  map.Emplace("key", "value");
  EXPECT_EQ(map.Get("key"), "value");

  // make sure we don't replace value
  map.Emplace("key", "wrong value");
  EXPECT_EQ(map.Get("key"), "value");

  // make sure we can replace value
  // replacing returns false
  EXPECT_FALSE(map.InsertOrAssign("key", "correct value"));
  EXPECT_EQ(map.Get("key"), "correct value");
  // inserting returns true
  EXPECT_TRUE(map.InsertOrAssign("new key", "value"));

  // can clone
  auto value = map.GetMapClone().at("key");
  EXPECT_EQ(value, "correct value");

  // make sure erase and contains works
  EXPECT_TRUE(map.Contains("key"));
  EXPECT_TRUE(map.Erase("key"));
  EXPECT_FALSE(map.Contains("key"));
  EXPECT_FALSE(map.Erase("key"));

  // test erase keys
  map.Emplace("key1", "value1");
  map.Emplace("key2", "value2");
  map.EraseKeys(absl::MakeConstSpan({"key1", "key2"}));
  EXPECT_FALSE(map.Contains("key1") || map.Contains("key2"));
};

TEST(ConcurrentFlatMap, TestConcurrentFlatMapVisitors) {
  ConcurrentFlatMap<int, int> map;
  map.Reserve(3);
  map.Emplace(1, 1);
  map.Emplace(2, 2);
  map.Emplace(3, 3);

  int keyval = 0;
  map.ReadVisit(absl::MakeConstSpan({1, 2}), [&keyval](int key, int val) {
    ++keyval;
    EXPECT_EQ(key, keyval);
    EXPECT_EQ(val, keyval);
  });
  EXPECT_EQ(keyval, 2);

  int num_iters = 0;
  map.ReadVisitAll([&num_iters](int key, int val) {
    EXPECT_EQ(key, val);
    ++num_iters;
  });
  EXPECT_EQ(num_iters, 3);

  map.WriteVisit(absl::MakeConstSpan({1, 2}), [](int key, int &value) { value = 10; });
  map.ReadVisit(absl::MakeConstSpan({1, 2}),
                [](int key, int value) { EXPECT_EQ(value, 10); });
}

}  // namespace ray
