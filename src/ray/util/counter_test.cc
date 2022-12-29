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

#include "gtest/gtest.h"
#include "ray/util/counter_map.h"

namespace ray {

class CounterMapTest : public ::testing::Test {};

TEST_F(CounterMapTest, TestBasic) {
  auto c = CounterMap<std::string>();
  c.Increment("k1");
  c.Increment("k1");
  c.Increment("k2");
  EXPECT_EQ(c.Get("k1"), 2);
  EXPECT_EQ(c.Get("k2"), 1);
  EXPECT_EQ(c.Get("k3"), 0);
  EXPECT_EQ(c.Total(), 3);
  EXPECT_EQ(c.Size(), 2);

  c.Decrement("k1");
  c.Decrement("k2");
  EXPECT_EQ(c.Get("k1"), 1);
  EXPECT_EQ(c.Get("k2"), 0);
  EXPECT_EQ(c.Get("k3"), 0);
  EXPECT_EQ(c.Total(), 1);
  EXPECT_EQ(c.Size(), 1);

  c.Swap("k1", "k2");
  EXPECT_EQ(c.Get("k1"), 0);
  EXPECT_EQ(c.Get("k2"), 1);
  EXPECT_EQ(c.Total(), 1);
  EXPECT_EQ(c.Size(), 1);

  // Test multi-value ops
  c.Increment("k1", 10);
  c.Increment("k2", 5);
  EXPECT_EQ(c.Get("k1"), 10);
  EXPECT_EQ(c.Get("k2"), 6);
  EXPECT_EQ(c.Total(), 16);
  EXPECT_EQ(c.Size(), 2);

  c.Decrement("k1", 5);
  c.Decrement("k2", 1);
  EXPECT_EQ(c.Get("k1"), 5);
  EXPECT_EQ(c.Get("k2"), 5);
  EXPECT_EQ(c.Total(), 10);
  EXPECT_EQ(c.Size(), 2);

  c.Swap("k1", "k2", 5);
  EXPECT_EQ(c.Get("k1"), 0);
  EXPECT_EQ(c.Get("k2"), 10);
  EXPECT_EQ(c.Total(), 10);
  EXPECT_EQ(c.Size(), 1);
}

TEST_F(CounterMapTest, TestCallback) {
  auto c = CounterMap<std::string>();
  int num_calls = 0;
  std::string last_call_key;
  int64_t last_call_value;
  c.SetOnChangeCallback([&](const std::string &key) mutable {
    num_calls += 1;
    last_call_key = key;
    last_call_value = c.Get(key);
  });

  c.Increment("k1");
  EXPECT_EQ(num_calls, 0);
  EXPECT_EQ(c.NumPendingCallbacks(), 1);
  c.FlushOnChangeCallbacks();
  EXPECT_EQ(c.NumPendingCallbacks(), 0);
  EXPECT_EQ(num_calls, 1);
  EXPECT_EQ(last_call_key, "k1");
  EXPECT_EQ(last_call_value, 1);

  c.Increment("k1");
  c.FlushOnChangeCallbacks();
  EXPECT_EQ(num_calls, 2);
  EXPECT_EQ(last_call_key, "k1");
  EXPECT_EQ(last_call_value, 2);

  c.Increment("k2");
  c.FlushOnChangeCallbacks();
  EXPECT_EQ(num_calls, 3);
  EXPECT_EQ(last_call_key, "k2");
  EXPECT_EQ(last_call_value, 1);

  c.Swap("k1", "k2");
  EXPECT_EQ(c.NumPendingCallbacks(), 2);
  c.FlushOnChangeCallbacks();
  EXPECT_EQ(c.NumPendingCallbacks(), 0);
  EXPECT_EQ(num_calls, 5);

  c.Decrement("k1");
  c.FlushOnChangeCallbacks();
  EXPECT_EQ(num_calls, 6);
  EXPECT_EQ(last_call_key, "k1");
  EXPECT_EQ(last_call_value, 0);
}

TEST_F(CounterMapTest, TestIterate) {
  auto c = CounterMap<std::string>();
  int num_keys = 0;
  c.Increment("k1");
  c.Increment("k1");
  c.Increment("k2");
  c.ForEachEntry([&](const std::string &key, int64_t value) mutable {
    num_keys += 1;
    if (key == "k1") {
      EXPECT_EQ(value, 2);
    } else {
      EXPECT_EQ(value, 1);
    }
  });
  EXPECT_EQ(num_keys, 2);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
