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
#include <limits.h>
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_set.h"

#include "ray/common/common_protocol.h"
#include "ray/common/task/task_priority.h"

namespace ray {

TEST(TaskPriorityTest, TestEquals) {
  Priority priority1({1, 2, 3});
  Priority priority2({1, 2, 3});

  ASSERT_EQ(priority1, priority2);

  priority2.SetScore(3, INT_MAX);
  ASSERT_EQ(priority1, priority2);

  priority1.SetScore(4, INT_MAX);
  ASSERT_EQ(priority1, priority2);

  priority2.SetScore(3, 3);
  ASSERT_NE(priority1, priority2);
  priority1.SetScore(3, 3);
  ASSERT_EQ(priority1, priority2);
}

TEST(TaskPriorityTest, TestCompare) {
  Priority priority1({1, 2, 3});
  Priority priority2({1, 2});
  RAY_LOG(INFO) << priority1;
  RAY_LOG(INFO) << priority2;

  ASSERT_LE(priority1, priority1);
  ASSERT_LT(priority1, priority2);

  priority1.SetScore(3, 4);
  priority2.SetScore(3, 4);
  ASSERT_LT(priority1, priority2);

  priority2.SetScore(2, 2);
  ASSERT_LT(priority2, priority1);

  Priority priority3({});
  ASSERT_LT(priority1, priority3);
  ASSERT_LT(priority2, priority3);
}

TEST(TaskPriorityTest, TestCompare2) {
  Priority priority1({1, 0});
  Priority priority2({2});

  ASSERT_LT(priority1, priority2);
}

TEST(TaskPriorityTest, TestSort) {
  std::set<Priority> queue;
  Priority p1({1, 2, 3});
  Priority p2({1, 2});
  Priority p3({});

  queue.insert(p1);
  queue.insert(p2);
  queue.insert(p3);
  {
    std::vector<Priority> expected_order({p1, p2, p3});
    for (auto &p : queue) {
      ASSERT_EQ(p, expected_order.front());
      expected_order.erase(expected_order.begin());
    }
  }

  queue.erase(p2);
  p2.SetScore(2, 2);

  queue.insert(p2);
  {
    std::vector<Priority> expected_order({p2, p1, p3});
    for (auto &p : queue) {
      ASSERT_EQ(p, expected_order.front());
      expected_order.erase(expected_order.begin());
    }
  }
}

TEST(TaskPriorityTest, TestDataStructures) {
  Priority p1({1, 2, 3});
  Priority p2({1, 2});
  Priority p3({});
  Priority p4({1, 3, 5});

  std::vector<std::pair<Priority, TaskID>> vec = {
    std::make_pair(p1, ObjectID::FromRandom().TaskId()),
    std::make_pair(p2, ObjectID::FromRandom().TaskId()),
    std::make_pair(p3, ObjectID::FromRandom().TaskId())
  };

  absl::btree_set<TaskKey> set;
  for (auto &p : vec) {
    ASSERT_TRUE(set.emplace(p).second);
    ASSERT_TRUE(set.find(p) != set.end());
  }
  TaskKey key(p4, vec[0].second);
  ASSERT_TRUE(set.find(key) == set.end());
  {
    auto it = set.begin();
    for (int i = 0; i < 3; i++) {
      ASSERT_EQ(*it, vec[i]);
      it++;
    }
  }

  absl::flat_hash_set<Priority> hash_set({p1, p2, p3});
  ASSERT_TRUE(hash_set.count(p1));
  ASSERT_TRUE(hash_set.count(p2));
  ASSERT_TRUE(hash_set.count(p3));
  ASSERT_EQ(hash_set.size(), 3);
  ASSERT_FALSE(hash_set.count(p4));

  // Length of the score vector does not matter, if all elements at the end are
  // null.
  auto p1_it = hash_set.find(p1);
  p1.extend(10);
  ASSERT_EQ(p1_it, hash_set.find(p1));
  ASSERT_NE(hash_set.find(p2), hash_set.find(p1));

  auto p3_it = hash_set.find(p3);
  p3.extend(10);
  ASSERT_EQ(p3_it, hash_set.find(p3));
  ASSERT_NE(hash_set.find(p2), hash_set.find(p3));

  absl::flat_hash_set<TaskKey> task_key_hash_set(vec.begin(), vec.end());
  ASSERT_TRUE(task_key_hash_set.count(vec[0]));
  ASSERT_TRUE(task_key_hash_set.count(vec[1]));
  ASSERT_TRUE(task_key_hash_set.count(vec[2]));
  ASSERT_FALSE(task_key_hash_set.count(TaskKey(p4, ObjectID::FromRandom().TaskId())));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
