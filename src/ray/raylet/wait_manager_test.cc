// Copyright 2022 The Ray Authors.
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

#include "ray/raylet/wait_manager.h"

#include "gtest/gtest.h"

namespace ray {
namespace raylet {

class WaitManagerTest : public ::testing::Test {
 protected:
  WaitManagerTest()
      : wait_manager(
            [this](const ObjectID &object_id) {
              return local_objects.count(object_id) > 0;
            },
            [this](std::function<void()> fn, int64_t ms) {
              delay_fn = fn;
              delay_ms = ms;
            }) {}

  void AssertNoLeaks() {
    ASSERT_TRUE(wait_manager.wait_requests_.empty());
    ASSERT_TRUE(wait_manager.object_to_wait_requests_.empty());
  }

  std::unordered_set<ObjectID> local_objects;
  std::function<void()> delay_fn;
  int64_t delay_ms = -1;
  WaitManager wait_manager;
};

TEST_F(WaitManagerTest, TestImmediatelyCompleteWait) {
  ObjectID obj1 = ObjectID::FromRandom();
  ObjectID obj2 = ObjectID::FromRandom();
  local_objects.emplace(obj1);
  std::vector<ObjectID> ready;
  std::vector<ObjectID> remaining;
  wait_manager.Wait(std::vector<ObjectID>{obj1, obj2}, -1, 1,
                    [&](std::vector<ObjectID> _ready, std::vector<ObjectID> _remaining) {
                      ready = _ready;
                      remaining = _remaining;
                    });
  ASSERT_EQ(ready, std::vector<ObjectID>{obj1});
  ASSERT_EQ(remaining, std::vector<ObjectID>{obj2});
  ASSERT_EQ(delay_ms, -1);

  local_objects.clear();
  ready.clear();
  remaining.clear();
  // The wait should immediately complete since the timeout is 0.
  wait_manager.Wait(std::vector<ObjectID>{obj1, obj2}, 0, 1,
                    [&](std::vector<ObjectID> _ready, std::vector<ObjectID> _remaining) {
                      ready = _ready;
                      remaining = _remaining;
                    });
  ASSERT_EQ(ready, std::vector<ObjectID>{});
  ASSERT_EQ(remaining, (std::vector<ObjectID>{obj1, obj2}));
  ASSERT_EQ(delay_ms, -1);

  AssertNoLeaks();
}

TEST_F(WaitManagerTest, TestMultiWaits) {
  ObjectID obj1 = ObjectID::FromRandom();
  std::vector<ObjectID> ready1;
  std::vector<ObjectID> remaining1;
  std::vector<ObjectID> ready2;
  std::vector<ObjectID> remaining2;
  wait_manager.Wait(std::vector<ObjectID>{obj1}, -1, 1,
                    [&](std::vector<ObjectID> _ready, std::vector<ObjectID> _remaining) {
                      ready1 = _ready;
                      remaining1 = _remaining;
                    });
  wait_manager.Wait(std::vector<ObjectID>{obj1}, -1, 1,
                    [&](std::vector<ObjectID> _ready, std::vector<ObjectID> _remaining) {
                      ready2 = _ready;
                      remaining2 = _remaining;
                    });
  ASSERT_TRUE(ready1.empty());
  ASSERT_TRUE(ready2.empty());
  ASSERT_EQ(delay_ms, -1);

  wait_manager.HandleObjectLocal(obj1);
  ASSERT_EQ(ready1, std::vector<ObjectID>{obj1});
  ASSERT_EQ(remaining1, std::vector<ObjectID>{});
  ASSERT_EQ(ready2, std::vector<ObjectID>{obj1});
  ASSERT_EQ(remaining2, std::vector<ObjectID>{});

  // Make sure this doesn't crash.
  wait_manager.HandleObjectLocal(ObjectID::FromRandom());

  AssertNoLeaks();
}

TEST_F(WaitManagerTest, TestWaitTimeout) {
  ObjectID obj1 = ObjectID::FromRandom();
  std::vector<ObjectID> ready;
  std::vector<ObjectID> remaining;
  wait_manager.Wait(std::vector<ObjectID>{obj1}, 1, 1,
                    [&](std::vector<ObjectID> _ready, std::vector<ObjectID> _remaining) {
                      ready = _ready;
                      remaining = _remaining;
                    });
  ASSERT_TRUE(ready.empty());
  ASSERT_TRUE(remaining.empty());
  ASSERT_EQ(delay_ms, 1);

  // Fire the timer
  delay_fn();
  ASSERT_EQ(ready, std::vector<ObjectID>{});
  ASSERT_EQ(remaining, std::vector<ObjectID>{obj1});

  AssertNoLeaks();
}

}  // namespace raylet
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
