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

#include "ray/util/sequencer.h"
#include <unistd.h>
#include "gtest/gtest.h"
#include "ray/util/logging.h"

namespace ray {

TEST(SequencerTest, ExecuteOrderedTest) {
  Sequencer<int> sequencer;
  std::deque<int> queue;
  int key = 1;
  int size = 100;
  for (int index = 0; index < size; ++index) {
    auto operation = [index, &queue](SequencerDoneCallback done_callback) {
      usleep(1000);
      queue.push_back(index);
      done_callback();
    };
    sequencer.Post(key, operation);
  }

  while (queue.size() < (size_t)size) {
    usleep(1000);
  }

  for (int index = 0; index < size; ++index) {
    ASSERT_EQ(queue.front(), index);
    queue.pop_front();
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
