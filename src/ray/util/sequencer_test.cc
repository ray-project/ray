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
    auto operation = [index, key, &queue, &sequencer] {
      sleep(1);
      queue.push_back(index);
      sequencer.post_execute(key);
    };
    sequencer.execute_ordered(key, operation);
  }

  while (queue.size() < size) {
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