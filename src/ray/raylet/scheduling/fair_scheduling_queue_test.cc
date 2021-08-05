#include "ray/raylet/scheduling/fair_scheduling_queue.h"
#include "ray/raylet/scheduling/test_util.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ray {

namespace raylet {

using ::testing::_;

class FairSchedulingQueueTest : public ::testing::Test {
 public:
  FairSchedulingQueue queue_;

  Work TaskToWork(Task &task) { return std::make_tuple(task, nullptr, nullptr); }

  void AssertNoLeaks() {
    ASSERT_EQ(queue_.active_tasks_.size(), 0);
    ASSERT_EQ(queue_.work_queue_.size(), 0);
  }
};

TEST_F(FairSchedulingQueueTest, TestBasic) {
  /*
   * When tasks are queued, make sure we prioritize the shapes that aren't
   * currently running.
   */
  Task task = CreateTask({{ray::kCPU_ResourceLabel, 4}});
  Task task1 = CreateTask({{ray::kCPU_ResourceLabel, 2}});
  Task task2 = CreateTask({{ray::kCPU_ResourceLabel, 1}});

  queue_.MarkRunning(task);
  queue_.MarkRunning(task);
  queue_.MarkRunning(task1);

  queue_.Push(TaskToWork(task));
  queue_.Push(TaskToWork(task1));
  queue_.Push(TaskToWork(task2));

  std::vector<std::pair<const SchedulingClass, std::deque<Work>>> ordered(queue_.begin(),
                                                                          queue_.end());

  Task expected[] = {task2, task1, task};
  ASSERT_EQ(ordered.size(), 3);
  for (int i = 0; i < 3; i++) {
    std::deque<Work> &deque = ordered[i].second;
    ASSERT_EQ(deque.size(), 1);
    const Work &work = deque.front();
    const Task &t = std::get<0>(work);
    ASSERT_EQ(t.GetTaskSpecification().TaskId(),
              expected[i].GetTaskSpecification().TaskId());
  }
}


TEST_F(FairSchedulingQueueTest, TestPushAll) {
  /*
    * When tasks are queued, make sure we prioritize the shapes that aren't
    * currently running.
    */
  std::deque<Work> to_add;

  Task task;

  for (int i = 0; i < 10; i++) {
    task = CreateTask({{ray::kCPU_ResourceLabel, 4}}, i);
    to_add.push_back(TaskToWork(task));
  }

  // queue_.PushAll(task.GetTaskSpecification().GetSchedulingClass(), to_add);

  // auto deque = queue_.begin()->second;

  // auto it1 = to_add.begin();
  // auto it2 = deque.begin();

  // while (it1 != to_add.end() && it2 != deque.end()) {
  //   ASSERT_EQ(*it1++, *it2++);
  // }

  // ASSERT_EQ(it1, to_add.end());
  // ASSERT_EQ(it2, deque.end());
}

TEST_F(FairSchedulingQueueTest, TestFreeAfterErase) {
  /*
   * Ensure we dont' leak memory by accidently storing keys after the queue has been
   * removed.
   */
  Task task = CreateTask({{ray::kCPU_ResourceLabel, 4}});
  queue_.Push(TaskToWork(task));
  auto it = queue_.begin();
  queue_.erase(it);

  AssertNoLeaks();
}

TEST_F(FairSchedulingQueueTest, TestFreeAfterTaskFinish) {
  /*
   * Ensure we dont' leak memory by accidently storing keys after the queue has been
   * removed.
   */
  Task task = CreateTask({{ray::kCPU_ResourceLabel, 4}});
  queue_.Push(TaskToWork(task));
  auto it = queue_.begin();
  queue_.erase(it);

  queue_.MarkRunning(task);
  queue_.MarkFinished(task);

  AssertNoLeaks();
}

TEST_F(FairSchedulingQueueTest, TestMultiLevelTasks) {
  /*
   * This test simulates what the scheduling queue sees when a some function
   * `f` calls another function `g`, and many instances of `f` are queued at
   * once.
   */

  Task running_f = CreateTask({{ray::kCPU_ResourceLabel, 1}});
  // Queue a bunch of waiting f's.
  for (int i = 0; i < 10; i++) {
    auto task = CreateTask({{ray::kCPU_ResourceLabel, 1}});
    queue_.Push(TaskToWork(task));
  }

  queue_.MarkRunning(running_f);

  // This task with a different scheduling class is funtion `g`.
  Task task_g = CreateTask({{ray::kCPU_ResourceLabel, 2}});
  queue_.Push(TaskToWork(task_g));

  ASSERT_EQ(queue_.begin()->first, task_g.GetTaskSpecification().GetSchedulingClass());
}

}  // namespace raylet
}  // namespace ray
