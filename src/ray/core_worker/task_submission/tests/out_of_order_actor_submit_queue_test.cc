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

#include "ray/core_worker/task_submission/out_of_order_actor_submit_queue.h"

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace ray {
namespace core {
class OutofOrderActorSubmitQueueTestPeer {
 public:
  using CandidateModel = std::map<std::string, std::set<uint64_t>>;

  static CandidateModel ActualCandidates(const OutofOrderActorSubmitQueue &queue) {
    CandidateModel actual;
    for (const auto &[group, positions] : queue.restart_failure_candidates_per_group_) {
      actual.emplace(group, std::set<uint64_t>(positions.begin(), positions.end()));
    }
    return actual;
  }

  static CandidateModel ExpectedCandidates(const OutofOrderActorSubmitQueue &queue) {
    CandidateModel expected;
    for (const auto &[group, sending] : queue.sending_queue_per_group_) {
      for (const auto &[position, task] : sending) {
        const auto &spec = task.first;
        if (!spec.IsRetry() && spec.MaxRetries() == 0) {
          expected[group].insert(position);
        }
      }
    }
    return expected;
  }

  static void ExpectInvariant(const OutofOrderActorSubmitQueue &queue) {
    EXPECT_EQ(ActualCandidates(queue), ExpectedCandidates(queue));
  }
};

namespace {

TaskSpecification BuildTaskSpec(uint64_t seq,
                                const std::string &concurrency_group = "",
                                int32_t max_retries = 0,
                                int32_t attempt_number = 0) {
  TaskSpecification spec;
  spec.GetMutableMessage().set_task_id(TaskID::FromRandom(JobID()).Binary());
  spec.GetMutableMessage().set_type(ray::rpc::TaskType::ACTOR_TASK);
  spec.GetMutableMessage().set_concurrency_group_name(concurrency_group);
  spec.GetMutableMessage().set_max_retries(max_retries);
  spec.GetMutableMessage().set_attempt_number(attempt_number);
  spec.GetMutableMessage()
      .mutable_actor_task_spec()
      ->set_concurrency_group_sequence_number(seq);
  return spec;
}

using TaskKey = std::pair<std::string, uint64_t>;

TaskKey GetTaskKey(const TaskSpecification &spec) {
  return {spec.ConcurrencyGroupName(), spec.ConcurrencyGroupSequenceNumber()};
}

std::set<TaskKey> GetTaskKeys(const std::vector<TaskSpecification> &specs) {
  std::set<TaskKey> keys;
  for (const auto &spec : specs) {
    keys.insert(GetTaskKey(spec));
  }
  return keys;
}

std::set<TaskKey> PopAllTaskKeys(OutofOrderActorSubmitQueue &queue) {
  std::set<TaskKey> keys;
  while (auto task = queue.PopNextTaskToSend()) {
    keys.insert(GetTaskKey(task->first));
  }
  return keys;
}

}  // namespace

TEST(OutofOrderActorSubmitQueueTest, PassThroughTest) {
  OutofOrderActorSubmitQueue queue;
  // insert request 0 1 2 3 4
  std::vector<TaskID> task_ids;
  for (uint64_t i = 0; i < 5; i++) {
    auto spec = BuildTaskSpec(i);
    task_ids.push_back(spec.TaskId());
    queue.Emplace("", i, std::move(spec));
  }
  // contains and gets
  for (uint64_t i = 0; i < 5; i++) {
    EXPECT_TRUE(queue.Contains("", i));
    EXPECT_FALSE(queue.DependenciesResolved("", i));
  }
  // dependency failure remove request 4
  queue.MarkDependencyFailed("", 4);
  for (uint64_t i = 0; i < 5; i++) {
    if (i != 4) {
      EXPECT_TRUE(queue.Contains("", i));
      EXPECT_FALSE(queue.DependenciesResolved("", i));
    } else {
      EXPECT_FALSE(queue.Contains("", i));
    }
  }

  // nothing is resolved.
  EXPECT_FALSE(queue.PopNextTaskToSend().has_value());

  // dependency resolved for request 1 and 3
  queue.MarkDependencyResolved("", 1);
  queue.MarkDependencyResolved("", 3);
  for (uint64_t i = 0; i < 4; i++) {
    EXPECT_TRUE(queue.Contains("", i));
    if (i == 1 || i == 3) {
      EXPECT_TRUE(queue.DependenciesResolved("", i));
    } else {
      EXPECT_FALSE(queue.DependenciesResolved("", i));
    }
  }

  // task 1 and task 3 is ready to send.
  EXPECT_EQ(queue.PopNextTaskToSend()->first.ConcurrencyGroupSequenceNumber(), 1);
  EXPECT_EQ(queue.PopNextTaskToSend()->first.ConcurrencyGroupSequenceNumber(), 3);
  EXPECT_FALSE(queue.PopNextTaskToSend().has_value());

  // only contains task 2 and 4.
  for (uint64_t i = 0; i < 5; i++) {
    if (i == 0 || i == 2) {
      EXPECT_TRUE(queue.Contains("", i));
      EXPECT_FALSE(queue.DependenciesResolved("", i));
    } else {
      EXPECT_FALSE(queue.Contains("", i));
    }
  }

  queue.MarkDependencyResolved("", 2);
  std::vector<TaskID> expected_cleared_task_ids = {task_ids[0], task_ids[2]};
  // clear all tasks.
  auto ret = queue.ClearAllTasks();
  EXPECT_EQ(ret, expected_cleared_task_ids);
  for (uint64_t i = 0; i < 5; i++) {
    EXPECT_FALSE(queue.Contains("", i));
  }
}

TEST(OutofOrderActorSubmitQueueTest, RetryableInitialTasksRemainBufferedOnRestart) {
  constexpr uint64_t kNumTasks = 256;
  OutofOrderActorSubmitQueue queue;

  for (uint64_t i = 0; i < kNumTasks; i++) {
    queue.Emplace("", i, BuildTaskSpec(i, "", /*max_retries=*/1));
  }
  for (uint64_t i = 0; i < kNumTasks; i++) {
    queue.MarkDependencyResolved("", i);
    EXPECT_TRUE(queue.PopTasksToFailOnActorRestart().empty());
  }

  for (uint64_t i = 0; i < kNumTasks; i++) {
    auto task = queue.PopNextTaskToSend();
    ASSERT_TRUE(task.has_value());
    EXPECT_EQ(task->first.ConcurrencyGroupSequenceNumber(), i);
  }
  EXPECT_TRUE(queue.Empty());
}

TEST(OutofOrderActorSubmitQueueTest, RetryAttemptsRemainBufferedOnRestart) {
  constexpr uint64_t kNumTasks = 8;
  OutofOrderActorSubmitQueue queue;

  for (uint64_t i = 0; i < kNumTasks; i++) {
    queue.Emplace("", i, BuildTaskSpec(i, "", /*max_retries=*/0, /*attempt_number=*/1));
    queue.MarkDependencyResolved("", i);
    EXPECT_TRUE(queue.PopTasksToFailOnActorRestart().empty());
  }

  for (uint64_t i = 0; i < kNumTasks; i++) {
    auto task = queue.PopNextTaskToSend();
    ASSERT_TRUE(task.has_value());
    EXPECT_EQ(task->first.ConcurrencyGroupSequenceNumber(), i);
    EXPECT_TRUE(task->first.IsRetry());
  }
  EXPECT_TRUE(queue.Empty());
}

TEST(OutofOrderActorSubmitQueueTest, RestartFailureDrainsOnlyZeroRetryInitialTasks) {
  constexpr uint64_t kNumRetryableTasks = 16;
  constexpr uint64_t kNumZeroRetryTasks = 8;
  OutofOrderActorSubmitQueue queue;

  for (uint64_t i = 0; i < kNumRetryableTasks + kNumZeroRetryTasks; i++) {
    const auto max_retries = i < kNumRetryableTasks ? 1 : 0;
    queue.Emplace("", i, BuildTaskSpec(i, "", max_retries));
  }

  std::vector<uint64_t> failed_positions;
  for (uint64_t i = 0; i < kNumRetryableTasks + kNumZeroRetryTasks; i++) {
    queue.MarkDependencyResolved("", i);
    for (const auto &task : queue.PopTasksToFailOnActorRestart()) {
      failed_positions.push_back(task.ConcurrencyGroupSequenceNumber());
    }
  }

  std::vector<uint64_t> expected_failed_positions;
  for (uint64_t i = kNumRetryableTasks; i < kNumRetryableTasks + kNumZeroRetryTasks;
       i++) {
    expected_failed_positions.push_back(i);
  }
  EXPECT_EQ(failed_positions, expected_failed_positions);
  EXPECT_TRUE(queue.PopTasksToFailOnActorRestart().empty());

  for (uint64_t i = 0; i < kNumRetryableTasks; i++) {
    auto task = queue.PopNextTaskToSend();
    ASSERT_TRUE(task.has_value());
    EXPECT_EQ(task->first.ConcurrencyGroupSequenceNumber(), i);
  }
  EXPECT_TRUE(queue.Empty());
}

TEST(OutofOrderActorSubmitQueueTest, CancellationRemovesRestartCandidates) {
  OutofOrderActorSubmitQueue queue;

  queue.Emplace("", 0, BuildTaskSpec(0));
  queue.MarkTaskCanceled("", 0);
  EXPECT_FALSE(queue.Contains("", 0));

  queue.Emplace("", 1, BuildTaskSpec(1));
  queue.MarkDependencyResolved("", 1);
  queue.MarkTaskCanceled("", 1);
  EXPECT_FALSE(queue.Contains("", 1));

  EXPECT_TRUE(queue.PopTasksToFailOnActorRestart().empty());
  EXPECT_FALSE(queue.PopNextTaskToSend().has_value());
  EXPECT_TRUE(queue.Empty());
}

TEST(OutofOrderActorSubmitQueueTest, NormalPopRemovesRestartCandidate) {
  OutofOrderActorSubmitQueue queue;
  queue.Emplace("", 0, BuildTaskSpec(0));
  queue.Emplace("", 1, BuildTaskSpec(1));
  queue.MarkDependencyResolved("", 0);
  queue.MarkDependencyResolved("", 1);

  auto sent_task = queue.PopNextTaskToSend();
  ASSERT_TRUE(sent_task.has_value());
  EXPECT_EQ(sent_task->first.ConcurrencyGroupSequenceNumber(), 0);

  auto tasks_to_fail = queue.PopTasksToFailOnActorRestart();
  ASSERT_EQ(tasks_to_fail.size(), 1);
  EXPECT_EQ(tasks_to_fail.front().ConcurrencyGroupSequenceNumber(), 1);
  EXPECT_TRUE(queue.Empty());
}

TEST(OutofOrderActorSubmitQueueTest, RestartCandidatesAreIndependentPerGroup) {
  OutofOrderActorSubmitQueue queue;
  queue.Emplace("group_a", 0, BuildTaskSpec(0, "group_a"));
  queue.Emplace("group_b", 0, BuildTaskSpec(0, "group_b"));
  queue.Emplace("group_a", 1, BuildTaskSpec(1, "group_a", /*max_retries=*/1));
  queue.Emplace("group_b", 1, BuildTaskSpec(1, "group_b", /*max_retries=*/1));
  queue.MarkDependencyResolved("group_a", 0);
  queue.MarkDependencyResolved("group_b", 0);
  queue.MarkDependencyResolved("group_a", 1);
  queue.MarkDependencyResolved("group_b", 1);

  const auto tasks_to_fail = queue.PopTasksToFailOnActorRestart();
  EXPECT_EQ(GetTaskKeys(tasks_to_fail),
            (std::set<TaskKey>{{"group_a", 0}, {"group_b", 0}}));
  EXPECT_EQ(PopAllTaskKeys(queue), (std::set<TaskKey>{{"group_a", 1}, {"group_b", 1}}));
  EXPECT_TRUE(queue.Empty());
}

TEST(OutofOrderActorSubmitQueueTest, ReadyCandidatesRemainDiscoverable) {
  OutofOrderActorSubmitQueue queue;
  for (const uint64_t position : {3, 1, 2}) {
    queue.Emplace("", position, BuildTaskSpec(position));
    queue.MarkDependencyResolved("", position);
  }
  queue.Emplace("", 0, BuildTaskSpec(0, "", /*max_retries=*/1));
  queue.MarkDependencyResolved("", 0);

  EXPECT_TRUE(queue.Contains("", 1));
  EXPECT_TRUE(queue.DependenciesResolved("", 1));

  const auto tasks_to_fail = queue.PopTasksToFailOnActorRestart();
  ASSERT_EQ(tasks_to_fail.size(), 3);
  EXPECT_EQ(tasks_to_fail[0].ConcurrencyGroupSequenceNumber(), 1);
  EXPECT_EQ(tasks_to_fail[1].ConcurrencyGroupSequenceNumber(), 2);
  EXPECT_EQ(tasks_to_fail[2].ConcurrencyGroupSequenceNumber(), 3);

  auto retained_task = queue.PopNextTaskToSend();
  ASSERT_TRUE(retained_task.has_value());
  EXPECT_EQ(retained_task->first.ConcurrencyGroupSequenceNumber(), 0);
  EXPECT_TRUE(queue.Empty());
}

TEST(OutofOrderActorSubmitQueueTest, DependencyFailureAndClearLeaveNoCandidates) {
  OutofOrderActorSubmitQueue queue;
  auto dependency_failed_task = BuildTaskSpec(0);
  const auto dependency_failed_task_id = dependency_failed_task.TaskId();
  queue.Emplace("", 0, dependency_failed_task);
  queue.MarkDependencyFailed("", 0);

  auto ready_task = BuildTaskSpec(1);
  const auto ready_task_id = ready_task.TaskId();
  queue.Emplace("", 1, ready_task);
  queue.MarkDependencyResolved("", 1);

  auto unresolved_task = BuildTaskSpec(2, "", /*max_retries=*/1);
  const auto unresolved_task_id = unresolved_task.TaskId();
  queue.Emplace("", 2, unresolved_task);

  const auto cleared_task_ids = queue.ClearAllTasks();
  EXPECT_EQ(cleared_task_ids.size(), 2);
  EXPECT_EQ(
      std::find(
          cleared_task_ids.begin(), cleared_task_ids.end(), dependency_failed_task_id),
      cleared_task_ids.end());
  EXPECT_NE(std::find(cleared_task_ids.begin(), cleared_task_ids.end(), ready_task_id),
            cleared_task_ids.end());
  EXPECT_NE(
      std::find(cleared_task_ids.begin(), cleared_task_ids.end(), unresolved_task_id),
      cleared_task_ids.end());
  EXPECT_TRUE(queue.PopTasksToFailOnActorRestart().empty());
  EXPECT_FALSE(queue.PopNextTaskToSend().has_value());
  EXPECT_TRUE(queue.Empty());
}

TEST(OutofOrderActorSubmitQueueTest, RestartFailureCandidateIndexInvariant) {
  OutofOrderActorSubmitQueue queue;
  const auto expect_invariant = [&queue]() {
    OutofOrderActorSubmitQueueTestPeer::ExpectInvariant(queue);
  };

  expect_invariant();

  queue.Emplace("group_a", 0, BuildTaskSpec(0, "group_a"));
  expect_invariant();
  queue.MarkDependencyResolved("group_a", 0);
  expect_invariant();

  queue.Emplace("group_a", 1, BuildTaskSpec(1, "group_a", /*max_retries=*/1));
  expect_invariant();
  queue.MarkDependencyResolved("group_a", 1);
  expect_invariant();

  queue.Emplace(
      "group_a", 2, BuildTaskSpec(2, "group_a", /*max_retries=*/0, /*attempt_number=*/1));
  expect_invariant();
  queue.MarkDependencyResolved("group_a", 2);
  expect_invariant();

  queue.Emplace("group_b", 0, BuildTaskSpec(0, "group_b"));
  expect_invariant();
  queue.MarkDependencyResolved("group_b", 0);
  expect_invariant();

  queue.Emplace("group_b", 1, BuildTaskSpec(1, "group_b"));
  expect_invariant();
  queue.MarkDependencyFailed("group_b", 1);
  expect_invariant();

  queue.Emplace("group_b", 2, BuildTaskSpec(2, "group_b"));
  expect_invariant();
  queue.MarkTaskCanceled("group_b", 2);
  expect_invariant();

  queue.MarkTaskCanceled("group_b", 0);
  expect_invariant();

  auto candidate_to_send = queue.PopNextTaskToSend();
  expect_invariant();
  ASSERT_TRUE(candidate_to_send.has_value());
  EXPECT_EQ(GetTaskKey(candidate_to_send->first), (TaskKey{"group_a", 0}));

  auto noncandidate_to_send = queue.PopNextTaskToSend();
  expect_invariant();
  ASSERT_TRUE(noncandidate_to_send.has_value());
  EXPECT_EQ(GetTaskKey(noncandidate_to_send->first), (TaskKey{"group_a", 1}));

  queue.Emplace("group_a", 3, BuildTaskSpec(3, "group_a"));
  expect_invariant();
  queue.MarkDependencyResolved("group_a", 3);
  expect_invariant();

  const auto restart_failures = queue.PopTasksToFailOnActorRestart();
  expect_invariant();
  ASSERT_EQ(restart_failures.size(), 1);
  EXPECT_EQ(GetTaskKey(restart_failures.front()), (TaskKey{"group_a", 3}));

  queue.Emplace("group_c", 0, BuildTaskSpec(0, "group_c"));
  expect_invariant();
  queue.Emplace("group_c", 1, BuildTaskSpec(1, "group_c"));
  expect_invariant();
  queue.MarkDependencyResolved("group_c", 1);
  expect_invariant();

  const auto cleared_tasks = queue.ClearAllTasks();
  expect_invariant();
  EXPECT_EQ(cleared_tasks.size(), 3);
  EXPECT_TRUE(queue.Empty());
}

}  // namespace core
}  // namespace ray
