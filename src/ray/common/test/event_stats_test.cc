// Copyright 2023 The Ray Authors.
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

#include "ray/common/event_stats.h"

#include "gtest/gtest.h"

TEST(EventStatsTest, TestRecordEnd) {
  EventTracker event_tracker;
  std::shared_ptr<StatsHandle> handle = event_tracker.RecordStart("method");
  auto event_stats = event_tracker.get_event_stats("method").value();
  ASSERT_EQ(event_stats.cum_count, 1);
  ASSERT_EQ(event_stats.curr_count, 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  event_tracker.RecordEnd(std::move(handle));
  event_stats = event_tracker.get_event_stats("method").value();
  ASSERT_EQ(event_stats.cum_count, 1);
  ASSERT_EQ(event_stats.curr_count, 0);
  ASSERT_GE(event_stats.cum_execution_time, 100000000);
}

TEST(EventStatsTest, TestRecordExecution) {
  EventTracker event_tracker;
  std::shared_ptr<StatsHandle> handle = event_tracker.RecordStart("method");
  auto event_stats = event_tracker.get_event_stats("method").value();
  ASSERT_EQ(event_stats.cum_count, 1);
  ASSERT_EQ(event_stats.curr_count, 1);
  // Queueing time
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  event_tracker.RecordExecution(
      [&] {
        // Execution time
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        event_stats = event_tracker.get_event_stats("method").value();
        ASSERT_EQ(event_stats.running_count, 1);
      },
      std::move(handle));
  event_stats = event_tracker.get_event_stats("method").value();
  ASSERT_EQ(event_stats.cum_count, 1);
  ASSERT_EQ(event_stats.curr_count, 0);
  ASSERT_EQ(event_stats.running_count, 0);
  ASSERT_GE(event_stats.cum_execution_time, 200000000);
  ASSERT_GE(event_stats.cum_queue_time, 100000000);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
