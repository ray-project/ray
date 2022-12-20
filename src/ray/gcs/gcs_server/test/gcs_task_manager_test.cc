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

#include "ray/gcs/gcs_server/gcs_task_manager.h"

#include <google/protobuf/util/message_differencer.h>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
namespace gcs {

class GcsTaskManagerTest : public ::testing::Test {
 public:
  GcsTaskManagerTest() {
    RayConfig::instance().initialize(
        R"(
{
  "task_events_max_num_task_in_gcs": 1000
}
  )");
  }

  virtual void SetUp() { task_manager.reset(new GcsTaskManager()); }

  virtual void TearDown() { task_manager->Stop(); }

  std::vector<TaskID> GenTaskIDs(size_t num_tasks) {
    std::vector<TaskID> task_ids;
    for (size_t i = 0; i < num_tasks; ++i) {
      task_ids.push_back(RandomTaskId());
    }
    return task_ids;
  }

  void ExpectTaskEventsEq(google::protobuf::RepeatedPtrField<rpc::TaskEvents> *expected,
                          google::protobuf::RepeatedPtrField<rpc::TaskEvents> *actual) {
    std::sort(expected->begin(), expected->end(), SortByTaskAttempt);
    std::sort(actual->begin(), actual->end(), SortByTaskAttempt);
    EXPECT_EQ(expected->size(), actual->size());
    for (int i = 0; i < expected->size(); ++i) {
      // Equivalent ignores default values.
      EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equivalent(expected->at(i),
                                                                         actual->at(i)))
          << "Expected: " << expected->at(i).DebugString()
          << "Actual: " << actual->at(i).DebugString();
    }
  }

  rpc::AddTaskEventDataReply SyncAddTaskEventData(const rpc::TaskEventData &events_data) {
    rpc::AddTaskEventDataRequest request;
    rpc::AddTaskEventDataReply reply;
    std::promise<bool> promise;

    request.mutable_data()->CopyFrom(events_data);
    task_manager->HandleAddTaskEventData(
        request,
        &reply,
        [&promise](Status, std::function<void()>, std::function<void()>) {
          promise.set_value(true);
        });

    promise.get_future().get();

    // Assert on RPC reply.
    EXPECT_EQ(StatusCode(reply.status().code()), StatusCode::OK);
    return reply;
  }

  rpc::GetTaskEventsReply SyncGetTaskEvents(
      absl::flat_hash_set<TaskID> task_ids,
      absl::optional<JobID> job_id = absl::nullopt) {
    rpc::GetTaskEventsRequest request;
    rpc::GetTaskEventsReply reply;
    std::promise<bool> promise;

    if (!task_ids.empty()) {
      for (const auto &task_id : task_ids) {
        request.mutable_task_ids()->add_vals(task_id.Binary());
      }
    }

    if (job_id) {
      request.set_job_id(job_id->Binary());
    }

    task_manager->HandleGetTaskEvents(
        request,
        &reply,
        [&promise](Status, std::function<void()>, std::function<void()>) {
          promise.set_value(true);
        });

    promise.get_future().get();

    EXPECT_EQ(StatusCode(reply.status().code()), StatusCode::OK);
    return reply;
  }

  static rpc::TaskInfoEntry GenTaskInfo(JobID job_id) {
    rpc::TaskInfoEntry task_info;
    task_info.set_job_id(job_id.Binary());
    return task_info;
  }

  static rpc::TaskStateUpdate GenStateUpdate() {
    rpc::TaskStateUpdate status_update;
    status_update.set_running_ts(1);
    return status_update;
  }

  static rpc::ProfileEvents GenProfileEvents(const std::string &name,
                                             uint64_t start,
                                             uint64_t end) {
    rpc::ProfileEvents profile_events;
    auto ev = profile_events.add_events();
    ev->set_event_name("event");
    ev->set_start_time(start);
    ev->set_end_time(end);
    return profile_events;
  }

  static std::vector<rpc::TaskEvents> GenTaskEvents(
      const std::vector<TaskID> &task_ids,
      int32_t attempt_number = 0,
      int32_t job_id = 0,
      absl::optional<rpc::ProfileEvents> profile_events = absl::nullopt,
      absl::optional<rpc::TaskStateUpdate> state_update = absl::nullopt,
      absl::optional<rpc::TaskInfoEntry> task_info = absl::nullopt) {
    std::vector<rpc::TaskEvents> result;
    for (auto const &task_id : task_ids) {
      rpc::TaskEvents events;
      events.set_task_id(task_id.Binary());
      events.set_job_id(JobID::FromInt(job_id).Binary());
      events.set_attempt_number(attempt_number);

      if (state_update.has_value()) {
        events.mutable_state_updates()->CopyFrom(*state_update);
      }

      if (profile_events.has_value()) {
        auto new_events = events.mutable_profile_events();
        new_events->CopyFrom(*profile_events);
      }

      if (task_info.has_value()) {
        events.mutable_task_info()->CopyFrom(*task_info);
      }

      result.push_back(events);
    }

    return result;
  }

  static bool SortByTaskAttempt(const rpc::TaskEvents &a, const rpc::TaskEvents &b) {
    return a.task_id() < b.task_id() || a.attempt_number() < b.attempt_number();
  }

  static std::vector<rpc::TaskEvents> ConcatTaskEvents(
      const std::vector<std::vector<rpc::TaskEvents>> &events_vec) {
    std::vector<rpc::TaskEvents> result;
    for (auto &events : events_vec) {
      std::copy(events.begin(), events.end(), std::back_inserter(result));
    }
    return result;
  }

  std::unique_ptr<GcsTaskManager> task_manager = nullptr;
};

class GcsTaskManagerMemoryLimitedTest : public GcsTaskManagerTest {
 public:
  GcsTaskManagerMemoryLimitedTest() : GcsTaskManagerTest() {
    RayConfig::instance().initialize(
        R"(
{
  "task_events_max_num_task_in_gcs": 100
}
  )");
  }
};

TEST_F(GcsTaskManagerTest, TestHandleAddTaskEventBasic) {
  size_t num_task_events = 100;
  int32_t num_status_events_dropped = 10;
  int32_t num_profile_events_dropped = 10;
  auto task_ids = GenTaskIDs(num_task_events);
  auto events = GenTaskEvents(task_ids, 0);
  auto events_data = Mocker::GenTaskEventsData(
      events, num_profile_events_dropped, num_status_events_dropped);

  auto reply = SyncAddTaskEventData(events_data);

  // Assert on RPC reply.
  EXPECT_EQ(StatusCode(reply.status().code()), StatusCode::OK);

  // Assert on actual data.
  {
    absl::MutexLock lock(&task_manager->mutex_);
    EXPECT_EQ(task_manager->task_event_storage_->task_events_.size(), num_task_events);
    EXPECT_EQ(task_manager->total_num_task_events_reported_, num_task_events);
    EXPECT_EQ(task_manager->total_num_profile_task_events_dropped_,
              num_profile_events_dropped);
    EXPECT_EQ(task_manager->total_num_status_task_events_dropped_,
              num_status_events_dropped);
  }
}

TEST_F(GcsTaskManagerTest, TestMergeTaskEventsSameTaskAttempt) {
  size_t num_task_events = 20;
  // Same task id and attempt
  auto task_ids = GenTaskIDs(1);
  int32_t attempt_number = 0;
  for (size_t i = 0; i < num_task_events; ++i) {
    auto profile_events = GenProfileEvents("event", i, i);
    auto events = GenTaskEvents(task_ids, attempt_number, 0, profile_events);
    auto events_data = Mocker::GenTaskEventsData(events);

    auto reply = SyncAddTaskEventData(events_data);
    EXPECT_EQ(StatusCode(reply.status().code()), StatusCode::OK);
  }

  // Assert on actual data
  {
    absl::MutexLock lock(&task_manager->mutex_);
    EXPECT_EQ(task_manager->task_event_storage_->task_events_.size(), 1);
    // Assert on events
    auto task_events = task_manager->task_event_storage_->task_events_[0];
    // Sort and assert profile events merged matched
    std::sort(task_events.mutable_profile_events()->mutable_events()->begin(),
              task_events.mutable_profile_events()->mutable_events()->end(),
              [](const rpc::ProfileEventEntry &a, const rpc::ProfileEventEntry &b) {
                return a.start_time() < b.start_time();
              });
    for (size_t i = 0; i < num_task_events; ++i) {
      auto ev = task_events.profile_events().events().at(i);
      EXPECT_EQ(ev.start_time(), i);
      EXPECT_EQ(ev.end_time(), i);
      EXPECT_EQ(ev.event_name(), "event");
    }
  }
}

TEST_F(GcsTaskManagerTest, TestGetTaskEvents) {
  // Add events
  size_t num_profile_events = 10;
  size_t num_status_events = 20;
  size_t num_both_events = 30;
  size_t num_profile_task_events_dropped = 10;
  size_t num_status_task_events_dropped = 20;

  std::vector<rpc::TaskEvents> events_with_profile;
  std::vector<rpc::TaskEvents> events_with_status;
  std::vector<rpc::TaskEvents> events_with_both;

  {
    auto task_ids1 = GenTaskIDs(num_profile_events);
    auto task_ids2 = GenTaskIDs(num_status_events);
    auto task_ids3 = GenTaskIDs(num_both_events);

    auto profile_events = GenProfileEvents("event", /*start*/ 1, /*end*/ 1);
    auto status_update = GenStateUpdate();

    events_with_profile =
        GenTaskEvents(task_ids1, /*attempt_number*/ 0, /* job_id */ 0, profile_events);
    events_with_status =
        GenTaskEvents(task_ids2, 0, 0, /*profile_events*/ absl::nullopt, status_update);
    events_with_both = GenTaskEvents(task_ids3, 0, 0, profile_events, status_update);

    auto all_events = {events_with_profile, events_with_status, events_with_both};
    for (auto &events : all_events) {
      auto data = Mocker::GenTaskEventsData(events);
      SyncAddTaskEventData(data);
    }
  }

  {
    // Add drop counter.
    auto data = Mocker::GenTaskEventsData(
        {}, num_profile_task_events_dropped, num_status_task_events_dropped);
    SyncAddTaskEventData(data);
  }

  // Test get all events
  {
    auto reply = SyncGetTaskEvents(/* task_ids */ {});
    // Expect all events
    std::vector<rpc::TaskEvents> expected_events =
        ConcatTaskEvents({events_with_status, events_with_profile, events_with_both});

    auto expected_data = Mocker::GenTaskEventsData(expected_events);
    // Expect match events
    ExpectTaskEventsEq(expected_data.mutable_events_by_task(),
                       reply.mutable_events_by_task());

    EXPECT_EQ(reply.num_profile_task_events_dropped(), num_profile_task_events_dropped);
    EXPECT_EQ(reply.num_status_task_events_dropped(), num_status_task_events_dropped);
  }
}

TEST_F(GcsTaskManagerTest, TestGetTaskEventsByTaskIDs) {
  int32_t num_events_task_1 = 10;
  int32_t num_events_task_2 = 20;

  rpc::TaskEventData events_data_task1;
  auto task_id1 = RandomTaskId();
  {
    std::vector<std::vector<rpc::TaskEvents>> all_events;
    for (int32_t attempt_num = 0; attempt_num < num_events_task_1; ++attempt_num) {
      all_events.push_back(GenTaskEvents({task_id1}, attempt_num));
    }
    auto events_task1 = ConcatTaskEvents(all_events);
    events_data_task1 = Mocker::GenTaskEventsData(events_task1);
    SyncAddTaskEventData(events_data_task1);
  }

  rpc::TaskEventData events_data_task2;
  auto task_id2 = RandomTaskId();
  {
    std::vector<std::vector<rpc::TaskEvents>> all_events;
    for (int32_t attempt_num = 0; attempt_num < num_events_task_2; ++attempt_num) {
      all_events.push_back(GenTaskEvents({task_id2}, attempt_num));
    }
    auto events_task2 = ConcatTaskEvents(all_events);
    events_data_task2 = Mocker::GenTaskEventsData(events_task2);
    SyncAddTaskEventData(events_data_task2);
  }

  auto reply_task1 = SyncGetTaskEvents({task_id1});
  auto reply_task2 = SyncGetTaskEvents({task_id2});

  // Check matched
  ExpectTaskEventsEq(events_data_task1.mutable_events_by_task(),
                     reply_task1.mutable_events_by_task());
  ExpectTaskEventsEq(events_data_task2.mutable_events_by_task(),
                     reply_task2.mutable_events_by_task());
}

TEST_F(GcsTaskManagerTest, TestGetTaskEventsByJob) {
  size_t num_task_job1 = 10;
  size_t num_task_job2 = 20;

  rpc::TaskEventData events_data_job1;
  {
    auto task_ids = GenTaskIDs(num_task_job1);
    auto task_info = GenTaskInfo(JobID::FromInt(1));
    auto events = GenTaskEvents(task_ids,
                                /* attempt_number */ 0,
                                /* job_id */ 1,
                                absl::nullopt,
                                absl::nullopt,
                                task_info);
    events_data_job1 = Mocker::GenTaskEventsData(events);
    SyncAddTaskEventData(events_data_job1);
  }

  rpc::TaskEventData events_data_job2;
  {
    auto task_ids = GenTaskIDs(num_task_job2);
    auto task_info = GenTaskInfo(JobID::FromInt(2));
    auto events = GenTaskEvents(task_ids,
                                /* attempt_number */
                                0,
                                /* job_id */ 2,
                                absl::nullopt,
                                absl::nullopt,
                                task_info);
    events_data_job2 = Mocker::GenTaskEventsData(events);
    SyncAddTaskEventData(events_data_job2);
  }

  auto reply_job1 = SyncGetTaskEvents(/* task_ids */ {}, JobID::FromInt(1));
  auto reply_job2 = SyncGetTaskEvents({}, JobID::FromInt(2));

  // Check matched
  ExpectTaskEventsEq(events_data_job1.mutable_events_by_task(),
                     reply_job1.mutable_events_by_task());
  ExpectTaskEventsEq(events_data_job2.mutable_events_by_task(),
                     reply_job2.mutable_events_by_task());
}

TEST_F(GcsTaskManagerMemoryLimitedTest, TestLimitTaskEvents) {
  size_t num_limit = 100;  // synced with test config

  size_t num_profile_events_to_drop = 70;
  size_t num_status_events_to_drop = 30;
  size_t num_batch1 = num_profile_events_to_drop + num_status_events_to_drop;
  size_t num_batch2 = 100;

  size_t num_profile_events_dropped_on_worker = 88;
  size_t num_status_events_dropped_on_worker = 22;
  {
    // Add profile event.
    auto events = GenTaskEvents(GenTaskIDs(num_profile_events_to_drop),
                                /* attempt_number */ 0,
                                /* job_id */ 0,
                                GenProfileEvents("event", 1, 1));
    auto events_data =
        Mocker::GenTaskEventsData(events, num_profile_events_dropped_on_worker);
    SyncAddTaskEventData(events_data);
  }
  {
    // Add status update events.
    auto events = GenTaskEvents(GenTaskIDs(num_status_events_to_drop),
                                /* attempt_number*/ 0,
                                /* job_id */ 0,
                                /* profile_events */ absl::nullopt,
                                GenStateUpdate());
    auto events_data = Mocker::GenTaskEventsData(events,
                                                 /*num_profile_task_events_dropped*/ 0,
                                                 num_status_events_dropped_on_worker);
    SyncAddTaskEventData(events_data);
  }

  // Expected events in the buffer
  std::vector<rpc::TaskEvents> expected_events;
  {
    // Add new task events to overwrite the existing ones.
    expected_events = GenTaskEvents(GenTaskIDs(num_batch2), 0);
    auto events_data = Mocker::GenTaskEventsData(expected_events);
    SyncAddTaskEventData(events_data);
  }

  // Assert on actual data.
  {
    absl::MutexLock lock(&task_manager->mutex_);
    EXPECT_EQ(task_manager->task_event_storage_->task_events_.size(), num_limit);
    EXPECT_EQ(task_manager->total_num_task_events_reported_, num_batch1 + num_batch2);

    std::sort(expected_events.begin(), expected_events.end(), SortByTaskAttempt);
    auto actual_events = task_manager->task_event_storage_->task_events_;
    std::sort(actual_events.begin(), actual_events.end(), SortByTaskAttempt);
    EXPECT_EQ(actual_events.size(), expected_events.size());
    for (size_t i = 0; i < actual_events.size(); ++i) {
      EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(actual_events[i],
                                                                     expected_events[i]));
    }

    // Assert on drop counts.
    EXPECT_EQ(task_manager->total_num_status_task_events_dropped_,
              num_status_events_to_drop + num_status_events_dropped_on_worker);
    EXPECT_EQ(task_manager->total_num_profile_task_events_dropped_,
              num_profile_events_to_drop + num_profile_events_dropped_on_worker);
  }
}

}  // namespace gcs
}  // namespace ray
