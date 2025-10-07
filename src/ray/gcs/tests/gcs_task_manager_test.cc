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

#include "ray/gcs/gcs_task_manager.h"

#include <google/protobuf/util/message_differencer.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/id.h"
#include "ray/common/protobuf_utils.h"
#include "ray/common/status.h"
#include "ray/common/test_utils.h"

namespace ray {
namespace gcs {

class GcsTaskManagerTest : public ::testing::Test {
 public:
  GcsTaskManagerTest() {
    RayConfig::instance().initialize(
        R"(
{
  "task_events_max_num_task_in_gcs": 1000,
  "gcs_mark_task_failed_on_job_done_delay_ms": 100
}
  )");
  }

  virtual void SetUp() {
    io_context_ = std::make_unique<InstrumentedIOContextWithThread>("GcsTaskManagerTest");
    task_manager = std::make_unique<GcsTaskManager>(io_context_->GetIoService());
  }

  virtual void TearDown() {
    task_manager.reset();
    io_context_.reset();
  }

  std::vector<TaskID> GenTaskIDs(size_t num_tasks) {
    std::vector<TaskID> task_ids;
    for (size_t i = 0; i < num_tasks; ++i) {
      task_ids.push_back(RandomTaskId());
    }
    return task_ids;
  }

  TaskID GenTaskIDForJob(int job_id) {
    auto random_parent = RandomTaskId();
    return TaskID::ForNormalTask(JobID::FromInt(job_id), random_parent, 0);
  }

  std::vector<WorkerID> GenWorkerIDs(size_t num_workers) {
    std::vector<WorkerID> worker_ids;
    for (size_t i = 0; i < num_workers; ++i) {
      worker_ids.push_back(WorkerID::FromRandom());
    }
    return worker_ids;
  }

  void ExpectTaskEventsEq(google::protobuf::RepeatedPtrField<rpc::TaskEvents> *expected,
                          google::protobuf::RepeatedPtrField<rpc::TaskEvents> *actual) {
    std::sort(expected->begin(), expected->end(), SortByTaskIdAndAttempt);
    std::sort(actual->begin(), actual->end(), SortByTaskIdAndAttempt);
    EXPECT_EQ(expected->size(), actual->size());
    for (int i = 0; i < expected->size(); ++i) {
      // Equivalent ignores default values.
      EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equivalent(expected->at(i),
                                                                         actual->at(i)))
          << "Expected: " << expected->at(i).DebugString()
          << "Actual: " << actual->at(i).DebugString();
    }
  }

  void ExpectTaskEventsEq(
      std::vector<google::protobuf::RepeatedPtrField<rpc::TaskEvents> *> expected,
      google::protobuf::RepeatedPtrField<rpc::TaskEvents> *actual) {
    google::protobuf::RepeatedPtrField<rpc::TaskEvents> expectedMerged;
    for (auto expectedEvent : expected) {
      expectedMerged.MergeFrom(*expectedEvent);
    }
    ExpectTaskEventsEq(&expectedMerged, actual);
  }

  void SyncAddTaskEvent(
      const std::vector<TaskID> &tasks,
      const std::vector<std::pair<rpc::TaskStatus, int64_t>> &status_timestamps,
      const TaskID &parent_task_id = TaskID::Nil(),
      int job_id = 0,
      std::optional<rpc::RayErrorInfo> error_info = absl::nullopt,
      ActorID actor_id = ActorID::Nil()) {
    auto events = GenTaskEvents(
        tasks,
        /* attempt_number */ 0,
        /* job_id */ job_id,
        /* profile event */ absl::nullopt,
        GenStateUpdate(status_timestamps),
        GenTaskInfo(JobID::FromInt(job_id),
                    parent_task_id,
                    actor_id.IsNil() ? TaskType::NORMAL_TASK : TaskType::ACTOR_TASK,
                    actor_id),
        error_info);
    auto events_data = GenTaskEventsData(events);
    SyncAddTaskEventData(events_data);
  }

  rpc::AddTaskEventDataReply SyncAddTaskEventData(const rpc::TaskEventData &events_data) {
    rpc::AddTaskEventDataRequest request;
    rpc::AddTaskEventDataReply reply;
    std::promise<bool> promise;

    request.mutable_data()->CopyFrom(events_data);
    // Dispatch so that it runs in GcsTaskManager's io service.
    io_context_->GetIoService().dispatch(
        [this, &promise, &request, &reply]() {
          task_manager->HandleAddTaskEventData(
              request,
              &reply,
              [&promise](Status, std::function<void()>, std::function<void()>) {
                promise.set_value(true);
              });
        },
        "SyncAddTaskEventData");

    promise.get_future().get();

    // Assert on RPC reply.
    EXPECT_EQ(StatusCode(reply.status().code()), StatusCode::OK);
    return reply;
  }

  rpc::events::AddEventsReply SyncAddEvents(
      const rpc::events::RayEventsData &events_data) {
    rpc::events::AddEventsRequest request;
    rpc::events::AddEventsReply reply;
    std::promise<bool> promise;

    request.mutable_events_data()->CopyFrom(events_data);
    // Dispatch so that it runs in GcsTaskManager's io service.
    io_context_->GetIoService().dispatch(
        [this, &promise, &request, &reply]() {
          task_manager->HandleAddEvents(
              request,
              &reply,
              [&promise](Status, std::function<void()>, std::function<void()>) {
                promise.set_value(true);
              });
        },
        "SyncAddEvent");

    promise.get_future().get();

    // Assert on RPC reply.
    EXPECT_EQ(StatusCode(reply.status().code()), StatusCode::OK);
    return reply;
  }

  rpc::GetTaskEventsReply SyncGetTaskEvents(
      const std::vector<TaskID> task_ids,
      const std::vector<rpc::FilterPredicate> task_id_predicates,
      const std::vector<JobID> job_ids = {},
      const std::vector<rpc::FilterPredicate> job_id_predicates = {},
      int64_t limit = -1,
      bool exclude_driver = true,
      const std::vector<std::string> &task_names = {},
      const std::vector<rpc::FilterPredicate> task_name_predicates = {},
      const std::vector<ActorID> &actor_ids = {},
      const std::vector<rpc::FilterPredicate> actor_id_predicates = {},
      const std::vector<std::string> &states = {},
      const std::vector<rpc::FilterPredicate> state_predicates = {},
      const StatusCode expectedStatusCode = StatusCode::OK) {
    rpc::GetTaskEventsRequest request;
    rpc::GetTaskEventsReply reply;
    std::promise<bool> promise;

    if (!task_ids.empty()) {
      for (size_t i = 0; i < task_ids.size(); i++) {
        auto task_filter = request.mutable_filters()->add_task_filters();
        task_filter->set_task_id(task_ids.at(i).Binary());
        task_filter->set_predicate(task_id_predicates.at(i));
      }
    }

    if (!task_names.empty()) {
      for (size_t i = 0; i < task_names.size(); i++) {
        auto task_name_filter = request.mutable_filters()->add_task_name_filters();
        task_name_filter->set_task_name(task_names.at(i));
        task_name_filter->set_predicate(task_name_predicates.at(i));
      }
    }

    if (!states.empty()) {
      for (size_t i = 0; i < states.size(); i++) {
        auto state_filter = request.mutable_filters()->add_state_filters();
        state_filter->set_state(states.at(i));
        state_filter->set_predicate(state_predicates.at(i));
      }
    }

    if (!actor_ids.empty()) {
      for (size_t i = 0; i < actor_ids.size(); i++) {
        auto actor_id_filter = request.mutable_filters()->add_actor_filters();
        actor_id_filter->set_actor_id(actor_ids.at(i).Binary());
        actor_id_filter->set_predicate(actor_id_predicates.at(i));
      }
    }

    if (!job_ids.empty()) {
      for (size_t i = 0; i < job_ids.size(); i++) {
        auto job_id_filter = request.mutable_filters()->add_job_filters();
        job_id_filter->set_job_id(job_ids.at(i).Binary());
        job_id_filter->set_predicate(job_id_predicates.at(i));
      }
    }

    if (limit >= 0) {
      request.set_limit(limit);
    }

    request.mutable_filters()->set_exclude_driver(exclude_driver);

    io_context_->GetIoService().dispatch(
        [this, &promise, &request, &reply]() {
          task_manager->HandleGetTaskEvents(
              request,
              &reply,
              [&promise](Status, std::function<void()>, std::function<void()>) {
                promise.set_value(true);
              });
        },
        "SyncGetTaskEvents");

    promise.get_future().get();

    EXPECT_EQ(StatusCode(reply.status().code()), expectedStatusCode);
    return reply;
  }

  rpc::GetTaskEventsReply SyncGetTaskEvents(absl::flat_hash_set<TaskID> task_ids,
                                            std::optional<JobID> job_id = absl::nullopt,
                                            int64_t limit = -1,
                                            bool exclude_driver = true,
                                            const std::string &task_name = "",
                                            const ActorID &actor_id = ActorID::Nil(),
                                            const std::string &state = "") {
    std::vector<rpc::FilterPredicate> task_id_predicates(task_ids.size(),
                                                         rpc::FilterPredicate::EQUAL);
    return SyncGetTaskEvents(
        std::vector<TaskID>(task_ids.begin(), task_ids.end()),
        task_id_predicates,
        job_id.has_value() ? std::vector<JobID>(1, job_id.value()) : std::vector<JobID>(),
        job_id.has_value()
            ? std::vector<rpc::FilterPredicate>(1, rpc::FilterPredicate::EQUAL)
            : std::vector<rpc::FilterPredicate>(),
        limit,
        exclude_driver,
        task_name.empty() ? std::vector<std::string>()
                          : std::vector<std::string>(1, task_name),
        task_name.empty()
            ? std::vector<rpc::FilterPredicate>()
            : std::vector<rpc::FilterPredicate>(1, rpc::FilterPredicate::EQUAL),
        actor_id.IsNil() ? std::vector<ActorID>() : std::vector<ActorID>(1, actor_id),
        actor_id.IsNil()
            ? std::vector<rpc::FilterPredicate>()
            : std::vector<rpc::FilterPredicate>(1, rpc::FilterPredicate::EQUAL),
        state.empty() ? std::vector<std::string>() : std::vector<std::string>(1, state),
        state.empty()
            ? std::vector<rpc::FilterPredicate>()
            : std::vector<rpc::FilterPredicate>(1, rpc::FilterPredicate::EQUAL));
  }

  static rpc::TaskInfoEntry GenTaskInfo(
      JobID job_id,
      TaskID parent_task_id = TaskID::Nil(),
      rpc::TaskType task_type = rpc::TaskType::NORMAL_TASK,
      const ActorID actor_id = ActorID::Nil(),
      const std::string name = "") {
    rpc::TaskInfoEntry task_info;
    task_info.set_job_id(job_id.Binary());
    task_info.set_parent_task_id(parent_task_id.Binary());
    task_info.set_type(task_type);
    task_info.set_actor_id(actor_id.Binary());
    task_info.set_name(name);
    return task_info;
  }

  static rpc::TaskStateUpdate GenStateUpdate(
      std::vector<std::pair<rpc::TaskStatus, int64_t>> status_timestamps,
      const WorkerID &worker_id = WorkerID::Nil()) {
    rpc::TaskStateUpdate state_update;
    for (auto status_ts : status_timestamps) {
      FillTaskStatusUpdateTime(status_ts.first, status_ts.second, &state_update);
    }
    if (!worker_id.IsNil()) {
      state_update.set_worker_id(worker_id.Binary());
    }
    return state_update;
  }

  static rpc::TaskStateUpdate GenStateUpdate() {
    return GenStateUpdate({{rpc::TaskStatus::RUNNING, 1}});
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
      std::optional<rpc::ProfileEvents> profile_events = absl::nullopt,
      std::optional<rpc::TaskStateUpdate> state_update = absl::nullopt,
      std::optional<rpc::TaskInfoEntry> task_info = absl::nullopt,
      std::optional<rpc::RayErrorInfo> error_info = absl::nullopt) {
    std::vector<rpc::TaskEvents> result;
    for (auto const &task_id : task_ids) {
      rpc::TaskEvents events;
      events.set_task_id(task_id.Binary());
      events.set_job_id(JobID::FromInt(job_id).Binary());
      events.set_attempt_number(attempt_number);

      if (state_update.has_value()) {
        events.mutable_state_updates()->CopyFrom(*state_update);
      }

      if (error_info.has_value()) {
        events.mutable_state_updates()->mutable_error_info()->CopyFrom(*error_info);
      }

      if (profile_events.has_value()) {
        auto new_events = events.mutable_profile_events();
        new_events->CopyFrom(*profile_events);
      }

      if (task_info.has_value()) {
        events.mutable_task_info()->CopyFrom(*task_info);
      } else {
        events.mutable_task_info()->CopyFrom(GenTaskInfo(JobID::FromInt(job_id)));
      }

      result.push_back(events);
    }

    return result;
  }

  static bool SortByTaskIdAndAttempt(const rpc::TaskEvents &a, const rpc::TaskEvents &b) {
    if (a.task_id() == b.task_id()) {
      return a.attempt_number() < b.attempt_number();
    } else {
      return a.task_id() < b.task_id();
    }
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
  std::unique_ptr<InstrumentedIOContextWithThread> io_context_ = nullptr;
};

class GcsTaskManagerMemoryLimitedTest : public GcsTaskManagerTest {
 public:
  GcsTaskManagerMemoryLimitedTest() : GcsTaskManagerTest() {
    RayConfig::instance().initialize(
        R"(
{
  "task_events_max_num_task_in_gcs": 10
}
  )");
  }
};

class GcsTaskManagerProfileEventsLimitTest : public GcsTaskManagerTest {
 public:
  GcsTaskManagerProfileEventsLimitTest() : GcsTaskManagerTest() {
    RayConfig::instance().initialize(
        R"(
{
  "task_events_max_num_profile_events_per_task": 10
}
  )");
  }
};

class GcsTaskManagerDroppedTaskAttemptsLimit : public GcsTaskManagerTest {
 public:
  GcsTaskManagerDroppedTaskAttemptsLimit() : GcsTaskManagerTest() {
    RayConfig::instance().initialize(
        R"(
{
  "task_events_max_num_task_in_gcs": 10,
  "task_events_max_dropped_task_attempts_tracked_per_job_in_gcs": 5
}
  )");
  }
};

TEST_F(GcsTaskManagerTest, TestHandleAddEventBasic) {
  size_t num_task_events = 100;
  auto task_ids = GenTaskIDs(num_task_events);
  auto events = GenTaskEvents(task_ids, 0);
  auto events_data = GenRayEventsData(events, {});
  auto reply = SyncAddEvents(events_data);

  // Assert on RPC reply.
  EXPECT_EQ(StatusCode(reply.status().code()), StatusCode::OK);

  // Assert on actual data.
  EXPECT_EQ(task_manager->task_event_storage_->GetTaskEvents().size(), num_task_events);
  EXPECT_EQ(task_manager->GetTotalNumTaskEventsReported(), num_task_events);
}

TEST_F(GcsTaskManagerTest, TestHandleAddTaskEventBasic) {
  size_t num_task_events = 100;
  int32_t num_status_events_dropped = 10;
  int32_t num_profile_events_dropped = 10;
  auto task_ids = GenTaskIDs(num_task_events);
  auto events = GenTaskEvents(task_ids, 0);
  auto events_data =
      GenTaskEventsData(events, num_profile_events_dropped, num_status_events_dropped);

  auto reply = SyncAddTaskEventData(events_data);

  // Assert on RPC reply.
  EXPECT_EQ(StatusCode(reply.status().code()), StatusCode::OK);

  // Assert on actual data.
  {
    EXPECT_EQ(task_manager->task_event_storage_->GetTaskEvents().size(), num_task_events);
    EXPECT_EQ(task_manager->GetTotalNumTaskEventsReported(), num_task_events);
    EXPECT_EQ(task_manager->GetTotalNumProfileTaskEventsDropped(),
              num_profile_events_dropped);
    EXPECT_EQ(task_manager->GetTotalNumTaskAttemptsDropped(), num_status_events_dropped);
  }
}

TEST_F(GcsTaskManagerTest, TestHandleAddEventsMultiJobGrouping) {
  // Prepare events for two jobs in a single AddEvents request
  auto task_ids_job0 = GenTaskIDs(3);
  auto task_ids_job1 = GenTaskIDs(2);

  auto events_job0 = GenTaskEvents(task_ids_job0, /*attempt_number*/ 0, /*job_id*/ 0);
  auto events_job1 = GenTaskEvents(task_ids_job1, /*attempt_number*/ 0, /*job_id*/ 1);

  // Build RayEventsData including dropped attempts for each job
  std::vector<rpc::TaskEvents> all_events;
  all_events.insert(all_events.end(), events_job0.begin(), events_job0.end());
  all_events.insert(all_events.end(), events_job1.begin(), events_job1.end());

  std::vector<TaskAttempt> dropped_attempts;
  dropped_attempts.emplace_back(GenTaskIDForJob(0), 0);
  dropped_attempts.emplace_back(GenTaskIDForJob(1), 0);

  auto ray_events_data = GenRayEventsData(all_events, dropped_attempts);

  // Send AddEvents once; converter should group by job id and GCS should record all
  auto reply = SyncAddEvents(ray_events_data);
  EXPECT_EQ(StatusCode(reply.status().code()), StatusCode::OK);

  // Verify all events stored
  EXPECT_EQ(task_manager->task_event_storage_->GetTaskEvents().size(),
            task_ids_job0.size() + task_ids_job1.size());

  // Verify per-job data loss counters populated from dropped attempts
  {
    auto reply_job0 = SyncGetTaskEvents(/* task_ids */ {}, JobID::FromInt(0));
    EXPECT_EQ(reply_job0.num_status_task_events_dropped(), 1);
  }
  {
    auto reply_job1 = SyncGetTaskEvents(/* task_ids */ {}, JobID::FromInt(1));
    EXPECT_EQ(reply_job1.num_status_task_events_dropped(), 1);
  }

  // Verify global counters reflect both drops
  {
    auto reply_all = SyncGetTaskEvents(/* task_ids */ {});
    EXPECT_EQ(reply_all.num_status_task_events_dropped(), 2);
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
    auto events_data = GenTaskEventsData(events);

    auto reply = SyncAddTaskEventData(events_data);
    EXPECT_EQ(StatusCode(reply.status().code()), StatusCode::OK);
  }

  // Assert on actual data
  {
    EXPECT_EQ(task_manager->task_event_storage_->GetTaskEvents().size(), 1);
    // Assert on events
    auto task_events = task_manager->task_event_storage_->GetTaskEvents()[0];
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
  size_t num_profile_events = 1;
  size_t num_status_events = 1;
  size_t num_both_events = 1;
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
      auto data = GenTaskEventsData(events);
      SyncAddTaskEventData(data);
    }
  }

  {
    // Add drop counter.
    auto data = GenTaskEventsData(
        {}, num_profile_task_events_dropped, num_status_task_events_dropped);
    SyncAddTaskEventData(data);
  }

  // Test get all events
  {
    auto reply = SyncGetTaskEvents(/* task_ids */ {});
    // Expect all events
    std::vector<rpc::TaskEvents> expected_events =
        ConcatTaskEvents({events_with_status, events_with_profile, events_with_both});

    auto expected_data = GenTaskEventsData(expected_events);
    // Expect match events
    ExpectTaskEventsEq(expected_data.mutable_events_by_task(),
                       reply.mutable_events_by_task());

    EXPECT_EQ(reply.num_profile_task_events_dropped(), num_profile_task_events_dropped);
    EXPECT_EQ(reply.num_status_task_events_dropped(), num_status_task_events_dropped);
    EXPECT_EQ(reply.num_total_stored(),
              num_profile_events + num_status_events + num_both_events);
    EXPECT_EQ(reply.num_filtered_on_gcs(), 0);
    EXPECT_EQ(reply.num_truncated(), 0);
  }
}

TEST_F(GcsTaskManagerTest, TestGetTaskEventsWithLimit) {
  // Add task events
  int32_t num_task_events = 100;
  {
    auto task_ids = GenTaskIDs(num_task_events);
    auto profile_events = GenProfileEvents("event", /*start*/ 1, /*end*/ 1);
    auto status_update = GenStateUpdate();
    auto events = GenTaskEvents(task_ids, 0, 0, profile_events, status_update);
    auto data = GenTaskEventsData(events);
    SyncAddTaskEventData(data);
  }

  {
    auto reply = SyncGetTaskEvents(/* task_ids */ {}, /* job_id */ absl::nullopt, 10);
    EXPECT_EQ(reply.events_by_task_size(), 10);
    EXPECT_EQ(reply.num_profile_task_events_dropped(), num_task_events - 10);
    EXPECT_EQ(reply.num_status_task_events_dropped(), num_task_events - 10);
    EXPECT_EQ(reply.num_total_stored(), num_task_events);
    EXPECT_EQ(reply.num_filtered_on_gcs(), 0);
    EXPECT_EQ(reply.num_truncated(), num_task_events - 10);
  }

  {
    auto reply = SyncGetTaskEvents(/* task_ids */ {}, /* job_id */ absl::nullopt, 0);
    EXPECT_EQ(reply.events_by_task_size(), 0);
    EXPECT_EQ(reply.num_profile_task_events_dropped(), num_task_events);
    EXPECT_EQ(reply.num_status_task_events_dropped(), num_task_events);
    EXPECT_EQ(reply.num_total_stored(), num_task_events);
    EXPECT_EQ(reply.num_filtered_on_gcs(), 0);
    EXPECT_EQ(reply.num_truncated(), num_task_events);
  }

  {
    auto reply = SyncGetTaskEvents(/* task_ids */ {}, /* job_id */ absl::nullopt, -1);
    EXPECT_EQ(reply.events_by_task_size(), num_task_events);
    EXPECT_EQ(reply.num_profile_task_events_dropped(), 0);
    EXPECT_EQ(reply.num_status_task_events_dropped(), 0);
    EXPECT_EQ(reply.num_total_stored(), num_task_events);
    EXPECT_EQ(reply.num_filtered_on_gcs(), 0);
    EXPECT_EQ(reply.num_truncated(), 0);
  }
}

TEST_F(GcsTaskManagerTest, TestGetTaskEventsByTaskIDs) {
  int32_t num_events_task_1 = 10;
  int32_t num_events_task_2 = 20;
  int32_t num_events_task_3 = 30;

  rpc::TaskEventData events_data_task1;
  auto task_id1 = RandomTaskId();
  {
    std::vector<std::vector<rpc::TaskEvents>> all_events;
    for (int32_t attempt_num = 0; attempt_num < num_events_task_1; ++attempt_num) {
      all_events.push_back(GenTaskEvents({task_id1}, attempt_num));
    }
    auto events_task1 = ConcatTaskEvents(all_events);
    events_data_task1 = GenTaskEventsData(events_task1);
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
    events_data_task2 = GenTaskEventsData(events_task2);
    SyncAddTaskEventData(events_data_task2);
  }

  rpc::TaskEventData events_data_task3;
  auto task_id3 = RandomTaskId();
  {
    std::vector<std::vector<rpc::TaskEvents>> all_events;
    for (int32_t attempt_num = 0; attempt_num < num_events_task_3; ++attempt_num) {
      all_events.push_back(GenTaskEvents({task_id3}, attempt_num));
    }
    auto events_task3 = ConcatTaskEvents(all_events);
    events_data_task3 = GenTaskEventsData(events_task3);
    SyncAddTaskEventData(events_data_task3);
  }

  auto reply_task1 = SyncGetTaskEvents({task_id1});
  auto reply_task2 = SyncGetTaskEvents({task_id2});
  auto reply_not_task1 = SyncGetTaskEvents({task_id1}, {rpc::FilterPredicate::NOT_EQUAL});
  auto reply_not_task2 = SyncGetTaskEvents({task_id2}, {rpc::FilterPredicate::NOT_EQUAL});
  auto reply_task1_task2 = SyncGetTaskEvents({task_id1, task_id2});
  auto reply_not_task1_task2 = SyncGetTaskEvents(
      {task_id1, task_id2},
      {rpc::FilterPredicate::NOT_EQUAL, rpc::FilterPredicate::NOT_EQUAL});
  auto reply_task2_not_task1 =
      SyncGetTaskEvents({task_id1, task_id2},
                        {rpc::FilterPredicate::NOT_EQUAL, rpc::FilterPredicate::EQUAL});

  // Check matched
  ExpectTaskEventsEq(events_data_task1.mutable_events_by_task(),
                     reply_task1.mutable_events_by_task());
  EXPECT_EQ(reply_task1.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_task1.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_task1.num_total_stored(), num_events_task_1);
  EXPECT_EQ(reply_task1.num_filtered_on_gcs(), 0);
  EXPECT_EQ(reply_task1.num_truncated(), 0);

  ExpectTaskEventsEq(events_data_task2.mutable_events_by_task(),
                     reply_task2.mutable_events_by_task());
  EXPECT_EQ(reply_task2.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_task2.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_task2.num_total_stored(), num_events_task_2);
  EXPECT_EQ(reply_task2.num_filtered_on_gcs(), 0);
  EXPECT_EQ(reply_task2.num_truncated(), 0);

  std::vector<google::protobuf::RepeatedPtrField<rpc::TaskEvents> *>
      task_events_task2_and_task3 = {events_data_task2.mutable_events_by_task(),
                                     events_data_task3.mutable_events_by_task()};
  ExpectTaskEventsEq(task_events_task2_and_task3,
                     reply_not_task1.mutable_events_by_task());
  EXPECT_EQ(reply_not_task1.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_task1.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_task1.num_total_stored(),
            num_events_task_1 + num_events_task_2 + num_events_task_3);
  EXPECT_EQ(reply_not_task1.num_filtered_on_gcs(), num_events_task_1);
  EXPECT_EQ(reply_not_task1.num_truncated(), 0);

  std::vector<google::protobuf::RepeatedPtrField<rpc::TaskEvents> *>
      task_events_task1_and_task3 = {events_data_task1.mutable_events_by_task(),
                                     events_data_task3.mutable_events_by_task()};
  ExpectTaskEventsEq(task_events_task1_and_task3,
                     reply_not_task2.mutable_events_by_task());
  EXPECT_EQ(reply_not_task2.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_task2.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_task2.num_total_stored(),
            num_events_task_1 + num_events_task_2 + num_events_task_3);
  EXPECT_EQ(reply_not_task2.num_filtered_on_gcs(), num_events_task_2);
  EXPECT_EQ(reply_not_task2.num_truncated(), 0);

  EXPECT_EQ(reply_task1_task2.events_by_task_size(), 0);
  EXPECT_EQ(reply_task1_task2.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_task1_task2.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_task1_task2.num_total_stored(), 0);
  EXPECT_EQ(reply_task1_task2.num_filtered_on_gcs(), 0);
  EXPECT_EQ(reply_task1_task2.num_truncated(), 0);

  ExpectTaskEventsEq(events_data_task3.mutable_events_by_task(),
                     reply_not_task1_task2.mutable_events_by_task());
  EXPECT_EQ(reply_not_task1_task2.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_task1_task2.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_task1_task2.num_total_stored(),
            num_events_task_1 + num_events_task_2 + num_events_task_3);
  EXPECT_EQ(reply_not_task1_task2.num_filtered_on_gcs(),
            num_events_task_1 + num_events_task_2);
  EXPECT_EQ(reply_not_task1_task2.num_truncated(), 0);

  ExpectTaskEventsEq(events_data_task2.mutable_events_by_task(),
                     reply_task2_not_task1.mutable_events_by_task());
  EXPECT_EQ(reply_task2_not_task1.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_task2_not_task1.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_task2_not_task1.num_total_stored(), num_events_task_2);
  EXPECT_EQ(reply_task2_not_task1.num_filtered_on_gcs(), 0);
  EXPECT_EQ(reply_task2_not_task1.num_truncated(), 0);
}

TEST_F(GcsTaskManagerTest, TestGetTaskEventsByJobs) {
  size_t num_task_job1 = 10;
  size_t num_task_job2 = 20;
  size_t num_task_job3 = 30;

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
    events_data_job1 = GenTaskEventsData(events);
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
    events_data_job2 = GenTaskEventsData(events);
    SyncAddTaskEventData(events_data_job2);
  }

  rpc::TaskEventData events_data_job3;
  {
    auto task_ids = GenTaskIDs(num_task_job3);
    auto task_info = GenTaskInfo(JobID::FromInt(3));
    auto events = GenTaskEvents(task_ids,
                                /* attempt_number */
                                0,
                                /* job_id */ 3,
                                absl::nullopt,
                                absl::nullopt,
                                task_info);
    events_data_job3 = GenTaskEventsData(events);
    SyncAddTaskEventData(events_data_job3);
  }

  auto reply_job1 = SyncGetTaskEvents(/* task_ids */ {}, JobID::FromInt(1));
  auto reply_job2 = SyncGetTaskEvents({}, JobID::FromInt(2));
  auto reply_not_job1 =
      SyncGetTaskEvents({}, {}, {JobID::FromInt(1)}, {rpc::FilterPredicate::NOT_EQUAL});
  auto reply_not_job2 =
      SyncGetTaskEvents({}, {}, {JobID::FromInt(2)}, {rpc::FilterPredicate::NOT_EQUAL});
  auto reply_job1_job2 =
      SyncGetTaskEvents({},
                        {},
                        {JobID::FromInt(1), JobID::FromInt(2)},
                        {rpc::FilterPredicate::EQUAL, rpc::FilterPredicate::EQUAL});
  auto reply_not_job1_not_job2 = SyncGetTaskEvents(
      {},
      {},
      {JobID::FromInt(1), JobID::FromInt(2)},
      {rpc::FilterPredicate::NOT_EQUAL, rpc::FilterPredicate::NOT_EQUAL});
  auto reply_job1_not_job2 =
      SyncGetTaskEvents({},
                        {},
                        {JobID::FromInt(1), JobID::FromInt(2)},
                        {rpc::FilterPredicate::EQUAL, rpc::FilterPredicate::NOT_EQUAL});

  // Check matched
  ExpectTaskEventsEq(events_data_job1.mutable_events_by_task(),
                     reply_job1.mutable_events_by_task());
  EXPECT_EQ(reply_job1.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_job1.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_job1.num_total_stored(), num_task_job1);
  EXPECT_EQ(reply_job1.num_filtered_on_gcs(), 0);
  EXPECT_EQ(reply_job1.num_truncated(), 0);

  ExpectTaskEventsEq(events_data_job2.mutable_events_by_task(),
                     reply_job2.mutable_events_by_task());
  EXPECT_EQ(reply_job2.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_job2.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_job2.num_total_stored(), num_task_job2);
  EXPECT_EQ(reply_job2.num_filtered_on_gcs(), 0);
  EXPECT_EQ(reply_job2.num_truncated(), 0);

  std::vector<google::protobuf::RepeatedPtrField<rpc::TaskEvents> *>
      task_events_job2_and_job3 = {events_data_job2.mutable_events_by_task(),
                                   events_data_job3.mutable_events_by_task()};
  ExpectTaskEventsEq(task_events_job2_and_job3, reply_not_job1.mutable_events_by_task());
  EXPECT_EQ(reply_not_job1.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_job1.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_job1.num_total_stored(),
            num_task_job1 + num_task_job2 + num_task_job3);
  EXPECT_EQ(reply_not_job1.num_filtered_on_gcs(), num_task_job1);
  EXPECT_EQ(reply_not_job1.num_truncated(), 0);

  std::vector<google::protobuf::RepeatedPtrField<rpc::TaskEvents> *>
      task_events_job1_and_job3 = {events_data_job1.mutable_events_by_task(),
                                   events_data_job3.mutable_events_by_task()};
  ExpectTaskEventsEq(task_events_job1_and_job3, reply_not_job2.mutable_events_by_task());
  EXPECT_EQ(reply_not_job2.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_job2.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_job2.num_total_stored(),
            num_task_job1 + num_task_job2 + num_task_job3);
  EXPECT_EQ(reply_not_job2.num_filtered_on_gcs(), num_task_job2);
  EXPECT_EQ(reply_not_job2.num_truncated(), 0);

  EXPECT_EQ(reply_job1_job2.events_by_task_size(), 0);
  EXPECT_EQ(reply_job1_job2.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_job1_job2.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_job1_job2.num_total_stored(), 0);
  EXPECT_EQ(reply_job1_job2.num_filtered_on_gcs(), 0);
  EXPECT_EQ(reply_job1_job2.num_truncated(), 0);

  ExpectTaskEventsEq(events_data_job3.mutable_events_by_task(),
                     reply_not_job1_not_job2.mutable_events_by_task());
  EXPECT_EQ(reply_not_job1_not_job2.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_job1_not_job2.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_job1_not_job2.num_total_stored(),
            num_task_job1 + num_task_job2 + num_task_job3);
  EXPECT_EQ(reply_not_job1_not_job2.num_filtered_on_gcs(), num_task_job1 + num_task_job2);
  EXPECT_EQ(reply_not_job1_not_job2.num_truncated(), 0);

  ExpectTaskEventsEq(events_data_job1.mutable_events_by_task(),
                     reply_job1_not_job2.mutable_events_by_task());
  EXPECT_EQ(reply_job1_not_job2.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_job1_not_job2.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_job1_not_job2.num_total_stored(), num_task_job1);
  EXPECT_EQ(reply_job1_not_job2.num_filtered_on_gcs(), 0);
  EXPECT_EQ(reply_job1_not_job2.num_truncated(), 0);
}

TEST_F(GcsTaskManagerTest, TestGetTaskEventsFilters) {
  // Generate task events

  // A task event with actor id, job id = 1
  ActorID actor_id = ActorID::Of(JobID::FromInt(1), TaskID::Nil(), 1);
  std::vector<TaskID> task_ids_actor_id_job1;
  rpc::TaskEventData event_data_actor_id_job1;
  {
    task_ids_actor_id_job1 = GenTaskIDs(1);
    auto task_info_actor_id =
        GenTaskInfo(JobID::FromInt(1), TaskID::Nil(), rpc::ACTOR_TASK, actor_id);
    auto events = GenTaskEvents(task_ids_actor_id_job1,
                                /* attempt_number */
                                0,
                                /* job_id */ 1,
                                absl::nullopt,
                                absl::nullopt,
                                task_info_actor_id);
    event_data_actor_id_job1 = GenTaskEventsData(events);
    SyncAddTaskEventData(event_data_actor_id_job1);
  }

  // A task event with name and job id = 1
  std::string task_name = "task_name";
  std::vector<TaskID> task_ids_task_name_job1;
  rpc::TaskEventData event_data_task_name_job1;
  {
    task_ids_task_name_job1 = GenTaskIDs(1);
    auto task_info_name = GenTaskInfo(
        JobID::FromInt(1), TaskID::Nil(), rpc::NORMAL_TASK, ActorID::Nil(), "task_name");
    auto events = GenTaskEvents(task_ids_task_name_job1,
                                /* attempt_number */
                                0,
                                /* job_id */ 1,
                                absl::nullopt,
                                absl::nullopt,
                                task_info_name);
    event_data_task_name_job1 = GenTaskEventsData(events);
    SyncAddTaskEventData(event_data_task_name_job1);
  }

  // A task event with state transitions and job id = 2
  rpc::TaskStatus task_status = rpc::TaskStatus::RUNNING;
  std::vector<TaskID> task_ids_task_status_job2;
  rpc::TaskEventData event_data_task_state_job2;
  {
    task_ids_task_status_job2 = GenTaskIDs(1);
    auto task_info = GenTaskInfo(JobID::FromInt(2), TaskID::Nil(), rpc::NORMAL_TASK);
    auto events = GenTaskEvents(
        task_ids_task_status_job2,
        /* attempt_number */
        0,
        /* job_id */ 1,
        absl::nullopt,
        GenStateUpdate({{rpc::TaskStatus::PENDING_NODE_ASSIGNMENT, 1}, {task_status, 5}},
                       WorkerID::Nil()),
        task_info);
    event_data_task_state_job2 = GenTaskEventsData(events);
    SyncAddTaskEventData(event_data_task_state_job2);
  }

  // Test filter by task and job
  auto reply_task_and_job = SyncGetTaskEvents({task_ids_task_status_job2},
                                              {rpc::FilterPredicate::EQUAL},
                                              {JobID::FromInt(1)},
                                              {rpc::FilterPredicate::EQUAL},
                                              /* limit */ -1,
                                              /* exclude_driver */ false);
  EXPECT_EQ(reply_task_and_job.events_by_task_size(), 0);
  EXPECT_EQ(reply_task_and_job.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_task_and_job.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_task_and_job.num_total_stored(), 1);
  EXPECT_EQ(reply_task_and_job.num_filtered_on_gcs(), 1);
  EXPECT_EQ(reply_task_and_job.num_truncated(), 0);

  auto reply_task_and_not_job = SyncGetTaskEvents({task_ids_task_status_job2},
                                                  {rpc::FilterPredicate::EQUAL},
                                                  {JobID::FromInt(2)},
                                                  {rpc::FilterPredicate::NOT_EQUAL},
                                                  /* limit */ -1,
                                                  /* exclude_driver */ false);
  EXPECT_EQ(reply_task_and_not_job.events_by_task_size(), 0);
  EXPECT_EQ(reply_task_and_not_job.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_task_and_not_job.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_task_and_not_job.num_total_stored(), 1);
  EXPECT_EQ(reply_task_and_not_job.num_filtered_on_gcs(), 1);
  EXPECT_EQ(reply_task_and_not_job.num_truncated(), 0);

  // Test filter by task name
  auto reply_name = SyncGetTaskEvents({},
                                      /* job_id */ absl::nullopt,
                                      /* limit */ -1,
                                      /* exclude_driver */ false,
                                      task_name);
  ExpectTaskEventsEq(event_data_task_name_job1.mutable_events_by_task(),
                     reply_name.mutable_events_by_task());
  EXPECT_EQ(reply_name.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_name.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_name.num_total_stored(), 3);
  EXPECT_EQ(reply_name.num_filtered_on_gcs(), 2);
  EXPECT_EQ(reply_name.num_truncated(), 0);

  auto reply_not_name = SyncGetTaskEvents({},
                                          {},
                                          /* job_ids */ {},
                                          /*job_id_predicates*/ {},
                                          /* limit */ -1,
                                          /* exclude_driver */ false,
                                          {task_name},
                                          {rpc::FilterPredicate::NOT_EQUAL});
  ExpectTaskEventsEq({event_data_actor_id_job1.mutable_events_by_task(),
                      event_data_task_state_job2.mutable_events_by_task()},
                     reply_not_name.mutable_events_by_task());
  EXPECT_EQ(reply_not_name.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_name.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_name.num_total_stored(), 3);
  EXPECT_EQ(reply_not_name.num_filtered_on_gcs(), 1);
  EXPECT_EQ(reply_not_name.num_truncated(), 0);

  auto reply_multiple_names =
      SyncGetTaskEvents({},
                        {},
                        /* job_ids */ {},
                        /*job_id_predicates*/ {},
                        /* limit */ -1,
                        /* exclude_driver */ false,
                        {task_name, "random_name"},
                        {rpc::FilterPredicate::EQUAL, rpc::FilterPredicate::EQUAL});
  EXPECT_EQ(reply_multiple_names.events_by_task_size(), 0);
  EXPECT_EQ(reply_multiple_names.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_multiple_names.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_multiple_names.num_total_stored(), 3);
  EXPECT_EQ(reply_multiple_names.num_filtered_on_gcs(), 3);
  EXPECT_EQ(reply_multiple_names.num_truncated(), 0);

  // Test filter by actor id
  auto reply_actor_id = SyncGetTaskEvents({},
                                          /* job_id */ absl::nullopt,
                                          /* limit */ -1,
                                          /* exclude_driver */ false,
                                          /* task_name */ "",
                                          actor_id);
  ExpectTaskEventsEq(event_data_actor_id_job1.mutable_events_by_task(),
                     reply_actor_id.mutable_events_by_task());
  EXPECT_EQ(reply_actor_id.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_actor_id.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_actor_id.num_total_stored(), 3);
  EXPECT_EQ(reply_actor_id.num_filtered_on_gcs(), 2);
  EXPECT_EQ(reply_actor_id.num_truncated(), 0);

  auto reply_not_actor_id = SyncGetTaskEvents({},
                                              {},
                                              /* job_ids */ {},
                                              /*job_id_predicates */ {},
                                              /* limit */ -1,
                                              /* exclude_driver */ false,
                                              /* task_names */ {},
                                              /* task_name_predicates */ {},
                                              {actor_id},
                                              {rpc::FilterPredicate::NOT_EQUAL});
  ExpectTaskEventsEq({event_data_task_name_job1.mutable_events_by_task(),
                      event_data_task_state_job2.mutable_events_by_task()},
                     reply_not_actor_id.mutable_events_by_task());
  EXPECT_EQ(reply_not_actor_id.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_actor_id.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_actor_id.num_total_stored(), 3);
  EXPECT_EQ(reply_not_actor_id.num_filtered_on_gcs(), 1);
  EXPECT_EQ(reply_not_actor_id.num_truncated(), 0);

  auto reply_multiple_actor_ids =
      SyncGetTaskEvents({},
                        {},
                        /* job_ids */ {},
                        /*job_id_predicates */ {},
                        /* limit */ -1,
                        /* exclude_driver */ false,
                        /* task_names */ {},
                        /* task_name_predicates */ {},
                        {actor_id, ActorID::Of(JobID::FromInt(1), TaskID::Nil(), 2)},
                        {rpc::FilterPredicate::EQUAL, rpc::FilterPredicate::EQUAL});
  EXPECT_EQ(reply_multiple_actor_ids.events_by_task_size(), 0);
  EXPECT_EQ(reply_multiple_actor_ids.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_multiple_actor_ids.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_multiple_actor_ids.num_total_stored(), 3);
  EXPECT_EQ(reply_multiple_actor_ids.num_filtered_on_gcs(), 3);
  EXPECT_EQ(reply_multiple_actor_ids.num_truncated(), 0);

  // Test filter by latest state
  auto reply_state = SyncGetTaskEvents({},
                                       /* job_id */ absl::nullopt,
                                       /* limit */ -1,
                                       /* exclude_driver */ false,
                                       /* task_name */ "",
                                       ActorID::Nil(),
                                       "RUnnING");
  ExpectTaskEventsEq(event_data_task_state_job2.mutable_events_by_task(),
                     reply_state.mutable_events_by_task());
  EXPECT_EQ(reply_name.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_name.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_name.num_total_stored(), 3);
  EXPECT_EQ(reply_name.num_filtered_on_gcs(), 2);
  EXPECT_EQ(reply_name.num_truncated(), 0);

  auto reply_not_state =
      SyncGetTaskEvents({},
                        {},
                        /* job_ids */ {},
                        /*job_id_predicates*/ {},
                        /* limit */ -1,
                        /* exclude_driver */ false,
                        /* task_names */ {},
                        /* task_name_predicates */ {},
                        /* actor_ids */ {},
                        /* actor_id_predicates */ {},
                        {"RUnnING"},
                        /* state_predicate */ {rpc::FilterPredicate::NOT_EQUAL});
  ExpectTaskEventsEq({event_data_task_name_job1.mutable_events_by_task(),
                      event_data_actor_id_job1.mutable_events_by_task()},
                     reply_not_state.mutable_events_by_task());
  EXPECT_EQ(reply_not_state.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_state.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_state.num_total_stored(), 3);
  EXPECT_EQ(reply_not_state.num_filtered_on_gcs(), 1);
  EXPECT_EQ(reply_not_state.num_truncated(), 0);

  reply_state = SyncGetTaskEvents({},
                                  /* job_id */ absl::nullopt,
                                  /* limit */ -1,
                                  /* exclude_driver */ false,
                                  /* task_name */ "",
                                  ActorID::Nil(),
                                  "NIL");
  ExpectTaskEventsEq({event_data_task_name_job1.mutable_events_by_task(),
                      event_data_actor_id_job1.mutable_events_by_task()},
                     reply_state.mutable_events_by_task());
  EXPECT_EQ(reply_state.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_state.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_state.num_total_stored(), 3);
  EXPECT_EQ(reply_state.num_filtered_on_gcs(), 1);
  EXPECT_EQ(reply_state.num_truncated(), 0);

  reply_not_state =
      SyncGetTaskEvents({},
                        {},
                        /* job_ids */ {},
                        /*job_id_predicates*/ {},
                        /* limit */ -1,
                        /* exclude_driver */ false,
                        /* task_names */ {},
                        /* task_name_predicates */ {},
                        /* actor_ids */ {},
                        /* actor_id_predicates */ {},
                        {"NIL"},
                        /* state_predicate */ {rpc::FilterPredicate::NOT_EQUAL});
  ExpectTaskEventsEq(event_data_task_state_job2.mutable_events_by_task(),
                     reply_not_state.mutable_events_by_task());
  EXPECT_EQ(reply_not_state.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_state.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_state.num_total_stored(), 3);
  EXPECT_EQ(reply_not_state.num_filtered_on_gcs(), 2);
  EXPECT_EQ(reply_not_state.num_truncated(), 0);

  reply_state = SyncGetTaskEvents({},
                                  /* job_id */ absl::nullopt,
                                  /* limit */ -1,
                                  /* exclude_driver */ false,
                                  /* task_name */ "",
                                  ActorID::Nil(),
                                  "PENDING_NODE_ASSIGNMENT");
  EXPECT_EQ(reply_state.events_by_task_size(), 0);
  EXPECT_EQ(reply_state.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_state.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_state.num_total_stored(), 3);
  EXPECT_EQ(reply_state.num_filtered_on_gcs(), 3);
  EXPECT_EQ(reply_state.num_truncated(), 0);

  reply_not_state =
      SyncGetTaskEvents({},
                        {},
                        /* job_ids */ {},
                        /*job_id_predicates*/ {},
                        /* limit */ -1,
                        /* exclude_driver */ false,
                        /* task_names */ {},
                        /* task_name_predicates */ {},
                        /* actor_ids */ {},
                        /* actor_id_predicates */ {},
                        {"PENDING_NODE_ASSIGNMENT"},
                        /* state_predicate */ {rpc::FilterPredicate::NOT_EQUAL});
  ExpectTaskEventsEq({event_data_task_name_job1.mutable_events_by_task(),
                      event_data_actor_id_job1.mutable_events_by_task(),
                      event_data_task_state_job2.mutable_events_by_task()},
                     reply_not_state.mutable_events_by_task());
  EXPECT_EQ(reply_not_state.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_state.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_not_state.num_total_stored(), 3);
  EXPECT_EQ(reply_not_state.num_filtered_on_gcs(), 0);
  EXPECT_EQ(reply_not_state.num_truncated(), 0);

  auto reply_multiple_state = SyncGetTaskEvents(
      {},
      {},
      /* job_ids */ {},
      /*job_id_predicates*/ {},
      /* limit */ -1,
      /* exclude_driver */ false,
      /* task_names */ {},
      /* task_name_predicates */ {},
      /* actor_ids */ {},
      /* actor_id_predicates */ {},
      {"RUnnING", "NIL"},
      /* state_predicates */ {rpc::FilterPredicate::EQUAL, rpc::FilterPredicate::EQUAL});
  EXPECT_EQ(reply_multiple_state.events_by_task_size(), 0);
  EXPECT_EQ(reply_multiple_state.num_profile_task_events_dropped(), 0);
  EXPECT_EQ(reply_multiple_state.num_status_task_events_dropped(), 0);
  EXPECT_EQ(reply_multiple_state.num_total_stored(), 3);
  EXPECT_EQ(reply_multiple_state.num_filtered_on_gcs(), 3);
  EXPECT_EQ(reply_multiple_state.num_truncated(), 0);

  // Test multiple filters
  auto reply_name_and_actor_id = SyncGetTaskEvents({},
                                                   /* job_id */ absl::nullopt,
                                                   /* limit */ -1,
                                                   /* exclude_driver */ false,
                                                   "task_name",
                                                   actor_id);
  EXPECT_EQ(reply_name_and_actor_id.events_by_task_size(), 0);

  auto reply_actor_id_and_state = SyncGetTaskEvents({},
                                                    /* job_id */ absl::nullopt,
                                                    /* limit */ -1,
                                                    /* exclude_driver */ false,
                                                    "",
                                                    actor_id,
                                                    "Running");
  EXPECT_EQ(reply_name_and_actor_id.events_by_task_size(), 0);

  // Test invalid predicate
  auto reply_invalid_predicate =
      SyncGetTaskEvents(/* task_ids */ {},
                        /* task_id_predicates */ {},
                        {JobID::FromInt(1)},
                        /* job_id_predicates */ {rpc::FilterPredicate(100)},
                        /* limit */ -1,
                        /* exclude_driver */ false,
                        /* task_names */ {},
                        /* task_name_predicates */ {},
                        /* actor_ids */ {},
                        /* actor_id_predicates */ {},
                        /* states */ {},
                        /* state_predicates */ {},
                        StatusCode::InvalidArgument);
}

TEST_F(GcsTaskManagerTest, TestMarkTaskAttemptFailedIfNeeded) {
  auto tasks = GenTaskIDs(3);
  auto tasks_running = tasks[0];
  auto tasks_finished = tasks[1];
  auto tasks_failed = tasks[2];

  SyncAddTaskEvent({tasks_running}, {{rpc::TaskStatus::RUNNING, 1}}, TaskID::Nil(), 1);
  SyncAddTaskEvent({tasks_finished}, {{rpc::TaskStatus::FINISHED, 2}}, TaskID::Nil(), 1);
  SyncAddTaskEvent({tasks_failed}, {{rpc::TaskStatus::FAILED, 3}}, TaskID::Nil(), 1);

  // Mark task attempt failed if needed for each task.
  for (auto &task : tasks) {
    auto loc = task_manager->task_event_storage_->GetTaskEventLocator({task, 0});
    task_manager->task_event_storage_->MarkTaskAttemptFailedIfNeeded(
        loc,
        /* failed time stamp ms*/ 4,
        rpc::RayErrorInfo());
  }

  // Check task attempt failed event is added for running task.
  {
    auto reply = SyncGetTaskEvents({tasks_running});
    auto task_event = *(reply.events_by_task().begin());
    EXPECT_EQ(task_event.state_updates().state_ts_ns().at(rpc::TaskStatus::FAILED), 4);
  }

  // Check task attempt failed event is not overriding failed tasks.
  {
    auto reply = SyncGetTaskEvents({tasks_failed});
    auto task_event = *(reply.events_by_task().begin());
    EXPECT_EQ(task_event.state_updates().state_ts_ns().at(rpc::TaskStatus::FAILED), 3);
  }

  // Check task attempt failed event is not overriding finished tasks.
  {
    auto reply = SyncGetTaskEvents({tasks_finished});
    auto task_event = *(reply.events_by_task().begin());
    EXPECT_FALSE(
        task_event.state_updates().state_ts_ns().contains(rpc::TaskStatus::FAILED));
    EXPECT_EQ(task_event.state_updates().state_ts_ns().at(rpc::TaskStatus::FINISHED), 2);
  }
}

TEST_F(GcsTaskManagerTest, TestJobFinishesWithoutTaskEvents) {
  // Test that if a job is finished, but no task events have been reported.
  // This should not crash.

  task_manager->OnJobFinished(JobID::FromInt(1), 5);  // in ms

  boost::asio::io_context io;
  boost::asio::deadline_timer timer(
      io,
      boost::posix_time::milliseconds(
          2 * RayConfig::instance().gcs_mark_task_failed_on_job_done_delay_ms()));
  timer.wait();

  auto reply = SyncGetTaskEvents({});
  EXPECT_EQ(reply.events_by_task_size(), 0);
}

TEST_F(GcsTaskManagerTest, TestJobFinishesFailAllRunningTasks) {
  auto tasks_running_job1 = GenTaskIDs(10);
  auto tasks_finished_job1 = GenTaskIDs(10);
  auto tasks_failed_job1 = GenTaskIDs(10);

  auto tasks_running_job2 = GenTaskIDs(5);

  SyncAddTaskEvent(tasks_running_job1, {{rpc::TaskStatus::RUNNING, 1}}, TaskID::Nil(), 1);
  SyncAddTaskEvent(
      tasks_finished_job1, {{rpc::TaskStatus::FINISHED, 2}}, TaskID::Nil(), 1);
  SyncAddTaskEvent(tasks_failed_job1, {{rpc::TaskStatus::FAILED, 3}}, TaskID::Nil(), 1);

  SyncAddTaskEvent(tasks_running_job2, {{rpc::TaskStatus::RUNNING, 4}}, TaskID::Nil(), 2);

  task_manager->OnJobFinished(JobID::FromInt(1), 5);  // in ms

  // Wait for longer than the default timer
  boost::asio::io_context io;
  boost::asio::deadline_timer timer(
      io,
      boost::posix_time::milliseconds(
          2 * RayConfig::instance().gcs_mark_task_failed_on_job_done_delay_ms()));
  timer.wait();

  // Running tasks from job1 failed at 5
  {
    google::protobuf::RepeatedPtrField<rpc::TaskEvents> mergedEvents;
    for (const auto &task_id : tasks_running_job1) {
      auto reply = SyncGetTaskEvents({task_id});
      mergedEvents.MergeFrom(reply.events_by_task());
    }
    EXPECT_EQ(mergedEvents.size(), 10);
    for (const auto &task_event : mergedEvents) {
      EXPECT_EQ(task_event.state_updates().state_ts_ns().at(rpc::TaskStatus::FAILED),
                /* 5 ms to ns */ 5 * 1000 * 1000);
      EXPECT_TRUE(task_event.state_updates().has_error_info());
      EXPECT_TRUE(task_event.state_updates().error_info().error_type() ==
                  rpc::ErrorType::WORKER_DIED);
      EXPECT_TRUE(task_event.state_updates().error_info().error_message().find(
                      "Job finishes") != std::string::npos);
    }
  }

  // Finished tasks from job1 remain finished
  {
    google::protobuf::RepeatedPtrField<rpc::TaskEvents> mergedEvents;
    for (const auto &task_id : tasks_finished_job1) {
      auto reply = SyncGetTaskEvents({task_id});
      mergedEvents.MergeFrom(reply.events_by_task());
    }
    EXPECT_EQ(mergedEvents.size(), 10);
    for (const auto &task_event : mergedEvents) {
      EXPECT_EQ(task_event.state_updates().state_ts_ns().at(rpc::TaskStatus::FINISHED),
                2);
      EXPECT_FALSE(
          task_event.state_updates().state_ts_ns().contains(rpc::TaskStatus::FAILED));
    }
  }

  // Failed tasks from job1 failed timestamp not overriden
  {
    google::protobuf::RepeatedPtrField<rpc::TaskEvents> mergedEvents;
    for (const auto &task_id : tasks_failed_job1) {
      auto reply = SyncGetTaskEvents({task_id});
      mergedEvents.MergeFrom(reply.events_by_task());
    }
    EXPECT_EQ(mergedEvents.size(), 10);
    for (const auto &task_event : mergedEvents) {
      EXPECT_EQ(task_event.state_updates().state_ts_ns().at(rpc::TaskStatus::FAILED), 3);
    }
  }

  // Tasks from job2 should not be affected.
  {
    google::protobuf::RepeatedPtrField<rpc::TaskEvents> mergedEvents;
    for (const auto &task_id : tasks_running_job2) {
      auto reply = SyncGetTaskEvents({task_id});
      mergedEvents.MergeFrom(reply.events_by_task());
    }
    EXPECT_EQ(mergedEvents.size(), 5);
    for (const auto &task_event : mergedEvents) {
      EXPECT_FALSE(
          task_event.state_updates().state_ts_ns().contains(rpc::TaskStatus::FAILED));
      EXPECT_FALSE(
          task_event.state_updates().state_ts_ns().contains(rpc::TaskStatus::FINISHED));
    }
  }
}

TEST_F(GcsTaskManagerMemoryLimitedTest, TestIndexNoLeak) {
  size_t num_limit = 10;  // synced with test config
  size_t num_total = 50;

  std::vector<TaskID> task_ids = GenTaskIDs(num_total);
  std::vector<WorkerID> worker_ids = GenWorkerIDs(10);

  // Add task attempts from different jobs, different task id, with different worker ids.
  for (size_t i = 0; i < num_total; i++) {
    auto task_id = task_ids[i % task_ids.size()];
    auto job_id = task_id.JobId();
    auto worker_id = worker_ids[i % worker_ids.size()];
    auto events = GenTaskEvents({task_id},
                                /* attempt_number */ 0,
                                job_id.ToInt(),
                                GenProfileEvents("event", 1, 1),
                                GenStateUpdate({}, worker_id),
                                GenTaskInfo(job_id));
    auto events_data = GenTaskEventsData(events);
    SyncAddTaskEventData(events_data);
  }

  {
    EXPECT_EQ(task_manager->task_event_storage_->stats_counter_.Get(kTotalNumNormalTask),
              task_ids.size());
  }

  // Evict all of them with tasks with single attempt, no parent, same job, no worker id.
  {
    auto tids = GenTaskIDs(num_limit);
    auto job_id = 0;
    auto attempt_number = 0;
    for (size_t i = 0; i < num_limit; i++) {
      auto events = GenTaskEvents({tids[i]},
                                  /* attempt_number */ attempt_number,
                                  job_id,
                                  GenProfileEvents("event", 1, 1),
                                  GenStateUpdate(),
                                  GenTaskInfo(JobID::FromInt(job_id)));
      auto events_data = GenTaskEventsData(events);
      SyncAddTaskEventData(events_data);
    }
  }
  // Assert on the indexes and the storage
  {
    EXPECT_EQ(task_manager->task_event_storage_->GetTaskEvents().size(), num_limit);
    EXPECT_EQ(task_manager->task_event_storage_->stats_counter_.Get(kTotalNumNormalTask),
              task_ids.size() + num_limit);

    // Only in memory entries.
    EXPECT_EQ(task_manager->task_event_storage_->task_index_.size(), num_limit);
    EXPECT_EQ(task_manager->task_event_storage_->job_index_.size(), 1);
    EXPECT_EQ(task_manager->task_event_storage_->primary_index_.size(), num_limit);
    EXPECT_EQ(task_manager->task_event_storage_->worker_index_.size(), 0);
  }
}

TEST_F(GcsTaskManagerMemoryLimitedTest, TestLimitTaskEvents) {
  size_t num_limit = 10;  // synced with test config

  size_t num_profile_events_to_drop = 7;
  size_t num_status_events_to_drop = 3;
  size_t num_batch1 = num_profile_events_to_drop + num_status_events_to_drop;
  size_t num_batch2 = 10;

  size_t num_profile_events_dropped_on_worker = 8;
  size_t num_status_events_dropped_on_worker = 2;
  {
    // Add profile event.
    auto events = GenTaskEvents(GenTaskIDs(num_profile_events_to_drop),
                                /* attempt_number */ 0,
                                /* job_id */ 0,
                                GenProfileEvents("event", 1, 1));
    auto events_data = GenTaskEventsData(events, num_profile_events_dropped_on_worker);
    SyncAddTaskEventData(events_data);
  }
  {
    // Add status update events.
    auto events = GenTaskEvents(GenTaskIDs(num_status_events_to_drop),
                                /* attempt_number*/ 0,
                                /* job_id */ 0,
                                /* profile_events */ absl::nullopt,
                                GenStateUpdate());
    auto events_data = GenTaskEventsData(events,
                                         /*num_profile_task_events_dropped*/ 0,
                                         num_status_events_dropped_on_worker);
    SyncAddTaskEventData(events_data);
  }

  // Expected events in the buffer
  std::vector<rpc::TaskEvents> expected_events;
  {
    // Add new task events to overwrite the existing ones.
    expected_events = GenTaskEvents(GenTaskIDs(num_batch2), 0);
    auto events_data = GenTaskEventsData(expected_events);
    SyncAddTaskEventData(events_data);
  }

  // Assert on actual data.
  {
    EXPECT_EQ(task_manager->GetNumTaskEventsStored(), num_limit);
    EXPECT_EQ(task_manager->GetTotalNumTaskEventsReported(), num_batch1 + num_batch2);

    std::sort(expected_events.begin(), expected_events.end(), SortByTaskIdAndAttempt);
    auto actual_events = task_manager->task_event_storage_->GetTaskEvents();
    std::sort(actual_events.begin(), actual_events.end(), SortByTaskIdAndAttempt);
    EXPECT_EQ(actual_events.size(), expected_events.size());
    for (size_t i = 0; i < actual_events.size(); ++i) {
      EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(actual_events[i],
                                                                     expected_events[i]));
    }

    // Assert on drop counts.
    EXPECT_EQ(task_manager->GetTotalNumTaskAttemptsDropped(),
              num_batch1 + num_status_events_dropped_on_worker);
    EXPECT_EQ(task_manager->GetTotalNumProfileTaskEventsDropped(),
              num_profile_events_to_drop + num_profile_events_dropped_on_worker);
  }
}

TEST_F(GcsTaskManagerTest, TestGetTaskEventsWithDriver) {
  // Add task events
  auto task_ids = GenTaskIDs(1);
  auto driver_task = task_ids[0];

  // Add Driver
  {
    auto events = GenTaskEvents(
        {driver_task},
        /* attempt_number */ 0,
        /* job_id */ 0,
        /* profile event */ absl::nullopt,
        /* status_update*/ absl::nullopt,
        GenTaskInfo(
            /* job_id */ JobID::FromInt(0), TaskID::Nil(), rpc::TaskType::DRIVER_TASK));
    auto events_data = GenTaskEventsData(events);
    SyncAddTaskEventData(events_data);
  }

  // Should get the event when including driver
  {
    auto reply = SyncGetTaskEvents(/* task_ids */ {},
                                   /* job_id */ absl::nullopt,
                                   /* limit */ -1,
                                   /* exclude_driver*/ false);
    EXPECT_EQ(reply.events_by_task_size(), 1);
  }

  // Default exclude driver
  {
    auto reply = SyncGetTaskEvents(/* task_ids */ {},
                                   /* job_id */ absl::nullopt,
                                   /* limit */ -1);
    EXPECT_EQ(reply.events_by_task_size(), 0);
  }
}

TEST_F(GcsTaskManagerMemoryLimitedTest, TestLimitReturnRecentTasksWhenGetAll) {
  // Keep adding tasks and make sure even with eviction, the returned tasks are
  // the mo
  size_t num_to_insert = 20;
  size_t num_query = 5;
  size_t inserted = 0;

  auto task_ids = GenTaskIDs(num_to_insert);

  for (size_t i = 0; i < num_to_insert; ++i) {
    // Add a task event
    {
      inserted++;
      auto events =
          GenTaskEvents({task_ids[i]},
                        /* attempt_number */ 0,
                        /* job_id */ 0,
                        /* profile event */ absl::nullopt,
                        GenStateUpdate({{rpc::TaskStatus::RUNNING, 1}}, WorkerID::Nil()));
      auto events_data = GenTaskEventsData(events);
      SyncAddTaskEventData(events_data);
    }

    if (inserted < num_query || inserted % num_query != 0) {
      continue;
    }

    // Expect returned tasks with limit are the most recently added ones.
    {
      absl::flat_hash_set<TaskID> query_ids(task_ids.begin() + (inserted - num_query),
                                            task_ids.begin() + inserted);
      auto reply = SyncGetTaskEvents(
          /* task_ids */ {}, /* job_id */ absl::nullopt, /* limit */ num_query);
      for (const auto &task_event : reply.events_by_task()) {
        EXPECT_EQ(query_ids.count(TaskID::FromBinary(task_event.task_id())), 1)
            << TaskID::FromBinary(task_event.task_id()).Hex() << "not there, at " << i;
      }
    }
  }
}

TEST_F(GcsTaskManagerTest, TestTaskDataLossWorker) {
  // Test that if a task is reported as data loss from the worker, GCS
  // should drop the entire task attempt.
  auto task_id = GenTaskIDForJob(0);

  // Add a task event.
  SyncAddTaskEvent({task_id}, {{rpc::TaskStatus::RUNNING, 1}});
  auto reply = SyncGetTaskEvents({});
  EXPECT_EQ(reply.events_by_task_size(), 1);

  // Report it as data loss.
  auto data = GenTaskEventsDataLoss({{task_id, 0}});
  SyncAddTaskEventData(data);

  // The task attempt should be dropped.
  reply = SyncGetTaskEvents({});
  EXPECT_EQ(reply.events_by_task_size(), 0);
  EXPECT_EQ(reply.num_status_task_events_dropped(), 1);

  // Adding any new task attempt should also be dropped.
  SyncAddTaskEvent({task_id}, {{rpc::TaskStatus::RUNNING, 2}});
  reply = SyncGetTaskEvents({});
  EXPECT_EQ(reply.events_by_task_size(), 0);
  EXPECT_EQ(reply.num_status_task_events_dropped(), 1);
}

TEST_F(GcsTaskManagerTest, TestMultipleJobsDataLoss) {
  auto job_task0 = GenTaskIDForJob(0);
  auto job_task1 = GenTaskIDForJob(1);

  SyncAddTaskEvent({job_task0}, {{rpc::TaskStatus::RUNNING, 1}}, TaskID::Nil(), 0);
  SyncAddTaskEvent({job_task1}, {{rpc::TaskStatus::RUNNING, 1}}, TaskID::Nil(), 1);

  // Make data loss happens on job 0.
  auto data = GenTaskEventsDataLoss({{job_task0, 0}}, 0);
  SyncAddTaskEventData(data);

  // Job 0 has data loss
  {
    auto reply = SyncGetTaskEvents({}, /* job_id */ JobID::FromInt(0));
    EXPECT_EQ(reply.events_by_task_size(), 0);
    EXPECT_EQ(reply.num_status_task_events_dropped(), 1);
  }

  // Job 1 no data loss
  {
    auto reply = SyncGetTaskEvents({}, /* job_id */ JobID::FromInt(1));
    EXPECT_EQ(reply.events_by_task_size(), 1);
    EXPECT_EQ(reply.num_status_task_events_dropped(), 0);
  }

  {
    auto reply = SyncGetTaskEvents({});
    EXPECT_EQ(reply.events_by_task_size(), 1);
    EXPECT_EQ(reply.num_status_task_events_dropped(), 1);
  }

  {
    // Finish the job should keep the data.
    task_manager->OnJobFinished(JobID::FromInt(0), 3);

    // Wait for longer than the default timer
    boost::asio::io_context io;
    boost::asio::deadline_timer timer(
        io,
        boost::posix_time::milliseconds(
            2 * RayConfig::instance().gcs_mark_task_failed_on_job_done_delay_ms()));
    timer.wait();

    auto reply = SyncGetTaskEvents({}, /* job_id */ JobID::FromInt(0));
    EXPECT_EQ(reply.events_by_task_size(), 0);
    EXPECT_EQ(reply.num_status_task_events_dropped(), 1);

    // Verify no leak from the dropped task attempts tracking.
    auto job_summary =
        task_manager->task_event_storage_->GetJobTaskSummary(JobID::FromInt(0));
    EXPECT_EQ(job_summary.dropped_task_attempts_.size(), 0);
  }
}

TEST_F(GcsTaskManagerDroppedTaskAttemptsLimit, TestDroppedTaskAttemptsLimit) {
  // Test that when the number of dropped task attempts exceeds the limit,
  // we should be them to prevent OOM.

  // Generate 20 task attempts, 10 should be dropped.
  {
    for (int i = 0; i < 20; i++) {
      auto t = GenTaskIDForJob(0);
      SyncAddTaskEvent({t}, {{rpc::TaskStatus::RUNNING, 1}}, TaskID::Nil(), 0);
    }
  }

  // Before GC we should have 10 dropped task attempts tracked.
  auto job_summary =
      task_manager->task_event_storage_->GetJobTaskSummary(JobID::FromInt(0));
  EXPECT_EQ(job_summary.dropped_task_attempts_.size(), 10);

  // After GC, there should only be 5 tracked.
  job_summary.GcOldDroppedTaskAttempts(JobID::FromInt(0));
  EXPECT_EQ(job_summary.dropped_task_attempts_.size(), 5);
  EXPECT_EQ(job_summary.num_dropped_task_attempts_evicted_, 5);
  EXPECT_EQ(job_summary.NumTaskAttemptsDropped(), 10);
}

TEST_F(GcsTaskManagerProfileEventsLimitTest, TestProfileEventsNoLeak) {
  auto task = GenTaskIDs(1)[0];

  // Keep generating profile events and make sure the number of profile events
  // is bounded.
  for (int i = 0; i < 100; i++) {
    auto events = GenTaskEvents({task},
                                /* attempt_number */ 0,
                                /* job_id */ 0,
                                GenProfileEvents("event", 1, 1));
    auto events_data = GenTaskEventsData(events);
    SyncAddTaskEventData(events_data);
  }

  // Assert on the profile events in the buffer.
  {
    auto reply = SyncGetTaskEvents({});
    EXPECT_EQ(reply.events_by_task_size(), 1);
    EXPECT_EQ(reply.events_by_task().begin()->profile_events().events().size(),
              RayConfig::instance().task_events_max_num_profile_events_per_task());

    // assert on the profile events dropped counter.
    EXPECT_EQ(reply.num_profile_task_events_dropped(),
              100 - RayConfig::instance().task_events_max_num_profile_events_per_task());
    EXPECT_EQ(task_manager->GetTotalNumProfileTaskEventsDropped(),
              100 - RayConfig::instance().task_events_max_num_profile_events_per_task());
  }
}

TEST_F(GcsTaskManagerMemoryLimitedTest, TestLimitGcPriorityBased) {
  size_t num_limit = 10;  // sync with class config
  // For the default gc policy, we evict tasks based (first to last):
  // 1. finished tasks
  // 2. non finished actor tasks
  // 3. other tasks.
  auto tasks = GenTaskIDs(10);
  auto task_running = tasks[0];
  auto task_failed = tasks[1];
  auto task_actor = tasks[2];
  auto task_finished = tasks[3];

  // Add all tasks running task
  SyncAddTaskEvent({task_running, task_finished, task_failed},
                   {{rpc::TaskStatus::RUNNING, 1}});

  // Add a failed task
  SyncAddTaskEvent({task_failed}, {{rpc::TaskStatus::FAILED, 1}});

  // Add a non finished actor task
  auto actor_id = ActorID::Of(JobID::FromInt(0), task_running, 0);
  SyncAddTaskEvent({task_actor},
                   {{rpc::TaskStatus::RUNNING, 1}},
                   TaskID::Nil(),
                   0,
                   absl::nullopt,
                   actor_id);

  // Add a finished task that's running and then finished
  SyncAddTaskEvent({task_finished}, {{rpc::TaskStatus::FINISHED, 2}});

  // Fill until buffer full
  for (size_t i = 4; i < num_limit; ++i) {
    SyncAddTaskEvent({tasks[i]}, {{rpc::TaskStatus::RUNNING, 1}});
  }

  // task_finished should be evicted.
  {
    SyncAddTaskEvent({GenTaskIDs(1)}, {{rpc::TaskStatus::RUNNING, 1}});

    auto reply = SyncGetTaskEvents({task_finished});
    EXPECT_EQ(reply.events_by_task_size(), 0);
    reply = SyncGetTaskEvents({});
    EXPECT_EQ(reply.events_by_task_size(), num_limit);
    EXPECT_EQ(reply.num_status_task_events_dropped(), 1);
  }

  // non finished actor task should be evicted.
  {
    SyncAddTaskEvent({GenTaskIDs(1)}, {{rpc::TaskStatus::RUNNING, 1}});

    auto reply = SyncGetTaskEvents({task_actor});
    EXPECT_EQ(reply.events_by_task_size(), 0);
    reply = SyncGetTaskEvents({});
    EXPECT_EQ(reply.events_by_task_size(), num_limit);
    EXPECT_EQ(reply.num_status_task_events_dropped(), 2);
  }

  // first added running task should be evicted.
  {
    SyncAddTaskEvent({GenTaskIDs(1)}, {{rpc::TaskStatus::RUNNING, 1}});

    auto reply = SyncGetTaskEvents({task_running});
    EXPECT_EQ(reply.events_by_task_size(), 0);
    reply = SyncGetTaskEvents({});
    EXPECT_EQ(reply.events_by_task_size(), num_limit);
    EXPECT_EQ(reply.num_status_task_events_dropped(), 3);
  }
}

}  // namespace gcs
}  // namespace ray
