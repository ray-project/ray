#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/gcs_task_manager.h"
#include "ray/observability/fake_metric.h"

namespace ray {
namespace gcs {

namespace {

rpc::TaskEvents MakeTaskEvent(const TaskID &task_id,
                              int32_t attempt_number,
                              int32_t job_id_int,
                              std::string_view task_name,
                              const ActorID &actor_id,
                              rpc::TaskStatus latest_state) {
  rpc::TaskEvents events;
  events.set_task_id(task_id.Binary());
  events.set_attempt_number(attempt_number);
  events.set_job_id(JobID::FromInt(job_id_int).Binary());

  auto *info = events.mutable_task_info();
  info->set_job_id(JobID::FromInt(job_id_int).Binary());
  info->set_name(std::string(task_name));
  info->set_type(rpc::TaskType::NORMAL_TASK);
  info->set_actor_id(actor_id.Binary());

  auto *state_updates = events.mutable_state_updates();
  (*state_updates->mutable_state_ts_ns())[static_cast<int32_t>(latest_state)] = 1;

  return events;
}

rpc::AddTaskEventDataReply SyncAddTaskEventData(GcsTaskManager &mgr,
                                                instrumented_io_context &io,
                                                rpc::TaskEventData data) {
  rpc::AddTaskEventDataRequest request;
  request.mutable_data()->Swap(&data);
  rpc::AddTaskEventDataReply reply;
  std::promise<void> promise;
  io.dispatch([&]() {
    mgr.HandleAddTaskEventData(
        std::move(request), &reply, [&promise](Status, std::function<void()>, void *) {
          promise.set_value();
        });
  });
  promise.get_future().get();
  return reply;
}

rpc::GetTaskEventsReply SyncGetTaskEvents(
    GcsTaskManager &mgr,
    instrumented_io_context &io,
    const rpc::GetTaskEventsRequest::Filters &filters) {
  rpc::GetTaskEventsRequest request;
  request.mutable_filters()->CopyFrom(filters);
  rpc::GetTaskEventsReply reply;
  std::promise<void> promise;
  io.dispatch([&]() {
    mgr.HandleGetTaskEvents(
        std::move(request), &reply, [&promise](Status, std::function<void()>, void *) {
          promise.set_value();
        });
  });
  promise.get_future().get();
  return reply;
}

}  // namespace

TEST(GcsTaskEventsQueryBenchmark, TaskNameEqual) {
  RayConfig::instance().initialize(
      R"(
{
  "task_events_max_num_task_in_gcs": 200000
}
  )");

  observability::FakeGauge fake_task_events_reported_gauge;
  observability::FakeGauge fake_task_events_dropped_gauge;
  observability::FakeGauge fake_task_events_stored_gauge;
  auto io_context =
      std::make_unique<InstrumentedIOContextWithThread>("GcsTaskEventsQueryBenchmark");
  auto mgr = std::make_unique<GcsTaskManager>(io_context->GetIoService(),
                                              fake_task_events_reported_gauge,
                                              fake_task_events_dropped_gauge,
                                              fake_task_events_stored_gauge);

  const int32_t job_id_int = 1;
  const ActorID actor_id = ActorID::Of(JobID::FromInt(job_id_int), TaskID::Nil(), 1);
  const std::string hot_name = "hot_task";
  const std::string cold_name = "cold_task";

  const int32_t total_tasks = 50000;
  const int32_t hot_tasks = 200;
  rpc::TaskEventData data;
  data.set_job_id(JobID::FromInt(job_id_int).Binary());
  for (int32_t i = 0; i < total_tasks; ++i) {
    TaskID task_id = TaskID::ForNormalTask(JobID::FromInt(job_id_int), RandomTaskId(), 0);
    const bool is_hot = (i < hot_tasks);
    *data.add_events_by_task() =
        MakeTaskEvent(task_id,
                      0,
                      job_id_int,
                      is_hot ? hot_name : cold_name,
                      actor_id,
                      is_hot ? rpc::TaskStatus::RUNNING : rpc::TaskStatus::FINISHED);
  }

  SyncAddTaskEventData(*mgr, io_context->GetIoService(), std::move(data));

  rpc::GetTaskEventsRequest::Filters filters;
  auto *name_filter = filters.add_task_name_filters();
  name_filter->set_predicate(rpc::FilterPredicate::EQUAL);
  name_filter->set_task_name(hot_name);

  const int iters = 2000;
  int64_t sink = 0;
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < iters; ++i) {
    auto reply = SyncGetTaskEvents(*mgr, io_context->GetIoService(), filters);
    sink += reply.events_by_task_size();
  }
  auto end = std::chrono::steady_clock::now();
  auto micros =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

  std::cout << "iters=" << iters << " total_tasks=" << total_tasks
            << " hot_tasks=" << hot_tasks << " time_us=" << micros << " sink=" << sink
            << "\n";
}

}  // namespace gcs
}  // namespace ray
