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

#include "ray/core_worker/task_event_buffer.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/grpc_util.h"

namespace ray {
namespace core {

namespace worker {

TaskEvent::TaskEvent(TaskID task_id, JobID job_id, int32_t attempt_number)
    : task_id_(task_id), job_id_(job_id), attempt_number_(attempt_number) {}

TaskStatusEvent::TaskStatusEvent(
    TaskID task_id,
    JobID job_id,
    int32_t attempt_number,
    const rpc::TaskStatus &task_status,
    int64_t timestamp,
    bool is_actor_task_event,
    std::string session_name,
    const std::shared_ptr<const TaskSpecification> &task_spec,
    std::optional<const TaskStatusEvent::TaskStateUpdate> state_update)
    : TaskEvent(task_id, job_id, attempt_number),
      task_status_(task_status),
      timestamp_(timestamp),
      is_actor_task_event_(is_actor_task_event),
      session_name_(session_name),
      task_spec_(task_spec),
      state_update_(std::move(state_update)) {}

TaskProfileEvent::TaskProfileEvent(TaskID task_id,
                                   JobID job_id,
                                   int32_t attempt_number,
                                   std::string component_type,
                                   std::string component_id,
                                   std::string node_ip_address,
                                   std::string event_name,
                                   int64_t start_time,
                                   std::string session_name)
    : TaskEvent(task_id, job_id, attempt_number),
      component_type_(std::move(component_type)),
      component_id_(std::move(component_id)),
      node_ip_address_(std::move(node_ip_address)),
      event_name_(std::move(event_name)),
      start_time_(start_time),
      session_name_(session_name) {}

void TaskStatusEvent::ToRpcTaskEvents(rpc::TaskEvents *rpc_task_events) {
  // Base fields
  rpc_task_events->set_task_id(task_id_.Binary());
  rpc_task_events->set_job_id(job_id_.Binary());
  rpc_task_events->set_attempt_number(attempt_number_);

  // Task info.
  if (task_spec_) {
    gcs::FillTaskInfo(rpc_task_events->mutable_task_info(), *task_spec_);
  }

  // Task status update.
  auto dst_state_update = rpc_task_events->mutable_state_updates();
  gcs::FillTaskStatusUpdateTime(task_status_, timestamp_, dst_state_update);

  if (!state_update_.has_value()) {
    return;
  }

  if (state_update_->node_id_.has_value()) {
    RAY_CHECK(task_status_ == rpc::TaskStatus::SUBMITTED_TO_WORKER)
        << "When task status changes to SUBMITTED_TO_WORKER, the Node ID should be "
           "included in the status update";
    dst_state_update->set_node_id(state_update_->node_id_->Binary());
  }

  if (state_update_->worker_id_.has_value()) {
    RAY_CHECK(task_status_ == rpc::TaskStatus::SUBMITTED_TO_WORKER)
        << "When task status changes to SUBMITTED_TO_WORKER, Worker ID should be "
           "included in the status update";
    dst_state_update->set_worker_id(state_update_->worker_id_->Binary());
  }

  if (state_update_->error_info_.has_value()) {
    *(dst_state_update->mutable_error_info()) = *state_update_->error_info_;
  }

  if (state_update_->task_log_info_.has_value()) {
    dst_state_update->mutable_task_log_info()->MergeFrom(
        state_update_->task_log_info_.value());
  }

  if (!state_update_->actor_repr_name_.empty()) {
    dst_state_update->set_actor_repr_name(state_update_->actor_repr_name_);
  }

  if (state_update_->pid_.has_value()) {
    dst_state_update->set_worker_pid(state_update_->pid_.value());
  }

  if (state_update_->is_debugger_paused_.has_value()) {
    dst_state_update->set_is_debugger_paused(state_update_->is_debugger_paused_.value());
  }
}

void TaskStatusEvent::ToRpcTaskExportEvents(
    std::shared_ptr<rpc::ExportTaskEventData> rpc_task_export_event_data) {
  // Base fields
  rpc_task_export_event_data->set_task_id(task_id_.Binary());
  rpc_task_export_event_data->set_job_id(job_id_.Binary());
  rpc_task_export_event_data->set_attempt_number(attempt_number_);

  // Task info.
  if (task_spec_) {
    gcs::FillExportTaskInfo(rpc_task_export_event_data->mutable_task_info(), *task_spec_);
  }

  // Task status update.
  auto dst_state_update = rpc_task_export_event_data->mutable_state_updates();
  gcs::FillExportTaskStatusUpdateTime(task_status_, timestamp_, dst_state_update);

  if (!state_update_.has_value()) {
    return;
  }

  if (state_update_->node_id_.has_value()) {
    RAY_CHECK(task_status_ == rpc::TaskStatus::SUBMITTED_TO_WORKER)
        << "Node ID should be included when task status changes to "
           "SUBMITTED_TO_WORKER.";
    dst_state_update->set_node_id(state_update_->node_id_->Binary());
  }

  if (state_update_->worker_id_.has_value()) {
    RAY_CHECK(task_status_ == rpc::TaskStatus::SUBMITTED_TO_WORKER)
        << "Worker ID should be included when task status changes to "
           "SUBMITTED_TO_WORKER.";
    dst_state_update->set_worker_id(state_update_->worker_id_->Binary());
  }

  if (state_update_->error_info_.has_value()) {
    auto error_info = dst_state_update->mutable_error_info();
    error_info->set_error_message((*state_update_->error_info_).error_message());
    error_info->set_error_type((*state_update_->error_info_).error_type());
  }

  if (state_update_->task_log_info_.has_value()) {
    rpc::ExportTaskEventData::TaskLogInfo export_task_log_info;
    gcs::TaskLogInfoToExport(state_update_->task_log_info_.value(),
                             &export_task_log_info);
    dst_state_update->mutable_task_log_info()->MergeFrom(export_task_log_info);
  }

  if (state_update_->pid_.has_value()) {
    dst_state_update->set_worker_pid(state_update_->pid_.value());
  }

  if (state_update_->is_debugger_paused_.has_value()) {
    dst_state_update->set_is_debugger_paused(state_update_->is_debugger_paused_.value());
  }
}

// Assuming the task_spec_ it not null
// populate the TaskDefinitionEvent or ActorTaskDefinitionEvent
template <typename T>
void TaskStatusEvent::PopulateRpcRayTaskDefinitionEvent(T &definition_event_data) {
  // Task identifier
  definition_event_data.set_task_id(task_id_.Binary());
  definition_event_data.set_task_attempt(attempt_number_);

  // Common fields
  definition_event_data.set_language(task_spec_->GetLanguage());
  const auto &required_resources = task_spec_->GetRequiredResources().GetResourceMap();
  definition_event_data.mutable_required_resources()->insert(
      std::make_move_iterator(required_resources.begin()),
      std::make_move_iterator(required_resources.end()));
  definition_event_data.set_serialized_runtime_env(
      task_spec_->RuntimeEnvInfo().serialized_runtime_env());
  // TODO(CORE-2277): Remove this once runtime_env_info is fully deprecated.
  definition_event_data.mutable_runtime_env_info()->CopyFrom(
      task_spec_->RuntimeEnvInfo());
  definition_event_data.set_job_id(job_id_.Binary());
  definition_event_data.set_parent_task_id(task_spec_->ParentTaskId().Binary());
  definition_event_data.set_placement_group_id(
      task_spec_->PlacementGroupBundleId().first.Binary());
  const auto &labels = task_spec_->GetMessage().labels();
  definition_event_data.mutable_ref_ids()->insert(labels.begin(), labels.end());

  // Specific fields
  if constexpr (std::is_same_v<T, rpc::events::ActorTaskDefinitionEvent>) {
    definition_event_data.mutable_actor_func()->CopyFrom(
        task_spec_->FunctionDescriptor()->GetMessage());
    definition_event_data.set_actor_id(task_spec_->ActorId().Binary());
    definition_event_data.set_actor_task_name(task_spec_->GetName());
  } else {
    definition_event_data.mutable_task_func()->CopyFrom(
        task_spec_->FunctionDescriptor()->GetMessage());
    definition_event_data.set_task_type(task_spec_->GetMessage().type());
    definition_event_data.set_task_name(task_spec_->GetName());
  }
}

void TaskStatusEvent::PopulateRpcRayTaskExecutionEvent(
    rpc::events::TaskExecutionEvent &execution_event_data,
    google::protobuf::Timestamp timestamp) {
  // Task identifier
  execution_event_data.set_task_id(task_id_.Binary());
  execution_event_data.set_task_attempt(attempt_number_);

  // Task state
  auto &task_state = *execution_event_data.mutable_task_state();
  if (task_status_ != rpc::TaskStatus::NIL) {
    task_state[task_status_] = timestamp;
  }

  // Task property updates
  if (!state_update_.has_value()) {
    return;
  }

  if (state_update_->error_info_.has_value()) {
    execution_event_data.mutable_ray_error_info()->CopyFrom(*state_update_->error_info_);
  }

  if (state_update_->node_id_.has_value()) {
    RAY_CHECK(task_status_ == rpc::TaskStatus::SUBMITTED_TO_WORKER)
            .WithField("TaskStatus", task_status_)
        << "Node ID should be included when task status changes to "
           "SUBMITTED_TO_WORKER.";
    execution_event_data.set_node_id(state_update_->node_id_->Binary());
  }

  if (state_update_->worker_id_.has_value()) {
    RAY_CHECK(task_status_ == rpc::TaskStatus::SUBMITTED_TO_WORKER)
            .WithField("TaskStatus", task_status_)
        << "Worker ID should be included when task status changes to "
           "SUBMITTED_TO_WORKER.";
    execution_event_data.set_worker_id(state_update_->worker_id_->Binary());
  }

  if (state_update_->pid_.has_value()) {
    execution_event_data.set_worker_pid(state_update_->pid_.value());
  }

  execution_event_data.set_job_id(job_id_.Binary());
}

void TaskStatusEvent::PopulateRpcRayEventBaseFields(
    rpc::events::RayEvent &ray_event,
    bool is_definition_event,
    google::protobuf::Timestamp timestamp) {
  ray_event.set_event_id(UniqueID::FromRandom().Binary());
  ray_event.set_source_type(rpc::events::RayEvent::CORE_WORKER);
  ray_event.mutable_timestamp()->CopyFrom(timestamp);
  ray_event.set_severity(rpc::events::RayEvent::INFO);
  ray_event.set_session_name(session_name_);

  if (is_definition_event) {
    if (is_actor_task_event_) {
      ray_event.set_event_type(rpc::events::RayEvent::ACTOR_TASK_DEFINITION_EVENT);
    } else {
      ray_event.set_event_type(rpc::events::RayEvent::TASK_DEFINITION_EVENT);
    }
  } else {
    ray_event.set_event_type(rpc::events::RayEvent::TASK_EXECUTION_EVENT);
  }
}

void TaskStatusEvent::ToRpcRayEvents(RayEventsTuple &ray_events_tuple) {
  google::protobuf::Timestamp timestamp = AbslTimeNanosToProtoTimestamp(timestamp_);

  // Populate the task definition event
  if (task_spec_ && !ray_events_tuple.task_definition_event) {
    PopulateRpcRayEventBaseFields(
        ray_events_tuple.task_definition_event.emplace(), true, timestamp);
    if (is_actor_task_event_) {
      auto actor_task_definition_event =
          ray_events_tuple.task_definition_event->mutable_actor_task_definition_event();
      PopulateRpcRayTaskDefinitionEvent(*actor_task_definition_event);
    } else {
      auto task_definition_event =
          ray_events_tuple.task_definition_event->mutable_task_definition_event();
      PopulateRpcRayTaskDefinitionEvent(*task_definition_event);
    }
  }

  // Populate the task execution event
  PopulateRpcRayEventBaseFields(ray_events_tuple.task_execution_event.has_value()
                                    ? ray_events_tuple.task_execution_event.value()
                                    : ray_events_tuple.task_execution_event.emplace(),
                                false,
                                timestamp);
  auto task_execution_event =
      ray_events_tuple.task_execution_event.value().mutable_task_execution_event();
  PopulateRpcRayTaskExecutionEvent(*task_execution_event, timestamp);
}

void TaskProfileEvent::ToRpcTaskEvents(rpc::TaskEvents *rpc_task_events) {
  // Rate limit on the number of profiling events from the task. This is especially the
  // case if a driver has many profiling events when submitting tasks
  auto profile_events = rpc_task_events->mutable_profile_events();

  // Base fields
  rpc_task_events->set_task_id(task_id_.Binary());
  rpc_task_events->set_job_id(job_id_.Binary());
  rpc_task_events->set_attempt_number(attempt_number_);
  profile_events->set_component_type(component_type_);
  profile_events->set_component_id(component_id_);
  profile_events->set_node_ip_address(node_ip_address_);
  auto event_entry = profile_events->add_events();
  event_entry->set_event_name(event_name_);
  event_entry->set_start_time(start_time_);
  event_entry->set_end_time(end_time_);
  event_entry->set_extra_data(extra_data_);
}

void TaskProfileEvent::ToRpcTaskExportEvents(
    std::shared_ptr<rpc::ExportTaskEventData> rpc_task_export_event_data) {
  auto profile_events = rpc_task_export_event_data->mutable_profile_events();

  // Base fields
  rpc_task_export_event_data->set_task_id(task_id_.Binary());
  rpc_task_export_event_data->set_job_id(job_id_.Binary());
  rpc_task_export_event_data->set_attempt_number(attempt_number_);
  profile_events->set_component_type(component_type_);
  profile_events->set_component_id(component_id_);
  profile_events->set_node_ip_address(node_ip_address_);
  auto event_entry = profile_events->add_events();
  event_entry->set_event_name(event_name_);
  event_entry->set_start_time(start_time_);
  event_entry->set_end_time(end_time_);
  event_entry->set_extra_data(std::move(extra_data_));
}

void TaskProfileEvent::PopulateRpcRayEventBaseFields(
    rpc::events::RayEvent &ray_event, google::protobuf::Timestamp timestamp) {
  ray_event.set_event_id(UniqueID::FromRandom().Binary());
  ray_event.set_source_type(rpc::events::RayEvent::CORE_WORKER);
  ray_event.mutable_timestamp()->CopyFrom(timestamp);
  ray_event.set_severity(rpc::events::RayEvent::INFO);
  ray_event.set_event_type(rpc::events::RayEvent::TASK_PROFILE_EVENT);
  ray_event.set_session_name(session_name_);
}

void TaskProfileEvent::ToRpcRayEvents(RayEventsTuple &ray_events_tuple) {
  // Using profile start time as the event generation timestamp
  google::protobuf::Timestamp timestamp = AbslTimeNanosToProtoTimestamp(start_time_);

  // Populate Ray event base fields
  auto &ray_event = ray_events_tuple.task_profile_event.emplace();
  PopulateRpcRayEventBaseFields(ray_event, timestamp);

  // Populate the task profile event
  auto *task_profile_events = ray_event.mutable_task_profile_events();
  task_profile_events->set_task_id(task_id_.Binary());
  task_profile_events->set_job_id(job_id_.Binary());
  task_profile_events->set_attempt_number(attempt_number_);
  auto profile_events = task_profile_events->mutable_profile_events();
  profile_events->set_component_type(component_type_);
  profile_events->set_component_id(component_id_);
  profile_events->set_node_ip_address(node_ip_address_);
  auto event_entry = profile_events->add_events();
  event_entry->set_event_name(event_name_);
  event_entry->set_start_time(start_time_);
  event_entry->set_end_time(end_time_);
  event_entry->set_extra_data(std::move(extra_data_));
}

bool TaskEventBufferImpl::RecordTaskStatusEventIfNeeded(
    const TaskID &task_id,
    const JobID &job_id,
    int32_t attempt_number,
    const TaskSpecification &spec,
    rpc::TaskStatus status,
    bool include_task_info,
    std::optional<const TaskStatusEvent::TaskStateUpdate> state_update) {
  if (!Enabled()) {
    return false;
  }
  if (!spec.EnableTaskEvents()) {
    return false;
  }

  auto task_event = std::make_unique<TaskStatusEvent>(
      task_id,
      job_id,
      attempt_number,
      status,
      /* timestamp */ absl::GetCurrentTimeNanos(),
      /*is_actor_task_event=*/spec.IsActorTask(),
      session_name_,
      include_task_info ? std::make_shared<const TaskSpecification>(spec) : nullptr,
      std::move(state_update));

  AddTaskEvent(std::move(task_event));
  return true;
}

TaskEventBufferImpl::TaskEventBufferImpl(
    std::unique_ptr<gcs::GcsClient> gcs_client,
    std::unique_ptr<rpc::EventAggregatorClient> event_aggregator_client,
    std::string session_name)
    : work_guard_(boost::asio::make_work_guard(io_service_)),
      periodical_runner_(PeriodicalRunner::Create(io_service_)),
      gcs_client_(std::move(gcs_client)),
      event_aggregator_client_(std::move(event_aggregator_client)),
      session_name_(session_name) {}

TaskEventBufferImpl::~TaskEventBufferImpl() { Stop(); }

Status TaskEventBufferImpl::Start(bool auto_flush) {
  absl::MutexLock lock(&mutex_);
  send_task_events_to_gcs_enabled_ =
      RayConfig::instance().enable_core_worker_task_event_to_gcs();
  send_ray_events_to_aggregator_enabled_ =
      RayConfig::instance().enable_core_worker_ray_event_to_aggregator();

  // We want to make sure that only one of the event export mechanism is enabled. And
  // if both are enabled, we will use the event aggregator instead of the export API.
  // This code will be removed when we deprecate the export API implementation.
  export_event_write_enabled_ = !send_ray_events_to_aggregator_enabled_ &&
                                TaskEventBufferImpl::IsExportAPIEnabledTask();
  auto report_interval_ms = RayConfig::instance().task_events_report_interval_ms();
  RAY_CHECK(report_interval_ms > 0)
      << "RAY_task_events_report_interval_ms should be > 0 to use TaskEventBuffer.";

  status_events_.set_capacity(
      RayConfig::instance().task_events_max_num_status_events_buffer_on_worker());
  status_events_for_export_.set_capacity(
      RayConfig::instance().task_events_max_num_export_status_events_buffer_on_worker());

  io_thread_ = std::thread([this]() {
#ifndef _WIN32
    // Block SIGINT and SIGTERM so they will be handled by the main thread.
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, nullptr);
#endif
    SetThreadName("task_event_buffer.io");
    io_service_.run();
    RAY_LOG(INFO) << "Task event buffer io service stopped.";
  });

  // Reporting to GCS, set up gcs client and and events flushing.
  auto status = gcs_client_->Connect(io_service_);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to connect to GCS, TaskEventBuffer will stop now. [status="
                   << status << "].";

    enabled_ = false;
    io_service_.stop();
    io_thread_.join();
    return status;
  }

  enabled_ = true;

  if (!auto_flush) {
    return Status::OK();
  }

  RAY_LOG(INFO) << "Reporting task events to GCS every " << report_interval_ms << "ms.";
  periodical_runner_->RunFnPeriodically([this] { FlushEvents(/*forced= */ false); },
                                        report_interval_ms,
                                        "CoreWorker.deadline_timer.flush_task_events");
  return Status::OK();
}

void TaskEventBufferImpl::Stop() {
  if (!enabled_) {
    return;
  }
  RAY_LOG(INFO) << "Shutting down TaskEventBuffer.";

  // Shutting down the io service to exit the io_thread. This should prevent
  // any other callbacks to be run on the io thread.
  io_service_.stop();
  if (io_thread_.joinable()) {
    RAY_LOG(DEBUG) << "Joining io thread from TaskEventBuffer";
    io_thread_.join();
  }

  {
    absl::MutexLock lock(&mutex_);
    // It's now safe to disconnect the GCS client since it will not be used by any
    // callbacks.
    if (gcs_client_) {
      gcs_client_->Disconnect();
    }
  }
}

bool TaskEventBufferImpl::Enabled() const { return enabled_; }

void TaskEventBufferImpl::GetTaskStatusEventsToSend(
    std::vector<std::shared_ptr<TaskEvent>> *status_events_to_send,
    std::vector<std::shared_ptr<TaskEvent>> *status_events_to_write_for_export,
    absl::flat_hash_set<TaskAttempt> *dropped_task_attempts_to_send) {
  absl::MutexLock lock(&mutex_);

  // Get the export events data to write.
  if (export_event_write_enabled_) {
    size_t num_to_write = std::min(
        static_cast<size_t>(RayConfig::instance().export_task_events_write_batch_size()),
        static_cast<size_t>(status_events_for_export_.size()));
    status_events_to_write_for_export->insert(
        status_events_to_write_for_export->end(),
        std::make_move_iterator(status_events_for_export_.begin()),
        std::make_move_iterator(status_events_for_export_.begin() + num_to_write));
    status_events_for_export_.erase(status_events_for_export_.begin(),
                                    status_events_for_export_.begin() + num_to_write);
    stats_counter_.Decrement(
        TaskEventBufferCounter::kNumTaskStatusEventsForExportAPIStored,
        status_events_to_write_for_export->size());
  }

  // No data to send.
  if (status_events_.empty() && dropped_task_attempts_unreported_.empty()) {
    return;
  }

  // Get data loss info.
  size_t num_dropped_task_attempts_to_send = 0;
  auto num_batch_size =
      RayConfig::instance().task_events_dropped_task_attempt_batch_size();
  // Iterate and erase task attempt dropped being tracked in buffer.
  while ((num_batch_size < 0 ||
          num_dropped_task_attempts_to_send < static_cast<size_t>(num_batch_size)) &&
         !dropped_task_attempts_unreported_.empty()) {
    // If there's more dropped task status events we are tracking, and we have not
    // reached the batch size limit, we take the first one.
    auto itr = dropped_task_attempts_unreported_.begin();
    dropped_task_attempts_to_send->insert(*itr);
    dropped_task_attempts_unreported_.erase(itr);
    num_dropped_task_attempts_to_send++;
  }

  // Get the events data to send.
  size_t num_to_send =
      std::min(static_cast<size_t>(RayConfig::instance().task_events_send_batch_size()),
               static_cast<size_t>(status_events_.size()));
  status_events_to_send->insert(
      status_events_to_send->end(),
      std::make_move_iterator(status_events_.begin()),
      std::make_move_iterator(status_events_.begin() + num_to_send));
  status_events_.erase(status_events_.begin(), status_events_.begin() + num_to_send);

  stats_counter_.Decrement(TaskEventBufferCounter::kNumTaskStatusEventsStored,
                           status_events_to_send->size());
  stats_counter_.Decrement(TaskEventBufferCounter::kNumDroppedTaskAttemptsStored,
                           num_dropped_task_attempts_to_send);
}

void TaskEventBufferImpl::GetTaskProfileEventsToSend(
    std::vector<std::shared_ptr<TaskEvent>> *profile_events_to_send) {
  absl::MutexLock lock(&profile_mutex_);

  auto batch_size =
      static_cast<size_t>(RayConfig::instance().task_events_send_batch_size());
  while (!profile_events_.empty() && profile_events_to_send->size() < batch_size) {
    auto itr = profile_events_.begin();
    auto num_to_send =
        std::min(batch_size - profile_events_to_send->size(), itr->second.size());

    profile_events_to_send->insert(
        profile_events_to_send->end(),
        std::make_move_iterator(itr->second.begin()),
        std::make_move_iterator(itr->second.begin() + num_to_send));
    itr->second.erase(itr->second.begin(), itr->second.begin() + num_to_send);

    if (itr->second.empty()) {
      profile_events_.erase(itr);
    }
  }

  stats_counter_.Decrement(TaskEventBufferCounter::kNumTaskProfileEventsStored,
                           profile_events_to_send->size());
}

std::unique_ptr<rpc::TaskEventData> TaskEventBufferImpl::CreateTaskEventDataToSend(
    absl::flat_hash_map<TaskAttempt, rpc::TaskEvents> &&agg_task_events,
    const absl::flat_hash_set<TaskAttempt> &dropped_task_attempts_to_send) {
  auto data = std::make_unique<rpc::TaskEventData>();
  for (auto &[_task_attempt, task_event] : agg_task_events) {
    auto events_by_task = data->add_events_by_task();
    *events_by_task = std::move(task_event);
  }

  // Add the data loss info.
  for (auto &task_attempt : dropped_task_attempts_to_send) {
    rpc::TaskAttempt rpc_task_attempt;
    rpc_task_attempt.set_task_id(task_attempt.first.Binary());
    rpc_task_attempt.set_attempt_number(task_attempt.second);
    *(data->add_dropped_task_attempts()) = std::move(rpc_task_attempt);
  }
  size_t num_profile_events_dropped = stats_counter_.Get(
      TaskEventBufferCounter::kNumTaskProfileEventDroppedSinceLastFlush);

  data->set_num_profile_events_dropped(num_profile_events_dropped);
  return data;
}

std::unique_ptr<rpc::events::RayEventsData>
TaskEventBufferImpl::CreateRayEventsDataToSend(
    absl::flat_hash_map<TaskAttempt, RayEventsTuple> &&agg_task_events,
    const absl::flat_hash_set<TaskAttempt> &dropped_task_attempts_to_send) {
  auto data = std::make_unique<rpc::events::RayEventsData>();
  // Move the ray events.
  for (auto &[task_attempt, ray_events_tuple] : agg_task_events) {
    if (ray_events_tuple.task_definition_event) {
      auto events = data->add_events();
      *events = std::move(ray_events_tuple.task_definition_event.value());
    }
    if (ray_events_tuple.task_execution_event) {
      auto events = data->add_events();
      *events = std::move(ray_events_tuple.task_execution_event.value());
    }
    if (ray_events_tuple.task_profile_event) {
      auto events = data->add_events();
      *events = std::move(ray_events_tuple.task_profile_event.value());
    }
  }

  // Add the data loss info.
  rpc::events::TaskEventsMetadata *metadata = data->mutable_task_events_metadata();
  for (auto &task_attempt : dropped_task_attempts_to_send) {
    rpc::TaskAttempt rpc_task_attempt;
    rpc_task_attempt.set_task_id(task_attempt.first.Binary());
    rpc_task_attempt.set_attempt_number(task_attempt.second);
    *(metadata->add_dropped_task_attempts()) = std::move(rpc_task_attempt);
  }
  return data;
}

TaskEventBuffer::TaskEventDataToSend TaskEventBufferImpl::CreateDataToSend(
    const std::vector<std::shared_ptr<TaskEvent>> &status_events_to_send,
    const std::vector<std::shared_ptr<TaskEvent>> &profile_events_to_send,
    const absl::flat_hash_set<TaskAttempt> &dropped_task_attempts_to_send) {
  // Aggregate the task events by TaskAttempt.
  absl::flat_hash_map<TaskAttempt, rpc::TaskEvents> agg_task_events;
  // (task_attempt, (task_definition_event, task_execution_event, task_profile_event))
  absl::flat_hash_map<TaskAttempt, RayEventsTuple> agg_ray_events;

  auto to_rpc_event_fn =
      [this, &agg_task_events, &agg_ray_events, &dropped_task_attempts_to_send](
          const std::shared_ptr<TaskEvent> &event) {
        if (dropped_task_attempts_to_send.contains(event->GetTaskAttempt())) {
          // We are marking this as data loss due to some missing task status updates.
          // We will not send this event to GCS.
          this->stats_counter_.Increment(
              TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush);
          return;
        }

        if (send_task_events_to_gcs_enabled_) {
          auto [itr_task_events, _] =
              agg_task_events.try_emplace(event->GetTaskAttempt());
          event->ToRpcTaskEvents(&(itr_task_events->second));
        }

        if (send_ray_events_to_aggregator_enabled_) {
          auto [itr_ray_events, _] = agg_ray_events.try_emplace(event->GetTaskAttempt());
          event->ToRpcRayEvents(itr_ray_events->second);
        }
      };

  std::for_each(
      status_events_to_send.begin(), status_events_to_send.end(), to_rpc_event_fn);
  std::for_each(
      profile_events_to_send.begin(), profile_events_to_send.end(), to_rpc_event_fn);

  // Create the data to send.
  TaskEventDataToSend data_to_send;

  // Convert to rpc::TaskEventsData
  if (send_task_events_to_gcs_enabled_) {
    auto task_event_data = CreateTaskEventDataToSend(std::move(agg_task_events),
                                                     dropped_task_attempts_to_send);
    data_to_send.task_event_data = std::move(task_event_data);
  }

  // Convert to rpc::events::RayEventsData
  if (send_ray_events_to_aggregator_enabled_) {
    auto ray_events_data = CreateRayEventsDataToSend(std::move(agg_ray_events),
                                                     dropped_task_attempts_to_send);
    data_to_send.ray_events_data = std::move(ray_events_data);
  }

  return data_to_send;
}

void TaskEventBufferImpl::WriteExportData(
    const std::vector<std::shared_ptr<TaskEvent>> &status_events_to_write_for_export,
    const std::vector<std::shared_ptr<TaskEvent>> &profile_events_to_send) {
  absl::flat_hash_map<TaskAttempt, std::shared_ptr<rpc::ExportTaskEventData>>
      agg_task_events;
  // Maintain insertion order to agg_task_events so events are written
  // in the same order as the buffer.
  std::vector<TaskAttempt> agg_task_event_insertion_order;
  auto to_rpc_event_fn = [&agg_task_events, &agg_task_event_insertion_order](
                             const std::shared_ptr<TaskEvent> &event) {
    // Aggregate events by task attempt before converting to proto
    auto itr = agg_task_events.find(event->GetTaskAttempt());
    if (itr == agg_task_events.end()) {
      // Insert event into agg_task_events if the task attempt of that
      // event wasn't already added.
      auto event_for_attempt = std::make_shared<rpc::ExportTaskEventData>();
      auto inserted =
          agg_task_events.insert({event->GetTaskAttempt(), event_for_attempt});
      RAY_CHECK(inserted.second);
      agg_task_event_insertion_order.push_back(event->GetTaskAttempt());
      event->ToRpcTaskExportEvents(event_for_attempt);
    } else {
      event->ToRpcTaskExportEvents(itr->second);
    }
  };

  std::for_each(status_events_to_write_for_export.begin(),
                status_events_to_write_for_export.end(),
                to_rpc_event_fn);
  std::for_each(
      profile_events_to_send.begin(), profile_events_to_send.end(), to_rpc_event_fn);

  for (const auto &task_attempt : agg_task_event_insertion_order) {
    auto it = agg_task_events.find(task_attempt);
    RAY_CHECK(it != agg_task_events.end());
    RayExportEvent(it->second).SendEvent();
  }
}

void TaskEventBufferImpl::SendTaskEventsToGCS(std::unique_ptr<rpc::TaskEventData> data) {
  gcs::TaskInfoAccessor *task_accessor = nullptr;
  {
    // Sending the protobuf to GCS.
    absl::MutexLock lock(&mutex_);
    // The flag should be unset when on_complete is invoked.
    task_accessor = &gcs_client_->Tasks();
  }

  gcs_grpc_in_progress_ = true;
  auto num_task_attempts_to_send = data->events_by_task_size();
  auto num_dropped_task_attempts_to_send = data->dropped_task_attempts_size();
  auto num_bytes_to_send = data->ByteSizeLong();

  auto on_complete = [this,
                      num_task_attempts_to_send,
                      num_dropped_task_attempts_to_send,
                      num_bytes_to_send](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Failed to push task events of  " << num_task_attempts_to_send
                       << " tasks attempts, and report "
                       << num_dropped_task_attempts_to_send
                       << " task attempts lost on worker to GCS."
                       << "[status=" << status << "]";

      this->stats_counter_.Increment(TaskEventBufferCounter::kTotalNumFailedToReport);
    } else {
      this->stats_counter_.Increment(kTotalNumTaskAttemptsReported,
                                     num_task_attempts_to_send);
      this->stats_counter_.Increment(kTotalNumLostTaskAttemptsReported,
                                     num_dropped_task_attempts_to_send);
      this->stats_counter_.Increment(kTotalTaskEventsBytesReported, num_bytes_to_send);
    }
    gcs_grpc_in_progress_ = false;
  };
  task_accessor->AsyncAddTaskEventData(std::move(data), on_complete);
}

void TaskEventBufferImpl::SendRayEventsToAggregator(
    std::unique_ptr<rpc::events::RayEventsData> data) {
  event_aggregator_grpc_in_progress_ = true;
  auto num_task_events_to_send = data->events_size();
  auto num_dropped_task_attempts_to_send =
      data->task_events_metadata().dropped_task_attempts_size();

  rpc::ClientCallback<rpc::events::AddEventsReply> on_complete =
      [this, num_task_events_to_send, num_dropped_task_attempts_to_send](
          const Status &status, const rpc::events::AddEventsReply &reply) {
        if (!status.ok()) {
          RAY_LOG(WARNING) << "GRPC Error: Failed to send task events of "
                           << num_task_events_to_send << " tasks attempts, and report "
                           << num_dropped_task_attempts_to_send
                           << " task attempts lost on worker to the event aggregator."
                           << "[status=" << status << "]";
          this->stats_counter_.Increment(
              TaskEventBufferCounter::kTotalNumFailedRequestsToAggregator);
          this->stats_counter_.Increment(
              TaskEventBufferCounter::kTotalNumTaskEventsFailedToReportToAggregator,
              num_task_events_to_send);
        } else {
          this->stats_counter_.Increment(
              TaskEventBufferCounter::kTotalNumTaskEventsReportedToAggregator,
              num_task_events_to_send);
          this->stats_counter_.Increment(
              TaskEventBufferCounter::kTotalNumLostTaskAttemptsReportedToAggregator,
              num_dropped_task_attempts_to_send);
        }
        event_aggregator_grpc_in_progress_ = false;
      };

  rpc::events::AddEventsRequest request;
  *request.mutable_events_data() = std::move(*data);
  event_aggregator_client_->AddEvents(request, on_complete);
}

void TaskEventBufferImpl::FlushEvents(bool forced) {
  if (!enabled_) {
    return;
  }

  // Skip if GCS or the event aggregator hasn't finished processing the previous
  // message. Here we don't keep different cursors for GCS and the event aggregator
  // because in most cases, the GCS and the event aggregator will not be enabled at the
  // same time.
  if ((gcs_grpc_in_progress_ || event_aggregator_grpc_in_progress_) && !forced) {
    RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
        << "GCS or the event aggregator hasn't replied to the previous flush events "
           "call (likely overloaded). "
           "Skipping reporting task state events and retry later."
        << "[gcs_grpc_in_progress=" << gcs_grpc_in_progress_ << "]"
        << "[event_aggregator_grpc_in_progress=" << event_aggregator_grpc_in_progress_
        << "]"
        << "[cur_status_events_size="
        << stats_counter_.Get(TaskEventBufferCounter::kNumTaskStatusEventsStored)
        << "][cur_profile_events_size="
        << stats_counter_.Get(TaskEventBufferCounter::kNumTaskProfileEventsStored) << "]";
    return;
  }

  // Take out status events from the buffer.
  std::vector<std::shared_ptr<TaskEvent>> status_events_to_send;
  std::vector<std::shared_ptr<TaskEvent>> status_events_to_write_for_export;
  absl::flat_hash_set<TaskAttempt> dropped_task_attempts_to_send;
  status_events_to_send.reserve(RayConfig::instance().task_events_send_batch_size());
  GetTaskStatusEventsToSend(&status_events_to_send,
                            &status_events_to_write_for_export,
                            &dropped_task_attempts_to_send);

  // Take profile events from the status events.
  std::vector<std::shared_ptr<TaskEvent>> profile_events_to_send;
  profile_events_to_send.reserve(RayConfig::instance().task_events_send_batch_size());
  GetTaskProfileEventsToSend(&profile_events_to_send);

  // Aggregate and prepare the data to send.
  TaskEventBuffer::TaskEventDataToSend data = CreateDataToSend(
      status_events_to_send, profile_events_to_send, dropped_task_attempts_to_send);

  ResetCountersForFlush();

  if (export_event_write_enabled_) {
    WriteExportData(status_events_to_write_for_export, profile_events_to_send);
  }
  if (send_task_events_to_gcs_enabled_) {
    SendTaskEventsToGCS(std::move(data.task_event_data));
  }
  if (send_ray_events_to_aggregator_enabled_) {
    SendRayEventsToAggregator(std::move(data.ray_events_data));
  }
}

void TaskEventBufferImpl::ResetCountersForFlush() {
  // Profile events dropped.
  auto num_profile_events_dropped_since_last_flush = stats_counter_.Get(
      TaskEventBufferCounter::kNumTaskProfileEventDroppedSinceLastFlush);
  stats_counter_.Decrement(
      TaskEventBufferCounter::kNumTaskProfileEventDroppedSinceLastFlush,
      num_profile_events_dropped_since_last_flush);
  stats_counter_.Increment(TaskEventBufferCounter::kTotalNumTaskProfileEventDropped,
                           num_profile_events_dropped_since_last_flush);

  // Task events dropped.
  auto num_status_events_dropped_since_last_flush = stats_counter_.Get(
      TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush);
  stats_counter_.Decrement(
      TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush,
      num_status_events_dropped_since_last_flush);
  stats_counter_.Increment(TaskEventBufferCounter::kTotalNumTaskStatusEventDropped,
                           num_status_events_dropped_since_last_flush);
}

void TaskEventBufferImpl::AddTaskEvent(std::unique_ptr<TaskEvent> task_event) {
  if (task_event->IsProfileEvent()) {
    AddTaskProfileEvent(std::move(task_event));
  } else {
    AddTaskStatusEvent(std::move(task_event));
  }
}

void TaskEventBufferImpl::AddTaskStatusEvent(std::unique_ptr<TaskEvent> status_event) {
  absl::MutexLock lock(&mutex_);
  if (!enabled_) {
    return;
  }
  std::shared_ptr<TaskEvent> status_event_shared_ptr = std::move(status_event);

  if (export_event_write_enabled_) {
    // If status_events_for_export_ is full, the oldest event will be
    // dropped in the circular buffer and replaced with the current event.
    if (!status_events_for_export_.full()) {
      stats_counter_.Increment(
          TaskEventBufferCounter::kNumTaskStatusEventsForExportAPIStored);
    }
    status_events_for_export_.push_back(status_event_shared_ptr);
  }
  if (dropped_task_attempts_unreported_.count(
          status_event_shared_ptr->GetTaskAttempt()) != 0u) {
    // This task attempt has been dropped before, so we drop this event.
    stats_counter_.Increment(
        TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush);
    return;
  }

  if (status_events_.full()) {
    const auto &to_evict = status_events_.front();
    auto inserted = dropped_task_attempts_unreported_.insert(to_evict->GetTaskAttempt());
    stats_counter_.Increment(
        TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush);

    RAY_LOG_EVERY_N(WARNING, 100000)
        << "Dropping task status events for task: "
        << status_event_shared_ptr->GetTaskAttempt().first
        << ", set a higher value for "
           "RAY_task_events_max_num_status_events_buffer_on_worker("
        << RayConfig::instance().task_events_max_num_status_events_buffer_on_worker()
        << ") to avoid this.";

    if (inserted.second) {
      stats_counter_.Increment(TaskEventBufferCounter::kNumDroppedTaskAttemptsStored);
    }
  } else {
    stats_counter_.Increment(TaskEventBufferCounter::kNumTaskStatusEventsStored);
  }
  status_events_.push_back(status_event_shared_ptr);
}

void TaskEventBufferImpl::AddTaskProfileEvent(std::unique_ptr<TaskEvent> profile_event) {
  absl::MutexLock lock(&profile_mutex_);
  if (!enabled_) {
    return;
  }
  std::shared_ptr<TaskEvent> profile_event_shared_ptr = std::move(profile_event);
  auto profile_events_itr =
      profile_events_.find(profile_event_shared_ptr->GetTaskAttempt());
  if (profile_events_itr == profile_events_.end()) {
    auto inserted = profile_events_.insert({profile_event_shared_ptr->GetTaskAttempt(),
                                            std::vector<std::shared_ptr<TaskEvent>>()});
    RAY_CHECK(inserted.second);
    profile_events_itr = inserted.first;
  }

  auto max_num_profile_event_per_task =
      RayConfig::instance().task_events_max_num_profile_events_per_task();
  auto max_profile_events_stored =
      RayConfig::instance().task_events_max_num_profile_events_buffer_on_worker();
  auto profile_event_stored = static_cast<size_t>(
      stats_counter_.Get(TaskEventBufferCounter::kNumTaskProfileEventsStored));

  // If we store too many per task or too many per kind of event, we drop the new event.
  if ((max_num_profile_event_per_task >= 0 &&
       profile_events_itr->second.size() >=
           static_cast<size_t>(max_num_profile_event_per_task)) ||
      profile_event_stored >= max_profile_events_stored) {
    stats_counter_.Increment(
        TaskEventBufferCounter::kNumTaskProfileEventDroppedSinceLastFlush);
    // Data loss. We are dropping the newly reported profile event.
    // This will likely happen on a driver task since the driver has a fixed placeholder
    // driver task id and it could generate large number of profile events when submitting
    // many tasks.
    RAY_LOG_EVERY_N(WARNING, 100000)
        << "Dropping profiling events for task: "
        << profile_event_shared_ptr->GetTaskAttempt().first
        << ", set a higher value for RAY_task_events_max_num_profile_events_per_task("
        << max_num_profile_event_per_task
        << "), or RAY_task_events_max_num_profile_events_buffer_on_worker ("
        << max_profile_events_stored << ") to avoid this.";
    return;
  }

  stats_counter_.Increment(TaskEventBufferCounter::kNumTaskProfileEventsStored);
  profile_events_itr->second.push_back(profile_event_shared_ptr);
}

std::string TaskEventBufferImpl::DebugString() {
  std::stringstream ss;

  if (!Enabled()) {
    ss << "Task Event Buffer is disabled.";
    return ss.str();
  }

  auto stats = stats_counter_.GetAll();
  ss << "\nIO Service Stats:\n";
  ss << io_service_.stats().StatsString();
  ss << "\nOther Stats:"
     << "\n\tgcs_grpc_in_progress:" << gcs_grpc_in_progress_
     << "\n\tevent_aggregator_grpc_in_progress:" << event_aggregator_grpc_in_progress_
     << "\n\tcurrent number of task status events in buffer: "
     << stats[TaskEventBufferCounter::kNumTaskStatusEventsStored]
     << "\n\tcurrent number of profile events in buffer: "
     << stats[TaskEventBufferCounter::kNumTaskProfileEventsStored]
     << "\n\tcurrent number of dropped task attempts tracked: "
     << stats[TaskEventBufferCounter::kNumDroppedTaskAttemptsStored]
     << "\n\ttotal task events sent: "
     << 1.0 * stats[TaskEventBufferCounter::kTotalTaskEventsBytesReported] / 1024 / 1024
     << " MiB"
     << "\n\ttotal number of task attempts sent: "
     << stats[TaskEventBufferCounter::kTotalNumTaskAttemptsReported]
     << "\n\ttotal number of task attempts dropped reported: "
     << stats[TaskEventBufferCounter::kTotalNumLostTaskAttemptsReported]
     << "\n\ttotal number of sent failure: "
     << stats[TaskEventBufferCounter::kTotalNumFailedToReport]
     << "\n\tnum status task events dropped: "
     << stats[TaskEventBufferCounter::kTotalNumTaskStatusEventDropped]
     << "\n\tnum profile task events dropped: "
     << stats[TaskEventBufferCounter::kTotalNumTaskProfileEventDropped]
     << "\n\tnum ray task events reported to aggregator: "
     << stats[TaskEventBufferCounter::kTotalNumTaskEventsReportedToAggregator]
     << "\n\tnum ray task events failed to report to aggregator: "
     << stats[TaskEventBufferCounter::kTotalNumTaskEventsFailedToReportToAggregator]
     << "\n\tnum of task attempts dropped reported to aggregator: "
     << stats[TaskEventBufferCounter::kTotalNumLostTaskAttemptsReportedToAggregator]
     << "\n\tnum of failed requests to aggregator: "
     << stats[TaskEventBufferCounter::kTotalNumFailedRequestsToAggregator];

  return ss.str();
}

}  // namespace worker

}  // namespace core
}  // namespace ray
