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

#include "ray/gcs/pb_util.h"

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
    const std::shared_ptr<const TaskSpecification> &task_spec,
    absl::optional<const TaskStatusEvent::TaskStateUpdate> state_update)
    : TaskEvent(task_id, job_id, attempt_number),
      task_status_(task_status),
      timestamp_(timestamp),
      task_spec_(task_spec),
      state_update_(state_update) {}

TaskProfileEvent::TaskProfileEvent(TaskID task_id,
                                   JobID job_id,
                                   int32_t attempt_number,
                                   const std::string &component_type,
                                   const std::string &component_id,
                                   const std::string &node_ip_address,
                                   const std::string &event_name,
                                   int64_t start_time)
    : TaskEvent(task_id, job_id, attempt_number),
      component_type_(component_type),
      component_id_(component_id),
      node_ip_address_(node_ip_address),
      event_name_(event_name),
      start_time_(start_time) {}

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

void TaskProfileEvent::ToRpcTaskEvents(rpc::TaskEvents *rpc_task_events) {
  // Rate limit on the number of profiling events from the task. This is especially the
  // case if a driver has many profiling events when submitting tasks
  auto profile_events = rpc_task_events->mutable_profile_events();

  // Base fields
  rpc_task_events->set_task_id(task_id_.Binary());
  rpc_task_events->set_job_id(job_id_.Binary());
  rpc_task_events->set_attempt_number(attempt_number_);
  profile_events->set_component_type(std::move(component_type_));
  profile_events->set_component_id(std::move(component_id_));
  profile_events->set_node_ip_address(std::move(node_ip_address_));
  auto event_entry = profile_events->add_events();
  event_entry->set_event_name(std::move(event_name_));
  event_entry->set_start_time(start_time_);
  event_entry->set_end_time(end_time_);
  event_entry->set_extra_data(std::move(extra_data_));
}

TaskEventBufferImpl::TaskEventBufferImpl(std::shared_ptr<gcs::GcsClient> gcs_client)
    : work_guard_(boost::asio::make_work_guard(io_service_)),
      periodical_runner_(io_service_),
      gcs_client_(std::move(gcs_client)),
      status_events_() {}

TaskEventBufferImpl::~TaskEventBufferImpl() { Stop(); }

Status TaskEventBufferImpl::Start(bool auto_flush) {
  absl::MutexLock lock(&mutex_);
  auto report_interval_ms = RayConfig::instance().task_events_report_interval_ms();
  RAY_CHECK(report_interval_ms > 0)
      << "RAY_task_events_report_interval_ms should be > 0 to use TaskEventBuffer.";

  status_events_.set_capacity(
      RayConfig::instance().task_events_max_num_status_events_buffer_on_worker());

  io_thread_ = std::thread([this]() {
#ifndef _WIN32
    // Block SIGINT and SIGTERM so they will be handled by the main thread.
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, NULL);
#endif
    SetThreadName("task_event_buffer.io");
    io_service_.run();
    RAY_LOG(INFO) << "Task event buffer io service stopped.";
  });

  // Reporting to GCS, set up gcs client and and events flushing.
  auto status = gcs_client_->Connect(io_service_);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to connect to GCS, TaskEventBuffer will stop now. [status="
                   << status.ToString() << "].";

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
  periodical_runner_.RunFnPeriodically([this] { FlushEvents(/* forced */ false); },
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
    std::vector<std::unique_ptr<TaskEvent>> *status_events_to_send,
    absl::flat_hash_set<TaskAttempt> *dropped_task_attempts_to_send) {
  absl::MutexLock lock(&mutex_);

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
    std::vector<std::unique_ptr<TaskEvent>> *profile_events_to_send) {
  absl::MutexLock lock(&profile_mutex_);

  size_t batch_size =
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

std::unique_ptr<rpc::TaskEventData> TaskEventBufferImpl::CreateDataToSend(
    std::vector<std::unique_ptr<TaskEvent>> &&status_events_to_send,
    std::vector<std::unique_ptr<TaskEvent>> &&profile_events_to_send,
    absl::flat_hash_set<TaskAttempt> &&dropped_task_attempts_to_send) {
  // Aggregate the task events by TaskAttempt.
  absl::flat_hash_map<TaskAttempt, rpc::TaskEvents> agg_task_events;
  auto to_rpc_event_fn = [this, &agg_task_events, &dropped_task_attempts_to_send](
                             std::unique_ptr<TaskEvent> &event) {
    if (dropped_task_attempts_to_send.count(event->GetTaskAttempt())) {
      // We are marking this as data loss due to some missing task status updates.
      // We will not send this event to GCS.
      stats_counter_.Increment(
          TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush);
      return;
    }

    if (!agg_task_events.count(event->GetTaskAttempt())) {
      auto inserted =
          agg_task_events.insert({event->GetTaskAttempt(), rpc::TaskEvents()});
      RAY_CHECK(inserted.second);
    }

    auto itr = agg_task_events.find(event->GetTaskAttempt());

    event->ToRpcTaskEvents(&(itr->second));
  };

  std::for_each(
      status_events_to_send.begin(), status_events_to_send.end(), to_rpc_event_fn);
  std::for_each(
      profile_events_to_send.begin(), profile_events_to_send.end(), to_rpc_event_fn);

  // Convert to rpc::TaskEventsData
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
    *(data->add_dropped_task_attempts()) = rpc_task_attempt;
  }
  size_t num_profile_events_dropped = stats_counter_.Get(
      TaskEventBufferCounter::kNumTaskProfileEventDroppedSinceLastFlush);

  data->set_num_profile_events_dropped(num_profile_events_dropped);

  return data;
}

void TaskEventBufferImpl::FlushEvents(bool forced) {
  if (!enabled_) {
    return;
  }

  // Skip if GCS hasn't finished processing the previous message.
  if (grpc_in_progress_ && !forced) {
    RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
        << "GCS hasn't replied to the previous flush events call (likely "
           "overloaded). "
           "Skipping reporting task state events and retry later."
        << "[cur_status_events_size="
        << stats_counter_.Get(TaskEventBufferCounter::kNumTaskStatusEventsStored)
        << "][cur_profile_events_size="
        << stats_counter_.Get(TaskEventBufferCounter::kNumTaskProfileEventsStored) << "]";
    return;
  }

  // Take out status events from the buffer.
  std::vector<std::unique_ptr<TaskEvent>> status_events_to_send;
  absl::flat_hash_set<TaskAttempt> dropped_task_attempts_to_send;
  status_events_to_send.reserve(RayConfig::instance().task_events_send_batch_size());
  GetTaskStatusEventsToSend(&status_events_to_send, &dropped_task_attempts_to_send);

  // Take profile events from the status events.
  std::vector<std::unique_ptr<TaskEvent>> profile_events_to_send;
  profile_events_to_send.reserve(RayConfig::instance().task_events_send_batch_size());
  GetTaskProfileEventsToSend(&profile_events_to_send);

  // Aggregate and prepare the data to send.
  std::unique_ptr<rpc::TaskEventData> data =
      CreateDataToSend(std::move(status_events_to_send),
                       std::move(profile_events_to_send),
                       std::move(dropped_task_attempts_to_send));

  gcs::TaskInfoAccessor *task_accessor;
  {
    // Sending the protobuf to GCS.
    absl::MutexLock lock(&mutex_);
    // The flag should be unset when on_complete is invoked.
    task_accessor = &gcs_client_->Tasks();
  }

  grpc_in_progress_ = true;
  auto num_task_attempts_to_send = data->events_by_task_size();
  auto num_dropped_task_attempts_to_send = data->dropped_task_attempts_size();
  auto num_bytes_to_send = data->ByteSizeLong();
  ResetCountersForFlush();

  auto on_complete = [this,
                      num_task_attempts_to_send,
                      num_dropped_task_attempts_to_send,
                      num_bytes_to_send](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Failed to push task events of  " << num_task_attempts_to_send
                       << " tasks attempts, and report "
                       << num_dropped_task_attempts_to_send
                       << " task attempts lost on worker to GCS."
                       << "[status=" << status.ToString() << "]";

      stats_counter_.Increment(TaskEventBufferCounter::kTotalNumFailedToReport);
    } else {
      stats_counter_.Increment(kTotalNumTaskAttemptsReported, num_task_attempts_to_send);
      stats_counter_.Increment(kTotalNumLostTaskAttemptsReported,
                               num_dropped_task_attempts_to_send);
      stats_counter_.Increment(kTotalTaskEventsBytesReported, num_bytes_to_send);
    }
    grpc_in_progress_ = false;
  };

  auto status = task_accessor->AsyncAddTaskEventData(std::move(data), on_complete);
  RAY_CHECK(status.ok());
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

  if (dropped_task_attempts_unreported_.count(status_event->GetTaskAttempt())) {
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
        << status_event->GetTaskAttempt().first
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
  status_events_.push_back(std::move(status_event));
}

void TaskEventBufferImpl::AddTaskProfileEvent(std::unique_ptr<TaskEvent> profile_event) {
  absl::MutexLock lock(&profile_mutex_);
  if (!enabled_) {
    return;
  }
  auto profile_events_itr = profile_events_.find(profile_event->GetTaskAttempt());
  if (profile_events_itr == profile_events_.end()) {
    auto inserted = profile_events_.insert(
        {profile_event->GetTaskAttempt(), std::vector<std::unique_ptr<TaskEvent>>()});
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
        << "Dropping profiling events for task: " << profile_event->GetTaskAttempt().first
        << ", set a higher value for RAY_task_events_max_num_profile_events_per_task("
        << max_num_profile_event_per_task
        << "), or RAY_task_events_max_num_profile_events_buffer_on_worker ("
        << max_profile_events_stored << ") to avoid this.";
    return;
  }

  stats_counter_.Increment(TaskEventBufferCounter::kNumTaskProfileEventsStored);
  profile_events_itr->second.push_back(std::move(profile_event));
}

const std::string TaskEventBufferImpl::DebugString() {
  std::stringstream ss;

  if (!Enabled()) {
    ss << "Task Event Buffer is disabled.";
    return ss.str();
  }

  auto stats = stats_counter_.GetAll();
  ss << "\nIO Service Stats:\n";
  ss << io_service_.stats().StatsString();
  ss << "\nOther Stats:"
     << "\n\tgrpc_in_progress:" << grpc_in_progress_
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
     << stats[TaskEventBufferCounter::kTotalNumTaskProfileEventDropped] << "\n";

  return ss.str();
}

}  // namespace worker

}  // namespace core
}  // namespace ray
