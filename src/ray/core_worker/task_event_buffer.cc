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

bool TaskStatusEvent::ToRpcTaskEventsOrDrop(rpc::TaskEvents *rpc_task_events) {
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
    return false;
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

  return false;
}

bool TaskProfileEvent::ToRpcTaskEventsOrDrop(rpc::TaskEvents *rpc_task_events) {
  // Rate limit on the number of profiling events from the task. This is especially the
  // case if a driver has many profiling events when submitting tasks
  auto profile_events = rpc_task_events->mutable_profile_events();
  auto profile_event_max_num =
      RayConfig::instance().task_events_max_num_profile_events_for_task();
  if (profile_event_max_num >= 0 &&
      profile_events->events_size() >= profile_event_max_num) {
    // Data loss. We are dropping the newly reported profile event.
    // This will likely happen on a driver task since the driver has a fixed placeholder
    // driver task id and it could generate large number of profile events when submitting
    // many tasks.
    // We are only dropping this TaskEvent (which corresponds to a single profile event),
    // rather than others. We will likely change the GC logic in the future as well.
    RAY_LOG_EVERY_N(WARNING, 100000)
        << "Dropping profiling events for task: " << task_id_
        << ", set a higher value for RAY_task_events_max_num_profile_events_for_task("
        << profile_event_max_num << ").";
    return true;
  }

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
  return false;
}

TaskEventBufferImpl::TaskEventBufferImpl(std::unique_ptr<gcs::GcsClient> gcs_client)
    : work_guard_(boost::asio::make_work_guard(io_service_)),
      periodical_runner_(io_service_),
      gcs_client_(std::move(gcs_client)),
      buffer_() {}

TaskEventBufferImpl::~TaskEventBufferImpl() { Stop(); }

Status TaskEventBufferImpl::Start(bool auto_flush) {
  absl::MutexLock lock(&mutex_);
  auto report_interval_ms = RayConfig::instance().task_events_report_interval_ms();
  RAY_CHECK(report_interval_ms > 0)
      << "RAY_task_events_report_interval_ms should be > 0 to use TaskEventBuffer.";

  buffer_.set_capacity(RayConfig::instance().task_events_max_buffer_size());
  // Reporting to GCS, set up gcs client and and events flushing.
  auto status = gcs_client_->Connect(io_service_);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to connect to GCS, TaskEventBuffer will stop now. [status="
                   << status.ToString() << "].";

    enabled_ = false;
    return status;
  }

  enabled_ = true;

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

void TaskEventBufferImpl::AddTaskEvent(std::unique_ptr<TaskEvent> task_event) {
  if (!enabled_) {
    return;
  }
  size_t num_profile_events_dropped = 0;
  size_t num_status_events_dropped = 0;
  size_t num_add = 0;

  absl::MutexLock lock(&mutex_);
  size_t prev_size = buffer_.size();
  {
    if (buffer_.full()) {
      const auto &to_evict = buffer_.front();
      if (to_evict->IsProfileEvent()) {
        num_profile_events_dropped++;
      } else {
        num_status_events_dropped++;
      }
    }
    buffer_.push_back(std::move(task_event));
    num_add = buffer_.size() - prev_size;
  }

  stats_counter_.Increment(
      TaskEventBufferCounter::kNumTaskProfileEventDroppedSinceLastFlush,
      num_profile_events_dropped);
  stats_counter_.Increment(
      TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush,
      num_status_events_dropped);
  stats_counter_.Increment(TaskEventBufferCounter::kNumTaskEventsStored, num_add);
}

void TaskEventBufferImpl::FlushEvents(bool forced) {
  if (!enabled_) {
    return;
  }
  std::vector<std::unique_ptr<TaskEvent>> to_send;
  to_send.reserve(RayConfig::instance().task_events_send_batch_size());

  {
    absl::MutexLock lock(&mutex_);

    // Skip if GCS hasn't finished processing the previous message.
    if (grpc_in_progress_ && !forced) {
      RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
          << "GCS hasn't replied to the previous flush events call (likely "
             "overloaded). "
             "Skipping reporting task state events and retry later."
          << "[cur_buffer_size=" << buffer_.size() << "].";
      return;
    }

    // No data to send.
    if (buffer_.empty()) {
      return;
    }

    size_t num_to_send =
        std::min(static_cast<size_t>(RayConfig::instance().task_events_send_batch_size()),
                 static_cast<size_t>(buffer_.size()));
    to_send.insert(to_send.end(),
                   std::make_move_iterator(buffer_.begin()),
                   std::make_move_iterator(buffer_.begin() + num_to_send));
    buffer_.erase(buffer_.begin(), buffer_.begin() + num_to_send);
  }

  // Aggregate
  absl::flat_hash_map<TaskAttempt, rpc::TaskEvents> agg_task_events;
  auto to_rpc_event_fn = [this, &agg_task_events](std::unique_ptr<TaskEvent> &event) {
    if (!agg_task_events.count(event->GetTaskAttempt())) {
      auto inserted =
          agg_task_events.insert({event->GetTaskAttempt(), rpc::TaskEvents()});
      RAY_CHECK(inserted.second);
    }

    auto itr = agg_task_events.find(event->GetTaskAttempt());

    if (event->ToRpcTaskEventsOrDrop(&(itr->second))) {
      RAY_CHECK(event->IsProfileEvent());
      stats_counter_.Increment(
          TaskEventBufferCounter::kNumTaskProfileEventDroppedSinceLastFlush);
    }
  };
  std::for_each(to_send.begin(), to_send.end(), to_rpc_event_fn);

  // Convert to rpc::TaskEventsData
  auto data = std::make_unique<rpc::TaskEventData>();
  size_t num_task_events = to_send.size();
  size_t num_profile_event_to_send = 0;
  size_t num_status_event_to_send = 0;
  for (auto &[_task_attempt, task_event] : agg_task_events) {
    auto events_by_task = data->add_events_by_task();
    if (task_event.has_profile_events()) {
      num_profile_event_to_send++;
    }
    if (task_event.has_state_updates()) {
      num_status_event_to_send++;
    }
    *events_by_task = std::move(task_event);
  }

  // Send and reset the counters
  stats_counter_.Decrement(TaskEventBufferCounter::kNumTaskEventsStored, to_send.size());
  size_t num_profile_task_events_dropped = stats_counter_.Get(
      TaskEventBufferCounter::kNumTaskProfileEventDroppedSinceLastFlush);
  stats_counter_.Decrement(
      TaskEventBufferCounter::kNumTaskProfileEventDroppedSinceLastFlush,
      num_profile_task_events_dropped);
  stats_counter_.Increment(TaskEventBufferCounter::kTotalNumTaskProfileEventDropped,
                           num_profile_task_events_dropped);

  size_t num_status_task_events_dropped = stats_counter_.Get(
      TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush);
  stats_counter_.Decrement(
      TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush,
      num_status_task_events_dropped);
  stats_counter_.Increment(TaskEventBufferCounter::kTotalNumTaskStatusEventDropped,
                           num_status_task_events_dropped);

  data->set_num_profile_task_events_dropped(num_profile_task_events_dropped);
  data->set_num_status_task_events_dropped(num_status_task_events_dropped);

  gcs::TaskInfoAccessor *task_accessor;
  {
    // Sending the protobuf to GCS.
    absl::MutexLock lock(&mutex_);
    // The flag should be unset when on_complete is invoked.
    task_accessor = &gcs_client_->Tasks();
  }

  grpc_in_progress_ = true;
  auto on_complete = [this, num_task_events](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Failed to push " << num_task_events
                       << " task state events to GCS. Data will be lost. [status="
                       << status.ToString() << "]";
    }
    grpc_in_progress_ = false;
  };

  auto status = task_accessor->AsyncAddTaskEventData(std::move(data), on_complete);
  {
    if (!status.ok()) {
      // If we couldn't even send the data by invoking client side callbacks, there's
      // something seriously wrong, and losing data in this case should not be too
      // worse. So we will silently drop these task events.
      RAY_LOG(WARNING)
          << "Failed to push task state events to GCS. Data will be lost. [status="
          << status.ToString() << "]";
      grpc_in_progress_ = false;

      // Fail to send, currently dropping events.
      stats_counter_.Increment(TaskEventBufferCounter::kTotalNumTaskProfileEventDropped,
                               num_profile_event_to_send);
      stats_counter_.Increment(TaskEventBufferCounter::kTotalNumTaskStatusEventDropped,
                               num_status_event_to_send);
    }
  }
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
     << "\n\tcurrent number of task events in buffer: "
     << stats[TaskEventBufferCounter::kNumTaskEventsStored]
     << "\n\ttotal task events sent: "
     << 1.0 * stats[TaskEventBufferCounter::kTotalTaskEventsBytesReported] / 1024 / 1024
     << " MiB"
     << "\n\ttotal number of task events sent: "
     << stats[TaskEventBufferCounter::kTotalTaskEventsReported]
     << "\n\tnum status task events dropped: "
     << stats[TaskEventBufferCounter::kTotalNumTaskProfileEventDropped]
     << "\n\tnum profile task events dropped: "
     << stats[TaskEventBufferCounter::kTotalNumTaskStatusEventDropped] << "\n";

  return ss.str();
}

}  // namespace worker

}  // namespace core
}  // namespace ray
