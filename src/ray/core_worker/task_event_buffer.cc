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
    absl::optional<NodeID> node_id,
    absl::optional<WorkerID> worker_id)
    : TaskEvent(task_id, job_id, attempt_number),
      task_status_(task_status),
      timestamp_(timestamp),
      task_spec_(task_spec),
      node_id_(node_id),
      worker_id_(worker_id) {}

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
  auto state_updates = rpc_task_events->mutable_state_updates();

  if (node_id_.has_value()) {
    RAY_CHECK(task_status_ == rpc::TaskStatus::SUBMITTED_TO_WORKER)
        << "Node ID should be included when task status changes to "
           "SUBMITTED_TO_WORKER.";
    state_updates->set_node_id(node_id_->Binary());
  }

  if (worker_id_.has_value()) {
    RAY_CHECK(task_status_ == rpc::TaskStatus::SUBMITTED_TO_WORKER)
        << "Worker ID should be included when task status changes to "
           "SUBMITTED_TO_WORKER.";
    state_updates->set_worker_id(worker_id_->Binary());
  }
  gcs::FillTaskStatusUpdateTime(task_status_, timestamp_, state_updates);

  return false;
}

bool TaskProfileEvent::ToRpcTaskEventsOrDrop(rpc::TaskEvents *rpc_task_events) {
  // Rate limit on the number of profiling events from the task. This is especially the
  // case if a driver has many profiling events when submitting tasks
  auto profile_events = rpc_task_events->mutable_profile_events();
  auto profile_event_max_num =
      RayConfig::instance().task_events_max_num_profile_events_for_task();
  if (profile_events->events_size() > profile_event_max_num) {
    // Data loss.
    RAY_LOG_EVERY_N(WARNING, 10000)
        << "Dropping profiling events for task: " << task_id_
        << ", set a higher value for RAY_task_events_max_num_profile_events_for_task("
        << profile_event_max_num << ").";
    return true;
  }

  // Base fields
  rpc_task_events->set_task_id(task_id_.Binary());
  rpc_task_events->set_job_id(job_id_.Binary());
  rpc_task_events->set_attempt_number(attempt_number_);

  // Profile data
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

Status TaskEventBufferImpl::Start(bool auto_flush) {
  absl::MutexLock lock(&mutex_);
  auto report_interval_ms = RayConfig::instance().task_events_report_interval_ms();
  RAY_CHECK(report_interval_ms > 0)
      << "RAY_task_events_report_interval_ms should be > 0 to use TaskEventBuffer.";
  buffer_.set_capacity(RayConfig::instance().task_events_worker_buffer_size());
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

  RAY_LOG(INFO) << "Reporting task events to GCS every " << report_interval_ms
                << "ms, with a circular buffer of capacity = "
                << RayConfig::instance().task_events_worker_buffer_size()
                << ", send batch size = "
                << RayConfig::instance().task_events_send_batch_size();
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
    gcs_client_->Disconnect();
  }
}

bool TaskEventBufferImpl::Enabled() const { return enabled_; }

void TaskEventBufferImpl::AddTaskProfileEvent(TaskProfileEvent e) {
  if (!enabled_) {
    return;
  }
  buf_map_mutex_.ReaderLock();
  auto &buf = GetOrCreateThreadBuffer();
  buf_map_mutex_.AssertReaderHeld();

  buf.AddTaskProfileEvent(std::move(e));
  buf_map_mutex_.ReaderUnlock();
}

void TaskEventBufferImpl::AddTaskStatusEvent(TaskStatusEvent e) {
  if (!enabled_) {
    return;
  }
  buf_map_mutex_.ReaderLock();
  auto &buf = GetOrCreateThreadBuffer();
  buf_map_mutex_.AssertReaderHeld();

  buf.AddTaskStatusEvent(std::move(e));
  buf_map_mutex_.ReaderUnlock();
}

TaskEventThreadBuffer &TaskEventBufferImpl::GetOrCreateThreadBuffer() {
  const auto itr = all_thd_buffer_.find(std::this_thread::get_id());
  if (itr != all_thd_buffer_.end()) {
    return itr->second;
  }
  buf_map_mutex_.AssertReaderHeld();
  buf_map_mutex_.ReaderUnlock();
  {
    buf_map_mutex_.WriterLock();
    auto inserted =
        all_thd_buffer_.insert({std::this_thread::get_id(), TaskEventThreadBuffer()});
    RAY_CHECK(inserted.second);
    buf_map_mutex_.WriterUnlock();
  }
  buf_map_mutex_.ReaderLock();
  return all_thd_buffer_.at(std::this_thread::get_id());
}

void TaskEventBufferImpl::GatherThreadBuffer() {
  std::vector<std::unique_ptr<std::vector<TaskStatusEvent>>> all_status_bufs;
  std::vector<std::unique_ptr<std::vector<TaskProfileEvent>>> all_profile_bufs;
  {
    absl::WriterMutexLock lock(&buf_map_mutex_);
    for (auto &[thd, thd_buf] : all_thd_buffer_) {
      all_status_bufs.push_back(thd_buf.ResetStatusEventBuffer());
      all_profile_bufs.push_back(thd_buf.ResetProfileEventBuffer());
    }
  }

  // Aggregate, convert to rpc::TaskEvent, and add to the sending circular buffer.
  absl::flat_hash_map<TaskAttempt, std::unique_ptr<rpc::TaskEvents>> agg_task_events;
  auto to_rpc_event_fn = [this, &agg_task_events](TaskEvent &event) {
    auto itr = agg_task_events.find(event.GetTaskAttempt());
    rpc::TaskEvents *rpc_event = nullptr;
    if (itr == agg_task_events.end()) {
      auto inserted = agg_task_events.insert(
          {event.GetTaskAttempt(), std::make_unique<rpc::TaskEvents>()});
      RAY_CHECK(inserted.second);
      rpc_event = inserted.first->second.get();
    } else {
      rpc_event = itr->second.get();
    }

    if (event.ToRpcTaskEventsOrDrop(rpc_event)) {
      RAY_CHECK(event.IsProfileEvent());
      stats_counter_.Increment(TaskEventBufferCounter::kNumTaskProfileEventDropped);
    }
  };

  for (auto &buf : all_status_bufs) {
    std::for_each(buf->begin(), buf->end(), to_rpc_event_fn);
  }

  for (auto &buf : all_profile_bufs) {
    std::for_each(buf->begin(), buf->end(), to_rpc_event_fn);
  }

  size_t num_profile_events_dropped = 0;
  size_t num_status_events_dropped = 0;
  size_t num_add = 0;
  {
    absl::MutexLock lock(&mutex_);
    // Add to CB to stage for sending.
    size_t prev_size = buffer_.size();
    for (auto &[_task_attempt, event] : agg_task_events) {
      if (buffer_.full()) {
        const auto &to_evict = buffer_.front();
        if (to_evict->has_profile_events()) {
          num_profile_events_dropped++;
        }

        if (to_evict->has_state_updates()) {
          num_status_events_dropped++;
        }
      }
      buffer_.push_back(std::move(event));
    }

    num_add = buffer_.size() - prev_size;
  }
  stats_counter_.Increment(TaskEventBufferCounter::kNumTaskProfileEventDropped,
                           num_profile_events_dropped);
  stats_counter_.Increment(TaskEventBufferCounter::kNumTaskStatusEventDropped,
                           num_status_events_dropped);
  stats_counter_.Increment(TaskEventBufferCounter::kNumTaskEventsStored, num_add);
}

void TaskEventBufferImpl::FlushEvents(bool forced) {
  if (!enabled_) {
    return;
  }

  // Gather all threads' buffered events to circular buffer.
  GatherThreadBuffer();

  // Skip if GCS hasn't finished processing the previous message.
  if (grpc_in_progress_ && !forced) {
    RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
        << "GCS hasn't replied to the previous flush events call (likely "
           "overloaded). "
           "Skipping reporting task state events and retry later."
        << "[cur_buffer_size="
        << stats_counter_.Get(TaskEventBufferCounter::kNumTaskEventsStored) << "].";
    return;
  }

  std::vector<std::unique_ptr<rpc::TaskEvents>> to_send;
  {
    absl::MutexLock lock(&mutex_);

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

  // Send and reset the counters
  stats_counter_.Decrement(TaskEventBufferCounter::kNumTaskEventsStored, to_send.size());
  size_t num_profile_task_events_dropped =
      stats_counter_.Get(TaskEventBufferCounter::kNumTaskProfileEventDropped);
  stats_counter_.Decrement(TaskEventBufferCounter::kNumTaskProfileEventDropped,
                           num_profile_task_events_dropped);

  size_t num_status_task_events_dropped =
      stats_counter_.Get(TaskEventBufferCounter::kNumTaskStatusEventDropped);
  stats_counter_.Decrement(TaskEventBufferCounter::kNumTaskStatusEventDropped,
                           num_status_task_events_dropped);

  // Convert to rpc::TaskEventsData
  auto data = std::make_unique<rpc::TaskEventData>();
  data->set_num_profile_task_events_dropped(num_profile_task_events_dropped);
  data->set_num_status_task_events_dropped(num_status_task_events_dropped);

  size_t num_task_events = to_send.size();
  size_t num_profile_event_to_send = 0;
  size_t num_status_event_to_send = 0;
  for (auto &task_event : to_send) {
    auto events_by_task = data->add_events_by_task();
    if (task_event->has_profile_events()) {
      num_profile_event_to_send++;
    }
    if (task_event->has_state_updates()) {
      num_status_event_to_send++;
    }
    *events_by_task = std::move(*task_event);
  }
  // Some debug tracking.
  stats_counter_.Increment(TaskEventBufferCounter::kTotalTaskEventsReported,
                           to_send.size());
  stats_counter_.Increment(TaskEventBufferCounter::kTotalTaskEventsBytesReported,
                           data->ByteSizeLong());

  gcs::TaskInfoAccessor *task_accessor;
  {
    // Sending the protobuf to GCS.
    absl::MutexLock lock(&mutex_);
    task_accessor = &gcs_client_->Tasks();
  }

  auto on_complete = [this, num_task_events](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Failed to push " << num_task_events
                       << " task state events to GCS. Data will be lost. [status="
                       << status.ToString() << "]";
    }
    grpc_in_progress_ = false;
  };

  // The flag should be unset when on_complete is invoked.
  grpc_in_progress_ = true;
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
      stats_counter_.Increment(TaskEventBufferCounter::kNumTaskProfileEventDropped,
                               num_profile_event_to_send);
      stats_counter_.Increment(TaskEventBufferCounter::kNumTaskStatusEventDropped,
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
     << stats[TaskEventBufferCounter::kNumTaskProfileEventDropped]
     << "\n\tnum profile task events dropped: "
     << stats[TaskEventBufferCounter::kNumTaskStatusEventDropped] << "\n";

  return ss.str();
}

}  // namespace worker

}  // namespace core
}  // namespace ray
