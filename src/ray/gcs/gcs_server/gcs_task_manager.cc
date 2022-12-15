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

#include <atomic>  // std::atomic

#include "ray/common/ray_config.h"
#include "ray/common/status.h"

namespace ray {
namespace gcs {

void GcsTaskManager::Stop() {
  io_service_.stop();
  if (io_service_thread_->joinable()) {
    io_service_thread_->join();
  }
}

std::vector<rpc::TaskEvents> GcsTaskManager::GcsTaskManagerStorage::GetTaskEvents(
    const absl::flat_hash_set<std::string> &task_ids,
    absl::optional<std::string> job_id) {
  std::vector<rpc::TaskEvents> results;

  if (!task_ids.empty()) {
    RAY_CHECK(!job_id.has_value())
        << "only one of job_id or task_ids should be provided. ";
    // Get all task events with task ids.
    for (const auto &task_event : task_events_) {
      if (task_ids.contains(task_event.task_id())) {
        results.push_back(task_event);
      }
    }
    return results;
  }

  if (job_id) {
    // Get all task events with job id
    for (const auto &task_event : task_events_) {
      if (task_event.has_task_info() && task_event.task_info().job_id() == *job_id) {
        results.push_back(task_event);
      }
    }
    return results;
  }

  // Return all task events
  return task_events_;
}

absl::optional<rpc::TaskEvents>
GcsTaskManager::GcsTaskManagerStorage::AddOrReplaceTaskEvent(
    rpc::TaskEvents &&events_by_task) {
  TaskID task_id = TaskID::FromBinary(events_by_task.task_id());
  int32_t attempt_number = events_by_task.attempt_number();
  TaskAttempt task_attempt = std::make_pair<>(task_id, attempt_number);

  // GCS perform merging of events/updates for a single task attempt from multiple
  // reports.
  auto itr = task_attempt_index_.find(task_attempt);
  if (itr != task_attempt_index_.end()) {
    // Existing task attempt entry, merge.
    auto idx = itr->second;
    auto &existing_events = task_events_.at(idx);

    // Update the events.
    num_bytes_task_events_ -= existing_events.ByteSizeLong();
    existing_events.MergeFrom(events_by_task);
    num_bytes_task_events_ += existing_events.ByteSizeLong();

    return absl::nullopt;
  }

  // A new task event, add to storage and index.

  // If limit enforced, replace one.
  if (max_num_task_events_ > 0 && task_events_.size() >= max_num_task_events_) {
    RAY_LOG_EVERY_MS(WARNING, 10000)
        << "Max number of tasks event (" << max_num_task_events_
        << ") allowed is reached. Old task events will be overwritten. Set "
           "`RAY_task_events_max_num_task_in_gcs` to a higher value to "
           "store more.";

    num_bytes_task_events_ -= task_events_[next_idx_to_overwrite_].ByteSizeLong();
    num_bytes_task_events_ += events_by_task.ByteSizeLong();

    // Change the underlying storage.
    auto &to_replaced = task_events_.at(next_idx_to_overwrite_);
    std::swap(to_replaced, events_by_task);
    auto replaced = std::move(events_by_task);

    // Update index.
    TaskAttempt replaced_attempt = std::make_pair<>(
        TaskID::FromBinary(replaced.task_id()), replaced.attempt_number());
    task_attempt_index_.erase(replaced_attempt);
    task_attempt_index_[task_attempt] = next_idx_to_overwrite_;

    // Update iter.
    next_idx_to_overwrite_ = (next_idx_to_overwrite_ + 1) % max_num_task_events_;

    return replaced;
  }

  // Add to index.
  task_attempt_index_[task_attempt] = task_events_.size();
  // Add a new task events.
  task_events_.push_back(std::move(events_by_task));
  return absl::nullopt;
}

void GcsTaskManager::HandleGetTaskEvents(rpc::GetTaskEventsRequest request,
                                         rpc::GetTaskEventsReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting task status:" << request.ShortDebugString();
  // Select candidate events by indexing.
  std::vector<rpc::TaskEvents> task_events;
  if (request.has_task_ids()) {
    absl::flat_hash_set<std::string> task_ids(request.task_ids().vals().begin(),
                                              request.task_ids().vals().end());
    task_events = task_event_storage_->GetTaskEvents(task_ids);
  } else if (request.has_job_id()) {
    task_events = task_event_storage_->GetTaskEvents(/* task_ids */ {}, request.job_id());
  } else {
    task_events = task_event_storage_->GetTaskEvents(/* task_ids */ {});
  }

  // Populate reply.
  for (auto &task_event : task_events) {
    auto events = reply->add_events_by_task();
    events->Swap(&task_event);
  }

  reply->set_num_profile_task_events_dropped(total_num_profile_task_events_dropped_);
  reply->set_num_status_task_events_dropped(total_num_status_task_events_dropped_);

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  return;
}

void GcsTaskManager::HandleAddTaskEventData(rpc::AddTaskEventDataRequest request,
                                            rpc::AddTaskEventDataReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  absl::MutexLock lock(&mutex_);
  RAY_LOG(DEBUG) << "Adding task state event:" << request.data().ShortDebugString();
  // Dispatch to the handler
  auto data = std::move(request.data());
  size_t num_to_process = data.events_by_task_size();
  // Update counters.
  total_num_profile_task_events_dropped_ += data.num_profile_task_events_dropped();
  total_num_status_task_events_dropped_ += data.num_status_task_events_dropped();

  for (auto events_by_task : *data.mutable_events_by_task()) {
    total_num_task_events_reported_++;
    auto task_id = TaskID::FromBinary(events_by_task.task_id());
    // TODO(rickyx): add logic to handle too many profile events for a single task
    // attempt.
    auto replaced_task_events =
        task_event_storage_->AddOrReplaceTaskEvent(std::move(events_by_task));

    if (replaced_task_events) {
      if (replaced_task_events->has_state_updates()) {
        // TODO(rickyx): should we un-flatten the status updates into a list of
        // StatusEvents? so that we could get an accurate number of status change
        // events being dropped like profile events.
        total_num_status_task_events_dropped_++;
      }
      if (replaced_task_events->has_profile_events()) {
        total_num_profile_task_events_dropped_ +=
            replaced_task_events->profile_events().events_size();
      }
    }
    RAY_LOG(DEBUG) << "Processed a task event. [task_id=" << task_id.Hex() << "]";
  }

  // Processed all the task events
  RAY_LOG(DEBUG) << "Processed all " << num_to_process << " task events";
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

std::string GcsTaskManager::DebugString() {
  absl::MutexLock lock(&mutex_);
  std::ostringstream ss;
  ss << "GcsTaskManager: "
     << "\n-Total num task events reported: " << total_num_task_events_reported_
     << "\n-Total num status task events dropped: "
     << total_num_status_task_events_dropped_
     << "\n-Total num profile events dropped: " << total_num_profile_task_events_dropped_
     << "\n-Total num bytes of task event stored: "
     << 1.0 * task_event_storage_->GetTaskEventsBytes() / 1024 / 1024 << "MiB"
     << "\n-Total num of task events stored: "
     << task_event_storage_->GetTaskEventsCount() << "\n";

  return ss.str();
}

void GcsTaskManager::RecordMetrics() {
  absl::MutexLock lock(&mutex_);
  ray::stats::STATS_gcs_task_manager_task_events_reported.Record(
      total_num_task_events_reported_);

  ray::stats::STATS_gcs_task_manager_task_events_dropped.Record(
      total_num_status_task_events_dropped_, ray::stats::kGcsTaskStatusEventDropped);
  ray::stats::STATS_gcs_task_manager_task_events_dropped.Record(
      total_num_profile_task_events_dropped_, ray::stats::kGcsProfileEventDropped);

  ray::stats::STATS_gcs_task_manager_task_events_stored.Record(
      task_event_storage_->GetTaskEventsCount());
  ray::stats::STATS_gcs_task_manager_task_events_stored_bytes.Record(
      task_event_storage_->GetTaskEventsBytes());
}

}  // namespace gcs
}  // namespace ray
