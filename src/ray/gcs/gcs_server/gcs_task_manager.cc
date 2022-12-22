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

std::vector<rpc::TaskEvents> GcsTaskManager::GcsTaskManagerStorage::GetTaskEvents()
    const {
  return task_events_;
}

std::vector<rpc::TaskEvents> GcsTaskManager::GcsTaskManagerStorage::GetTaskEvents(
    JobID job_id) const {
  auto task_attempts_itr = job_to_task_attempt_index_.find(job_id);
  if (task_attempts_itr == job_to_task_attempt_index_.end()) {
    // Not found any tasks related to this job.
    return {};
  }
  return GetTaskEvents(task_attempts_itr->second);
}

std::vector<rpc::TaskEvents> GcsTaskManager::GcsTaskManagerStorage::GetTaskEvents(
    const absl::flat_hash_set<TaskID> &task_ids) const {
  absl::flat_hash_set<TaskAttempt> select_task_attempts;
  for (const auto &task_id : task_ids) {
    auto task_attempts_itr = task_to_task_attempt_index_.find(task_id);
    if (task_attempts_itr != task_to_task_attempt_index_.end()) {
      select_task_attempts.insert(task_attempts_itr->second.begin(),
                                  task_attempts_itr->second.end());
    }
  }

  return GetTaskEvents(select_task_attempts);
}

std::vector<rpc::TaskEvents> GcsTaskManager::GcsTaskManagerStorage::GetTaskEvents(
    const absl::flat_hash_set<TaskAttempt> &task_attempts) const {
  std::vector<rpc::TaskEvents> result;
  for (const auto &task_attempt : task_attempts) {
    auto idx_itr = task_attempt_index_.find(task_attempt);
    if (idx_itr != task_attempt_index_.end()) {
      result.push_back(task_events_.at(idx_itr->second));
    }
  }

  return result;
}

absl::optional<rpc::TaskEvents>
GcsTaskManager::GcsTaskManagerStorage::AddOrReplaceTaskEvent(
    rpc::TaskEvents &&events_by_task) {
  TaskID task_id = TaskID::FromBinary(events_by_task.task_id());
  JobID job_id = JobID::FromBinary(events_by_task.job_id());
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
  // TODO(rickyx): Optimize this to per job limit with bounded FIFO map.
  // https://github.com/ray-project/ray/issues/31071
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

    // Update task_attempt -> buffer index mapping.
    TaskAttempt replaced_attempt = std::make_pair<>(
        TaskID::FromBinary(replaced.task_id()), replaced.attempt_number());

    // Update task attempt -> idx mapping.
    task_attempt_index_.erase(replaced_attempt);
    task_attempt_index_[task_attempt] = next_idx_to_overwrite_;

    // Update the job -> task attempt mapping.
    auto replaced_job_id = JobID::FromBinary(replaced.job_id());
    job_to_task_attempt_index_[job_id].erase(replaced_attempt);
    if (job_to_task_attempt_index_[replaced_job_id].empty()) {
      job_to_task_attempt_index_.erase(replaced_job_id);
    }
    job_to_task_attempt_index_[job_id].insert(task_attempt);

    // Update the task -> task attempt mapping.
    auto replaced_task_id = TaskID::FromBinary(replaced.task_id());
    task_to_task_attempt_index_[replaced_task_id].erase(replaced_attempt);
    if (task_to_task_attempt_index_[replaced_task_id].empty()) {
      task_to_task_attempt_index_.erase(replaced_task_id);
    }
    task_to_task_attempt_index_[task_id].insert(task_attempt);

    // Update iter.
    next_idx_to_overwrite_ = (next_idx_to_overwrite_ + 1) % max_num_task_events_;

    return replaced;
  }

  // Add to index.
  task_attempt_index_[task_attempt] = task_events_.size();
  job_to_task_attempt_index_[job_id].insert(task_attempt);
  task_to_task_attempt_index_[task_id].insert(task_attempt);
  // Add a new task events.
  task_events_.push_back(std::move(events_by_task));
  return absl::nullopt;
}

void GcsTaskManager::HandleGetTaskEvents(rpc::GetTaskEventsRequest request,
                                         rpc::GetTaskEventsReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting task status:" << request.ShortDebugString();
  absl::MutexLock lock(&mutex_);

  // Select candidate events by indexing.
  std::vector<rpc::TaskEvents> task_events;
  if (request.has_task_ids()) {
    absl::flat_hash_set<TaskID> task_ids;
    for (const auto &task_id_str : request.task_ids().vals()) {
      task_ids.insert(TaskID::FromBinary(task_id_str));
    }
    task_events = task_event_storage_->GetTaskEvents(task_ids);
  } else if (request.has_job_id()) {
    task_events = task_event_storage_->GetTaskEvents(JobID::FromBinary(request.job_id()));
  } else {
    task_events = task_event_storage_->GetTaskEvents();
  }

  // Populate reply.
  auto limit = request.has_limit() ? request.limit() : -1;
  // Simple limit.
  auto count = 0;
  int32_t num_profile_event_limit = 0;
  int32_t num_status_event_limit = 0;
  for (auto &task_event : task_events) {
    if (limit < 0 || count++ < limit) {
      auto events = reply->add_events_by_task();
      events->Swap(&task_event);
    } else {
      num_profile_event_limit +=
          task_event.has_profile_events() ? task_event.profile_events().events_size() : 0;
      num_status_event_limit += task_event.has_state_updates()
                                    ? task_event.state_updates().status_events_size()
                                    : 0;
    }
  }
  // TODO(rickyx): We will need to revisit the data loss semantics, to report data loss
  // on a single task retry(attempt) rather than the actual events.
  // https://github.com/ray-project/ray/issues/31280
  reply->set_num_profile_task_events_dropped(total_num_profile_task_events_dropped_ +
                                             num_profile_event_limit);
  reply->set_num_status_task_events_dropped(total_num_status_task_events_dropped_ +
                                            num_status_event_limit);

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
    // attempt.  https://github.com/ray-project/ray/issues/31279
    auto replaced_task_events =
        task_event_storage_->AddOrReplaceTaskEvent(std::move(events_by_task));

    if (replaced_task_events) {
      if (replaced_task_events->has_state_updates()) {
        total_num_status_task_events_dropped_ +=
            replaced_task_events->state_updates().status_events_size();
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
