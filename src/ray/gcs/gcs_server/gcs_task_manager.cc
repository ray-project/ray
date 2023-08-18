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
#include "ray/gcs/pb_util.h"

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
  std::vector<rpc::TaskEvents> ret;
  // NOTE(rickyx): This could be done better if we expose an iterator - which we
  // probably have to do if we are supporting pagination in the future.
  // As for now, this will make sure data is returned w.r.t insertion order, so we could
  // return the more recent entries when limit applies.
  RAY_CHECK(next_idx_to_overwrite_ == 0 || next_idx_to_overwrite_ < task_events_.size())
      << "next_idx_to_overwrite=" << next_idx_to_overwrite_
      << " should be in bound. (size=" << task_events_.size() << ")";
  // Copy from the least recently generated data, where `next_idx_to_overwrite_` points to
  // the least recently added data.
  std::copy(task_events_.begin() + next_idx_to_overwrite_,
            task_events_.end(),
            std::back_inserter(ret));
  // Copy the wrapped around if any
  if (next_idx_to_overwrite_ > 0) {
    std::copy(task_events_.begin(),
              task_events_.begin() + next_idx_to_overwrite_,
              std::back_inserter(ret));
  }
  return ret;
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

absl::optional<TaskAttempt> GcsTaskManager::GcsTaskManagerStorage::GetLatestTaskAttempt(
    const TaskID &task_id) const {
  RAY_CHECK(!task_id.IsNil());
  auto task_attempts_itr = task_to_task_attempt_index_.find(task_id);
  if (task_attempts_itr == task_to_task_attempt_index_.end()) {
    // No task attempt for the task yet. This could happen if a task has not been stored
    // or already evicted (when there are many tasks).
    return absl::nullopt;
  }
  const auto &task_attempts = task_attempts_itr->second;
  int32_t highest_attempt_number = static_cast<int32_t>(task_attempts.size()) - 1;
  TaskAttempt latest_task_attempt = std::make_pair<>(task_id, highest_attempt_number);
  if (highest_attempt_number < 0 || !task_attempts.count(latest_task_attempt)) {
    // Missing data as the highest task attempt not found as data has been dropped on the
    // worker. In this case, it's not possible to tell if the latest task attempt is
    // correctly stored due to data loss. We simply treat it as non-failure and users will
    // be notified of the data loss from the drop count.
    return absl::nullopt;
  }
  return latest_task_attempt;
}

void GcsTaskManager::GcsTaskManagerStorage::MarkTasksFailedOnWorkerDead(
    const WorkerID &worker_id, const rpc::WorkerTableData &worker_failure_data) {
  auto task_attempts_itr = worker_to_task_attempt_index_.find(worker_id);
  if (task_attempts_itr == worker_to_task_attempt_index_.end()) {
    // No tasks by the worker.
    return;
  }

  rpc::RayErrorInfo error_info;
  error_info.set_error_type(rpc::ErrorType::WORKER_DIED);
  std::stringstream error_message;
  error_message << "Worker running the task (" << worker_id.Hex()
                << ") died with exit_type: " << worker_failure_data.exit_type()
                << " with error_message: " << worker_failure_data.exit_detail();
  error_info.set_error_message(error_message.str());

  for (const auto &task_attempt : task_attempts_itr->second) {
    RAY_LOG(INFO) << "Marking task attempts of worker " << worker_id << " as failed: "
                  << " with error_message: " << error_message.str()
                  << " for task attempt: " << task_attempt.first
                  << ", attempt_number: " << task_attempt.second << ".";

    MarkTaskAttemptFailedIfNeeded(
        task_attempt, worker_failure_data.end_time_ms() * 1000, error_info);
  }
}

rpc::TaskEvents &GcsTaskManager::GcsTaskManagerStorage::GetTaskEvent(
    const TaskAttempt &task_attempt) {
  auto idx_itr = task_attempt_index_.find(task_attempt);
  RAY_CHECK(idx_itr != task_attempt_index_.end())
      << "Task attempt of task: " << task_attempt.first
      << ", attempt_number: " << task_attempt.second
      << " should have task events in the buffer but missing.";
  return task_events_.at(idx_itr->second);
}

const rpc::TaskEvents &GcsTaskManager::GcsTaskManagerStorage::GetTaskEvent(
    const TaskAttempt &task_attempt) const {
  auto idx_itr = task_attempt_index_.find(task_attempt);
  RAY_CHECK(idx_itr != task_attempt_index_.end())
      << "Task attempt of task: " << task_attempt.first
      << ", attempt_number: " << task_attempt.second
      << " should have task events in the buffer but missing.";
  return task_events_.at(idx_itr->second);
}

void GcsTaskManager::GcsTaskManagerStorage::MarkTaskAttemptFailedIfNeeded(
    const TaskAttempt &task_attempt,
    int64_t failed_ts,
    const rpc::RayErrorInfo &error_info) {
  auto &task_event = GetTaskEvent(task_attempt);
  // We don't mark tasks as failed if they are already terminated.
  if (IsTaskTerminated(task_event)) {
    return;
  }

  // We could mark the task as failed even if might not have state updates yet (i.e. only
  // profiling events are reported).
  auto state_updates = task_event.mutable_state_updates();
  state_updates->set_failed_ts(failed_ts);
  state_updates->mutable_error_info()->CopyFrom(error_info);
}

void GcsTaskManager::GcsTaskManagerStorage::MarkTasksFailedOnJobEnds(
    const JobID &job_id, int64_t job_finish_time_ns) {
  auto task_attempts_itr = job_to_task_attempt_index_.find(job_id);
  if (task_attempts_itr == job_to_task_attempt_index_.end()) {
    // No tasks in the job.
    return;
  }

  rpc::RayErrorInfo error_info;
  error_info.set_error_type(rpc::ErrorType::WORKER_DIED);
  std::stringstream error_message;
  error_message << "Job finishes (" << job_id.Hex()
                << ") as driver exits. Marking all non-terminal tasks as failed.";
  error_info.set_error_message(error_message.str());

  // Iterate all task attempts from the job.
  for (const auto &task_attempt : task_attempts_itr->second) {
    MarkTaskAttemptFailedIfNeeded(task_attempt, job_finish_time_ns, error_info);
  }
}

absl::optional<rpc::TaskEvents>
GcsTaskManager::GcsTaskManagerStorage::AddOrReplaceTaskEvent(
    rpc::TaskEvents &&events_by_task) {
  const TaskID task_id = TaskID::FromBinary(events_by_task.task_id());
  const JobID job_id = JobID::FromBinary(events_by_task.job_id());
  int32_t attempt_number = events_by_task.attempt_number();
  TaskAttempt task_attempt = std::make_pair<>(task_id, attempt_number);

  // Add the worker index if it's available.
  const WorkerID worker_id = GetWorkerID(events_by_task);
  if (!worker_id.IsNil()) {
    worker_to_task_attempt_index_[worker_id].insert(task_attempt);
  }
  // GCS perform merging of events/updates for a single task attempt from multiple
  // reports.
  auto itr = task_attempt_index_.find(task_attempt);
  if (itr != task_attempt_index_.end()) {
    // Existing task attempt entry, merge.
    auto idx = itr->second;
    auto &existing_events = task_events_.at(idx);

    // Update the events.
    if (events_by_task.has_task_info() && !existing_events.has_task_info()) {
      stats_counter_.Increment(
          kTaskTypeToCounterType.at(events_by_task.task_info().type()));
    }

    stats_counter_.Decrement(kNumTaskEventsBytesStored, existing_events.ByteSizeLong());
    existing_events.MergeFrom(events_by_task);
    stats_counter_.Increment(kNumTaskEventsBytesStored, existing_events.ByteSizeLong());

    return absl::nullopt;
  }

  // A new task event, add to storage and index.

  // Bump the task counters by type.
  if (events_by_task.has_task_info() && events_by_task.attempt_number() == 0) {
    stats_counter_.Increment(
        kTaskTypeToCounterType.at(events_by_task.task_info().type()));
  }

  // If limit enforced, replace one.
  // TODO(rickyx): Optimize this to per job limit with bounded FIFO map.
  // https://github.com/ray-project/ray/issues/31071
  if (max_num_task_events_ > 0 && task_events_.size() >= max_num_task_events_) {
    RAY_LOG_EVERY_MS(WARNING, 10000)
        << "Max number of tasks event (" << max_num_task_events_
        << ") allowed is reached. Old task events will be overwritten. Set "
           "`RAY_task_events_max_num_task_in_gcs` to a higher value to "
           "store more.";

    stats_counter_.Decrement(kNumTaskEventsBytesStored,
                             task_events_[next_idx_to_overwrite_].ByteSizeLong());
    stats_counter_.Increment(kNumTaskEventsBytesStored, events_by_task.ByteSizeLong());

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
    job_to_task_attempt_index_[replaced_job_id].erase(replaced_attempt);
    if (job_to_task_attempt_index_[replaced_job_id].empty()) {
      job_to_task_attempt_index_.erase(replaced_job_id);
    }
    job_to_task_attempt_index_[job_id].insert(task_attempt);

    // Update the worker -> task attempt mapping.
    auto replaced_worker_id = GetWorkerID(replaced);
    if (!replaced_worker_id.IsNil()) {
      worker_to_task_attempt_index_[replaced_worker_id].erase(replaced_attempt);
      if (worker_to_task_attempt_index_[replaced_worker_id].empty()) {
        worker_to_task_attempt_index_.erase(replaced_worker_id);
      }
    }
    // Add the worker mapping.
    if (!worker_id.IsNil()) {
      worker_to_task_attempt_index_[worker_id].insert(task_attempt);
    }

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
  stats_counter_.Increment(kNumTaskEventsBytesStored, events_by_task.ByteSizeLong());
  stats_counter_.Increment(kNumTaskEventsStored);

  task_events_.push_back(std::move(events_by_task));

  return absl::nullopt;
}

void GcsTaskManager::HandleGetTaskEvents(rpc::GetTaskEventsRequest request,
                                         rpc::GetTaskEventsReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting task status:" << request.ShortDebugString();

  // Select candidate events by indexing if possible.
  std::vector<rpc::TaskEvents> task_events;
  const auto &filters = request.filters();
  if (filters.task_ids_size() > 0) {
    absl::flat_hash_set<TaskID> task_ids;
    for (const auto &task_id_str : filters.task_ids()) {
      task_ids.insert(TaskID::FromBinary(task_id_str));
    }
    task_events = task_event_storage_->GetTaskEvents(task_ids);
  } else if (filters.has_job_id()) {
    task_events = task_event_storage_->GetTaskEvents(JobID::FromBinary(filters.job_id()));
  } else {
    task_events = task_event_storage_->GetTaskEvents();
  }

  // Populate reply.
  auto limit = request.has_limit() ? request.limit() : -1;
  // Simple limit.
  auto count = 0;
  int32_t num_profile_event_limit = 0;
  int32_t num_status_event_limit = 0;

  // A lambda filter fn, where it returns true for task events to be included in the
  // result. Task ids and job ids are already filtered by the storage with indexing above.
  auto filter_fn = [&filters](const rpc::TaskEvents &task_event) {
    if (!task_event.has_task_info()) {
      // Skip task events w/o task info.
      return false;
    }
    if (filters.exclude_driver() &&
        task_event.task_info().type() == rpc::TaskType::DRIVER_TASK) {
      return false;
    }

    if (filters.has_actor_id() && task_event.task_info().has_actor_id() &&
        ActorID::FromBinary(task_event.task_info().actor_id()) !=
            ActorID::FromBinary(filters.actor_id())) {
      return false;
    }

    if (filters.has_name() && task_event.task_info().name() != filters.name()) {
      return false;
    }

    if (filters.exclude_internal() &&
        task_event.task_info().name().find("__ray") != std::string::npos) {
      // Filter out ray internal functions.
      return false;
    }

    return true;
  };

  for (auto itr = task_events.rbegin(); itr != task_events.rend(); ++itr) {
    auto &task_event = *itr;
    if (!filter_fn(task_event)) {
      continue;
    }

    if (limit < 0 || count++ < limit) {
      auto events = reply->add_events_by_task();
      events->Swap(&task_event);
    } else {
      num_profile_event_limit +=
          task_event.has_profile_events() ? task_event.profile_events().events_size() : 0;
      num_status_event_limit += task_event.has_state_updates() ? 1 : 0;
    }
  }
  // TODO(rickyx): We will need to revisit the data loss semantics, to report data loss
  // on a single task retry(attempt) rather than the actual events.
  // https://github.com/ray-project/ray/issues/31280
  reply->set_num_profile_task_events_dropped(
      stats_counter_.Get(kTotalNumProfileTaskEventsDropped) + num_profile_event_limit);
  reply->set_num_status_task_events_dropped(
      stats_counter_.Get(kTotalNumStatusTaskEventsDropped) + num_status_event_limit);

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  return;
}

void GcsTaskManager::HandleAddTaskEventData(rpc::AddTaskEventDataRequest request,
                                            rpc::AddTaskEventDataReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  auto data = std::move(request.data());
  // Update counters.
  stats_counter_.Increment(kTotalNumProfileTaskEventsDropped,
                           data.num_profile_task_events_dropped());
  stats_counter_.Increment(kTotalNumStatusTaskEventsDropped,
                           data.num_status_task_events_dropped());

  for (auto events_by_task : *data.mutable_events_by_task()) {
    stats_counter_.Increment(kTotalNumTaskEventsReported);
    // TODO(rickyx): add logic to handle too many profile events for a single task
    // attempt.  https://github.com/ray-project/ray/issues/31279

    auto replaced_task_events =
        task_event_storage_->AddOrReplaceTaskEvent(std::move(events_by_task));

    if (replaced_task_events) {
      if (replaced_task_events->has_state_updates()) {
        // TODO(rickyx): should we un-flatten the status updates into a list of
        // StatusEvents? so that we could get an accurate number of status change
        // events being dropped like profile events.
        stats_counter_.Increment(kTotalNumStatusTaskEventsDropped);
      }
      if (replaced_task_events->has_profile_events()) {
        stats_counter_.Increment(kTotalNumProfileTaskEventsDropped,
                                 replaced_task_events->profile_events().events_size());
      }
    }
  }

  // Processed all the task events
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

std::string GcsTaskManager::DebugString() {
  std::ostringstream ss;
  auto counters = stats_counter_.GetAll();
  ss << "GcsTaskManager: "
     << "\n-Total num task events reported: " << counters[kTotalNumTaskEventsReported]
     << "\n-Total num status task events dropped: "
     << counters[kTotalNumStatusTaskEventsDropped]
     << "\n-Total num profile events dropped: "
     << counters[kTotalNumProfileTaskEventsDropped]
     << "\n-Total num bytes of task event stored: "
     << 1.0 * counters[kNumTaskEventsBytesStored] / 1024 / 1024 << "MiB"
     << "\n-Current num of task events stored: " << counters[kNumTaskEventsStored]
     << "\n-Total num of actor creation tasks: " << counters[kTotalNumActorCreationTask]
     << "\n-Total num of actor tasks: " << counters[kTotalNumActorTask]
     << "\n-Total num of normal tasks: " << counters[kTotalNumNormalTask]
     << "\n-Total num of driver tasks: " << counters[kTotalNumDriverTask];

  return ss.str();
}

void GcsTaskManager::RecordMetrics() {
  auto counters = stats_counter_.GetAll();
  ray::stats::STATS_gcs_task_manager_task_events_reported.Record(
      counters[kTotalNumTaskEventsReported]);

  ray::stats::STATS_gcs_task_manager_task_events_dropped.Record(
      counters[kTotalNumStatusTaskEventsDropped], ray::stats::kGcsTaskStatusEventDropped);
  ray::stats::STATS_gcs_task_manager_task_events_dropped.Record(
      counters[kTotalNumProfileTaskEventsDropped], ray::stats::kGcsProfileEventDropped);

  ray::stats::STATS_gcs_task_manager_task_events_stored.Record(
      counters[kNumTaskEventsStored]);
  ray::stats::STATS_gcs_task_manager_task_events_stored_bytes.Record(
      counters[kNumTaskEventsBytesStored]);

  {
    absl::MutexLock lock(&mutex_);
    if (usage_stats_client_) {
      usage_stats_client_->RecordExtraUsageCounter(
          usage::TagKey::NUM_ACTOR_CREATION_TASKS, counters[kTotalNumActorCreationTask]);
      usage_stats_client_->RecordExtraUsageCounter(usage::TagKey::NUM_ACTOR_TASKS,
                                                   counters[kTotalNumActorTask]);
      usage_stats_client_->RecordExtraUsageCounter(usage::TagKey::NUM_NORMAL_TASKS,
                                                   counters[kTotalNumNormalTask]);
      usage_stats_client_->RecordExtraUsageCounter(usage::TagKey::NUM_DRIVERS,
                                                   counters[kTotalNumDriverTask]);
    }
  }
}

void GcsTaskManager::SetUsageStatsClient(UsageStatsClient *usage_stats_client) {
  absl::MutexLock lock(&mutex_);
  usage_stats_client_ = usage_stats_client;
}

void GcsTaskManager::OnWorkerDead(
    const WorkerID &worker_id, const std::shared_ptr<rpc::WorkerTableData> &worker_data) {
  RAY_LOG(DEBUG) << "Marking all running tasks of worker " << worker_id << " as failed.";

  std::shared_ptr<boost::asio::deadline_timer> timer =
      std::make_shared<boost::asio::deadline_timer>(
          io_service_,
          boost::posix_time::milliseconds(
              RayConfig::instance().gcs_mark_task_failed_on_worker_dead_delay_ms()));

  timer->async_wait(
      [this, timer, worker_id, worker_data](const boost::system::error_code &error) {
        if (error == boost::asio::error::operation_aborted) {
          // timer canceled or aborted.
          return;
        }
        // If there are any non-terminated tasks from the worker, mark them failed since
        // all workers associated with the worker will be failed.
        task_event_storage_->MarkTasksFailedOnWorkerDead(worker_id, *worker_data);
      });
}

void GcsTaskManager::OnJobFinished(const JobID &job_id, int64_t job_finish_time_ms) {
  RAY_LOG(DEBUG) << "Marking all running tasks of job " << job_id.Hex() << " as failed.";

  std::shared_ptr<boost::asio::deadline_timer> timer =
      std::make_shared<boost::asio::deadline_timer>(
          io_service_,
          boost::posix_time::milliseconds(
              RayConfig::instance().gcs_mark_task_failed_on_job_done_delay_ms()));

  timer->async_wait(
      [this, timer, job_id, job_finish_time_ms](const boost::system::error_code &error) {
        if (error == boost::asio::error::operation_aborted) {
          // timer canceled or aborted.
          return;
        }
        // If there are any non-terminated tasks from the job, mark them failed since all
        // workers associated with the job will be killed.
        task_event_storage_->MarkTasksFailedOnJobEnds(job_id,
                                                      job_finish_time_ms * 1000 * 1000);
      });
}

}  // namespace gcs
}  // namespace ray
