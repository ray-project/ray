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
  std::vector<rpc::TaskEvents> ret;
  // From the higher priority to the lower priority list.
  for (int i = gc_policy_->NumList() - 1; i >= 0; --i) {
    // Reverse iterate the list to get the latest task events.
    for (auto itr = task_events_list_[i].rbegin(); itr != task_events_list_[i].rend();
         ++itr) {
      ret.push_back(*itr);
    }
  }

  return ret;
}

std::vector<rpc::TaskEvents> GcsTaskManager::GcsTaskManagerStorage::GetTaskEvents(
    JobID job_id) const {
  auto task_attempts_itr = job_index_.find(job_id);
  if (task_attempts_itr == job_index_.end()) {
    // Not found any tasks related to this job.
    return {};
  }
  return GetTaskEvents(task_attempts_itr->second);
}

std::vector<rpc::TaskEvents> GcsTaskManager::GcsTaskManagerStorage::GetTaskEvents(
    const absl::flat_hash_set<TaskID> &task_ids) const {
  absl::flat_hash_set<std::shared_ptr<TaskEventLocator>> select_task_attempts;
  for (const auto &task_id : task_ids) {
    auto task_attempts_itr = task_index_.find(task_id);
    if (task_attempts_itr != task_index_.end()) {
      select_task_attempts.insert(task_attempts_itr->second.begin(),
                                  task_attempts_itr->second.end());
    }
  }

  return GetTaskEvents(select_task_attempts);
}

std::vector<rpc::TaskEvents> GcsTaskManager::GcsTaskManagerStorage::GetTaskEvents(
    const absl::flat_hash_set<std::shared_ptr<TaskEventLocator>> &task_attempts) const {
  std::vector<rpc::TaskEvents> result;
  for (const auto &task_attempt_loc : task_attempts) {
    // Copy the task event to the output.
    result.push_back(task_attempt_loc->GetTaskEvents());
  }

  return result;
}

void GcsTaskManager::GcsTaskManagerStorage::MarkTasksFailedOnWorkerDead(
    const WorkerID &worker_id, const rpc::WorkerTableData &worker_failure_data) {
  auto task_attempts_itr = worker_index_.find(worker_id);
  if (task_attempts_itr == worker_index_.end()) {
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

  for (const auto &task_locator : task_attempts_itr->second) {
    MarkTaskAttemptFailedIfNeeded(
        task_locator, worker_failure_data.end_time_ms() * 1000, error_info);
  }
}

void GcsTaskManager::GcsTaskManagerStorage::MarkTaskAttemptFailedIfNeeded(
    const std::shared_ptr<TaskEventLocator> &locator,
    int64_t failed_ts,
    const rpc::RayErrorInfo &error_info) {
  auto &task_event = locator->GetTaskEvents();
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
  auto task_attempts_itr = job_index_.find(job_id);
  if (task_attempts_itr == job_index_.end()) {
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
  for (const auto &task_locator : task_attempts_itr->second) {
    MarkTaskAttemptFailedIfNeeded(task_locator, job_finish_time_ns, error_info);
  }
}

void GcsTaskManager::GcsTaskManagerStorage::UpdateExistingTaskAttempt(
    const std::shared_ptr<GcsTaskManager::GcsTaskManagerStorage::TaskEventLocator> &loc,
    const rpc::TaskEvents &task_events) {
  auto &existing_task = loc->GetTaskEvents();
  // Update the tracking
  if (task_events.has_task_info() && !existing_task.has_task_info()) {
    stats_counter_.Increment(kTaskTypeToCounterType.at(task_events.task_info().type()));
  }

  // Update the task event.
  stats_counter_.Decrement(kNumTaskEventsBytesStored, existing_task.ByteSizeLong());
  existing_task.MergeFrom(task_events);
  stats_counter_.Increment(kNumTaskEventsBytesStored, existing_task.ByteSizeLong());

  // Move the task events around different gc priority list.
  auto target_list_index = gc_policy_->GetTaskListIndex(existing_task);
  auto cur_list_index = loc->GetCurrentListIndex();
  if (target_list_index != cur_list_index) {
    // Need to add to the new list first.
    task_events_list_[target_list_index].push_front(std::move(existing_task));

    task_events_list_[cur_list_index].erase(loc->GetCurrentListIterator());
    loc->SetCurrentList(target_list_index, task_events_list_[target_list_index].begin());
  }

  // Update the index if needed. Adding to index is idempotent so it is safe to call it
  // multiple times.
  AddToIndex(loc);
}

std::shared_ptr<GcsTaskManager::GcsTaskManagerStorage::TaskEventLocator>
GcsTaskManager::GcsTaskManagerStorage::AddNewTaskEvent(rpc::TaskEvents &&task_events) {
  // Create a new locator.
  auto target_list_index = gc_policy_->GetTaskListIndex(task_events);
  task_events_list_.at(target_list_index).push_front(std::move(task_events));
  auto list_itr = task_events_list_.at(target_list_index).begin();

  auto loc = std::make_shared<TaskEventLocator>(list_itr, target_list_index);

  // Add to index.
  AddToIndex(loc);

  const auto &added_task_events = loc->GetTaskEvents();

  // Stats tracking
  stats_counter_.Increment(kNumTaskEventsBytesStored, added_task_events.ByteSizeLong());
  stats_counter_.Increment(kNumTaskEventsStored);
  // Bump the task counters by type.
  if (added_task_events.has_task_info() && added_task_events.attempt_number() == 0) {
    stats_counter_.Increment(
        kTaskTypeToCounterType.at(added_task_events.task_info().type()));
  }

  return loc;
}

void GcsTaskManager::GcsTaskManagerStorage::AddToIndex(
    const std::shared_ptr<TaskEventLocator> &loc) {
  const auto &task_events = loc->GetTaskEvents();
  const auto task_attempt = GetTaskAttempt(task_events);
  const auto job_id = JobID::FromBinary(task_events.job_id());
  const auto task_id = TaskID::FromBinary(task_events.task_id());
  const auto worker_id = GetWorkerID(task_events);

  primary_index_.insert({task_attempt, loc});
  RAY_CHECK(!job_id.IsNil());
  RAY_CHECK(!task_id.IsNil());

  task_index_[task_id].insert(loc);
  job_index_[job_id].insert(loc);
  if (!worker_id.IsNil()) {
    worker_index_[worker_id].insert(loc);
  }
}

void GcsTaskManager::GcsTaskManagerStorage::RemoveFromIndex(
    const std::shared_ptr<TaskEventLocator> &loc) {
  const auto &task_events = loc->GetTaskEvents();
  const auto task_attempt = GetTaskAttempt(task_events);
  const auto job_id = JobID::FromBinary(task_events.job_id());
  const auto task_id = TaskID::FromBinary(task_events.task_id());
  const auto worker_id = GetWorkerID(task_events);

  // Remove from secondary indices.
  RAY_CHECK(!job_id.IsNil());
  auto job_attempts_iter = job_index_.find(job_id);
  RAY_CHECK(job_attempts_iter != job_index_.end());
  RAY_CHECK(job_attempts_iter->second.erase(loc) == 1);
  if (job_attempts_iter->second.empty()) {
    job_index_.erase(job_attempts_iter);
  }

  RAY_CHECK(!task_id.IsNil());
  auto task_attempts_iter = task_index_.find(task_id);
  RAY_CHECK(task_attempts_iter != task_index_.end());
  RAY_CHECK(task_attempts_iter->second.erase(loc) == 1);
  if (task_attempts_iter->second.empty()) {
    task_index_.erase(task_attempts_iter);
  }

  if (!worker_id.IsNil()) {
    auto worker_attempts_iter = worker_index_.find(worker_id);
    RAY_CHECK(worker_attempts_iter != worker_index_.end());
    RAY_CHECK(worker_attempts_iter->second.erase(loc) == 1);
    if (worker_attempts_iter->second.empty()) {
      worker_index_.erase(worker_attempts_iter);
    }
  }

  // Remove from primary index.
  primary_index_.erase(task_attempt);
}

std::shared_ptr<GcsTaskManager::GcsTaskManagerStorage::TaskEventLocator>
GcsTaskManager::GcsTaskManagerStorage::UpdateOrInitTaskEventLocator(
    rpc::TaskEvents &&events_by_task) {
  const TaskID task_id = TaskID::FromBinary(events_by_task.task_id());
  int32_t attempt_number = events_by_task.attempt_number();
  TaskAttempt task_attempt = std::make_pair<>(task_id, attempt_number);

  auto loc_itr = primary_index_.find(task_attempt);
  if (loc_itr != primary_index_.end()) {
    // Merge with an existing entry.
    UpdateExistingTaskAttempt(loc_itr->second, events_by_task);
    return loc_itr->second;
  }

  // A new task attempt
  auto loc = AddNewTaskEvent(std::move(events_by_task));

  return loc;
}

void GcsTaskManager::GcsTaskManagerStorage::RemoveTaskAttempt(
    std::shared_ptr<TaskEventLocator> loc) {
  const auto &to_remove = loc->GetTaskEvents();

  const auto job_id = JobID::FromBinary(to_remove.job_id());

  // Update the tracking
  job_task_summary_[job_id].RecordProfileEventsDropped(NumProfileEvents(to_remove));
  job_task_summary_[job_id].RecordTaskAttemptDropped(GetTaskAttempt(to_remove));
  stats_counter_.Decrement(kNumTaskEventsBytesStored, to_remove.ByteSizeLong());
  stats_counter_.Decrement(kNumTaskEventsStored);
  stats_counter_.Increment(kTotalNumTaskAttemptsDropped);
  stats_counter_.Increment(kTotalNumProfileTaskEventsDropped,
                           NumProfileEvents(to_remove));

  // Remove from the index.
  RemoveFromIndex(loc);

  // Lastly, remove from the underlying list.
  task_events_list_[loc->GetCurrentListIndex()].erase(loc->GetCurrentListIterator());
}

void GcsTaskManager::GcsTaskManagerStorage::EvictTaskEvent() {
  // Choose one task event to evict
  size_t list_index = 0;
  for (; list_index < gc_policy_->NumList(); ++list_index) {
    // Find the lowest priority list to gc.
    if (!task_events_list_[list_index].empty()) {
      break;
    }
  }
  RAY_CHECK(list_index < gc_policy_->NumList());

  // Evict from the end.
  const auto &to_evict = task_events_list_[list_index].back();
  const auto &loc_iter = primary_index_.find(GetTaskAttempt(to_evict));
  RAY_CHECK(loc_iter != primary_index_.end());

  RemoveTaskAttempt(loc_iter->second);
}

void GcsTaskManager::GcsTaskManagerStorage::AddOrReplaceTaskEvent(
    rpc::TaskEvents &&events_by_task) {
  auto job_id = JobID::FromBinary(events_by_task.job_id());
  // We are dropping this task.
  if (job_task_summary_[job_id].ShouldDropTaskAttempt(GetTaskAttempt(events_by_task))) {
    // This task attempt has been dropped.
    RAY_LOG(DEBUG) << "already dropping task "
                   << TaskID::FromBinary(events_by_task.task_id()) << " attempt "
                   << events_by_task.attempt_number() << " of job " << job_id;
    return;
  }

  // Get or init a task locator: if the task event is not stored, a new entry
  // will be added to the storage.
  std::shared_ptr<TaskEventLocator> loc =
      UpdateOrInitTaskEventLocator(std::move(events_by_task));

  // If limit enforced, replace one.
  if (max_num_task_events_ > 0 && static_cast<size_t>(stats_counter_.Get(
                                      kNumTaskEventsStored)) > max_num_task_events_) {
    RAY_LOG_EVERY_MS(WARNING, 10000)
        << "Max number of tasks event (" << max_num_task_events_
        << ") allowed is reached. Old task events will be overwritten. Set "
           "`RAY_task_events_max_num_task_in_gcs` to a higher value to "
           "store more.";
    EvictTaskEvent();
  }
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
    const auto job_id = JobID::FromBinary(filters.job_id());
    task_events = task_event_storage_->GetTaskEvents(job_id);
    // Populate per-job data loss.
    if (task_event_storage_->HasJob(job_id)) {
      const auto &job_summary = task_event_storage_->GetJobTaskSummary(job_id);
      reply->set_num_profile_task_events_dropped(job_summary.NumProfileEventsDropped());
      reply->set_num_status_task_events_dropped(job_summary.NumTaskAttemptsDropped());
    }
  } else {
    task_events = task_event_storage_->GetTaskEvents();
    // Populate all jobs data loss
    reply->set_num_profile_task_events_dropped(
        task_event_storage_->NumProfileEventsDropped());
    reply->set_num_status_task_events_dropped(
        task_event_storage_->NumTaskAttemptsDropped());
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

  reply->set_num_profile_task_events_dropped(reply->num_profile_task_events_dropped() +
                                             num_profile_event_limit);
  reply->set_num_status_task_events_dropped(reply->num_status_task_events_dropped() +
                                            num_status_event_limit);

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  return;
}

void GcsTaskManager::GcsTaskManagerStorage::RecordDataLossFromWorker(
    const rpc::TaskEventData &data) {
  for (const auto &dropped_attempt : data.dropped_task_attempts()) {
    const auto task_id = TaskID::FromBinary(dropped_attempt.task_id());
    auto attempt_number = dropped_attempt.attempt_number();
    auto job_id = task_id.JobId();
    job_task_summary_[job_id].RecordTaskAttemptDropped(
        std::make_pair<>(task_id, attempt_number));
    stats_counter_.Increment(kTotalNumTaskAttemptsDropped);

    // We will also remove any existing task events for this task attempt from the storage
    // since we want to make data loss at task attempt granularity.
    const auto &loc_iter = primary_index_.find(std::make_pair<>(task_id, attempt_number));
    if (loc_iter != primary_index_.end()) {
      RemoveTaskAttempt(loc_iter->second);
    }
  }

  if (data.num_profile_events_dropped() > 0) {
    job_task_summary_[JobID::FromBinary(data.job_id())].RecordProfileEventsDropped(
        data.num_profile_events_dropped());
    stats_counter_.Increment(kTotalNumProfileTaskEventsDropped,
                             data.num_profile_events_dropped());
  }
}

void GcsTaskManager::HandleAddTaskEventData(rpc::AddTaskEventDataRequest request,
                                            rpc::AddTaskEventDataReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  auto data = std::move(request.data());
  task_event_storage_->RecordDataLossFromWorker(data);

  for (auto events_by_task : *data.mutable_events_by_task()) {
    stats_counter_.Increment(kTotalNumTaskEventsReported);
    task_event_storage_->AddOrReplaceTaskEvent(std::move(events_by_task));
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
     << counters[kTotalNumTaskAttemptsDropped] << "\n-Total num profile events dropped: "
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
      counters[kTotalNumTaskAttemptsDropped], ray::stats::kGcsTaskStatusEventDropped);
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
