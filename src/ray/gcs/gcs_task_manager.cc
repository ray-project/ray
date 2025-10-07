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

#include <algorithm>
#include <boost/range/adaptor/reversed.hpp>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_ray_event_converter.h"
#include "ray/stats/metric_defs.h"

namespace ray {
namespace gcs {

GcsTaskManager::GcsTaskManager(instrumented_io_context &io_service)
    : io_service_(io_service),
      task_event_storage_(std::make_unique<GcsTaskManagerStorage>(
          RayConfig::instance().task_events_max_num_task_in_gcs(),
          stats_counter_,
          std::make_unique<FinishedTaskActorTaskGcPolicy>())),
      periodical_runner_(PeriodicalRunner::Create(io_service_)) {
  periodical_runner_->RunFnPeriodically([this] { task_event_storage_->GcJobSummary(); },
                                        5 * 1000,
                                        "GcsTaskManager.GcJobSummary");
}

std::vector<rpc::TaskEvents> GcsTaskManager::GcsTaskManagerStorage::GetTaskEvents()
    const {
  std::vector<rpc::TaskEvents> ret;
  ret.reserve(gc_policy_->MaxPriority());
  // From the higher priority to the lower priority list.
  for (int i = gc_policy_->MaxPriority() - 1; i >= 0; --i) {
    // Reverse iterate the list to get the latest task events.
    for (auto itr = task_events_list_[i].rbegin(); itr != task_events_list_[i].rend();
         ++itr) {
      ret.push_back(*itr);
    }
  }

  return ret;
}

std::vector<rpc::TaskEvents> GcsTaskManager::GcsTaskManagerStorage::GetTaskEvents(
    const JobID &job_id) const {
  auto task_locators_itr = job_index_.find(job_id);
  if (task_locators_itr == job_index_.end()) {
    // Not found any tasks related to this job.
    return {};
  }
  return GetTaskEvents(task_locators_itr->second);
}

std::vector<rpc::TaskEvents> GcsTaskManager::GcsTaskManagerStorage::GetTaskEvents(
    const absl::flat_hash_set<TaskID> &task_ids) const {
  absl::flat_hash_set<std::shared_ptr<TaskEventLocator>> select_task_locators;
  for (const auto &task_id : task_ids) {
    auto task_locator_itr = task_index_.find(task_id);
    if (task_locator_itr != task_index_.end()) {
      select_task_locators.insert(task_locator_itr->second.begin(),
                                  task_locator_itr->second.end());
    }
  }

  return GetTaskEvents(select_task_locators);
}

std::vector<rpc::TaskEvents> GcsTaskManager::GcsTaskManagerStorage::GetTaskEvents(
    const absl::flat_hash_set<std::shared_ptr<TaskEventLocator>> &task_locators) const {
  std::vector<rpc::TaskEvents> result;
  result.reserve(task_locators.size());
  for (const auto &task_attempt_loc : task_locators) {
    // Copy the task event to the output.
    result.push_back(task_attempt_loc->GetTaskEventsMutable());
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
        task_locator, worker_failure_data.end_time_ms() * 1000 * 1000, error_info);
  }
}

void GcsTaskManager::GcsTaskManagerStorage::MarkTaskAttemptFailedIfNeeded(
    const std::shared_ptr<TaskEventLocator> &locator,
    int64_t failed_ts_ns,
    const rpc::RayErrorInfo &error_info) {
  auto &task_events = locator->GetTaskEventsMutable();
  // We don't mark tasks as failed if they are already terminated.
  if (IsTaskTerminated(task_events)) {
    return;
  }

  // We could mark the task as failed even if might not have state updates yet (i.e. only
  // profiling events are reported).
  auto state_updates = task_events.mutable_state_updates();
  (*state_updates->mutable_state_ts_ns())[ray::rpc::TaskStatus::FAILED] = failed_ts_ns;
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
  auto &existing_task = loc->GetTaskEventsMutable();
  // Update the tracking
  if (task_events.has_task_info() && !existing_task.has_task_info()) {
    stats_counter_.Increment(kTaskTypeToCounterType.at(task_events.task_info().type()));
  }

  // Update the task event.
  existing_task.MergeFrom(task_events);

  // Truncate the profile events if needed.
  auto max_num_profile_events_per_task =
      RayConfig::instance().task_events_max_num_profile_events_per_task();
  if (existing_task.profile_events().events_size() > max_num_profile_events_per_task) {
    auto to_drop =
        existing_task.profile_events().events_size() - max_num_profile_events_per_task;
    existing_task.mutable_profile_events()->mutable_events()->DeleteSubrange(0, to_drop);

    // Update the tracking per job
    auto job_id = JobID::FromBinary(existing_task.job_id());
    job_task_summary_[job_id].RecordProfileEventsDropped(to_drop);
    stats_counter_.Increment(kTotalNumProfileTaskEventsDropped, to_drop);
  }

  // Move the task events around different gc priority list.
  auto target_list_index = gc_policy_->GetTaskListPriority(existing_task);
  auto cur_list_index = loc->GetCurrentListIndex();
  if (target_list_index != cur_list_index) {
    // Need to add to the new list first.
    task_events_list_[target_list_index].push_front(std::move(existing_task));

    task_events_list_[cur_list_index].erase(loc->GetCurrentListIterator());
    loc->SetCurrentList(target_list_index, task_events_list_[target_list_index].begin());
  }

  // Update the index if needed. Adding to index is idempotent so it is safe to call it
  // multiple times.
  UpdateIndex(loc);
}

std::shared_ptr<GcsTaskManager::GcsTaskManagerStorage::TaskEventLocator>
GcsTaskManager::GcsTaskManagerStorage::AddNewTaskEvent(rpc::TaskEvents &&task_events) {
  // Create a new locator.
  auto target_list_index = gc_policy_->GetTaskListPriority(task_events);
  task_events_list_.at(target_list_index).push_front(std::move(task_events));
  auto list_itr = task_events_list_.at(target_list_index).begin();

  auto loc = std::make_shared<TaskEventLocator>(list_itr, target_list_index);

  // Add to index.
  UpdateIndex(loc);

  const auto &added_task_events = loc->GetTaskEventsMutable();

  // Stats tracking
  stats_counter_.Increment(kNumTaskEventsStored);
  // Bump the task counters by type.
  if (added_task_events.has_task_info() && added_task_events.attempt_number() == 0) {
    stats_counter_.Increment(
        kTaskTypeToCounterType.at(added_task_events.task_info().type()));
  }

  return loc;
}

void GcsTaskManager::GcsTaskManagerStorage::UpdateIndex(
    const std::shared_ptr<TaskEventLocator> &loc) {
  const auto &task_events = loc->GetTaskEventsMutable();
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
  const auto &task_events = loc->GetTaskEventsMutable();
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
  TaskAttempt task_attempt = std::make_pair(task_id, attempt_number);

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
  const auto &to_remove = loc->GetTaskEventsMutable();

  const auto job_id = JobID::FromBinary(to_remove.job_id());

  // Update the tracking
  job_task_summary_[job_id].RecordProfileEventsDropped(NumProfileEvents(to_remove));
  job_task_summary_[job_id].RecordTaskAttemptDropped(GetTaskAttempt(to_remove));
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
  for (; list_index < gc_policy_->MaxPriority(); ++list_index) {
    // Find the lowest priority list to gc.
    if (!task_events_list_[list_index].empty()) {
      break;
    }
  }
  RAY_CHECK(list_index < gc_policy_->MaxPriority());

  // Evict from the end.
  const auto &to_evict = task_events_list_[list_index].back();
  const auto &loc_iter = primary_index_.find(GetTaskAttempt(to_evict));
  RAY_CHECK(loc_iter != primary_index_.end());

  RemoveTaskAttempt(loc_iter->second);
}

void GcsTaskManager::GcsTaskManagerStorage::AddOrReplaceTaskEvent(
    rpc::TaskEvents &&events_by_task) {
  auto job_id = JobID::FromBinary(events_by_task.job_id());
  auto task_id = TaskID::FromBinary(events_by_task.task_id());

  if (job_id.IsNil() || task_id.IsNil()) {
    // Skip invalid task events.
    RAY_LOG(DEBUG)
        << "Skip invalid task event with missing job id or task id. This "
           "could happen when profiling events are created without a task id : "
        << events_by_task.DebugString();
    return;
  }

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

namespace {

template <typename T>
bool apply_predicate(const T &lhs, rpc::FilterPredicate predicate, const T &rhs) {
  switch (predicate) {
  case rpc::FilterPredicate::EQUAL:
    return lhs == rhs;
  case rpc::FilterPredicate::NOT_EQUAL:
    return lhs != rhs;
  default:
    RAY_LOG(ERROR) << "Unknown filter predicate: " << rpc::FilterPredicate_Name(predicate)
                   << ". Supported predicates are '=' and '!='.";
    throw std::invalid_argument("Unknown filter predicate: " +
                                rpc::FilterPredicate_Name(predicate));
  }
}

bool apply_predicate_ignore_case(std::string_view lhs,
                                 rpc::FilterPredicate predicate,
                                 std::string_view rhs) {
  switch (predicate) {
  case rpc::FilterPredicate::EQUAL:
    return absl::EqualsIgnoreCase(lhs, rhs);
  case rpc::FilterPredicate::NOT_EQUAL:
    return !absl::EqualsIgnoreCase(lhs, rhs);
  default:
    RAY_LOG(ERROR) << "Unknown filter predicate: " << rpc::FilterPredicate_Name(predicate)
                   << ". Supported predicates are '=' and '!='.";
    throw std::invalid_argument("Unknown filter predicate: " +
                                rpc::FilterPredicate_Name(predicate));
  }
}

}  // namespace

void GcsTaskManager::HandleGetTaskEvents(rpc::GetTaskEventsRequest request,
                                         rpc::GetTaskEventsReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting task status:" << request.ShortDebugString();

  // TODO(meyan): In the future, we could improve the query performance by leveraging
  // the index on all the equal filters.
  // Select candidate events by indexing if possible.
  std::optional<std::vector<rpc::TaskEvents>> task_events;
  const auto &filters = request.filters();
  if (filters.task_filters_size() > 0) {
    absl::flat_hash_set<TaskID> task_ids;
    for (const auto &task_filter_obj : filters.task_filters()) {
      if (task_filter_obj.predicate() == rpc::FilterPredicate::EQUAL) {
        task_ids.insert(TaskID::FromBinary(task_filter_obj.task_id()));
      }
    }
    if (task_ids.size() == 1) {
      task_events = task_event_storage_->GetTaskEvents(task_ids);
    } else if (task_ids.size() > 1) {
      task_events = std::vector<rpc::TaskEvents>();
    }
  } else if (filters.job_filters_size() > 0) {
    absl::flat_hash_set<JobID> job_ids;
    for (const auto &job_filter_obj : filters.job_filters()) {
      if (job_filter_obj.predicate() == rpc::FilterPredicate::EQUAL) {
        job_ids.insert(JobID::FromBinary(job_filter_obj.job_id()));
      }
    }

    if (job_ids.size() == 1) {
      const JobID &job_id = *job_ids.begin();
      task_events = task_event_storage_->GetTaskEvents(job_id);

      // Populate per-job data loss.
      if (task_event_storage_->HasJob(job_id)) {
        const auto &job_summary = task_event_storage_->GetJobTaskSummary(job_id);
        reply->set_num_profile_task_events_dropped(job_summary.NumProfileEventsDropped());
        reply->set_num_status_task_events_dropped(job_summary.NumTaskAttemptsDropped());
      }
    } else if (job_ids.size() > 1) {
      task_events = std::vector<rpc::TaskEvents>();
    }
  }

  if (!task_events.has_value()) {
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
  int64_t num_profile_event_limit = 0;
  int64_t num_status_event_limit = 0;
  int64_t num_limit_truncated = 0;

  // A lambda filter fn, where it returns true for task events to be included in the
  // result. Task ids or job ids with equal predicate are already filtered by the
  // storage with indexing above.
  auto filter_fn = [&filters](const rpc::TaskEvents &task_event) {
    if (!task_event.has_task_info()) {
      // Skip task events w/o task info.
      return false;
    }
    if (filters.exclude_driver() &&
        task_event.task_info().type() == rpc::TaskType::DRIVER_TASK) {
      return false;
    }

    if (filters.task_filters_size() > 0) {
      if (!std::all_of(filters.task_filters().begin(),
                       filters.task_filters().end(),
                       [&task_event](const auto &task_filter) {
                         return apply_predicate(
                             TaskID::FromBinary(task_event.task_id()),
                             task_filter.predicate(),
                             TaskID::FromBinary(task_filter.task_id()));
                       })) {
        return false;
      }
    }

    if (filters.job_filters_size() > 0) {
      if (!std::all_of(filters.job_filters().begin(),
                       filters.job_filters().end(),
                       [&task_event](const auto &job_filter) {
                         return apply_predicate(
                             JobID::FromBinary(task_event.task_info().job_id()),
                             job_filter.predicate(),
                             JobID::FromBinary(job_filter.job_id()));
                       })) {
        return false;
      }
    }

    if (filters.actor_filters_size() > 0) {
      if (!std::all_of(filters.actor_filters().begin(),
                       filters.actor_filters().end(),
                       [&task_event](const auto &actor_filter) {
                         return apply_predicate(
                             ActorID::FromBinary(task_event.task_info().actor_id()),
                             actor_filter.predicate(),
                             ActorID::FromBinary(actor_filter.actor_id()));
                       })) {
        return false;
      }
    }

    if (filters.task_name_filters_size() > 0) {
      if (!std::all_of(filters.task_name_filters().begin(),
                       filters.task_name_filters().end(),
                       [&task_event](const auto &task_name_filter) {
                         return apply_predicate_ignore_case(task_event.task_info().name(),
                                                            task_name_filter.predicate(),
                                                            task_name_filter.task_name());
                       })) {
        return false;
      }
    }

    if (filters.state_filters_size() > 0) {
      const google::protobuf::EnumDescriptor *task_status_descriptor =
          ray::rpc::TaskStatus_descriptor();

      // Figure out the latest state of a task.
      ray::rpc::TaskStatus state = ray::rpc::TaskStatus::NIL;
      if (task_event.has_state_updates()) {
        for (int i = task_status_descriptor->value_count() - 1; i >= 0; --i) {
          if (task_event.state_updates().state_ts_ns().contains(
                  task_status_descriptor->value(i)->number())) {
            state = static_cast<ray::rpc::TaskStatus>(
                task_status_descriptor->value(i)->number());
            break;
          }
        }
      }

      if (!std::all_of(filters.state_filters().begin(),
                       filters.state_filters().end(),
                       [&state, &task_status_descriptor](const auto &state_filter) {
                         return apply_predicate_ignore_case(
                             task_status_descriptor->FindValueByNumber(state)->name(),
                             state_filter.predicate(),
                             state_filter.state());
                       })) {
        return false;
      }
    }

    return true;
  };

  int64_t num_filtered = 0;
  Status status = Status::OK();
  try {
    for (auto &task_event : *task_events | boost::adaptors::reversed) {
      if (!filter_fn(task_event)) {
        num_filtered++;
        continue;
      }

      if (limit < 0 || count++ < limit) {
        auto events = reply->add_events_by_task();
        events->Swap(&task_event);
      } else {
        num_profile_event_limit += task_event.has_profile_events()
                                       ? task_event.profile_events().events_size()
                                       : 0;
        num_status_event_limit += task_event.has_state_updates() ? 1 : 0;
        num_limit_truncated++;
      }
    }

    // Take into account truncation.
    reply->set_num_profile_task_events_dropped(reply->num_profile_task_events_dropped() +
                                               num_profile_event_limit);
    reply->set_num_status_task_events_dropped(reply->num_status_task_events_dropped() +
                                              num_status_event_limit);

    reply->set_num_total_stored(task_events->size());
    reply->set_num_truncated(num_limit_truncated);
    reply->set_num_filtered_on_gcs(num_filtered);
  } catch (std::invalid_argument &e) {
    // When encounter invalid filter predicate
    status = Status::InvalidArgument(e.what());
    reply->Clear();
  }

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
}

void GcsTaskManager::GcsTaskManagerStorage::RecordDataLossFromWorker(
    const rpc::TaskEventData &data) {
  for (const auto &dropped_attempt : data.dropped_task_attempts()) {
    const auto task_id = TaskID::FromBinary(dropped_attempt.task_id());
    auto attempt_number = dropped_attempt.attempt_number();
    auto job_id = task_id.JobId();
    job_task_summary_[job_id].RecordTaskAttemptDropped(
        std::make_pair(task_id, attempt_number));
    stats_counter_.Increment(kTotalNumTaskAttemptsDropped);

    // We will also remove any existing task events for this task attempt from the storage
    // since we want to make data loss at task attempt granularity.
    const auto &loc_iter = primary_index_.find(std::make_pair(task_id, attempt_number));
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

void GcsTaskManager::RecordTaskEventData(rpc::AddTaskEventDataRequest &request) {
  auto data = std::move(*request.mutable_data());
  task_event_storage_->RecordDataLossFromWorker(data);

  for (auto &events_by_task : *data.mutable_events_by_task()) {
    stats_counter_.Increment(kTotalNumTaskEventsReported);
    task_event_storage_->AddOrReplaceTaskEvent(std::move(events_by_task));
  }
}

void GcsTaskManager::HandleAddTaskEventData(rpc::AddTaskEventDataRequest request,
                                            rpc::AddTaskEventDataReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  RecordTaskEventData(request);

  // Processed all the task events
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsTaskManager::HandleAddEvents(rpc::events::AddEventsRequest request,
                                     rpc::events::AddEventsReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  auto task_event_data_requests = ConvertToTaskEventDataRequests(std::move(request));

  for (auto &task_event_data : task_event_data_requests) {
    RecordTaskEventData(task_event_data);
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
      counters[kTotalNumTaskAttemptsDropped], "STATUS_EVENT");
  ray::stats::STATS_gcs_task_manager_task_events_dropped.Record(
      counters[kTotalNumProfileTaskEventsDropped], "PROFILE_EVENT");

  ray::stats::STATS_gcs_task_manager_task_events_stored.Record(
      counters[kNumTaskEventsStored]);

  {
    absl::MutexLock lock(&mutex_);
    if (usage_stats_client_ != nullptr) {
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

  auto timer = std::make_shared<boost::asio::deadline_timer>(
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
  auto timer = std::make_shared<boost::asio::deadline_timer>(
      io_service_,
      boost::posix_time::milliseconds(
          RayConfig::instance().gcs_mark_task_failed_on_job_done_delay_ms()));

  timer->async_wait([this, timer, job_id, job_finish_time_ms](
                        const boost::system::error_code &error) {
    if (error == boost::asio::error::operation_aborted) {
      // timer canceled or aborted.
      return;
    }
    RAY_LOG(INFO) << "Marking all running tasks of job " << job_id.Hex() << " as failed.";
    // If there are any non-terminated tasks from the job, mark them failed since all
    // workers associated with the job will be killed.
    task_event_storage_->MarkTasksFailedOnJobEnds(job_id,
                                                  job_finish_time_ms * 1000 * 1000);

    // Clear and summarize the job summary info (since it's now finalized).
    task_event_storage_->UpdateJobSummaryOnJobDone(job_id);
  });
}

void GcsTaskManager::GcsTaskManagerStorage::JobTaskSummary::GcOldDroppedTaskAttempts(
    const JobID &job_id) {
  const auto max_dropped_task_attempts_tracked_in_gcs = static_cast<size_t>(
      RayConfig::instance()
          .task_events_max_dropped_task_attempts_tracked_per_job_in_gcs());
  if (dropped_task_attempts_.size() <= max_dropped_task_attempts_tracked_in_gcs) {
    return;
  }
  RAY_LOG(INFO) << "Evict extra dropped task attempts(" << dropped_task_attempts_.size()
                << " > " << max_dropped_task_attempts_tracked_in_gcs
                << ") tracked in GCS for job=" << job_id.Hex() << ". Setting the "
                << "RAY_task_events_max_dropped_task_attempts_tracked_per_job_in_gcs"
                << " to a higher value to store more.";

  // If there's still more than
  // task_events_max_dropped_task_attempts_tracked_per_job_in_gcs, just take and evict
  // to prevent OOM.
  size_t num_to_evict = 0;
  if (dropped_task_attempts_.size() > max_dropped_task_attempts_tracked_in_gcs) {
    num_to_evict =
        dropped_task_attempts_.size() - max_dropped_task_attempts_tracked_in_gcs;

    // Add 10% to mitigate thrashing.
    num_to_evict = std::min(dropped_task_attempts_.size(),
                            num_to_evict + static_cast<size_t>(0.1 * num_to_evict));
  }
  num_task_attempts_dropped_tracked_ = dropped_task_attempts_.size();
  if (num_to_evict == 0) {
    return;
  }

  // Evict ignoring timestamp.
  num_dropped_task_attempts_evicted_ += num_to_evict;
  dropped_task_attempts_.erase(dropped_task_attempts_.begin(),
                               std::next(dropped_task_attempts_.begin(), num_to_evict));
  num_task_attempts_dropped_tracked_ = dropped_task_attempts_.size();
}

}  // namespace gcs
}  // namespace ray
