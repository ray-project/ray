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

void GcsTaskManager::HandleAddTaskEventData(rpc::AddTaskEventDataRequest request,
                                            rpc::AddTaskEventDataReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Adding task state event:" << request.data().ShortDebugString();

  size_t num_to_process = request.data().events_by_task_size();

  // Context to keep track async operations across events from multiple tasks.
  // In fact, since the current underlying event loop on GCS table is single-threaded, non
  // atomic fields should also be fine.
  auto num_success = std::make_shared<std::atomic<uint>>(0);
  auto num_failure = std::make_shared<std::atomic<uint>>(0);

  auto cb_on_done =
      [this, num_to_process, num_success, num_failure, send_reply_callback, reply](
          const Status &status, const TaskID &task_id) {
        if (!status.ok()) {
          ++(*num_failure);
          RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
              << "Failed to add task state events for task. [task_id=" << task_id.Hex()
              << "][status=" << status.ToString() << "].";
        } else {
          ++(*num_success);
        }
        RAY_CHECK(*num_success + *num_failure <= num_to_process)
            << "Processed more task events than available. Too many callbacks called.";

        // Processed all the task events
        if (*num_success + *num_failure == num_to_process) {
          RAY_LOG(DEBUG) << "Processed all " << num_to_process
                         << " task state events, failed=" << *num_failure
                         << ",success=" << *num_success;
          GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
        }

        RAY_LOG(DEBUG) << "Processed a task event. [task_id=" << task_id.Hex() << "]";
      };

  AddTaskEvents(std::move(*request.release_data()), std::move(cb_on_done));
}

void GcsTaskManager::HandleGetAllTaskEvent(rpc::GetAllTaskEventRequest request,
                                           rpc::GetAllTaskEventReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all task state events: " << request.ShortDebugString();
  auto limit = request.has_limit() ? request.limit() : -1;

  auto on_done = [this, reply, limit, send_reply_callback](
                     const absl::flat_hash_map<TaskID, rpc::TaskEvents> &result) {
    absl::MutexLock lock(&mutex_);
    int count = 0;
    for (const auto &data : result) {
      if (limit >= 0 && count++ >= limit) {
        break;
      }
      reply->add_events_by_task()->CopyFrom(data.second);
    }
    reply->set_total(tasks_reported_.size());
    RAY_LOG(DEBUG) << "Finished getting all task states info, with "
                   << reply->ShortDebugString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };

  Status status = gcs_table_storage_->TaskEventTable().GetAll(on_done);
  if (!status.ok()) {
    on_done(absl::flat_hash_map<TaskID, rpc::TaskEvents>());
  }
}

void GcsTaskManager::AddTaskEventForTask(const TaskID &task_id,
                                         rpc::TaskEvents &&events_by_task,
                                         AddTaskEventCallback cb_on_done) {
  RAY_CHECK(cb_on_done) << "AddTaskEventCallback callback should not be empty";
  // Callback on async get done
  auto cb_on_get_done = [this,
                         task_id,
                         cb_on_done,
                         events_by_task = std::move(events_by_task)](
                            Status status,
                            const boost::optional<rpc::TaskEvents> &result) {
    absl::MutexLock lock(&mutex_);
    // Failed to get the entry
    if (!status.ok()) {
      cb_on_done(status, task_id);
      return;
    }

    if (!result.has_value()) {
      // Adding another new task state event entry in GCS.
      tasks_reported_.insert(task_id);

      // TODO(rickyx): We might want to adopt a better throttling strategy, i.e.
      // spilling data to disk. But for now, dropping events if there are too many in GCS.
      if (tasks_reported_.size() >
          RayConfig::instance().task_state_events_max_num_task_in_gcs()) {
        RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 1000)
            << "Max number of tasks event states("
            << RayConfig::instance().task_state_events_max_num_task_in_gcs()
            << ") allowed reached. Dropping task state events. Set "
               "`RAY_task_state_events_max_num_task_in_gcs` to a higher value to store "
               "more.";
        cb_on_done(Status::OK(), task_id);
        return;
      }
    }

    // Merge events of a single task.
    rpc::TaskEvents empty_task_state_events;
    auto cur_task_state_events = result.value_or(empty_task_state_events);

    cur_task_state_events.MergeFrom(events_by_task);

    // Callback on async put done
    auto cb_on_put_done = [task_id, cb_on_done](Status status) {
      // Fail callback if put failed, and invoke succeed callback to indicate entire
      // sequence of operations succeed.
      cb_on_done(status, task_id);
    };

    // Overwrite the current task state event in the GCS table
    // TODO(rickyx): We could do an in-placed mutation actually if we send in-place
    // mutation callback to the underlying gcs storage table.
    auto put_status = gcs_table_storage_->TaskEventTable().Put(
        task_id, std::move(cur_task_state_events), cb_on_put_done);

    if (!put_status.ok()) {
      cb_on_done(put_status, task_id);
    }
  };

  // Get the current task state events and update it.
  auto get_status = gcs_table_storage_->TaskEventTable().Get(task_id, cb_on_get_done);
  if (!get_status.ok()) {
    cb_on_done(get_status, task_id);
  }
}

void GcsTaskManager::AddTaskEvents(rpc::TaskEventData &&data,
                                   AddTaskEventCallback cb_on_done) {
  // Update each task.
  for (auto &events_by_task : *data.mutable_events_by_task()) {
    auto task_id = TaskID::FromBinary(events_by_task.task_id());
    AddTaskEventForTask(task_id, std::move(events_by_task), cb_on_done);
  }
}

}  // namespace gcs
}  // namespace ray
