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

void GcsTaskManager::HandleAddTaskStateEventData(
    rpc::AddTaskStateEventDataRequest request,
    rpc::AddTaskStateEventDataReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto node_id = request.data().node_id();
  RAY_LOG(INFO) << "Adding events of tasks from node [node_id=" << node_id << "]";
  RAY_LOG(INFO) << request.data().DebugString();
  size_t num_to_process = request.data().events_by_task_size();
  if (num_to_process == 0) {
    RAY_LOG(WARNING) << "Missing task events in task state event request: "
                     << request.DebugString();

    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    return;
  }

  // Context to keep track async operations across events from multiple tasks.
  // In fact, since the current underlying event loop on GCS table is single-threaded, non
  // atomic fields should also be fine.
  auto num_success = std::make_shared<std::atomic<uint>>(0);
  auto num_failure = std::make_shared<std::atomic<uint>>(0);

  auto cb_on_done =
      [this, num_to_process, num_success, num_failure, send_reply_callback, reply](
          const Status &status, const TaskID &task_id) {
        RAY_LOG(INFO) << "in done callback: " << status << ", task_id=" << task_id.Hex();
        if (!status.ok()) {
          ++(*num_failure);
          RAY_LOG(WARNING) << "Failed to add task state events for task. [task_id="
                           << task_id.Hex() << "][status=" << status.ToString() << "].";
        } else {
          ++(*num_success);
        }
        RAY_CHECK(*num_success + *num_failure <= num_to_process)
            << "Processed more task events than available. Too many callbacks called.";

        // Processed all the task events
        if (*num_success + *num_failure == num_to_process) {
          RAY_LOG(INFO) << "Processed all " << num_to_process
                        << " task state events, failed=" << *num_failure
                        << ",success=" << *num_success;
          GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
        }

        RAY_LOG(INFO) << "Processed a task event. [task_id=" << task_id.Hex() << "]";
      };

  AddTaskStateEvents(std::move(*request.release_data()), std::move(cb_on_done));
}

void GcsTaskManager::HandleGetAllTaskStateEvent(
    rpc::GetAllTaskStateEventRequest request,
    rpc::GetAllTaskStateEventReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all task state events.";
  auto on_done = [reply, send_reply_callback](
                     const absl::flat_hash_map<TaskID, rpc::TaskStateEvents> &result) {
    for (const auto &data : result) {
      RAY_LOG(INFO) << data.second.DebugString();
      reply->add_events_by_task()->CopyFrom(data.second);
    }
    reply->set_total(result.size());
    RAY_LOG(DEBUG) << "Finished getting all task states info.";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };

  Status status = gcs_table_storage_->TaskStateEventTable().GetAll(on_done);
  if (!status.ok()) {
    on_done(absl::flat_hash_map<TaskID, rpc::TaskStateEvents>());
  }
}

void GcsTaskManager::AddTaskStateEventForTask(const TaskID &task_id,
                                              rpc::TaskStateEvents &&events_by_task,
                                              AddTaskStateEventCallback cb_on_done) {
  RAY_CHECK(cb_on_done) << "AddTaskStateEventCallback callback is empty";

  RAY_LOG(INFO) << "events by task:" << events_by_task.DebugString();
  // Callback on async get done
  auto cb_on_get_done =
      [this, task_id, cb_on_done, events_by_task = std::move(events_by_task)](
          Status status, const boost::optional<rpc::TaskStateEvents> &result) {
        // Failed to get the entry
        if (!status.ok()) {
          cb_on_done(status, task_id);
          return;
        }

        // Merge events
        rpc::TaskStateEvents empty_task_state_events;
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
        auto put_status = gcs_table_storage_->TaskStateEventTable().Put(
            task_id, std::move(cur_task_state_events), cb_on_put_done);

        if (!put_status.ok()) {
          cb_on_done(put_status, task_id);
        }
      };

  // Get the current task state events and update it.
  auto get_status =
      gcs_table_storage_->TaskStateEventTable().Get(task_id, cb_on_get_done);
  if (!get_status.ok()) {
    cb_on_done(get_status, task_id);
  }
}

void GcsTaskManager::AddTaskStateEvents(rpc::TaskStateEventData &&data,
                                        AddTaskStateEventCallback cb_on_done) {
  // Update each task
  for (auto &events_by_task : *data.mutable_events_by_task()) {
    auto task_id = TaskID::FromBinary(events_by_task.task_id());
    AddTaskStateEventForTask(task_id, std::move(events_by_task), cb_on_done);
  }
}

}  // namespace gcs
}  // namespace ray
