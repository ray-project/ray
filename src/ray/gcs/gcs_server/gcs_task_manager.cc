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
  // Update each task.
  reply->set_num_failure(0);
  reply->set_num_success(0);

  auto data = request.data();
  for (auto &events_by_task : *data.mutable_events_by_task()) {
    auto task_id = TaskID::FromBinary(events_by_task.task_id());
    auto status = AddTaskEventForTask(task_id, std::move(events_by_task));

    if (!status.ok()) {
      reply->set_num_failure(reply->num_failure() + 1);
      RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
          << "Failed to add task state events for task. [task_id=" << task_id.Hex()
          << "][status=" << status.ToString() << "].";
    } else {
      reply->set_num_success(reply->num_success() + 1);
    }
    RAY_CHECK(reply->num_success() + reply->num_failure() <= num_to_process)
        << "Processed more task events than available. Too many callbacks called.";

    // Processed all the task events
    if (reply->num_success() + reply->num_failure() == num_to_process) {
      RAY_LOG(DEBUG) << "Processed all " << num_to_process
                     << " task state events, failed=" << reply->num_failure()
                     << ",success=" << reply->num_success();
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      return;
    }

    RAY_LOG(DEBUG) << "Processed a task event. [task_id=" << task_id.Hex() << "]";
  }
  RAY_LOG(WARNING) << "Empty task events data, num_to_process=" << num_to_process;
  GCS_RPC_SEND_REPLY(
      send_reply_callback, reply, Status::UnknownError("Empty task event grpc data"));
}

void GcsTaskManager::HandleGetAllTaskEvent(rpc::GetAllTaskEventRequest request,
                                           rpc::GetAllTaskEventReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all task state events: " << request.ShortDebugString();
  auto limit = request.has_limit() ? request.limit() : -1;
  int count = 0;
  {
    absl::MutexLock lock(&mutex_);
    for (const auto &[_task_id, data] : task_events_) {
      if (limit >= 0 && count++ >= limit) {
        break;
      }
      reply->add_events_by_task()->CopyFrom(data);
    }
    reply->set_total(tasks_reported_.size());
  }

  RAY_LOG(DEBUG) << "Finished getting all task states info, with "
                 << reply->ShortDebugString();
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

Status GcsTaskManager::AddTaskEventForTask(const TaskID &task_id,
                                           rpc::TaskEvents &&events_by_task) {
  absl::MutexLock lock(&mutex_);

  RAY_LOG_EVERY_MS(INFO, 30000)
      << "GCS current stores " << task_events_.size()
      << " task event entries, approx size=" << 1.0 * num_bytes_task_events_ / 1024 / 1024
      << "MiB";

  // Try adding to the storage.
  auto task_event_itr = task_events_.find(task_id);
  auto max_num_events = RayConfig::instance().task_events_max_num_task_in_gcs();
  if (task_event_itr == task_events_.end()) {
    if (max_num_events >= 0 &&
        task_events_.size() >= static_cast<size_t>(max_num_events)) {
      // TODO(rickyx): We might want to evict those events with some semantics, e.g.
      // finished events, oldest events.
      RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 1000)
          << "Max number of tasks event states(" << max_num_events
          << ") allowed reached. Dropping task state events. Set "
             "`RAY_task_events_max_num_task_in_gcs` to a higher value to "
             "store "
             "more.";
      return Status::ResourceExhausted("Max number of task events in GCS reached");
    }

    num_bytes_task_events_ += events_by_task.ByteSizeLong();
    auto inserted = task_events_.emplace(task_id, std::move(events_by_task));
    if (!inserted.second) {
      RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
          << "Failed to add a new TasEvents to the storage.";
      return Status::UnknownError("Insert to map failed.");
    }
    tasks_reported_.insert(task_id);
    return Status::OK();
  }

  RAY_CHECK(task_event_itr != task_events_.end())
      << "Should have a valid iterator to the events map.";

  num_bytes_task_events_ += events_by_task.ByteSizeLong();
  // Merge the events with an existing one.
  task_event_itr->second.MergeFrom(events_by_task);
  return Status::OK();
}
}  // namespace gcs
}  // namespace ray
