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

GcsTaskManager::GcsTaskManager() {
  io_service_thread_ = std::make_unique<std::thread>([this] {
    SetThreadName("task_events");
    // Keep io_service_ alive.
    boost::asio::io_service::work io_service_work_(io_service_);
    io_service_.run();
  });
}

void GcsTaskManager::Stop() {
  io_service_.stop();
  if (io_service_thread_->joinable()) {
    io_service_thread_->join();
  }
}

void GcsTaskManager::HandleAddTaskEventData(rpc::AddTaskEventDataRequest request,
                                            rpc::AddTaskEventDataReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Adding task state event:" << request.data().ShortDebugString();
  // Dispatch to the handler
  io_service_.post(
      [this, reply, send_reply_callback, request = std::move(request)]() {
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
        GCS_RPC_SEND_REPLY(send_reply_callback,
                           reply,
                           Status::UnknownError("Empty task event grpc data"));
      },
      "GcsTaskManager::HandleAddTaskEventData");
}

Status GcsTaskManager::AddTaskEventForTask(const TaskID &task_id,
                                           rpc::TaskEvents &&events_by_task) {
  absl::MutexLock lock(&mutex_);

  RAY_LOG_EVERY_MS(INFO, 30000)
      << ":GCS current stores " << task_events_.size()
      << " task event entries, approx size=" << 1.0 * num_bytes_task_events_ / 1024 / 1024
      << "MiB";

  // Try adding to the storage.
  auto task_event_itr = task_events_.find(task_id);
  auto max_num_events =
      static_cast<size_t>(RayConfig::instance().task_events_max_num_task_in_gcs());
  if (task_event_itr == task_events_.end()) {
    // A newly reported task events
    all_tasks_reported_.insert(task_id);

    if (max_num_events >= 0) {
      if (task_events_.size() >= max_num_events) {
        // Reached max events, do overriding.
        RAY_CHECK(next_idx_to_override_ < stored_task_ids_.size())
            << "Tracked indexing of stored task events is out of bound"
            << next_idx_to_override_ << " >= " << stored_task_ids_.size();
        auto evict_task_id = stored_task_ids_[next_idx_to_override_];
        task_events_.erase(evict_task_id);

        // Maintain the override indexing.
        stored_task_ids_[next_idx_to_override_] = task_id;
        next_idx_to_override_ = (next_idx_to_override_ + 1) % max_num_events;

        RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 1000)
            << "Max number of tasks event states(" << max_num_events
            << ") allowed reached. Dropping old task state events. Set "
               "`RAY_task_events_max_num_task_in_gcs` to a higher value to "
               "store more.";
      } else {
        // Not reached max number of events yet
        stored_task_ids_.push_back(task_id);
      }
    }

    num_bytes_task_events_ += events_by_task.ByteSizeLong();
    auto inserted = task_events_.emplace(task_id, std::move(events_by_task));
    if (!inserted.second) {
      RAY_LOG_EVERY_N_OR_DEBUG(WARNING, 100)
          << "Failed to add a new TasEvents to the storage.";
      return Status::UnknownError("Insert to map failed.");
    }
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
