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

        total_num_task_events_dropped_ += request.data().num_task_events_dropped();

        auto data = request.data();
        for (auto &events_by_task : *data.mutable_events_by_task()) {
          auto task_id = TaskID::FromBinary(events_by_task.task_id());
          AddTaskEventForTask(std::move(events_by_task));
          RAY_LOG(DEBUG) << "Processed a task event. [task_id=" << task_id.Hex() << "]";
        }

        // Processed all the task events
        RAY_LOG(DEBUG) << "Processed all " << num_to_process << " task events";
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      },
      "GcsTaskManager::HandleAddTaskEventData");
}

void GcsTaskManager::AddTaskEventForTask(rpc::TaskEvents &&events_by_task) {
  // TODO(rickyx): revert this interval
  RAY_LOG_EVERY_MS(INFO, 5000) << "GCS currently stores " << task_events_.size()
                               << " task event entries, approximate size="
                               << 1.0 * num_bytes_task_events_ / 1024 / 1024 << "MiB"
                               << ", total number of task events dropped on worker="
                               << total_num_task_events_dropped_
                               << ", total number of task events reported="
                               << total_num_task_events_reported_;
  // Add to storage.
  auto max_num_events =
      static_cast<size_t>(RayConfig::instance().task_events_max_num_task_in_gcs());

  total_num_task_events_reported_++;
  num_bytes_task_events_ += events_by_task.ByteSizeLong();

  // If limit enforced.
  if (max_num_events > 0 && task_events_.size() >= max_num_events) {
    num_bytes_task_events_ -= task_events_[next_idx_to_overwrite_].ByteSizeLong();
    task_events_[next_idx_to_overwrite_] = events_by_task;

    next_idx_to_overwrite_ = (next_idx_to_overwrite_ + 1) % max_num_events;
    RAY_LOG_EVERY_MS(WARNING, 10000)
        << "Max number of tasks event (" << max_num_events
        << ") allowed is reached. Old task events will be overwritten. Set "
           "`RAY_task_events_max_num_task_in_gcs` to a higher value to "
           "store more.";
    return;
  }

  num_bytes_task_events_ += events_by_task.ByteSizeLong();
  task_events_.push_back(events_by_task);
}

}  // namespace gcs
}  // namespace ray
