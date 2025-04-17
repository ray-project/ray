// Copyright 2017 The Ray Authors.
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

#include "ray/gcs/gcs_server/gcs_worker_manager.h"

#include "ray/stats/metric_defs.h"

namespace ray {
namespace gcs {

namespace {
bool IsIntentionalWorkerFailure(rpc::WorkerExitType exit_type) {
  return exit_type == rpc::WorkerExitType::INTENDED_USER_EXIT ||
         exit_type == rpc::WorkerExitType::INTENDED_SYSTEM_EXIT;
}
}  // namespace

void GcsWorkerManager::HandleReportWorkerFailure(
    rpc::ReportWorkerFailureRequest request,
    rpc::ReportWorkerFailureReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const rpc::Address worker_address = request.worker_failure().worker_address();
  const auto worker_id = WorkerID::FromBinary(worker_address.worker_id());
  GetWorkerInfo(
      worker_id,
      {[this,
        reply,
        send_reply_callback,
        worker_id = std::move(worker_id),
        request = std::move(request),
        worker_address = std::move(worker_address)](
           const std::optional<rpc::WorkerTableData> &result) {
         const auto node_id = NodeID::FromBinary(worker_address.raylet_id());
         std::string message =
             absl::StrCat("Reporting worker exit, worker id = ",
                          worker_id.Hex(),
                          ", node id = ",
                          node_id.Hex(),
                          ", address = ",
                          worker_address.ip_address(),
                          ", exit_type = ",
                          rpc::WorkerExitType_Name(request.worker_failure().exit_type()),
                          ", exit_detail = ",
                          request.worker_failure().exit_detail());
         if (IsIntentionalWorkerFailure(request.worker_failure().exit_type())) {
           RAY_LOG(DEBUG) << message;
         } else {
           RAY_LOG(WARNING)
               << message
               << ". Unintentional worker failures have been reported. If there "
                  "are lots of this logs, that might indicate there are "
                  "unexpected failures in the cluster.";
         }
         auto worker_failure_data = std::make_shared<rpc::WorkerTableData>();
         if (result) {
           worker_failure_data->CopyFrom(*result);
         }
         worker_failure_data->MergeFrom(request.worker_failure());
         worker_failure_data->set_is_alive(false);

         for (auto &listener : worker_dead_listeners_) {
           listener(worker_failure_data);
         }

         AddDeadWorkerToCache(worker_failure_data);

         auto on_done = [this,
                         worker_address,
                         worker_id,
                         node_id,
                         worker_failure_data,
                         reply,
                         send_reply_callback](const Status &status) {
           if (!status.ok()) {
             RAY_LOG(ERROR) << "Failed to report worker failure, worker id = "
                            << worker_id << ", node id = " << node_id
                            << ", address = " << worker_address.ip_address();
           } else {
             if (!IsIntentionalWorkerFailure(worker_failure_data->exit_type())) {
               stats::UnintentionalWorkerFailures.Record(1);
             }
             // Only publish worker_id and raylet_id in address as they are the only
             // fields used by sub clients.
             rpc::WorkerDeltaData worker_failure;
             worker_failure.set_worker_id(
                 worker_failure_data->worker_address().worker_id());
             worker_failure.set_raylet_id(
                 worker_failure_data->worker_address().raylet_id());
             RAY_CHECK_OK(
                 gcs_publisher_.PublishWorkerFailure(worker_id, worker_failure, nullptr));
           }
           GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
         };

         // As soon as the worker starts, it will register with GCS. It ensures that GCS
         // receives the worker registration information first and then the worker failure
         // message, so we delete the get operation. Related issues:
         // https://github.com/ray-project/ray/pull/11599
         Status status = gcs_table_storage_.WorkerTable().Put(
             worker_id, *worker_failure_data, {on_done, io_context_});
         if (!status.ok()) {
           on_done(status);
         }

         if (request.worker_failure().exit_type() == rpc::WorkerExitType::SYSTEM_ERROR ||
             request.worker_failure().exit_type() ==
                 rpc::WorkerExitType::NODE_OUT_OF_MEMORY) {
           usage::TagKey key;
           int count = 0;
           if (request.worker_failure().exit_type() ==
               rpc::WorkerExitType::SYSTEM_ERROR) {
             worker_crash_system_error_count_ += 1;
             key = usage::TagKey::WORKER_CRASH_SYSTEM_ERROR;
             count = worker_crash_system_error_count_;
           } else {
             RAY_CHECK_EQ(request.worker_failure().exit_type(),
                          rpc::WorkerExitType::NODE_OUT_OF_MEMORY);
             worker_crash_oom_count_ += 1;
             key = usage::TagKey::WORKER_CRASH_OOM;
             count = worker_crash_oom_count_;
           }
           if (usage_stats_client_) {
             usage_stats_client_->RecordExtraUsageCounter(key, count);
           }
         }
       },
       io_context_});
}

void GcsWorkerManager::HandleGetWorkerInfo(rpc::GetWorkerInfoRequest request,
                                           rpc::GetWorkerInfoReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  WorkerID worker_id = WorkerID::FromBinary(request.worker_id());
  RAY_LOG(DEBUG) << "Getting worker info, worker id = " << worker_id;

  GetWorkerInfo(worker_id,
                {[reply, send_reply_callback, worker_id = std::move(worker_id)](
                     const std::optional<rpc::WorkerTableData> &result) {
                   if (result) {
                     reply->mutable_worker_table_data()->CopyFrom(*result);
                   }
                   RAY_LOG(DEBUG)
                       << "Finished getting worker info, worker id = " << worker_id;
                   GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
                 },
                 io_context_});
}

void GcsWorkerManager::HandleGetAllWorkerInfo(
    rpc::GetAllWorkerInfoRequest request,
    rpc::GetAllWorkerInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  int64_t limit =
      request.has_limit() ? request.limit() : std::numeric_limits<int64_t>::max();

  RAY_LOG(DEBUG) << "Getting all worker info.";

  bool filter_exist_paused_threads = request.filters().exist_paused_threads();
  bool filter_is_alive = request.filters().is_alive();

  auto filter_fn = [filter_exist_paused_threads,
                    filter_is_alive](const rpc::WorkerTableData &worker_data) {
    if (filter_exist_paused_threads && worker_data.num_paused_threads() == 0) {
      return false;
    }
    if (filter_is_alive && !worker_data.is_alive()) {
      return false;
    }
    return true;
  };
  auto on_done = [reply, send_reply_callback, limit, filter_fn](
                     absl::flat_hash_map<WorkerID, rpc::WorkerTableData> &&result) {
    int64_t total_workers = result.size();
    reply->set_total(total_workers);

    int64_t num_added = 0;
    int64_t num_filtered = 0;

    for (auto &pair : result) {
      if (num_added >= limit) {
        break;
      }
      if (filter_fn(pair.second)) {
        reply->add_worker_table_data()->Swap(&pair.second);
        num_added += 1;
      } else {
        num_filtered += 1;
      }
    }
    reply->set_num_filtered(num_filtered);

    RAY_LOG(DEBUG) << "Finished getting all worker info.";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };
  Status status = gcs_table_storage_.WorkerTable().GetAll({on_done, io_context_});
  if (!status.ok()) {
    on_done(absl::flat_hash_map<WorkerID, rpc::WorkerTableData>());
  }
}

void GcsWorkerManager::HandleAddWorkerInfo(rpc::AddWorkerInfoRequest request,
                                           rpc::AddWorkerInfoReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  auto worker_data = std::make_shared<rpc::WorkerTableData>();
  worker_data->Swap(request.mutable_worker_data());
  auto worker_id = WorkerID::FromBinary(worker_data->worker_address().worker_id());
  RAY_LOG(DEBUG) << "Adding worker " << worker_id;

  auto on_done =
      [worker_id, worker_data, reply, send_reply_callback](const Status &status) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to add worker information, "
                         << worker_data->DebugString();
        }
        RAY_LOG(DEBUG) << "Finished adding worker " << worker_id;
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      };

  Status status = gcs_table_storage_.WorkerTable().Put(
      worker_id, *worker_data, {on_done, io_context_});
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsWorkerManager::HandleUpdateWorkerDebuggerPort(
    rpc::UpdateWorkerDebuggerPortRequest request,
    rpc::UpdateWorkerDebuggerPortReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto worker_id = WorkerID::FromBinary(request.worker_id());
  auto debugger_port = request.debugger_port();
  RAY_LOG(DEBUG) << "updating worker " << worker_id << "with debugger port "
                 << debugger_port;

  auto on_worker_update_done =
      [worker_id, debugger_port, reply, send_reply_callback](const Status &status) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to update debugger port on worker id " << worker_id
                         << "with value" << debugger_port;
        }
        RAY_LOG(DEBUG) << "Finished updating debugger port on worker " << worker_id;
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      };

  auto on_worker_get_done =
      [&, worker_id, reply, debugger_port, on_worker_update_done, send_reply_callback](
          const Status &status, const std::optional<rpc::WorkerTableData> &result) {
        if (!status.ok()) {
          RAY_LOG(WARNING) << "Failed to get worker info, worker id = " << worker_id
                           << ", status = " << status;
          GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
        } else {
          // Update the debugger port
          auto worker_data = std::make_shared<rpc::WorkerTableData>();
          worker_data->CopyFrom(*result);
          worker_data->set_debugger_port(debugger_port);
          Status status = gcs_table_storage_.WorkerTable().Put(
              worker_id, *worker_data, {on_worker_update_done, io_context_});
          if (!status.ok()) {
            GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
          }
        }
      };

  Status status =
      gcs_table_storage_.WorkerTable().Get(worker_id, {on_worker_get_done, io_context_});
  if (!status.ok()) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
}

void GcsWorkerManager::HandleUpdateWorkerNumPausedThreads(
    rpc::UpdateWorkerNumPausedThreadsRequest request,
    rpc::UpdateWorkerNumPausedThreadsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto worker_id = WorkerID::FromBinary(request.worker_id());
  auto num_paused_threads_delta = request.num_paused_threads_delta();
  RAY_LOG(DEBUG) << "updating worker " << worker_id << "with num_paused_threads_delta "
                 << num_paused_threads_delta;

  auto on_worker_update_done = [worker_id,
                                num_paused_threads_delta,
                                reply,
                                send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to update num_paused_threads_delta on worker id "
                     << worker_id << "with value" << num_paused_threads_delta;
    }
    RAY_LOG(DEBUG) << "Finished updating num_paused_threads_delta on worker "
                   << worker_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  auto on_worker_get_done = [&,
                             worker_id,
                             reply,
                             num_paused_threads_delta,
                             on_worker_update_done,
                             send_reply_callback](
                                const Status &status,
                                const std::optional<rpc::WorkerTableData> &result) {
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Failed to get worker info, worker id = " << worker_id
                       << ", status = " << status;
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    } else {
      // Update the num_paused_threads_delta
      auto worker_data = std::make_shared<rpc::WorkerTableData>();
      worker_data->CopyFrom(*result);
      auto current_num_paused_threads =
          worker_data->has_num_paused_threads() ? worker_data->num_paused_threads() : 0;
      worker_data->set_num_paused_threads(current_num_paused_threads +
                                          num_paused_threads_delta);
      Status status = gcs_table_storage_.WorkerTable().Put(
          worker_id, *worker_data, {on_worker_update_done, io_context_});
      if (!status.ok()) {
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      }
    }
  };

  Status status =
      gcs_table_storage_.WorkerTable().Get(worker_id, {on_worker_get_done, io_context_});
  if (!status.ok()) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
}

void GcsWorkerManager::AddWorkerDeadListener(
    std::function<void(std::shared_ptr<rpc::WorkerTableData>)> listener) {
  RAY_CHECK(listener != nullptr);
  worker_dead_listeners_.emplace_back(std::move(listener));
}

void GcsWorkerManager::GetWorkerInfo(
    const WorkerID &worker_id,
    Postable<void(std::optional<rpc::WorkerTableData>)> callback) const {
  RAY_CHECK_OK(gcs_table_storage_.WorkerTable().Get(
      worker_id,
      std::move(callback).TransformArg(
          [worker_id](Status status, std::optional<rpc::WorkerTableData> data) {
            if (!status.ok()) {
              RAY_LOG(WARNING) << "Failed to get worker info, worker id = " << worker_id
                               << ", status = " << status;
            }
            return data;
          })));
}

void GcsWorkerManager::Initialize(const GcsInitData &gcs_init_data) {
  for (auto &entry : gcs_init_data.Workers()) {
    if (!entry.second.is_alive()) {
      dead_workers_.emplace(entry.first,
                            std::make_shared<rpc::WorkerTableData>(entry.second));
      sorted_dead_worker_list_.emplace_back(entry.first, entry.second.timestamp());
    }
  }
  sorted_dead_worker_list_.sort([](const std::pair<WorkerID, int64_t> &left,
                                   const std::pair<WorkerID, int64_t> &right) {
    return left.second < right.second;
  });
}

void GcsWorkerManager::AddDeadWorkerToCache(
    const std::shared_ptr<rpc::WorkerTableData> &worker_data) {
  if (dead_workers_.size() >=
      RayConfig::instance().maximum_gcs_dead_worker_cached_count()) {
    EvictOneDeadWorker();
  }
  auto worker_id = WorkerID::FromBinary(worker_data->worker_address().worker_id());
  dead_workers_.emplace(worker_id, worker_data);
  sorted_dead_worker_list_.emplace_back(worker_id, worker_data->timestamp());
}

void GcsWorkerManager::EvictOneDeadWorker() {
  if (!sorted_dead_worker_list_.empty()) {
    auto iter = sorted_dead_worker_list_.begin();
    const auto &worker_id = iter->first;
    RAY_CHECK_OK(
        gcs_table_storage_.WorkerTable().Delete(worker_id, {[](auto) {}, io_context_}));
    dead_workers_.erase(worker_id);
    sorted_dead_worker_list_.erase(iter);
  }
}

void GcsWorkerManager::EvictExpiredWorkers() {
  RAY_LOG(INFO) << "Try evicting expired workers, there are "
                << sorted_dead_worker_list_.size() << " dead workers in the cache.";
  int evicted_worker_number = 0;

  size_t batch_size = RayConfig::instance().gcs_dead_data_max_batch_delete_size();
  std::vector<WorkerID> batch_ids;
  batch_ids.reserve(batch_size);

  std::vector<UniqueID> batch_worker_process_ids;
  batch_worker_process_ids.reserve(batch_size);

  auto current_time_ms = current_sys_time_ms();
  auto gcs_dead_worker_data_keep_duration_ms =
      RayConfig::instance().gcs_dead_worker_data_keep_duration_ms();
  while (!sorted_dead_worker_list_.empty()) {
    auto timestamp = sorted_dead_worker_list_.begin()->second;
    if (timestamp + gcs_dead_worker_data_keep_duration_ms > current_time_ms) {
      break;
    }

    auto iter = sorted_dead_worker_list_.begin();
    const auto &worker_id = iter->first;
    batch_ids.emplace_back(worker_id);

    auto dead_worker_iter = dead_workers_.find(worker_id);
    if (dead_worker_iter != dead_workers_.end()) {
      dead_workers_.erase(dead_worker_iter);
    }

    sorted_dead_worker_list_.erase(iter);
    ++evicted_worker_number;

    if (batch_ids.size() == batch_size) {
      RAY_CHECK_OK(gcs_table_storage_.WorkerTable().BatchDelete(
          batch_ids, {[](auto) {}, io_context_}));
      batch_ids.clear();
    }
  }

  if (!batch_ids.empty()) {
    RAY_CHECK_OK(gcs_table_storage_.WorkerTable().BatchDelete(
        batch_ids, {[](auto) {}, io_context_}));
  }

  RAY_LOG(INFO) << evicted_worker_number << " workers are evicted, there are still "
                << sorted_dead_worker_list_.size() << " dead workers in the cache.";
}

}  // namespace gcs
}  // namespace ray
