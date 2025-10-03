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

#include "ray/gcs_rpc_client/accessors/worker_info_accessor.h"

#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/pubsub/gcs_subscriber.h"
#include "ray/util/container_util.h"

namespace ray {
namespace gcs {

WorkerInfoAccessor::WorkerInfoAccessor(GcsClientContext *context) : context_(context) {}

Status WorkerInfoAccessor::AsyncSubscribeToWorkerFailures(
    const ItemCallback<rpc::WorkerDeltaData> &subscribe, const StatusCallback &done) {
  RAY_CHECK(subscribe != nullptr);
  subscribe_operation_ = [this, subscribe](const StatusCallback &done_callback) {
    return context_->GetGcsSubscriber().SubscribeAllWorkerFailures(subscribe,
                                                                   done_callback);
  };
  return subscribe_operation_(done);
}

void WorkerInfoAccessor::AsyncResubscribe() {
  // TODO(iycheng): Fix the case where messages has been pushed to GCS but
  // resubscribe hasn't been done yet. In this case, we'll lose that message.
  RAY_LOG(DEBUG) << "Reestablishing subscription for worker failures.";
  // The pub-sub server has restarted, we need to resubscribe to the pub-sub server.
  if (subscribe_operation_ != nullptr) {
    RAY_CHECK_OK(subscribe_operation_(nullptr));
  }
}

void WorkerInfoAccessor::AsyncReportWorkerFailure(
    const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
    const StatusCallback &callback) {
  rpc::Address worker_address = data_ptr->worker_address();
  RAY_LOG(DEBUG) << "Reporting worker failure, " << worker_address.DebugString();
  rpc::ReportWorkerFailureRequest request;
  request.mutable_worker_failure()->CopyFrom(*data_ptr);
  context_->GetGcsRpcClient().ReportWorkerFailure(
      std::move(request),
      [worker_address, callback](const Status &status,
                                 rpc::ReportWorkerFailureReply &&reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG) << "Finished reporting worker failure, "
                       << worker_address.DebugString() << ", status = " << status;
      });
}

void WorkerInfoAccessor::AsyncGet(
    const WorkerID &worker_id,
    const OptionalItemCallback<rpc::WorkerTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting worker info, worker id = " << worker_id;
  rpc::GetWorkerInfoRequest request;
  request.set_worker_id(worker_id.Binary());
  context_->GetGcsRpcClient().GetWorkerInfo(
      std::move(request),
      [worker_id, callback](const Status &status, rpc::GetWorkerInfoReply &&reply) {
        if (reply.has_worker_table_data()) {
          callback(status, reply.worker_table_data());
        } else {
          callback(status, std::nullopt);
        }
        RAY_LOG(DEBUG) << "Finished getting worker info, worker id = " << worker_id;
      });
}

void WorkerInfoAccessor::AsyncGetAll(
    const MultiItemCallback<rpc::WorkerTableData> &callback) {
  RAY_LOG(DEBUG) << "Getting all worker info.";
  rpc::GetAllWorkerInfoRequest request;
  context_->GetGcsRpcClient().GetAllWorkerInfo(
      std::move(request),
      [callback](const Status &status, rpc::GetAllWorkerInfoReply &&reply) {
        callback(status,
                 VectorFromProtobuf(std::move(*reply.mutable_worker_table_data())));
        RAY_LOG(DEBUG) << "Finished getting all worker info, status = " << status;
      });
}

void WorkerInfoAccessor::AsyncAdd(const std::shared_ptr<rpc::WorkerTableData> &data_ptr,
                                  const StatusCallback &callback) {
  rpc::AddWorkerInfoRequest request;
  request.mutable_worker_data()->CopyFrom(*data_ptr);
  context_->GetGcsRpcClient().AddWorkerInfo(
      std::move(request),
      [callback](const Status &status, rpc::AddWorkerInfoReply &&reply) {
        if (callback) {
          callback(status);
        }
      });
}

void WorkerInfoAccessor::AsyncUpdateDebuggerPort(const WorkerID &worker_id,
                                                 uint32_t debugger_port,
                                                 const StatusCallback &callback) {
  rpc::UpdateWorkerDebuggerPortRequest request;
  request.set_worker_id(worker_id.Binary());
  request.set_debugger_port(debugger_port);
  RAY_LOG(DEBUG) << "Updating the worker debugger port, worker id = " << worker_id
                 << ", port = " << debugger_port << ".";
  context_->GetGcsRpcClient().UpdateWorkerDebuggerPort(
      std::move(request),
      [callback](const Status &status, rpc::UpdateWorkerDebuggerPortReply &&reply) {
        if (callback) {
          callback(status);
        }
      });
}

void WorkerInfoAccessor::AsyncUpdateWorkerNumPausedThreads(
    const WorkerID &worker_id,
    int num_paused_threads_delta,
    const StatusCallback &callback) {
  rpc::UpdateWorkerNumPausedThreadsRequest request;
  request.set_worker_id(worker_id.Binary());
  request.set_num_paused_threads_delta(num_paused_threads_delta);
  RAY_LOG(DEBUG).WithField(worker_id)
      << "Update the num paused threads by delta = " << num_paused_threads_delta << ".";
  context_->GetGcsRpcClient().UpdateWorkerNumPausedThreads(
      std::move(request),
      [callback](const Status &status, rpc::UpdateWorkerNumPausedThreadsReply &&reply) {
        if (callback) {
          callback(status);
        }
      });
}

}  // namespace gcs
}  // namespace ray
