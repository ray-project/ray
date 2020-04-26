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

#include "worker_info_handler_impl.h"

namespace ray {
namespace rpc {

void DefaultWorkerInfoHandler::HandleReportWorkerFailure(
    const ReportWorkerFailureRequest &request, ReportWorkerFailureReply *reply,
    SendReplyCallback send_reply_callback) {
  Address worker_address = request.worker_failure().worker_address();
  RAY_LOG(DEBUG) << "Reporting worker failure, " << worker_address.DebugString();
  auto worker_failure_data = std::make_shared<WorkerFailureData>();
  worker_failure_data->CopyFrom(request.worker_failure());
  auto need_reschedule = !worker_failure_data->intentional_disconnect();
  auto node_id = ClientID::FromBinary(worker_address.raylet_id());
  auto worker_id = WorkerID::FromBinary(worker_address.worker_id());
  gcs_actor_manager_.ReconstructActorOnWorker(node_id, worker_id, need_reschedule);

  auto on_done = [this, worker_address, worker_id, worker_failure_data, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to report worker failure, "
                     << worker_address.DebugString();
    } else {
      RAY_CHECK_OK(gcs_pub_sub_->Publish(WORKER_FAILURE_CHANNEL, worker_id.Binary(),
                                         worker_failure_data->SerializeAsString(),
                                         nullptr));
      RAY_LOG(DEBUG) << "Finished reporting worker failure, "
                     << worker_address.DebugString();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status =
      gcs_client_.Workers().AsyncReportWorkerFailure(worker_failure_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultWorkerInfoHandler::HandleRegisterWorker(
    const RegisterWorkerRequest &request, RegisterWorkerReply *reply,
    SendReplyCallback send_reply_callback) {
  auto worker_type = request.worker_type();
  auto worker_id = WorkerID::FromBinary(request.worker_id());
  auto worker_info = MapFromProtobuf(request.worker_info());

  auto on_done = [worker_id, reply, send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to register worker " << worker_id;
    } else {
      RAY_LOG(DEBUG) << "Finished registering worker " << worker_id;
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };

  Status status = gcs_client_.Workers().AsyncRegisterWorker(worker_type, worker_id,
                                                            worker_info, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

}  // namespace rpc
}  // namespace ray
