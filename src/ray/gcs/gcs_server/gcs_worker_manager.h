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

#pragma once

#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

/// This implementation class of `WorkerInfoHandler`.
class GcsWorkerManager : public rpc::WorkerInfoHandler {
 public:
  explicit GcsWorkerManager(std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
                            std::shared_ptr<GcsPublisher> &gcs_publisher,
                            std::shared_ptr<InternalKVInterface> &kv_instance)
      : gcs_table_storage_(gcs_table_storage),
        gcs_publisher_(gcs_publisher),
        kv_instance_(kv_instance) {}

  void HandleReportWorkerFailure(rpc::ReportWorkerFailureRequest request,
                                 rpc::ReportWorkerFailureReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetWorkerInfo(rpc::GetWorkerInfoRequest request,
                           rpc::GetWorkerInfoReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllWorkerInfo(rpc::GetAllWorkerInfoRequest request,
                              rpc::GetAllWorkerInfoReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  void HandleAddWorkerInfo(rpc::AddWorkerInfoRequest request,
                           rpc::AddWorkerInfoReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void AddWorkerDeadListener(
      std::function<void(std::shared_ptr<WorkerTableData>)> listener);

 private:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<GcsPublisher> gcs_publisher_;
  std::shared_ptr<InternalKVInterface> kv_instance_;
  std::vector<std::function<void(std::shared_ptr<WorkerTableData>)>>
      worker_dead_listeners_;

  /// Tracks the number of occurences of worker crash due to system error
  int32_t worker_crash_system_error_count_ = 0;

  /// Tracks the number of occurences of worker crash due to OOM
  int32_t worker_crash_oom_count_ = 0;
};

}  // namespace gcs
}  // namespace ray
