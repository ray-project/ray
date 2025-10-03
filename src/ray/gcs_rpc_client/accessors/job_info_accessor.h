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

#include "ray/gcs_rpc_client/accessors/job_info_accessor_interface.h"
#include "ray/gcs_rpc_client/gcs_client_context.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {
using FetchDataOperation = std::function<void(const StatusCallback &done)>;
using SubscribeOperation = std::function<Status(const StatusCallback &done)>;

/// \class JobInfoAccessor
/// Implementation of JobInfoAccessorInterface that accesses job information
/// in the GCS.
class JobInfoAccessor : public JobInfoAccessorInterface {
 public:
  JobInfoAccessor() = default;
  explicit JobInfoAccessor(GcsClientContext *context);
  virtual ~JobInfoAccessor() = default;

  void AsyncAdd(const std::shared_ptr<rpc::JobTableData> &data_ptr,
                const StatusCallback &callback) override;

  void AsyncMarkFinished(const JobID &job_id, const StatusCallback &callback) override;

  Status AsyncSubscribeAll(const SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
                           const StatusCallback &done) override;

  void AsyncGetAll(const std::optional<std::string> &job_or_submission_id,
                   bool skip_submission_job_info_field,
                   bool skip_is_running_tasks_field,
                   const MultiItemCallback<rpc::JobTableData> &callback,
                   int64_t timeout_ms) override;

  Status GetAll(const std::optional<std::string> &job_or_submission_id,
                bool skip_submission_job_info_field,
                bool skip_is_running_tasks_field,
                std::vector<rpc::JobTableData> &job_data_list,
                int64_t timeout_ms) override;

  void AsyncResubscribe() override;

  void AsyncGetNextJobID(const ItemCallback<JobID> &callback) override;

 private:
  // GCS client implementation.
  GcsClientContext *context_ = nullptr;

  /// Save the fetch data operation in this function, so we can call it again when GCS
  /// server restarts from a failure.
  FetchDataOperation fetch_all_data_operation_;

  /// Save the subscribe operation in this function, so we can call it again when PubSub
  /// server restarts from a failure.
  SubscribeOperation subscribe_operation_;
};

}  // namespace gcs
}  // namespace ray
