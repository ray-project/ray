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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "ray/common/gcs_callback_types.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// \class JobInfoAccessorInterface
/// Interface for accessing job information in the GCS.
class JobInfoAccessorInterface {
 public:
  virtual ~JobInfoAccessorInterface() = default;

  /// Add a job to GCS asynchronously.
  ///
  /// \param data_ptr The job that will be add to GCS.
  /// \param callback Callback that will be called after job has been added
  /// to GCS.
  virtual void AsyncAdd(const std::shared_ptr<rpc::JobTableData> &data_ptr,
                        const StatusCallback &callback) = 0;

  /// Mark job as finished in GCS asynchronously.
  ///
  /// \param job_id ID of the job that will be make finished to GCS.
  /// \param callback Callback that will be called after update finished.
  virtual void AsyncMarkFinished(const JobID &job_id, const StatusCallback &callback) = 0;

  /// Subscribe to job updates.
  ///
  /// \param subscribe Callback that will be called each time when a job updates.
  /// \param done Callback that will be called when subscription is complete.
  /// \return Status
  virtual Status AsyncSubscribeAll(
      const SubscribeCallback<JobID, rpc::JobTableData> &subscribe,
      const StatusCallback &done) = 0;

  /// Get all job info from GCS asynchronously.
  ///
  /// \param job_or_submission_id If not null, filter the jobs with this id.
  /// \param skip_submission_job_info_field Skip submission job info field.
  /// \param skip_is_running_tasks_field Skip is_running_tasks field.
  /// \param callback Callback that will be called after lookup finished.
  /// \param timeout_ms Timeout in milliseconds.
  virtual void AsyncGetAll(const std::optional<std::string> &job_or_submission_id,
                           bool skip_submission_job_info_field,
                           bool skip_is_running_tasks_field,
                           const MultiItemCallback<rpc::JobTableData> &callback,
                           int64_t timeout_ms) = 0;

  /// Get all job info from GCS synchronously.
  ///
  /// \param job_or_submission_id If not null, filter the jobs with this id.
  /// \param skip_submission_job_info_field Skip submission job info field.
  /// \param skip_is_running_tasks_field Skip is_running_tasks field.
  /// \param[out] job_data_list The list of job data retrieved from GCS.
  /// \param timeout_ms -1 means infinite.
  /// \return Status
  virtual Status GetAll(const std::optional<std::string> &job_or_submission_id,
                        bool skip_submission_job_info_field,
                        bool skip_is_running_tasks_field,
                        std::vector<rpc::JobTableData> &job_data_list,
                        int64_t timeout_ms) = 0;

  /// Reestablish subscription.
  /// This should be called when GCS server restarts from a failure.
  virtual void AsyncResubscribe() = 0;

  /// Increment and get next job id. This is not idempotent.
  ///
  /// \param callback Callback that will be called when request successfully.
  virtual void AsyncGetNextJobID(const ItemCallback<JobID> &callback) = 0;
};

}  // namespace gcs
}  // namespace ray
