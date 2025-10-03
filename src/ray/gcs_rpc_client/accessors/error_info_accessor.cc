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

#include "ray/gcs_rpc_client/accessors/error_info_accessor.h"

#include "ray/gcs_rpc_client/rpc_client.h"

namespace ray {
namespace gcs {

ErrorInfoAccessor::ErrorInfoAccessor(GcsClientContext *context) : context_(context) {}

void ErrorInfoAccessor::AsyncReportJobError(rpc::ErrorTableData data) {
  auto job_id = JobID::FromBinary(data.job_id());
  RAY_LOG(DEBUG) << "Publishing job error, job id = " << job_id;
  rpc::ReportJobErrorRequest request;
  *request.mutable_job_error() = std::move(data);
  context_->GetGcsRpcClient().ReportJobError(
      std::move(request),
      [job_id](const Status &status, rpc::ReportJobErrorReply &&reply) {
        RAY_LOG(DEBUG) << "Finished publishing job error, job id = " << job_id;
      });
}

}  // namespace gcs
}  // namespace ray
