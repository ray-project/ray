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

#include "ray/gcs_rpc_client/accessors/autoscaler_state_accessor_interface.h"
#include "ray/gcs_rpc_client/gcs_client_context.h"

namespace ray {
namespace gcs {

/// \class AutoscalerStateAccessor
/// Implementation of AutoscalerStateAccessorInterface.
class AutoscalerStateAccessor : public AutoscalerStateAccessorInterface {
 public:
  AutoscalerStateAccessor() = default;
  explicit AutoscalerStateAccessor(GcsClientContext *context);
  virtual ~AutoscalerStateAccessor() = default;

  virtual Status RequestClusterResourceConstraint(
      int64_t timeout_ms,
      const std::vector<std::unordered_map<std::string, double>> &bundles,
      const std::vector<int64_t> &count_array);

  virtual Status GetClusterResourceState(int64_t timeout_ms,
                                         std::string &serialized_reply);

  virtual Status GetClusterStatus(int64_t timeout_ms, std::string &serialized_reply);

  virtual void AsyncGetClusterStatus(
      int64_t timeout_ms,
      const OptionalItemCallback<rpc::autoscaler::GetClusterStatusReply> &callback);

  virtual Status ReportAutoscalingState(int64_t timeout_ms,
                                        const std::string &serialized_state);

  virtual Status ReportClusterConfig(int64_t timeout_ms,
                                     const std::string &serialized_cluster_config);

  virtual Status DrainNode(const std::string &node_id,
                           int32_t reason,
                           const std::string &reason_message,
                           int64_t deadline_timestamp_ms,
                           int64_t timeout_ms,
                           bool &is_accepted,
                           std::string &rejection_reason_message);

 private:
  GcsClientContext *context_;
};

}  // namespace gcs
}  // namespace ray
