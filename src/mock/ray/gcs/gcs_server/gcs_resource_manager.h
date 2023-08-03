// Copyright  The Ray Authors.
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

#include "ray/common/asio/instrumented_io_context.h"

namespace ray {
namespace gcs {
static instrumented_io_context __mock_io_context_;
static ClusterResourceManager __mock_cluster_resource_manager_(__mock_io_context_);
class MockGcsResourceManager : public GcsResourceManager {
 public:
  using GcsResourceManager::GcsResourceManager;
  explicit MockGcsResourceManager()
      : GcsResourceManager(__mock_io_context_,
                           __mock_cluster_resource_manager_,
                           NodeID::FromRandom(),
                           nullptr) {}
  explicit MockGcsResourceManager(ClusterResourceManager &cluster_resource_manager)
      : GcsResourceManager(
            __mock_io_context_, cluster_resource_manager, NodeID::FromRandom(), nullptr) {
  }

  MOCK_METHOD(void,
              HandleGetResources,
              (rpc::GetResourcesRequest request,
               rpc::GetResourcesReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetAllAvailableResources,
              (rpc::GetAllAvailableResourcesRequest request,
               rpc::GetAllAvailableResourcesReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleReportResourceUsage,
              (rpc::ReportResourceUsageRequest request,
               rpc::ReportResourceUsageReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetAllResourceUsage,
              (rpc::GetAllResourceUsageRequest request,
               rpc::GetAllResourceUsageReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
};

}  // namespace gcs
}  // namespace ray
