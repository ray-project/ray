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

namespace ray {
namespace gcs {

class MockGcsResourceManager : public GcsResourceManager {
 public:
  using GcsResourceManager::GcsResourceManager;
  MOCK_METHOD(void,
              HandleGetResources,
              (const rpc::GetResourcesRequest &request,
               rpc::GetResourcesReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetAllAvailableResources,
              (const rpc::GetAllAvailableResourcesRequest &request,
               rpc::GetAllAvailableResourcesReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleReportResourceUsage,
              (const rpc::ReportResourceUsageRequest &request,
               rpc::ReportResourceUsageReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetAllResourceUsage,
              (const rpc::GetAllResourceUsageRequest &request,
               rpc::GetAllResourceUsageReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
};

}  // namespace gcs
}  // namespace ray
