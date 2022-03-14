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

class MockGcsJobManager : public GcsJobManager {
 public:
  MOCK_METHOD(void,
              HandleAddJob,
              (const rpc::AddJobRequest &request,
               rpc::AddJobReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleMarkJobFinished,
              (const rpc::MarkJobFinishedRequest &request,
               rpc::MarkJobFinishedReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetAllJobInfo,
              (const rpc::GetAllJobInfoRequest &request,
               rpc::GetAllJobInfoReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleReportJobError,
              (const rpc::ReportJobErrorRequest &request,
               rpc::ReportJobErrorReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetNextJobID,
              (const rpc::GetNextJobIDRequest &request,
               rpc::GetNextJobIDReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              AddJobFinishedListener,
              (std::function<void(std::shared_ptr<JobID>)> listener),
              (override));
};

}  // namespace gcs
}  // namespace ray
