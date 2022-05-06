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
namespace rpc {

class MockDefaultStatsHandler : public DefaultStatsHandler {
 public:
  MOCK_METHOD(void,
              HandleAddProfileData,
              (const AddProfileDataRequest &request,
               AddProfileDataReply *reply,
               SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetAllProfileInfo,
              (const rpc::GetAllProfileInfoRequest &request,
               rpc::GetAllProfileInfoReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
};

}  // namespace rpc
}  // namespace ray
