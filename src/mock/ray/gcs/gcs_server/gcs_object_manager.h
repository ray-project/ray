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

class MockGcsObjectManager : public GcsObjectManager {
 public:
  MOCK_METHOD(void, HandleGetObjectLocations,
              (const rpc::GetObjectLocationsRequest &request,
               rpc::GetObjectLocationsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleGetAllObjectLocations,
              (const rpc::GetAllObjectLocationsRequest &request,
               rpc::GetAllObjectLocationsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleAddObjectLocation,
              (const rpc::AddObjectLocationRequest &request,
               rpc::AddObjectLocationReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleRemoveObjectLocation,
              (const rpc::RemoveObjectLocationRequest &request,
               rpc::RemoveObjectLocationReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
};

}  // namespace gcs
}  // namespace ray
