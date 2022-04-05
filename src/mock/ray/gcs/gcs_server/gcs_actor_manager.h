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

class MockGcsActor : public GcsActor {
 public:
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockGcsActorManager : public GcsActorManager {
 public:
  MOCK_METHOD(void,
              HandleRegisterActor,
              (const rpc::RegisterActorRequest &request,
               rpc::RegisterActorReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleCreateActor,
              (const rpc::CreateActorRequest &request,
               rpc::CreateActorReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetActorInfo,
              (const rpc::GetActorInfoRequest &request,
               rpc::GetActorInfoReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetNamedActorInfo,
              (const rpc::GetNamedActorInfoRequest &request,
               rpc::GetNamedActorInfoReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleListNamedActors,
              (const rpc::ListNamedActorsRequest &request,
               rpc::ListNamedActorsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleGetAllActorInfo,
              (const rpc::GetAllActorInfoRequest &request,
               rpc::GetAllActorInfoReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void,
              HandleKillActorViaGcs,
              (const rpc::KillActorViaGcsRequest &request,
               rpc::KillActorViaGcsReply *reply,
               rpc::SendReplyCallback send_reply_callback),
              (override));
};

}  // namespace gcs
}  // namespace ray
