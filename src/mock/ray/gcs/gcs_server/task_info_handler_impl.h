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

class MockDefaultTaskInfoHandler : public DefaultTaskInfoHandler {
 public:
  MOCK_METHOD(void, HandleAddTask,
              (const AddTaskRequest &request, AddTaskReply *reply,
               SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleGetTask,
              (const GetTaskRequest &request, GetTaskReply *reply,
               SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleAddTaskLease,
              (const AddTaskLeaseRequest &request, AddTaskLeaseReply *reply,
               SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleGetTaskLease,
              (const GetTaskLeaseRequest &request, GetTaskLeaseReply *reply,
               SendReplyCallback send_reply_callback),
              (override));
  MOCK_METHOD(void, HandleAttemptTaskReconstruction,
              (const AttemptTaskReconstructionRequest &request,
               AttemptTaskReconstructionReply *reply,
               SendReplyCallback send_reply_callback),
              (override));
};

}  // namespace rpc
}  // namespace ray
