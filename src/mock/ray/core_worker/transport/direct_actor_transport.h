// Copyright 2021 The Ray Authors.
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
namespace core {
class MockCoreWorkerDirectActorTaskSubmitterInterface
    : public CoreWorkerDirectActorTaskSubmitterInterface {
 public:
  MOCK_METHOD(void,
              AddActorQueueIfNotExists,
              (const ActorID &actor_id, int32_t max_pending_calls),
              (override));
  MOCK_METHOD(void,
              ConnectActor,
              (const ActorID &actor_id,
               const rpc::Address &address,
               int64_t num_restarts),
              (override));
  MOCK_METHOD(void,
              DisconnectActor,
              (const ActorID &actor_id,
               int64_t num_restarts,
               bool dead,
               const rpc::RayException *creation_task_exception),
              (override));
  MOCK_METHOD(void,
              KillActor,
              (const ActorID &actor_id, bool force_kill, bool no_restart),
              (override));
  MOCK_METHOD(void, CheckTimeoutTasks, (), (override));
};

class MockDependencyWaiter : public DependencyWaiter {
 public:
  MOCK_METHOD(void,
              Wait,
              (const std::vector<rpc::ObjectReference> &dependencies,
               std::function<void()> on_dependencies_available),
              (override));
};

class MockSchedulingQueue : public SchedulingQueue {
 public:
  MOCK_METHOD(void,
              Add,
              (int64_t seq_no,
               int64_t client_processed_up_to,
               std::function<void(rpc::SendReplyCallback)> accept_request,
               std::function<void(rpc::SendReplyCallback)> reject_request,
               rpc::SendReplyCallback send_reply_callback,
               const std::string &concurrency_group_name,
               const ray::FunctionDescriptor &function_descriptor,
               TaskID task_id,
               const std::vector<rpc::ObjectReference> &dependencies),
              (override));
  MOCK_METHOD(void, ScheduleRequests, (), (override));
  MOCK_METHOD(bool, TaskQueueEmpty, (), (const, override));
  MOCK_METHOD(size_t, Size, (), (const, override));
  MOCK_METHOD(bool, CancelTaskIfFound, (TaskID task_id), (override));
};
}  // namespace core
}  // namespace ray
