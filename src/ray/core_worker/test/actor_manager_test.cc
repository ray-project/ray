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

#include "ray/core_worker/actor_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/actor_reporter.h"
#include "ray/core_worker/reference_count.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/gcs/redis_accessor.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {

using ::testing::_;

TaskSpecification CreateActorTaskHelper(ActorID actor_id, WorkerID caller_worker_id,
                                        int64_t counter,
                                        TaskID caller_id = TaskID::Nil()) {
  TaskSpecification task;
  task.GetMutableMessage().set_task_id(TaskID::Nil().Binary());
  task.GetMutableMessage().set_caller_id(caller_id.Binary());
  task.GetMutableMessage().set_type(TaskType::ACTOR_CREATION_TASK);
  task.GetMutableMessage().mutable_caller_address()->set_worker_id(
      caller_worker_id.Binary());
  task.GetMutableMessage().mutable_actor_task_spec()->set_actor_id(actor_id.Binary());
  task.GetMutableMessage().mutable_actor_task_spec()->set_actor_counter(counter);
  task.GetMutableMessage().set_num_returns(1);
  return task;
}

class MockActorInfoAccessor : public gcs::RedisActorInfoAccessor {
 public:
  MockActorInfoAccessor(gcs::RedisGcsClient *client)
      : gcs::RedisActorInfoAccessor(client) {}

  MOCK_METHOD2(AsyncRegister,
               ray::Status(const std::shared_ptr<gcs::ActorTableData> &data_ptr,
                           const gcs::StatusCallback &callback));

  ~MockActorInfoAccessor() {}
};

class MockGcsClient : public gcs::RedisGcsClient {
 public:
  MockGcsClient(const gcs::GcsClientOptions &options) : gcs::RedisGcsClient(options) {}

  void Init(MockActorInfoAccessor *actor_accesor_mock) {
    actor_accessor_.reset(actor_accesor_mock);
  }

  ~MockGcsClient() {}
};

class MockDirectActorSubmitter : public CoreWorkerDirectActorTaskSubmitterInterface {
 public:
  MockDirectActorSubmitter() : CoreWorkerDirectActorTaskSubmitterInterface() {}

  MOCK_METHOD1(AddActorQueueIfNotExists, void(const ActorID &actor_id));
  MOCK_METHOD2(ConnectActor, void(const ActorID &actor_id, const rpc::Address &address));
  MOCK_METHOD2(DisconnectActor, void(const ActorID &actor_id, bool dead));
  MOCK_METHOD3(KillActor,
               void(const ActorID &actor_id, bool force_kill, bool no_restart));

  virtual ~MockDirectActorSubmitter() {}
};

class MockReferenceCounter : public ReferenceCounterInterface {
 public:
  MockReferenceCounter() : ReferenceCounterInterface() {}

  MOCK_METHOD2(AddLocalReference,
               void(const ObjectID &object_id, const std::string &call_sit));
  MOCK_METHOD4(AddBorrowedObject,
               bool(const ObjectID &object_id, const ObjectID &outer_id,
                    const TaskID &owner_id, const rpc::Address &owner_address));
  MOCK_METHOD8(AddOwnedObject,
               void(const ObjectID &object_id, const std::vector<ObjectID> &contained_ids,
                    const TaskID &owner_id, const rpc::Address &owner_address,
                    const std::string &call_site, const int64_t object_size,
                    bool is_reconstructable,
                    const absl::optional<ClientID> &pinned_at_raylet_id));
  MOCK_METHOD2(SetDeleteCallback,
               bool(const ObjectID &object_id,
                    const std::function<void(const ObjectID &)> callback));

  virtual ~MockReferenceCounter() {}
};

class ActorManagerTest : public ::testing::Test {
 public:
  ActorManagerTest()
      : options_("", 1, ""),
        gcs_client_mock_(new MockGcsClient(options_)),
        actor_info_accessor_(new MockActorInfoAccessor(gcs_client_mock_.get())),
        direct_actor_submitter_(new MockDirectActorSubmitter()),
        reference_counter_(new MockReferenceCounter()) {
    gcs_client_mock_->Init(actor_info_accessor_);
  }

  ~ActorManagerTest() {}

  void SetUp() {
    actor_manager_ = std::make_shared<ActorManager>(
        gcs_client_mock_, direct_actor_submitter_, reference_counter_);
  }

  void TearDown() { actor_manager_.reset(); }

  gcs::GcsClientOptions options_;
  std::shared_ptr<MockGcsClient> gcs_client_mock_;
  MockActorInfoAccessor *actor_info_accessor_;
  std::shared_ptr<MockDirectActorSubmitter> direct_actor_submitter_;
  std::shared_ptr<MockReferenceCounter> reference_counter_;
  std::shared_ptr<ActorManager> actor_manager_;
};

TEST_F(ActorManagerTest, AddAndGetActorHandle) {
  JobID job_id = JobID::FromRandom();
  std::unique_ptr<ActorHandle> actor_handle = absl::make_unique<ActorHandle>(
      ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1), TaskID::Nil(),
      rpc::Address(), job_id, ObjectID::FromRandom(), function.GetLanguage(),
      function.GetFunctionDescriptor(), "", 0);
  actor_manager_->AddActorHandle(actor_handle, )
}

// TEST_F(ActorManagerTest, AddCopiedHandle) {
//   JobID job_id = JobID::FromRandom();
//   std::shared_ptr<ActorHandle> actor_handle =
//   std::make_shared<ActorHandle>(ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1),
//                            TaskID::Nil(), rpc::Address(), job_id,
//                            ObjectID::FromRandom(), function.GetLanguage(),
//                            function.GetFunctionDescriptor(), "", 0);
//   // Expect actor subscription
//   // Expect deletion callback set
// }

// TEST_F(ActorManagerTest, GetHandleThatDoesntExist) {
//   JobID job_id = JobID::FromRandom();
//   std::shared_ptr<ActorHandle> actor_handle =
//   std::make_shared<ActorHandle>(ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1),
//                            TaskID::Nil(), rpc::Address(), job_id,
//                            ObjectID::FromRandom(), function.GetLanguage(),
//                            function.GetFunctionDescriptor(), "", 0);
//   // Expect actor subscription
//   // Expect deletion callback set
// }

// TEST_F(ActorManagerTest, SerializeAndDeserializeActorHandle) {
//   JobID job_id = JobID::FromRandom();
//   std::shared_ptr<ActorHandle> actor_handle =
//   std::make_shared<ActorHandle>(ActorID::Of(job_id, TaskID::ForDriverTask(job_id), 1),
//                            TaskID::Nil(), rpc::Address(), job_id,
//                            ObjectID::FromRandom(), function.GetLanguage(),
//                            function.GetFunctionDescriptor(), "", 0);
//   // Expect actor subscription
//   // Expect deletion callback set
// }

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
