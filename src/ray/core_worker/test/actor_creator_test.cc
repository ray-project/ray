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
// clang-format off
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/core_worker/actor_creator.h"
#include "ray/common/test_util.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
// clang-format on

namespace ray {
namespace core {

class ActorCreatorTest : public ::testing::Test {
 public:
  ActorCreatorTest() {}
  void SetUp() override {
    gcs_client = std::make_shared<ray::gcs::MockGcsClient>();
    actor_creator = std::make_unique<DefaultActorCreator>(gcs_client);
  }
  TaskSpecification GetTaskSpec(const ActorID &actor_id) {
    rpc::TaskSpec task_spec;
    task_spec.set_type(rpc::TaskType::ACTOR_CREATION_TASK);
    rpc::ActorCreationTaskSpec actor_creation_task_spec;
    actor_creation_task_spec.set_actor_id(actor_id.Binary());
    task_spec.mutable_actor_creation_task_spec()->CopyFrom(actor_creation_task_spec);
    return TaskSpecification(task_spec);
  }
  std::shared_ptr<ray::gcs::MockGcsClient> gcs_client;
  std::unique_ptr<DefaultActorCreator> actor_creator;
};

TEST_F(ActorCreatorTest, IsRegister) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id));
  auto task_spec = GetTaskSpec(actor_id);
  std::function<void(Status)> cb;
  EXPECT_CALL(*gcs_client->mock_actor_accessor,
              AsyncRegisterActor(task_spec, ::testing::_, ::testing::_))
      .WillOnce(
          ::testing::DoAll(::testing::SaveArg<1>(&cb), ::testing::Return(Status::OK())));
  ASSERT_TRUE(actor_creator->AsyncRegisterActor(task_spec, nullptr).ok());
  ASSERT_TRUE(actor_creator->IsActorInRegistering(actor_id));
  cb(Status::OK());
  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id));
}

TEST_F(ActorCreatorTest, AsyncWaitForFinish) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  auto task_spec = GetTaskSpec(actor_id);
  std::function<void(Status)> cb;
  EXPECT_CALL(*gcs_client->mock_actor_accessor,
              AsyncRegisterActor(::testing::_, ::testing::_, ::testing::_))
      .WillRepeatedly(
          ::testing::DoAll(::testing::SaveArg<1>(&cb), ::testing::Return(Status::OK())));
  int cnt = 0;
  auto per_finish_cb = [&cnt](Status status) {
    ASSERT_TRUE(status.ok());
    cnt++;
  };
  ASSERT_TRUE(actor_creator->AsyncRegisterActor(task_spec, per_finish_cb).ok());
  ASSERT_TRUE(actor_creator->IsActorInRegistering(actor_id));
  for (int i = 0; i < 100; ++i) {
    actor_creator->AsyncWaitForActorRegisterFinish(actor_id, per_finish_cb);
  }
  cb(Status::OK());
  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id));
  ASSERT_EQ(101, cnt);
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog,
                                         argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  ray::RayLog::InstallFailureSignalHandler(argv[0]);
  return RUN_ALL_TESTS();
}
