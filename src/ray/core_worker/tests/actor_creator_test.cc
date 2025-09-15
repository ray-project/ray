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

#include "ray/core_worker/actor_creator.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/gcs_client/gcs_client.h"
#include "ray/common/test_utils.h"
#include "ray/util/path_utils.h"
#include "ray/util/raii.h"

namespace ray {
namespace core {

class ActorCreatorTest : public ::testing::Test {
 public:
  ActorCreatorTest() {}
  void SetUp() override {
    gcs_client = std::make_shared<ray::gcs::MockGcsClient>();
    actor_creator = std::make_unique<ActorCreator>(gcs_client->Actors());
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
  std::unique_ptr<ActorCreator> actor_creator;
};

TEST_F(ActorCreatorTest, IsRegister) {
  auto actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id));
  auto task_spec = GetTaskSpec(actor_id);
  std::function<void(Status)> cb;
  EXPECT_CALL(*gcs_client->mock_actor_accessor,
              AsyncRegisterActor(task_spec, ::testing::_, ::testing::_))
      .WillOnce(::testing::DoAll(::testing::SaveArg<1>(&cb)));
  actor_creator->AsyncRegisterActor(task_spec, nullptr);
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
      .WillRepeatedly(::testing::DoAll(::testing::SaveArg<1>(&cb)));
  int count = 0;
  auto per_finish_cb = [&count](Status status) {
    ASSERT_TRUE(status.ok());
    count++;
  };
  actor_creator->AsyncRegisterActor(task_spec, per_finish_cb);
  ASSERT_TRUE(actor_creator->IsActorInRegistering(actor_id));
  for (int i = 0; i < 10; ++i) {
    actor_creator->AsyncWaitForActorRegisterFinish(actor_id, per_finish_cb);
  }
  cb(Status::OK());
  ASSERT_FALSE(actor_creator->IsActorInRegistering(actor_id));
  ASSERT_EQ(11, count);
}

}  // namespace core
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  InitShutdownRAII ray_log_shutdown_raii(
      ray::RayLog::StartRayLog,
      ray::RayLog::ShutDownRayLog,
      argv[0],
      ray::RayLogLevel::INFO,
      ray::GetLogFilepathFromDirectory(/*log_dir=*/"", /*app_name=*/argv[0]),
      ray::GetErrLogFilepathFromDirectory(/*log_dir=*/"", /*app_name=*/argv[0]),
      ray::RayLog::GetRayLogRotationMaxBytesOrDefault(),
      ray::RayLog::GetRayLogRotationBackupCountOrDefault());
  ray::RayLog::InstallFailureSignalHandler(argv[0]);
  return RUN_ALL_TESTS();
}
