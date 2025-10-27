// Copyright 2024 The Ray Authors.
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

#include <string>

#include "gtest/gtest.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

class ProtoSchemaTest : public ::testing::Test {
 public:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(ProtoSchemaTest, TestActorDeathCauseBackwardCompatibility) {
  // The ActorDeathCause proto schema is public and must be backward compatible
  // for the State API and Export API. This test verifies all fields can be populated.

  rpc::RayException ray_exception;
  rpc::ActorDeathCause actor_death_cause1;
  ray_exception.set_language(rpc::Language::CPP);
  ray_exception.set_formatted_exception_string("exception string");
  std::string set_serialized_exception_byte_data = "serialized exception bytes data";
  ray_exception.set_serialized_exception(set_serialized_exception_byte_data);
  actor_death_cause1.mutable_creation_task_failure_context()->CopyFrom(ray_exception);

  rpc::RuntimeEnvFailedContext runtime_env_failed_context;
  rpc::ActorDeathCause actor_death_cause2;
  runtime_env_failed_context.set_error_message("error message string");
  actor_death_cause2.mutable_runtime_env_failed_context()->CopyFrom(
      runtime_env_failed_context);

  rpc::NodeDeathInfo::Reason node_death_info_reason_arr[5] = {
      rpc::NodeDeathInfo::UNSPECIFIED,
      rpc::NodeDeathInfo::EXPECTED_TERMINATION,
      rpc::NodeDeathInfo::UNEXPECTED_TERMINATION,
      rpc::NodeDeathInfo::AUTOSCALER_DRAIN_PREEMPTED,
      rpc::NodeDeathInfo::AUTOSCALER_DRAIN_IDLE};
  for (rpc::NodeDeathInfo::Reason node_death_info_reason : node_death_info_reason_arr) {
    rpc::ActorDiedErrorContext actor_died_context;
    rpc::NodeDeathInfo node_death_info;
    rpc::ActorDeathCause actor_death_cause3;
    node_death_info.set_reason(node_death_info_reason);
    node_death_info.set_reason_message("reason message string");
    actor_died_context.set_error_message("error message string");
    actor_died_context.set_owner_id("ownerId1");
    actor_died_context.set_owner_ip_address("127.0.0.1");
    actor_died_context.set_node_ip_address("127.0.0.1");
    actor_died_context.set_pid(123);
    actor_died_context.set_name("actor_name");
    actor_died_context.set_ray_namespace("actor_ray_namespace");
    actor_died_context.set_class_name("actor_class_name");
    actor_died_context.set_actor_id("actorId1");
    actor_died_context.set_never_started(false);
    actor_died_context.mutable_node_death_info()->CopyFrom(node_death_info);
    actor_death_cause3.mutable_actor_died_error_context()->CopyFrom(actor_died_context);
  }

  rpc::ActorUnschedulableContext actor_unschedulable_context;
  rpc::ActorDeathCause actor_death_cause4;
  actor_unschedulable_context.set_error_message("error message string");
  actor_death_cause4.mutable_actor_unschedulable_context()->CopyFrom(
      actor_unschedulable_context);

  rpc::OomContext oom_context;
  rpc::ActorDeathCause actor_death_cause5;
  oom_context.set_error_message("error message string");
  oom_context.set_fail_immediately(true);
  actor_death_cause5.mutable_oom_context()->CopyFrom(oom_context);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
