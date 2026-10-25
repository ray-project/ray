// Copyright 2026 The Ray Authors.
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

#include "ray/observability/ray_actor_task_definition_event.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "gtest/gtest.h"
#include "ray/common/function_descriptor.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"

namespace ray {
namespace observability {

class RayActorTaskDefinitionEventTest : public ::testing::Test {
 public:
  std::shared_ptr<const TaskSpecification> BuildActorTaskSpec(const TaskID &task_id,
                                                              const ActorID &actor_id) {
    TaskSpecBuilder builder;
    rpc::Address empty_address;
    rpc::JobConfig config;
    std::unordered_map<std::string, double> resources = {{"CPU", 1}};
    std::unordered_map<std::string, std::string> labels = {{"label1", "value1"}};
    builder.SetCommonTaskSpec(task_id,
                              "dummy_actor_task",
                              Language::PYTHON,
                              FunctionDescriptorBuilder::BuildPython(
                                  "dummy_module", "dummy_class", "dummy_function", ""),
                              JobID::Nil(),
                              config,
                              TaskID::Nil(),
                              0,
                              TaskID::Nil(),
                              empty_address,
                              1,
                              false,
                              false,
                              -1,
                              resources,
                              resources,
                              "",
                              0,
                              TaskID::Nil(),
                              "",
                              std::make_shared<rpc::RuntimeEnvInfo>(),
                              "",
                              true,
                              labels);
    const ObjectID actor_creation_dummy_object_id =
        ObjectID::FromIndex(TaskID::ForActorCreationTask(actor_id), /*index=*/1);
    builder.SetActorTaskSpec(actor_id,
                             actor_creation_dummy_object_id,
                             /*max_retries=*/0,
                             /*retry_exceptions=*/false,
                             /*serialized_retry_exception_allowlist=*/"",
                             /*concurrency_group_sequence_number=*/0,
                             /*tensor_transport=*/std::nullopt,
                             /*is_detached_actor=*/false);
    return std::make_shared<const TaskSpecification>(
        std::move(builder).ConsumeAndBuild());
  }
};

// The actor task spec is turned into an ActorTaskDefinitionEvent when the event is
// serialized.
TEST_F(RayActorTaskDefinitionEventTest, TestSerialize) {
  ActorID actor_id = ActorID::FromHex("f4ce02420592ca68c1738a0d01000000");
  TaskID task_id = TaskID::ForActorTask(JobID::FromInt(1), TaskID::Nil(), 0, actor_id);
  JobID job_id = JobID::FromInt(1);
  auto task_spec = BuildActorTaskSpec(task_id, actor_id);

  RayActorTaskDefinitionEvent event(task_spec,
                                    task_id,
                                    job_id,
                                    /*task_attempt=*/2,
                                    "sess1",
                                    /*timestamp=*/1000);

  ASSERT_EQ(event.GetEntityId(), task_id.Binary() + "2");
  ASSERT_EQ(event.GetTaskAttempt().first, task_id.Binary());
  ASSERT_EQ(event.GetTaskAttempt().second, 2);

  auto serialized_event = std::move(event).Serialize().value();

  ASSERT_EQ(serialized_event.source_type(), rpc::events::RayEvent::CORE_WORKER);
  ASSERT_EQ(serialized_event.event_type(),
            rpc::events::RayEvent::ACTOR_TASK_DEFINITION_EVENT);
  ASSERT_EQ(serialized_event.severity(), rpc::events::RayEvent::INFO);
  ASSERT_EQ(serialized_event.session_name(), "sess1");
  ASSERT_TRUE(serialized_event.has_actor_task_definition_event());

  const auto &definition = serialized_event.actor_task_definition_event();
  ASSERT_EQ(definition.task_id(), task_id.Binary());
  ASSERT_EQ(definition.task_attempt(), 2);
  ASSERT_EQ(definition.job_id(), job_id.Binary());
  ASSERT_EQ(definition.actor_task_name(), "dummy_actor_task");
  ASSERT_EQ(definition.actor_id(), actor_id.Binary());
  ASSERT_EQ(definition.language(), Language::PYTHON);
  ASSERT_EQ(definition.actor_func().python_function_descriptor().module_name(),
            "dummy_module");
  ASSERT_EQ(definition.actor_func().python_function_descriptor().function_name(),
            "dummy_function");
  ASSERT_EQ(definition.required_resources().at("CPU"), 1);
  ASSERT_EQ(definition.ref_ids().at("label1"), "value1");
}

}  // namespace observability
}  // namespace ray
