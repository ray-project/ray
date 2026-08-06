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

#include "ray/observability/ray_task_definition_event.h"

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

class RayTaskDefinitionEventTest : public ::testing::Test {
 public:
  std::shared_ptr<const TaskSpecification> BuildTaskSpec(const TaskID &task_id) {
    TaskSpecBuilder builder;
    rpc::Address empty_address;
    rpc::JobConfig config;
    std::unordered_map<std::string, double> resources = {{"CPU", 1}};
    std::unordered_map<std::string, std::string> labels = {{"label1", "value1"}};
    builder.SetCommonTaskSpec(task_id,
                              "dummy_task",
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
    return std::make_shared<const TaskSpecification>(
        std::move(builder).ConsumeAndBuild());
  }
};

// The task spec is turned into a TaskDefinitionEvent when the event is serialized.
TEST_F(RayTaskDefinitionEventTest, TestSerialize) {
  TaskID task_id = TaskID::FromRandom(JobID::FromInt(1));
  JobID job_id = JobID::FromInt(1);
  auto task_spec = BuildTaskSpec(task_id);
  auto session_name = std::make_shared<const std::string>("sess1");

  RayTaskDefinitionEvent event(task_spec,
                               task_id,
                               job_id,
                               /*task_attempt=*/2,
                               session_name,
                               /*timestamp=*/1000);

  ASSERT_EQ(event.GetEntityId(), task_id.Binary() + "2");
  ASSERT_EQ(event.GetTaskAttempt().first, task_id);
  ASSERT_EQ(event.GetTaskAttempt().second, 2);

  auto serialized_event = std::move(event).Serialize().value();

  ASSERT_EQ(serialized_event.source_type(), rpc::events::RayEvent::CORE_WORKER);
  ASSERT_EQ(serialized_event.event_type(), rpc::events::RayEvent::TASK_DEFINITION_EVENT);
  ASSERT_EQ(serialized_event.severity(), rpc::events::RayEvent::INFO);
  ASSERT_EQ(serialized_event.session_name(), "sess1");
  ASSERT_TRUE(serialized_event.has_task_definition_event());

  const auto &definition = serialized_event.task_definition_event();
  ASSERT_EQ(definition.task_id(), task_id.Binary());
  ASSERT_EQ(definition.task_attempt(), 2);
  ASSERT_EQ(definition.job_id(), job_id.Binary());
  ASSERT_EQ(definition.task_name(), "dummy_task");
  ASSERT_EQ(definition.language(), Language::PYTHON);
  ASSERT_EQ(definition.task_func().python_function_descriptor().module_name(),
            "dummy_module");
  ASSERT_EQ(definition.task_func().python_function_descriptor().function_name(),
            "dummy_function");
  ASSERT_EQ(definition.required_resources().at("CPU"), 1);
  ASSERT_EQ(definition.ref_ids().at("label1"), "value1");
  ASSERT_FALSE(definition.is_detached_actor());
}

}  // namespace observability
}  // namespace ray
