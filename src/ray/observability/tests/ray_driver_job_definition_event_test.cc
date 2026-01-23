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

#include "ray/observability/ray_driver_job_definition_event.h"

#include "gtest/gtest.h"

namespace ray {
namespace observability {

class RayDriverJobDefinitionEventTest : public ::testing::Test {};

TEST_F(RayDriverJobDefinitionEventTest, TestSerialize) {
  rpc::JobTableData job_data;
  job_data.set_job_id("test_job_id_123");
  job_data.set_driver_pid(12345);
  job_data.mutable_driver_address()->set_node_id("test_node_id");
  job_data.set_entrypoint("python script.py");

  // Set config metadata including job_submission_id
  (*job_data.mutable_config()->mutable_metadata())["job_submission_id"] = "sub_456";
  (*job_data.mutable_config()->mutable_metadata())["user"] = "test_user";
  job_data.mutable_config()->mutable_runtime_env_info()->set_serialized_runtime_env(
      "{\"pip\": [\"requests\"]}");

  // Set job_info fields
  auto *job_info = job_data.mutable_job_info();
  job_info->set_driver_agent_http_address("http://127.0.0.1:8265");
  job_info->set_entrypoint_num_cpus(2.0);
  job_info->set_entrypoint_num_gpus(1.0);
  job_info->set_entrypoint_memory(1024 * 1024 * 1024);  // 1GB
  (*job_info->mutable_entrypoint_resources())["custom_resource"] = 3.0;

  auto event = std::make_unique<RayDriverJobDefinitionEvent>(job_data, "test_session");
  auto serialized_event = std::move(*event).Serialize();

  ASSERT_EQ(serialized_event.source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(serialized_event.session_name(), "test_session");
  ASSERT_EQ(serialized_event.event_type(),
            rpc::events::RayEvent::DRIVER_JOB_DEFINITION_EVENT);
  ASSERT_EQ(serialized_event.severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(serialized_event.has_driver_job_definition_event());

  const auto &job_def = serialized_event.driver_job_definition_event();

  ASSERT_EQ(job_def.job_id(), "test_job_id_123");
  ASSERT_EQ(job_def.driver_pid(), 12345);
  ASSERT_EQ(job_def.driver_node_id(), "test_node_id");
  ASSERT_EQ(job_def.entrypoint(), "python script.py");
  ASSERT_EQ(job_def.config().metadata().at("user"), "test_user");
  ASSERT_EQ(job_def.config().serialized_runtime_env(), "{\"pip\": [\"requests\"]}");
  ASSERT_TRUE(job_def.is_submission_job());
  ASSERT_EQ(job_def.submission_id(), "sub_456");
  ASSERT_EQ(job_def.driver_agent_http_address(), "http://127.0.0.1:8265");
  ASSERT_DOUBLE_EQ(job_def.entrypoint_num_cpus(), 2.0);
  ASSERT_DOUBLE_EQ(job_def.entrypoint_num_gpus(), 1.0);
  ASSERT_EQ(job_def.entrypoint_memory(), 1024 * 1024 * 1024);
  ASSERT_EQ(job_def.entrypoint_resources().size(), 1);
  ASSERT_DOUBLE_EQ(job_def.entrypoint_resources().at("custom_resource"), 3.0);
}

TEST_F(RayDriverJobDefinitionEventTest, TestSerializeDriverJob) {
  // Test a driver job (no submission_id, no job_info)
  rpc::JobTableData job_data;
  job_data.set_job_id("driver_job_789");
  job_data.set_driver_pid(99999);
  job_data.mutable_driver_address()->set_node_id("driver_node");
  job_data.set_entrypoint("python main.py");

  // No job_submission_id in metadata
  (*job_data.mutable_config()->mutable_metadata())["custom_key"] = "custom_value";

  auto event = std::make_unique<RayDriverJobDefinitionEvent>(job_data, "test_session");
  auto serialized_event = std::move(*event).Serialize();

  const auto &job_def = serialized_event.driver_job_definition_event();

  // Driver job assertions
  ASSERT_FALSE(job_def.is_submission_job());
  ASSERT_EQ(job_def.submission_id(), "");

  // Default value assertions when job_info not present
  ASSERT_EQ(job_def.driver_agent_http_address(), "");
  ASSERT_DOUBLE_EQ(job_def.entrypoint_num_cpus(), 0.0);
  ASSERT_DOUBLE_EQ(job_def.entrypoint_num_gpus(), 0.0);
  ASSERT_EQ(job_def.entrypoint_memory(), 0);
  ASSERT_EQ(job_def.entrypoint_resources().size(), 0);
}

}  // namespace observability
}  // namespace ray
