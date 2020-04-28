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

#ifndef RAY_GCS_TEST_UTIL_H
#define RAY_GCS_TEST_UTIL_H

#include <memory>
#include <utility>
#include "gmock/gmock.h"

#include "src/ray/common/task/task.h"
#include "src/ray/common/task/task_util.h"
#include "src/ray/common/test_util.h"
#include "src/ray/util/asio_util.h"

#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {

struct Mocker {
  static TaskSpecification GenActorCreationTask(const JobID &job_id,
                                                int max_reconstructions, bool detached,
                                                const rpc::Address &owner_address) {
    TaskSpecBuilder builder;
    rpc::Address empty_address;
    ray::FunctionDescriptor empty_descriptor =
        ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
    auto actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
    auto task_id = TaskID::ForActorCreationTask(actor_id);
    builder.SetCommonTaskSpec(task_id, Language::PYTHON, empty_descriptor, job_id,
                              TaskID::Nil(), 0, TaskID::Nil(), owner_address, 1, {}, {});
    builder.SetActorCreationTaskSpec(actor_id, max_reconstructions, {}, 1, detached);
    return builder.Build();
  }

  static rpc::CreateActorRequest GenCreateActorRequest(const JobID &job_id,
                                                       int max_reconstructions = 0,
                                                       bool detached = false) {
    rpc::CreateActorRequest request;
    rpc::Address owner_address;
    if (owner_address.raylet_id().empty()) {
      owner_address.set_raylet_id(ClientID::FromRandom().Binary());
      owner_address.set_ip_address("1234");
      owner_address.set_port(5678);
      owner_address.set_worker_id(WorkerID::FromRandom().Binary());
    }
    auto actor_creation_task_spec =
        GenActorCreationTask(job_id, max_reconstructions, detached, owner_address);
    request.mutable_task_spec()->CopyFrom(actor_creation_task_spec.GetMessage());
    return request;
  }

  static std::shared_ptr<rpc::GcsNodeInfo> GenNodeInfo(uint16_t port = 0) {
    auto node = std::make_shared<rpc::GcsNodeInfo>();
    node->set_node_id(ClientID::FromRandom().Binary());
    node->set_node_manager_port(port);
    node->set_node_manager_address("127.0.0.1");
    return node;
  }

  static std::shared_ptr<rpc::JobTableData> GenJobTableData(JobID job_id) {
    auto job_table_data = std::make_shared<rpc::JobTableData>();
    job_table_data->set_job_id(job_id.Binary());
    job_table_data->set_is_dead(false);
    job_table_data->set_timestamp(std::time(nullptr));
    job_table_data->set_driver_ip_address("127.0.0.1");
    job_table_data->set_driver_pid(5667L);
    return job_table_data;
  }

  static std::shared_ptr<rpc::ActorTableData> GenActorTableData(const JobID &job_id) {
    auto actor_table_data = std::make_shared<rpc::ActorTableData>();
    ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
    actor_table_data->set_actor_id(actor_id.Binary());
    actor_table_data->set_job_id(job_id.Binary());
    actor_table_data->set_state(
        rpc::ActorTableData_ActorState::ActorTableData_ActorState_ALIVE);
    actor_table_data->set_max_reconstructions(1);
    actor_table_data->set_remaining_reconstructions(1);
    return actor_table_data;
  }

  static std::shared_ptr<rpc::TaskTableData> GenTaskTableData(
      const std::string &job_id, const std::string &task_id) {
    auto task_table_data = std::make_shared<rpc::TaskTableData>();
    rpc::Task task;
    rpc::TaskSpec task_spec;
    task_spec.set_job_id(job_id);
    task_spec.set_task_id(task_id);
    task.mutable_task_spec()->CopyFrom(task_spec);
    task_table_data->mutable_task()->CopyFrom(task);
    return task_table_data;
  }

  static std::shared_ptr<rpc::TaskLeaseData> GenTaskLeaseData(
      const std::string &task_id, const std::string &node_id) {
    auto task_lease_data = std::make_shared<rpc::TaskLeaseData>();
    task_lease_data->set_task_id(task_id);
    task_lease_data->set_node_manager_id(node_id);
    return task_lease_data;
  }

  static std::shared_ptr<rpc::ProfileTableData> GenProfileTableData(
      const ClientID &node_id) {
    auto profile_table_data = std::make_shared<rpc::ProfileTableData>();
    profile_table_data->set_component_id(node_id.Binary());
    return profile_table_data;
  }

  static std::shared_ptr<rpc::ErrorTableData> GenErrorTableData(const JobID &job_id) {
    auto error_table_data = std::make_shared<rpc::ErrorTableData>();
    error_table_data->set_job_id(job_id.Binary());
    return error_table_data;
  }

  static std::shared_ptr<rpc::WorkerFailureData> GenWorkerFailureData() {
    auto worker_failure_data = std::make_shared<rpc::WorkerFailureData>();
    worker_failure_data->set_timestamp(std::time(nullptr));
    return worker_failure_data;
  }
};

}  // namespace ray

#endif  // RAY_GCS_TEST_UTIL_H
