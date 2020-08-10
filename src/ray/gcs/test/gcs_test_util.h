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

#pragma once

#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "ray/common/placement_group.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"
#include "ray/util/asio_util.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {

struct Mocker {
  static TaskSpecification GenActorCreationTask(const JobID &job_id, int max_restarts,
                                                bool detached, const std::string &name,
                                                const rpc::Address &owner_address) {
    TaskSpecBuilder builder;
    rpc::Address empty_address;
    ray::FunctionDescriptor empty_descriptor =
        ray::FunctionDescriptorBuilder::BuildPython("", "", "", "");
    auto actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
    auto task_id = TaskID::ForActorCreationTask(actor_id);
    auto resource = std::unordered_map<std::string, double>();
    builder.SetCommonTaskSpec(task_id, Language::PYTHON, empty_descriptor, job_id,
                              TaskID::Nil(), 0, TaskID::Nil(), owner_address, 1, resource,
                              resource);
    builder.SetActorCreationTaskSpec(actor_id, max_restarts, {}, 1, detached, name);
    return builder.Build();
  }

  static rpc::CreateActorRequest GenCreateActorRequest(const JobID &job_id,
                                                       int max_restarts = 0,
                                                       bool detached = false,
                                                       const std::string name = "") {
    rpc::Address owner_address;
    owner_address.set_raylet_id(ClientID::FromRandom().Binary());
    owner_address.set_ip_address("1234");
    owner_address.set_port(5678);
    owner_address.set_worker_id(WorkerID::FromRandom().Binary());
    auto actor_creation_task_spec =
        GenActorCreationTask(job_id, max_restarts, detached, name, owner_address);
    rpc::CreateActorRequest request;
    request.mutable_task_spec()->CopyFrom(actor_creation_task_spec.GetMessage());
    return request;
  }

  static rpc::RegisterActorRequest GenRegisterActorRequest(const JobID &job_id,
                                                           int max_restarts = 0,
                                                           bool detached = false,
                                                           const std::string name = "") {
    rpc::Address owner_address;
    owner_address.set_raylet_id(ClientID::FromRandom().Binary());
    owner_address.set_ip_address("1234");
    owner_address.set_port(5678);
    owner_address.set_worker_id(WorkerID::FromRandom().Binary());
    auto actor_creation_task_spec =
        GenActorCreationTask(job_id, max_restarts, detached, name, owner_address);
    rpc::RegisterActorRequest request;
    request.mutable_task_spec()->CopyFrom(actor_creation_task_spec.GetMessage());
    return request;
  }

  static PlacementGroupSpecification GenPlacementGroupCreation(
      const std::string &name,
      std::vector<std::unordered_map<std::string, double>> &bundles,
      rpc::PlacementStrategy strategy) {
    PlacementGroupSpecBuilder builder;

    auto placement_group_id = PlacementGroupID::FromRandom();
    builder.SetPlacementGroupSpec(placement_group_id, name, bundles, strategy);
    return builder.Build();
  }

  static rpc::CreatePlacementGroupRequest GenCreatePlacementGroupRequest(
      const std::string name = "") {
    rpc::CreatePlacementGroupRequest request;
    std::vector<std::unordered_map<std::string, double>> bundles;
    rpc::PlacementStrategy strategy = rpc::PlacementStrategy::SPREAD;
    std::unordered_map<std::string, double> bundle;
    bundle["CPU"] = 1.0;
    bundles.push_back(bundle);
    bundles.push_back(bundle);
    auto placement_group_creation_spec =
        GenPlacementGroupCreation(name, bundles, strategy);
    request.mutable_placement_group_spec()->CopyFrom(
        placement_group_creation_spec.GetMessage());
    return request;
  }
  static std::shared_ptr<rpc::GcsNodeInfo> GenNodeInfo(
      uint16_t port = 0, const std::string address = "127.0.0.1") {
    auto node = std::make_shared<rpc::GcsNodeInfo>();
    node->set_node_id(ClientID::FromRandom().Binary());
    node->set_node_manager_port(port);
    node->set_node_manager_address(address);
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
    actor_table_data->set_state(rpc::ActorTableData::ALIVE);
    actor_table_data->set_max_restarts(1);
    actor_table_data->set_num_restarts(0);
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
    task_lease_data->set_timeout(9999);
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

  static std::shared_ptr<rpc::WorkerTableData> GenWorkerTableData() {
    auto worker_table_data = std::make_shared<rpc::WorkerTableData>();
    worker_table_data->set_timestamp(std::time(nullptr));
    return worker_table_data;
  }
};

}  // namespace ray
