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
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/placement_group.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {

struct Mocker {
  static TaskSpecification GenActorCreationTask(
      const JobID &job_id,
      int max_restarts,
      bool detached,
      const std::string &name,
      const std::string &ray_namespace,
      const rpc::Address &owner_address,
      std::unordered_map<std::string, double> required_resources =
          std::unordered_map<std::string, double>(),
      std::unordered_map<std::string, double> required_placement_resources =
          std::unordered_map<std::string, double>()) {
    TaskSpecBuilder builder;
    auto actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
    auto task_id = TaskID::ForActorCreationTask(actor_id);
    FunctionDescriptor function_descriptor;
    function_descriptor = FunctionDescriptorBuilder::BuildPython("", "", "", "");
    builder.SetCommonTaskSpec(task_id,
                              name + ":" + function_descriptor->CallString(),
                              Language::PYTHON,
                              function_descriptor,
                              job_id,
                              TaskID::Nil(),
                              0,
                              TaskID::Nil(),
                              owner_address,
                              1,
                              required_resources,
                              required_placement_resources,
                              "",
                              0);
    rpc::SchedulingStrategy scheduling_strategy;
    scheduling_strategy.mutable_default_scheduling_strategy();
    builder.SetActorCreationTaskSpec(actor_id,
                                     {},
                                     scheduling_strategy,
                                     max_restarts,
                                     /*max_task_retries=*/0,
                                     {},
                                     1,
                                     detached,
                                     name,
                                     ray_namespace);
    return builder.Build();
  }

  static rpc::CreateActorRequest GenCreateActorRequest(
      const JobID &job_id,
      int max_restarts = 0,
      bool detached = false,
      const std::string &name = "",
      const std::string &ray_namespace = "") {
    rpc::Address owner_address;
    owner_address.set_raylet_id(NodeID::FromRandom().Binary());
    owner_address.set_ip_address("1234");
    owner_address.set_port(5678);
    owner_address.set_worker_id(WorkerID::FromRandom().Binary());
    auto actor_creation_task_spec = GenActorCreationTask(
        job_id, max_restarts, detached, name, ray_namespace, owner_address);
    rpc::CreateActorRequest request;
    request.mutable_task_spec()->CopyFrom(actor_creation_task_spec.GetMessage());
    return request;
  }

  static rpc::RegisterActorRequest GenRegisterActorRequest(
      const JobID &job_id,
      int max_restarts = 0,
      bool detached = false,
      const std::string &name = "",
      const std::string &ray_namespace = "") {
    rpc::Address owner_address;
    owner_address.set_raylet_id(NodeID::FromRandom().Binary());
    owner_address.set_ip_address("1234");
    owner_address.set_port(5678);
    owner_address.set_worker_id(WorkerID::FromRandom().Binary());
    auto actor_creation_task_spec = GenActorCreationTask(
        job_id, max_restarts, detached, name, ray_namespace, owner_address);
    rpc::RegisterActorRequest request;
    request.mutable_task_spec()->CopyFrom(actor_creation_task_spec.GetMessage());
    return request;
  }

  static std::vector<std::shared_ptr<const BundleSpecification>> GenBundleSpecifications(
      const PlacementGroupID &placement_group_id,
      absl::flat_hash_map<std::string, double> &unit_resource,
      int bundles_size = 1) {
    std::vector<std::shared_ptr<const BundleSpecification>> bundle_specs;
    for (int i = 0; i < bundles_size; i++) {
      rpc::Bundle bundle;
      auto mutable_bundle_id = bundle.mutable_bundle_id();
      // The bundle index is start from 1.
      mutable_bundle_id->set_bundle_index(i + 1);
      mutable_bundle_id->set_placement_group_id(placement_group_id.Binary());
      auto mutable_unit_resources = bundle.mutable_unit_resources();
      for (auto &resource : unit_resource) {
        mutable_unit_resources->insert({resource.first, resource.second});
      }
      bundle_specs.emplace_back(std::make_shared<BundleSpecification>(bundle));
    }
    return bundle_specs;
  }

  // TODO(@clay4444): Remove this once we did the batch rpc request refactor.
  static BundleSpecification GenBundleCreation(
      const PlacementGroupID &placement_group_id,
      const int bundle_index,
      absl::flat_hash_map<std::string, double> &unit_resource) {
    rpc::Bundle bundle;
    auto mutable_bundle_id = bundle.mutable_bundle_id();
    mutable_bundle_id->set_bundle_index(bundle_index);
    mutable_bundle_id->set_placement_group_id(placement_group_id.Binary());
    auto mutable_unit_resources = bundle.mutable_unit_resources();
    for (auto &resource : unit_resource) {
      mutable_unit_resources->insert({resource.first, resource.second});
    }
    return BundleSpecification(bundle);
  }

  static PlacementGroupSpecification GenPlacementGroupCreation(
      const std::string &name,
      std::vector<std::unordered_map<std::string, double>> &bundles,
      rpc::PlacementStrategy strategy,
      const JobID &job_id,
      const ActorID &actor_id) {
    PlacementGroupSpecBuilder builder;

    auto placement_group_id = PlacementGroupID::Of(job_id);
    builder.SetPlacementGroupSpec(placement_group_id,
                                  name,
                                  bundles,
                                  strategy,
                                  /* is_detached */ false,
                                  job_id,
                                  actor_id,
                                  /* is_creator_detached */ false);
    return builder.Build();
  }

  static rpc::CreatePlacementGroupRequest GenCreatePlacementGroupRequest(
      const std::string name = "",
      rpc::PlacementStrategy strategy = rpc::PlacementStrategy::SPREAD,
      int bundles_count = 2,
      double cpu_num = 1.0,
      const JobID job_id = JobID::FromInt(1),
      const ActorID &actor_id = ActorID::Nil()) {
    rpc::CreatePlacementGroupRequest request;
    std::vector<std::unordered_map<std::string, double>> bundles;
    std::unordered_map<std::string, double> bundle;
    bundle["CPU"] = cpu_num;
    for (int index = 0; index < bundles_count; ++index) {
      bundles.push_back(bundle);
    }
    auto placement_group_creation_spec =
        GenPlacementGroupCreation(name, bundles, strategy, job_id, actor_id);
    request.mutable_placement_group_spec()->CopyFrom(
        placement_group_creation_spec.GetMessage());
    return request;
  }
  static std::shared_ptr<rpc::GcsNodeInfo> GenNodeInfo(
      uint16_t port = 0, const std::string address = "127.0.0.1") {
    auto node = std::make_shared<rpc::GcsNodeInfo>();
    node->set_node_id(NodeID::FromRandom().Binary());
    node->set_node_manager_port(port);
    node->set_node_manager_address(address);
    return node;
  }

  static std::shared_ptr<rpc::JobTableData> GenJobTableData(JobID job_id) {
    auto job_table_data = std::make_shared<rpc::JobTableData>();
    job_table_data->set_job_id(job_id.Binary());
    job_table_data->set_is_dead(false);
    job_table_data->set_timestamp(current_sys_time_ms());
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

  static std::shared_ptr<rpc::ProfileTableData> GenProfileTableData(
      const NodeID &node_id) {
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

  static std::shared_ptr<rpc::AddJobRequest> GenAddJobRequest(
      const JobID &job_id,
      const std::string &ray_namespace,
      uint32_t num_java_worker_per_process) {
    auto job_config_data = std::make_shared<rpc::JobConfig>();
    job_config_data->set_ray_namespace(ray_namespace);
    job_config_data->set_num_java_workers_per_process(num_java_worker_per_process);

    auto job_table_data = std::make_shared<rpc::JobTableData>();
    job_table_data->set_job_id(job_id.Binary());
    job_table_data->mutable_config()->CopyFrom(*job_config_data);

    auto add_job_request = std::make_shared<rpc::AddJobRequest>();
    add_job_request->mutable_data()->CopyFrom(*job_table_data);
    return add_job_request;
  }
};

}  // namespace ray
