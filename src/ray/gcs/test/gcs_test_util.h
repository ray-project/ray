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
#include "ray/gcs/pb_util.h"
#include "src/ray/protobuf/autoscaler.grpc.pb.h"
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
    static rpc::JobConfig kJobConfig;
    auto actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
    auto task_id = TaskID::ForActorCreationTask(actor_id);
    FunctionDescriptor function_descriptor;
    function_descriptor = FunctionDescriptorBuilder::BuildPython("", "", "", "");
    builder.SetCommonTaskSpec(task_id,
                              name + ":" + function_descriptor->CallString(),
                              Language::PYTHON,
                              function_descriptor,
                              job_id,
                              kJobConfig,
                              TaskID::Nil(),
                              0,
                              TaskID::Nil(),
                              owner_address,
                              1,
                              false,
                              false,
                              required_resources,
                              required_placement_resources,
                              "",
                              0,
                              TaskID::Nil());
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
      const std::string &ray_namespace = "test") {
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
                                  /* max_cpu_fraction_per_node */ 1.0,
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
      uint16_t port = 0,
      const std::string address = "127.0.0.1",
      const std::string node_name = "Mocker_node") {
    auto node = std::make_shared<rpc::GcsNodeInfo>();
    node->set_node_id(NodeID::FromRandom().Binary());
    node->set_node_manager_port(port);
    node->set_node_manager_address(address);
    node->set_node_name(node_name);
    node->set_instance_id("instance_x");
    node->set_state(rpc::GcsNodeInfo::ALIVE);
    return node;
  }

  static std::shared_ptr<rpc::JobTableData> GenJobTableData(JobID job_id) {
    auto job_table_data = std::make_shared<rpc::JobTableData>();
    job_table_data->set_job_id(job_id.Binary());
    job_table_data->set_is_dead(false);
    job_table_data->set_timestamp(current_sys_time_ms());
    job_table_data->set_driver_ip_address("127.0.0.1");
    rpc::Address address;
    address.set_ip_address("127.0.0.1");
    address.set_port(1234);
    address.set_raylet_id(UniqueID::FromRandom().Binary());
    address.set_worker_id(UniqueID::FromRandom().Binary());
    job_table_data->mutable_driver_address()->CopyFrom(address);
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
      const std::optional<std::string> &submission_id = std::nullopt,
      const std::optional<rpc::Address> &address = std::nullopt) {
    auto job_config_data = std::make_shared<rpc::JobConfig>();
    job_config_data->set_ray_namespace(ray_namespace);

    auto job_table_data = std::make_shared<rpc::JobTableData>();
    job_table_data->set_job_id(job_id.Binary());
    job_table_data->mutable_config()->CopyFrom(*job_config_data);
    if (address.has_value()) {
      job_table_data->mutable_driver_address()->CopyFrom(address.value());
    } else {
      rpc::Address dummy_address;
      dummy_address.set_port(1234);
      dummy_address.set_raylet_id(NodeID::FromRandom().Binary());
      dummy_address.set_ip_address("123.456.7.8");
      dummy_address.set_worker_id(WorkerID::FromRandom().Binary());
      job_table_data->mutable_driver_address()->CopyFrom(dummy_address);
    }
    if (submission_id.has_value()) {
      job_table_data->mutable_config()->mutable_metadata()->insert(
          {"job_submission_id", submission_id.value()});
    }

    auto add_job_request = std::make_shared<rpc::AddJobRequest>();
    add_job_request->mutable_data()->CopyFrom(*job_table_data);
    return add_job_request;
  }

  static rpc::TaskEventData GenTaskEventsData(
      const std::vector<rpc::TaskEvents> &task_events,
      int32_t num_profile_task_events_dropped = 0,
      int32_t num_status_task_events_dropped = 0) {
    rpc::TaskEventData data;
    for (auto &events : task_events) {
      auto new_events = data.add_events_by_task();
      new_events->CopyFrom(events);
    }

    for (int i = 0; i < num_status_task_events_dropped; ++i) {
      rpc::TaskAttempt rpc_task_attempt;
      rpc_task_attempt.set_task_id(RandomTaskId().Binary());
      rpc_task_attempt.set_attempt_number(0);
      *(data.add_dropped_task_attempts()) = rpc_task_attempt;
    }

    data.set_num_profile_events_dropped(num_profile_task_events_dropped);
    data.set_job_id(JobID::FromInt(0).Binary());

    return data;
  }

  static rpc::ResourceDemand GenResourceDemand(
      const absl::flat_hash_map<std::string, double> &resource_demands,
      int64_t num_ready_queued,
      int64_t num_infeasible,
      int64_t num_backlog) {
    rpc::ResourceDemand resource_demand;
    for (const auto &resource : resource_demands) {
      (*resource_demand.mutable_shape())[resource.first] = resource.second;
    }
    resource_demand.set_num_ready_requests_queued(num_ready_queued);
    resource_demand.set_num_infeasible_requests_queued(num_infeasible);
    resource_demand.set_backlog_size(num_backlog);
    return resource_demand;
  }

  static void FillResourcesData(
      rpc::ResourcesData &resources_data,
      const NodeID &node_id,
      const absl::flat_hash_map<std::string, double> &available_resources,
      const absl::flat_hash_map<std::string, double> &total_resources,
      int64_t idle_ms = 0,
      bool is_draining = false) {
    resources_data.set_node_id(node_id.Binary());
    for (const auto &resource : available_resources) {
      (*resources_data.mutable_resources_available())[resource.first] = resource.second;
    }
    for (const auto &resource : total_resources) {
      (*resources_data.mutable_resources_total())[resource.first] = resource.second;
    }
    resources_data.set_idle_duration_ms(idle_ms);
    resources_data.set_is_draining(is_draining);
  }

  static void FillResourcesData(rpc::ResourcesData &data,
                                const std::string &node_id,
                                std::vector<rpc::ResourceDemand> demands,
                                bool resource_load_changed = true) {
    auto load_by_shape = data.mutable_resource_load_by_shape();
    auto agg_load = data.mutable_resource_load();
    for (const auto &demand : demands) {
      load_by_shape->add_resource_demands()->CopyFrom(demand);
      for (const auto &resource : demand.shape()) {
        (*agg_load)[resource.first] +=
            (resource.second * (demand.num_ready_requests_queued() +
                                demand.num_infeasible_requests_queued()));
      }
    }
    data.set_resource_load_changed(resource_load_changed);
    data.set_node_id(node_id);
  }

  static std::shared_ptr<rpc::PlacementGroupLoad> GenPlacementGroupLoad(
      std::vector<rpc::PlacementGroupTableData> placement_group_table_data_vec) {
    auto placement_group_load = std::make_shared<rpc::PlacementGroupLoad>();
    for (auto &placement_group_table_data : placement_group_table_data_vec) {
      placement_group_load->add_placement_group_data()->CopyFrom(
          placement_group_table_data);
    }
    return placement_group_load;
  }

  static rpc::PlacementGroupTableData GenPlacementGroupTableData(
      const PlacementGroupID &placement_group_id,
      const JobID &job_id,
      const std::vector<std::unordered_map<std::string, double>> &bundles,
      const std::vector<std::string> &nodes,
      rpc::PlacementStrategy strategy,
      const rpc::PlacementGroupTableData::PlacementGroupState state,
      const std::string &name = "",
      const ActorID &actor_id = ActorID::Nil()) {
    rpc::PlacementGroupTableData placement_group_table_data;
    placement_group_table_data.set_placement_group_id(placement_group_id.Binary());
    placement_group_table_data.set_state(state);
    placement_group_table_data.set_name(name);
    placement_group_table_data.set_strategy(strategy);
    RAY_CHECK(bundles.size() == nodes.size());
    size_t i = 0;
    for (auto &bundle : bundles) {
      // Add unit resources
      auto bundle_spec = placement_group_table_data.add_bundles();
      for (auto &resource : bundle) {
        (*bundle_spec->mutable_unit_resources())[resource.first] = resource.second;
      }

      // Add node id
      const auto &node = nodes[i];
      if (!node.empty()) {
        bundle_spec->set_node_id(node);
      }

      i++;
    }
    return placement_group_table_data;
  }
  static rpc::autoscaler::ClusterResourceConstraint GenClusterResourcesConstraint(
      const std::vector<std::unordered_map<std::string, double>> &request_resources,
      const std::vector<int64_t> &count_array) {
    rpc::autoscaler::ClusterResourceConstraint constraint;
    RAY_CHECK(request_resources.size() == count_array.size());
    for (size_t i = 0; i < request_resources.size(); i++) {
      auto &resource = request_resources[i];
      auto count = count_array[i];
      auto bundle = constraint.add_min_bundles();
      bundle->set_count(count);
      bundle->mutable_request()->mutable_resources_bundle()->insert(resource.begin(),
                                                                    resource.end());
    }
    return constraint;
  }
};

}  // namespace ray
