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

#include "ray/common/test_utils.h"

#include <fstream>
#include <functional>
#ifndef _WIN32
#include <sys/socket.h>
#else
#include <winsock2.h>
#endif

#include "absl/strings/escaping.h"
#include "ray/common/buffer.h"
#include "ray/common/ray_object.h"
#include "ray/common/task/task_util.h"
#include "ray/util/cmd_line_utils.h"
#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/network_util.h"
#include "ray/util/path_utils.h"
#include "ray/util/process.h"
#include "ray/util/time.h"

namespace ray {

void TestSetupUtil::StartUpRedisServers(const std::vector<int> &redis_server_ports,
                                        bool save) {
  if (redis_server_ports.empty()) {
    TEST_REDIS_SERVER_PORTS.push_back(StartUpRedisServer(0, save));
  } else {
    for (const auto &port : redis_server_ports) {
      TEST_REDIS_SERVER_PORTS.push_back(StartUpRedisServer(port, save));
    }
  }
}

// start a redis server with specified port, use random one when 0 given
int TestSetupUtil::StartUpRedisServer(int port, bool save) {
  int actual_port = port;
  if (port == 0) {
    static std::atomic<bool> srand_called(false);
    if (!srand_called.exchange(true)) {
      srand(current_time_ms() % RAND_MAX);
    }
    // Use random port (in range [2000, 7000) to avoid port conflicts between UTs.
    do {
      actual_port = rand() % 5000 + 2000;
    } while (!CheckPortFree(AF_INET, actual_port));
  }

  std::string program = TEST_REDIS_SERVER_EXEC_PATH;
#ifdef _WIN32
  std::vector<std::string> cmdargs({program, "--loglevel", "warning"});
#else
  std::vector<std::string> cmdargs;
  if (!save) {
    cmdargs = {program, "--loglevel", "warning", "--save", "", "--appendonly", "no"};
  } else {
    cmdargs = {program, "--loglevel", "warning"};
  }
#endif
  cmdargs.insert(cmdargs.end(), {"--port", std::to_string(actual_port)});
  RAY_LOG(INFO) << "Start redis command is: " << CreateCommandLine(cmdargs);
  RAY_CHECK(!Process::Spawn(cmdargs, true).second);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  return actual_port;
}

void TestSetupUtil::ShutDownRedisServers() {
  for (const auto &port : TEST_REDIS_SERVER_PORTS) {
    ShutDownRedisServer(port);
  }
  TEST_REDIS_SERVER_PORTS = std::vector<int>();
}

void TestSetupUtil::ShutDownRedisServer(int port) {
  std::vector<std::string> cmdargs(
      {TEST_REDIS_CLIENT_EXEC_PATH, "-p", std::to_string(port), "shutdown"});
  RAY_LOG(INFO) << "Stop redis command is: " << CreateCommandLine(cmdargs);
  if (Process::Call(cmdargs) != std::error_code()) {
    RAY_LOG(WARNING) << "Failed to stop redis. The redis process may no longer exist.";
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

void TestSetupUtil::FlushAllRedisServers() {
  for (const auto &port : TEST_REDIS_SERVER_PORTS) {
    FlushRedisServer(port);
  }
}

void TestSetupUtil::ExecuteRedisCmd(int port, std::vector<std::string> cmd) {
  std::vector<std::string> cmdargs(
      {TEST_REDIS_CLIENT_EXEC_PATH, "-p", std::to_string(port)});
  cmdargs.insert(cmdargs.end(), cmd.begin(), cmd.end());
  RAY_LOG(INFO) << "Send command to redis: " << CreateCommandLine(cmdargs);
  if (Process::Call(cmdargs)) {
    RAY_LOG(WARNING) << "Failed to send request to redis.";
  }
}

void TestSetupUtil::FlushRedisServer(int port) {
  std::vector<std::string> cmdargs(
      {TEST_REDIS_CLIENT_EXEC_PATH, "-p", std::to_string(port), "flushall"});
  RAY_LOG(INFO) << "Cleaning up redis with command: " << CreateCommandLine(cmdargs);
  if (Process::Call(cmdargs)) {
    RAY_LOG(WARNING) << "Failed to flush redis. The redis process may no longer exist.";
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

bool WaitReady(std::future<bool> future, const std::chrono::milliseconds &timeout_ms) {
  auto status = future.wait_for(timeout_ms);
  return status == std::future_status::ready && future.get();
}

bool WaitForCondition(std::function<bool()> condition, int timeout_ms) {
  int wait_time = 0;
  while (true) {
    if (condition()) {
      return true;
    }

    // sleep 10ms.
    const int wait_interval_ms = 10;
    std::this_thread::sleep_for(std::chrono::milliseconds(wait_interval_ms));
    wait_time += wait_interval_ms;
    if (wait_time > timeout_ms) {
      break;
    }
  }
  return false;
}

void WaitForExpectedCount(std::atomic<int> &current_count,
                          int expected_count,
                          int timeout_ms) {
  auto condition = [&current_count, expected_count]() {
    return current_count == expected_count;
  };
  EXPECT_TRUE(WaitForCondition(condition, timeout_ms));
}

TaskID RandomTaskId() {
  std::string data(TaskID::Size(), 0);
  FillRandom(&data);
  return TaskID::FromBinary(data);
}

JobID RandomJobId() {
  std::string data(JobID::Size(), 0);
  FillRandom(&data);
  return JobID::FromBinary(data);
}

std::shared_ptr<Buffer> GenerateRandomBuffer() {
  auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::mt19937 gen(seed);
  std::uniform_int_distribution<> dis(1, 10);
  std::uniform_int_distribution<> value_dis(1, 255);

  std::vector<uint8_t> arg1(dis(gen), value_dis(gen));
  return std::make_shared<LocalMemoryBuffer>(arg1.data(), arg1.size(), true);
}

std::shared_ptr<RayObject> GenerateRandomObject(
    const std::vector<ObjectID> &inlined_ids) {
  std::vector<rpc::ObjectReference> refs;
  for (const auto &inlined_id : inlined_ids) {
    rpc::ObjectReference ref;
    ref.set_object_id(inlined_id.Binary());
    refs.push_back(ref);
  }
  return std::make_shared<RayObject>(GenerateRandomBuffer(), nullptr, refs);
}

TaskSpecification GenActorCreationTask(
    const JobID &job_id,
    int max_restarts,
    bool detached,
    const std::string &name,
    const std::string &ray_namespace,
    const rpc::Address &owner_address,
    std::unordered_map<std::string, double> required_resources,
    std::unordered_map<std::string, double> required_placement_resources) {
  TaskSpecBuilder builder;
  rpc::JobConfig kJobConfig;
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
                            -1,
                            required_resources,
                            required_placement_resources,
                            "",
                            0,
                            TaskID::Nil(),
                            "");
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
  return std::move(builder).ConsumeAndBuild();
}

rpc::CreateActorRequest GenCreateActorRequest(const JobID &job_id,
                                              int max_restarts,
                                              bool detached,
                                              const std::string &name,
                                              const std::string &ray_namespace) {
  rpc::Address owner_address;
  owner_address.set_node_id(NodeID::FromRandom().Binary());
  owner_address.set_ip_address("1234");
  owner_address.set_port(5678);
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());
  auto actor_creation_task_spec = GenActorCreationTask(
      job_id, max_restarts, detached, name, ray_namespace, owner_address);
  rpc::CreateActorRequest request;
  request.mutable_task_spec()->CopyFrom(actor_creation_task_spec.GetMessage());
  return request;
}

rpc::RegisterActorRequest GenRegisterActorRequest(const JobID &job_id,
                                                  int max_restarts,
                                                  bool detached,
                                                  const std::string &name,
                                                  const std::string &ray_namespace) {
  rpc::Address owner_address;
  owner_address.set_node_id(NodeID::FromRandom().Binary());
  owner_address.set_ip_address("1234");
  owner_address.set_port(5678);
  owner_address.set_worker_id(WorkerID::FromRandom().Binary());
  auto actor_creation_task_spec = GenActorCreationTask(
      job_id, max_restarts, detached, name, ray_namespace, owner_address);
  rpc::RegisterActorRequest request;
  request.mutable_task_spec()->CopyFrom(actor_creation_task_spec.GetMessage());
  return request;
}

PlacementGroupSpecification GenPlacementGroupCreation(
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
                                /* soft_target_node_id */ NodeID::Nil(),
                                job_id,
                                actor_id,
                                /* is_creator_detached */ false);
  return builder.Build();
}

rpc::CreatePlacementGroupRequest GenCreatePlacementGroupRequest(
    const std::string name,
    rpc::PlacementStrategy strategy,
    int bundles_count,
    double cpu_num,
    const JobID job_id,
    const ActorID &actor_id) {
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
std::shared_ptr<rpc::GcsNodeInfo> GenNodeInfo(uint16_t port,
                                              const std::string address,
                                              const std::string node_name) {
  auto node = std::make_shared<rpc::GcsNodeInfo>();
  node->set_node_id(NodeID::FromRandom().Binary());
  node->set_node_manager_port(port);
  node->set_node_manager_address(address);
  node->set_node_name(node_name);
  node->set_instance_id("instance_x");
  node->set_state(rpc::GcsNodeInfo::ALIVE);
  return node;
}

std::shared_ptr<rpc::JobTableData> GenJobTableData(JobID job_id) {
  auto job_table_data = std::make_shared<rpc::JobTableData>();
  job_table_data->set_job_id(job_id.Binary());
  job_table_data->set_is_dead(false);
  job_table_data->set_timestamp(current_sys_time_ms());
  job_table_data->set_driver_ip_address("127.0.0.1");
  rpc::Address address;
  address.set_ip_address("127.0.0.1");
  address.set_port(1234);
  address.set_node_id(UniqueID::FromRandom().Binary());
  address.set_worker_id(UniqueID::FromRandom().Binary());
  job_table_data->mutable_driver_address()->CopyFrom(address);
  job_table_data->set_driver_pid(5667L);
  return job_table_data;
}

std::shared_ptr<rpc::ActorTableData> GenActorTableData(const JobID &job_id) {
  auto actor_table_data = std::make_shared<rpc::ActorTableData>();
  ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), 0);
  actor_table_data->set_actor_id(actor_id.Binary());
  actor_table_data->set_job_id(job_id.Binary());
  actor_table_data->set_state(rpc::ActorTableData::ALIVE);
  actor_table_data->set_max_restarts(1);
  actor_table_data->set_num_restarts(0);
  return actor_table_data;
}

std::shared_ptr<rpc::ErrorTableData> GenErrorTableData(const JobID &job_id) {
  auto error_table_data = std::make_shared<rpc::ErrorTableData>();
  error_table_data->set_job_id(job_id.Binary());
  return error_table_data;
}

std::shared_ptr<rpc::WorkerTableData> GenWorkerTableData() {
  auto worker_table_data = std::make_shared<rpc::WorkerTableData>();
  worker_table_data->set_timestamp(std::time(nullptr));
  return worker_table_data;
}

std::shared_ptr<rpc::AddJobRequest> GenAddJobRequest(
    const JobID &job_id,
    const std::string &ray_namespace,
    const std::optional<std::string> &submission_id,
    const std::optional<rpc::Address> &address) {
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
    dummy_address.set_node_id(NodeID::FromRandom().Binary());
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

rpc::TaskEventData GenTaskEventsData(const std::vector<rpc::TaskEvents> &task_events,
                                     int32_t num_profile_task_events_dropped,
                                     int32_t num_status_task_events_dropped) {
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

rpc::events::RayEventsData GenRayEventsData(
    const std::vector<rpc::TaskEvents> &task_events,
    const std::vector<TaskAttempt> &drop_tasks) {
  rpc::events::RayEventsData data;
  rpc::events::TaskEventsMetadata metadata;
  for (const auto &task_attempt : drop_tasks) {
    rpc::TaskAttempt rpc_task_attempt;
    rpc_task_attempt.set_task_id(task_attempt.first.Binary());
    rpc_task_attempt.set_attempt_number(task_attempt.second);
    *(metadata.add_dropped_task_attempts()) = rpc_task_attempt;
  }
  data.mutable_task_events_metadata()->CopyFrom(metadata);
  for (const auto &task_event : task_events) {
    rpc::events::RayEvent ray_event;
    rpc::events::TaskDefinitionEvent task_definition_event;
    task_definition_event.set_task_id(task_event.task_id());
    task_definition_event.set_task_attempt(task_event.attempt_number());
    task_definition_event.set_job_id(task_event.job_id());
    if (task_event.has_task_info()) {
      const auto &task_info = task_event.task_info();
      task_definition_event.set_task_type(task_info.type());
      task_definition_event.set_task_name(task_info.name());
      task_definition_event.set_language(task_info.language());
    }
    ray_event.set_event_id(task_event.task_id());
    ray_event.set_event_type(rpc::events::RayEvent::TASK_DEFINITION_EVENT);
    ray_event.set_message("test");
    ray_event.mutable_task_definition_event()->CopyFrom(task_definition_event);
    *(data.add_events()) = ray_event;
  }

  return data;
}

rpc::TaskEventData GenTaskEventsDataLoss(const std::vector<TaskAttempt> &drop_tasks,
                                         int job_id) {
  rpc::TaskEventData data;
  for (const auto &task_attempt : drop_tasks) {
    rpc::TaskAttempt rpc_task_attempt;
    rpc_task_attempt.set_task_id(task_attempt.first.Binary());
    rpc_task_attempt.set_attempt_number(task_attempt.second);
    *(data.add_dropped_task_attempts()) = rpc_task_attempt;
  }
  data.set_job_id(JobID::FromInt(job_id).Binary());

  return data;
}

rpc::ResourceDemand GenResourceDemand(
    const absl::flat_hash_map<std::string, double> &resource_demands,
    int64_t num_ready_queued,
    int64_t num_infeasible,
    int64_t num_backlog,
    const std::vector<ray::rpc::LabelSelector> &label_selectors) {
  rpc::ResourceDemand resource_demand;
  for (const auto &resource : resource_demands) {
    (*resource_demand.mutable_shape())[resource.first] = resource.second;
  }
  resource_demand.set_num_ready_requests_queued(num_ready_queued);
  resource_demand.set_num_infeasible_requests_queued(num_infeasible);
  resource_demand.set_backlog_size(num_backlog);
  for (const auto &selector : label_selectors) {
    *resource_demand.add_label_selectors() = selector;
  }
  return resource_demand;
}

void FillResourcesData(
    rpc::ResourcesData &resources_data,
    const NodeID &node_id,
    const absl::flat_hash_map<std::string, double> &available_resources,
    const absl::flat_hash_map<std::string, double> &total_resources,
    int64_t idle_ms,
    bool is_draining,
    int64_t draining_deadline_timestamp_ms) {
  resources_data.set_node_id(node_id.Binary());
  for (const auto &resource : available_resources) {
    (*resources_data.mutable_resources_available())[resource.first] = resource.second;
  }
  for (const auto &resource : total_resources) {
    (*resources_data.mutable_resources_total())[resource.first] = resource.second;
  }
  resources_data.set_idle_duration_ms(idle_ms);
  resources_data.set_is_draining(is_draining);
  resources_data.set_draining_deadline_timestamp_ms(draining_deadline_timestamp_ms);
}

void FillResourcesData(rpc::ResourcesData &data,
                       const std::string &node_id,
                       std::vector<rpc::ResourceDemand> demands) {
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
  data.set_node_id(node_id);
}

std::shared_ptr<rpc::PlacementGroupLoad> GenPlacementGroupLoad(
    std::vector<rpc::PlacementGroupTableData> placement_group_table_data_vec) {
  auto placement_group_load = std::make_shared<rpc::PlacementGroupLoad>();
  for (auto &placement_group_table_data : placement_group_table_data_vec) {
    placement_group_load->add_placement_group_data()->CopyFrom(
        placement_group_table_data);
  }
  return placement_group_load;
}

rpc::PlacementGroupTableData GenPlacementGroupTableData(
    const PlacementGroupID &placement_group_id,
    const JobID &job_id,
    const std::vector<std::unordered_map<std::string, double>> &bundles,
    const std::vector<std::string> &nodes,
    rpc::PlacementStrategy strategy,
    const rpc::PlacementGroupTableData::PlacementGroupState state,
    const std::string &name,
    const ActorID &actor_id) {
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
rpc::autoscaler::ClusterResourceConstraint GenClusterResourcesConstraint(
    const std::vector<std::unordered_map<std::string, double>> &request_resources,
    const std::vector<int64_t> &count_array) {
  rpc::autoscaler::ClusterResourceConstraint constraint;
  RAY_CHECK(request_resources.size() == count_array.size());
  for (size_t i = 0; i < request_resources.size(); i++) {
    auto &resource = request_resources[i];
    auto count = count_array[i];
    auto bundle = constraint.add_resource_requests();
    bundle->set_count(count);
    bundle->mutable_request()->mutable_resources_bundle()->insert(resource.begin(),
                                                                  resource.end());
  }
  return constraint;
}
// Read all lines of a file into vector vc
void ReadContentFromFile(std::vector<std::string> &vc, std::string log_file) {
  std::string line;
  std::ifstream read_file;
  read_file.open(log_file, std::ios::binary);
  while (std::getline(read_file, line)) {
    vc.push_back(line);
  }
  read_file.close();
}

/// Path to redis server executable binary.
std::string TEST_REDIS_SERVER_EXEC_PATH;
/// Path to redis client executable binary.
std::string TEST_REDIS_CLIENT_EXEC_PATH;
/// Ports of redis server.
std::vector<int> TEST_REDIS_SERVER_PORTS;

}  // namespace ray
