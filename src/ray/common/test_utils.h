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

#include <functional>
#include <future>
#include <string>

#include "gtest/gtest.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/id.h"
#include "ray/common/placement_group.h"
#include "ray/common/task/task_spec.h"
#include "src/ray/protobuf/autoscaler.pb.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.grpc.pb.h"

namespace ray {

static inline std::vector<rpc::ObjectReference> ObjectIdsToRefs(
    std::vector<ObjectID> object_ids) {
  std::vector<rpc::ObjectReference> refs;
  for (const auto &object_id : object_ids) {
    rpc::ObjectReference ref;
    ref.set_object_id(object_id.Binary());
    refs.push_back(ref);
  }
  return refs;
}

class Buffer;
class RayObject;

/// Wait until the future is ready, or timeout is reached.
///
/// \param[in] future The future to wait for.
/// \param[in] timeout_ms Timeout in milliseconds to wait for for.
/// \return Whether the future is ready.
bool WaitReady(std::future<bool> future, const std::chrono::milliseconds &timeout_ms);

/// Wait until the condition is met, or timeout is reached.
///
/// \param[in] condition The condition to wait for.
/// \param[in] timeout_ms Timeout in milliseconds to wait for for.
/// \return Whether the condition is met.
bool WaitForCondition(std::function<bool()> condition, int timeout_ms);

/// Wait until the expected count is met, or timeout is reached.
///
/// \param[in] current_count The current count.
/// \param[in] expected_count The expected count.
/// \param[in] timeout_ms Timeout in milliseconds to wait for for.
/// \return Whether the expected count is met.
void WaitForExpectedCount(std::atomic<int> &current_count,
                          int expected_count,
                          int timeout_ms = 60000);

// A helper function to return a random task id.
TaskID RandomTaskId();

// A helper function to return a random job id.
JobID RandomJobId();

std::shared_ptr<Buffer> GenerateRandomBuffer();

std::shared_ptr<RayObject> GenerateRandomObject(
    const std::vector<ObjectID> &inlined_ids = {});

/// Path to redis server executable binary.
extern std::string TEST_REDIS_SERVER_EXEC_PATH;
/// Path to redis client executable binary.
extern std::string TEST_REDIS_CLIENT_EXEC_PATH;
/// Ports of redis server.
extern std::vector<int> TEST_REDIS_SERVER_PORTS;

//--------------------------------------------------------------------------------
// COMPONENT MANAGEMENT CLASSES FOR TEST CASES
//--------------------------------------------------------------------------------
/// Test cases can use it to start/stop/flush redis server(s).
class TestSetupUtil {
 public:
  static void StartUpRedisServers(const std::vector<int> &redis_server_ports,
                                  bool save = false);
  static void ShutDownRedisServers();
  static void FlushAllRedisServers();

  static void ExecuteRedisCmd(int port, std::vector<std::string> cmd);
  static int StartUpRedisServer(int port, bool save = false);
  static void ShutDownRedisServer(int port);
  static void FlushRedisServer(int port);
};

template <size_t k, typename T>
struct SaveArgToUniquePtrAction {
  std::unique_ptr<T> *pointer;

  template <typename... Args>
  void operator()(const Args &...args) const {
    *pointer = std::make_unique<T>(std::get<k>(std::tie(args...)));
  }
};

// Copies the k-th arg with make_unique(arg<k>) into ptr.
template <size_t k, typename T>
SaveArgToUniquePtrAction<k, T> SaveArgToUniquePtr(std::unique_ptr<T> *ptr) {
  return {ptr};
}

TaskSpecification GenActorCreationTask(
    const JobID &job_id,
    int max_restarts,
    bool detached,
    const std::string &name,
    const std::string &ray_namespace,
    const rpc::Address &owner_address,
    std::unordered_map<std::string, double> required_resources =
        std::unordered_map<std::string, double>(),
    std::unordered_map<std::string, double> required_placement_resources =
        std::unordered_map<std::string, double>());

rpc::CreateActorRequest GenCreateActorRequest(const JobID &job_id,
                                              int max_restarts = 0,
                                              bool detached = false,
                                              const std::string &name = "",
                                              const std::string &ray_namespace = "");

rpc::RegisterActorRequest GenRegisterActorRequest(
    const JobID &job_id,
    int max_restarts = 0,
    bool detached = false,
    const std::string &name = "",
    const std::string &ray_namespace = "test");

PlacementGroupSpecification GenPlacementGroupCreation(
    const std::string &name,
    std::vector<std::unordered_map<std::string, double>> &bundles,
    rpc::PlacementStrategy strategy,
    const JobID &job_id,
    const ActorID &actor_id);

rpc::CreatePlacementGroupRequest GenCreatePlacementGroupRequest(
    const std::string name = "",
    rpc::PlacementStrategy strategy = rpc::PlacementStrategy::SPREAD,
    int bundles_count = 2,
    double cpu_num = 1.0,
    const JobID job_id = JobID::FromInt(1),
    const ActorID &actor_id = ActorID::Nil());

std::shared_ptr<rpc::GcsNodeInfo> GenNodeInfo(
    uint16_t port = 0,
    const std::string address = "127.0.0.1",
    const std::string node_name = "Mocker_node");

std::shared_ptr<rpc::JobTableData> GenJobTableData(JobID job_id);

std::shared_ptr<rpc::ActorTableData> GenActorTableData(const JobID &job_id);

std::shared_ptr<rpc::ErrorTableData> GenErrorTableData(const JobID &job_id);

std::shared_ptr<rpc::WorkerTableData> GenWorkerTableData();

std::shared_ptr<rpc::AddJobRequest> GenAddJobRequest(
    const JobID &job_id,
    const std::string &ray_namespace,
    const std::optional<std::string> &submission_id = std::nullopt,
    const std::optional<rpc::Address> &address = std::nullopt);

rpc::TaskEventData GenTaskEventsData(const std::vector<rpc::TaskEvents> &task_events,
                                     int32_t num_profile_task_events_dropped = 0,
                                     int32_t num_status_task_events_dropped = 0);

rpc::events::RayEventsData GenRayEventsData(
    const std::vector<rpc::TaskEvents> &task_events,
    const std::vector<TaskAttempt> &drop_tasks);

rpc::TaskEventData GenTaskEventsDataLoss(const std::vector<TaskAttempt> &drop_tasks,
                                         int job_id = 0);

rpc::ResourceDemand GenResourceDemand(
    const absl::flat_hash_map<std::string, double> &resource_demands,
    int64_t num_ready_queued,
    int64_t num_infeasible,
    int64_t num_backlog,
    const std::vector<ray::rpc::LabelSelector> &label_selectors = {});

void FillResourcesData(
    rpc::ResourcesData &resources_data,
    const NodeID &node_id,
    const absl::flat_hash_map<std::string, double> &available_resources,
    const absl::flat_hash_map<std::string, double> &total_resources,
    int64_t idle_ms = 0,
    bool is_draining = false,
    int64_t draining_deadline_timestamp_ms = -1);

void FillResourcesData(rpc::ResourcesData &data,
                       const std::string &node_id,
                       std::vector<rpc::ResourceDemand> demands);

std::shared_ptr<rpc::PlacementGroupLoad> GenPlacementGroupLoad(
    std::vector<rpc::PlacementGroupTableData> placement_group_table_data_vec);

rpc::PlacementGroupTableData GenPlacementGroupTableData(
    const PlacementGroupID &placement_group_id,
    const JobID &job_id,
    const std::vector<std::unordered_map<std::string, double>> &bundles,
    const std::vector<std::string> &nodes,
    rpc::PlacementStrategy strategy,
    const rpc::PlacementGroupTableData::PlacementGroupState state,
    const std::string &name = "",
    const ActorID &actor_id = ActorID::Nil());

rpc::autoscaler::ClusterResourceConstraint GenClusterResourcesConstraint(
    const std::vector<std::unordered_map<std::string, double>> &request_resources,
    const std::vector<int64_t> &count_array);

// Read all lines of a file into vector vc
void ReadContentFromFile(std::vector<std::string> &vc, std::string log_file);

}  // namespace ray
