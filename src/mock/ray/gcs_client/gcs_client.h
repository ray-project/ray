// Copyright  The Ray Authors.
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

#include "mock/ray/gcs_client/accessor.h"
#include "ray/gcs_rpc_client/gcs_client.h"

namespace ray {
namespace gcs {

class MockGcsClientOptions : public GcsClientOptions {
 public:
};

}  // namespace gcs
}  // namespace ray

namespace ray {
namespace gcs {

class MockGcsClient : public GcsClient {
 public:
  MOCK_METHOD(Status,
              Connect,
              (instrumented_io_context & io_service, int64_t timeout_ms),
              (override));
  MOCK_METHOD(void, Disconnect, (), (override));
  MOCK_METHOD((std::pair<std::string, int>), GetGcsServerAddress, (), (const, override));
  MOCK_METHOD(std::string, DebugString, (), (const, override));
  MOCK_METHOD(ClusterID, GetClusterId, (), (const, override));
  MOCK_METHOD(InternalKVAccessorInterface &, InternalKV, (), (override));
  MOCK_METHOD(pubsub::GcsSubscriber &, GetGcsSubscriber, (), (override));
  MOCK_METHOD(rpc::GcsRpcClient &, GetGcsRpcClient, (), (override));

  // Override accessor methods to return our mock accessors
  ActorInfoAccessorInterface &Actors() { return *mock_actor_accessor; }
  JobInfoAccessorInterface &Jobs() { return *mock_job_accessor; }
  NodeInfoAccessorInterface &Nodes() { return *mock_node_accessor; }
  NodeResourceInfoAccessorInterface &NodeResources() {
    return *mock_node_resource_accessor;
  }
  ErrorInfoAccessorInterface &Errors() { return *mock_error_accessor; }
  TaskInfoAccessorInterface &Tasks() { return *mock_task_accessor; }
  WorkerInfoAccessorInterface &Workers() { return *mock_worker_accessor; }
  PlacementGroupInfoAccessorInterface &PlacementGroups() {
    return *mock_placement_group_accessor;
  }
  RuntimeEnvAccessorInterface &RuntimeEnvs() { return *mock_runtime_env_accessor; }
  AutoscalerStateAccessorInterface &Autoscaler() {
    return *mock_autoscaler_state_accessor;
  }
  PublisherAccessorInterface &Publisher() { return *mock_publisher_accessor; }

  MockGcsClient() {
    // Create mock accessors for backward compatibility with existing tests
    mock_actor_accessor = std::make_shared<MockActorInfoAccessor>();
    mock_job_accessor = std::make_shared<MockJobInfoAccessor>();
    mock_node_accessor = std::make_shared<MockNodeInfoAccessor>();
    mock_node_resource_accessor = std::make_shared<MockNodeResourceInfoAccessor>();
    mock_error_accessor = std::make_shared<MockErrorInfoAccessor>();
    mock_task_accessor = std::make_shared<MockTaskInfoAccessor>();
    mock_worker_accessor = std::make_shared<MockWorkerInfoAccessor>();
    mock_placement_group_accessor = std::make_shared<MockPlacementGroupInfoAccessor>();
    mock_internal_kv_accessor = std::make_shared<MockInternalKVAccessor>();
    mock_runtime_env_accessor = std::make_shared<MockRuntimeEnvAccessor>();
    mock_autoscaler_state_accessor = std::make_shared<MockAutoscalerStateAccessor>();
    mock_publisher_accessor = std::make_shared<MockPublisherAccessor>();
  }

  // Mock accessor members for backward compatibility with existing tests
  std::shared_ptr<MockActorInfoAccessor> mock_actor_accessor;
  std::shared_ptr<MockJobInfoAccessor> mock_job_accessor;
  std::shared_ptr<MockNodeInfoAccessor> mock_node_accessor;
  std::shared_ptr<MockNodeResourceInfoAccessor> mock_node_resource_accessor;
  std::shared_ptr<MockErrorInfoAccessor> mock_error_accessor;
  std::shared_ptr<MockTaskInfoAccessor> mock_task_accessor;
  std::shared_ptr<MockWorkerInfoAccessor> mock_worker_accessor;
  std::shared_ptr<MockPlacementGroupInfoAccessor> mock_placement_group_accessor;
  std::shared_ptr<MockInternalKVAccessor> mock_internal_kv_accessor;
  std::shared_ptr<MockRuntimeEnvAccessor> mock_runtime_env_accessor;
  std::shared_ptr<MockAutoscalerStateAccessor> mock_autoscaler_state_accessor;
  std::shared_ptr<MockPublisherAccessor> mock_publisher_accessor;
};

}  // namespace gcs
}  // namespace ray
