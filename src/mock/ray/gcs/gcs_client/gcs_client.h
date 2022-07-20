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

#include "mock/ray/gcs/gcs_client/accessor.h"

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
  MOCK_METHOD(Status, Connect, (instrumented_io_context & io_service), (override));
  MOCK_METHOD(void, Disconnect, (), (override));
  MOCK_METHOD((std::pair<std::string, int>), GetGcsServerAddress, (), (const, override));
  MOCK_METHOD(std::string, DebugString, (), (const, override));

  MockGcsClient() {
    mock_job_accessor = new MockJobInfoAccessor();
    mock_actor_accessor = new MockActorInfoAccessor();
    mock_node_accessor = new MockNodeInfoAccessor();
    mock_node_resource_accessor = new MockNodeResourceInfoAccessor();
    mock_error_accessor = new MockErrorInfoAccessor();
    mock_stats_accessor = new MockStatsInfoAccessor();
    mock_worker_accessor = new MockWorkerInfoAccessor();
    mock_placement_group_accessor = new MockPlacementGroupInfoAccessor();
    mock_internal_kv_accessor = new MockInternalKVAccessor();

    GcsClient::job_accessor_.reset(mock_job_accessor);
    GcsClient::actor_accessor_.reset(mock_actor_accessor);
    GcsClient::node_accessor_.reset(mock_node_accessor);
    GcsClient::node_resource_accessor_.reset(mock_node_resource_accessor);
    GcsClient::stats_accessor_.reset(mock_stats_accessor);
    GcsClient::error_accessor_.reset(mock_error_accessor);
    GcsClient::worker_accessor_.reset(mock_worker_accessor);
    GcsClient::placement_group_accessor_.reset(mock_placement_group_accessor);
  }
  MockActorInfoAccessor *mock_actor_accessor;
  MockJobInfoAccessor *mock_job_accessor;
  MockNodeInfoAccessor *mock_node_accessor;
  MockNodeResourceInfoAccessor *mock_node_resource_accessor;
  MockErrorInfoAccessor *mock_error_accessor;
  MockStatsInfoAccessor *mock_stats_accessor;
  MockWorkerInfoAccessor *mock_worker_accessor;
  MockPlacementGroupInfoAccessor *mock_placement_group_accessor;
  MockInternalKVAccessor *mock_internal_kv_accessor;
};

}  // namespace gcs
}  // namespace ray
