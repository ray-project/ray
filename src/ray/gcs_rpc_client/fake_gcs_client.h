// Copyright 2025 The Ray Authors.
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

#include <string>
#include <utility>

#include "ray/gcs_rpc_client/accessors/fake_actor_info_accessor.h"
#include "ray/gcs_rpc_client/fake_accessor.h"
#include "ray/gcs_rpc_client/gcs_client.h"

namespace ray {
namespace gcs {

// Hand-written fake GcsClient. Installs the
// hand-written Fake accessors (and the FakeActorInfoAccessor for the
// actor accessor) into the protected GcsClient::*_accessor_ members and exposes
// them as public typed pointers so tests can inspect recorded calls and drive
// stored callbacks. Connect/Disconnect/GetGcsServerAddress/DebugString are
// stubbed so no real GCS connection is ever attempted.
class FakeGcsClient : public GcsClient {
 public:
  FakeGcsClient() : GcsClient(GcsClientOptions()) {
    fake_job_accessor = new FakeJobInfoAccessor();
    fake_actor_accessor = new FakeActorInfoAccessor();
    fake_node_accessor = new FakeNodeInfoAccessor();
    fake_node_resource_accessor = new FakeNodeResourceInfoAccessor();
    fake_error_accessor = new FakeErrorInfoAccessor();
    fake_worker_accessor = new FakeWorkerInfoAccessor();
    fake_placement_group_accessor = new FakePlacementGroupInfoAccessor();
    fake_internal_kv_accessor = new FakeInternalKVAccessor();
    fake_task_accessor = new FakeTaskInfoAccessor();

    GcsClient::job_accessor_.reset(fake_job_accessor);
    GcsClient::actor_accessor_.reset(fake_actor_accessor);
    GcsClient::node_accessor_.reset(fake_node_accessor);
    GcsClient::node_resource_accessor_.reset(fake_node_resource_accessor);
    GcsClient::error_accessor_.reset(fake_error_accessor);
    GcsClient::worker_accessor_.reset(fake_worker_accessor);
    GcsClient::placement_group_accessor_.reset(fake_placement_group_accessor);
    GcsClient::internal_kv_accessor_.reset(fake_internal_kv_accessor);
    GcsClient::task_accessor_.reset(fake_task_accessor);
  }

  Status Connect(instrumented_io_context &io_service, int64_t timeout_ms = -1) override {
    connect_call_count++;
    return connect_status;
  }

  void Disconnect() override {}

  std::pair<std::string, int> GetGcsServerAddress() const override {
    return {"127.0.0.1", 0};
  }

  std::string DebugString() const override { return "FakeGcsClient"; }

  // Settable return / recorded state for Connect.
  Status connect_status = Status::OK();
  int connect_call_count = 0;

  FakeActorInfoAccessor *fake_actor_accessor;
  FakeJobInfoAccessor *fake_job_accessor;
  FakeNodeInfoAccessor *fake_node_accessor;
  FakeNodeResourceInfoAccessor *fake_node_resource_accessor;
  FakeErrorInfoAccessor *fake_error_accessor;
  FakeWorkerInfoAccessor *fake_worker_accessor;
  FakePlacementGroupInfoAccessor *fake_placement_group_accessor;
  FakeInternalKVAccessor *fake_internal_kv_accessor;
  FakeTaskInfoAccessor *fake_task_accessor;
};

}  // namespace gcs
}  // namespace ray
