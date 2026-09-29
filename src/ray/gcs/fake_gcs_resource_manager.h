// Copyright The Ray Authors.
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
#include <vector>

#include "ray/asio/instrumented_io_context.h"
#include "ray/asio/periodical_runner.h"
#include "ray/gcs/fake_gcs_node_manager.h"
#include "ray/gcs/gcs_resource_manager.h"
#include "ray/util/clock.h"

namespace ray {
namespace gcs {

// Owns the io_context (and, for the default constructor, a ClusterResourceManager
// and GcsNodeManager) that back GcsResourceManager. Inherited privately and first
// by the fake so these are constructed before the GcsResourceManager base
// (base-from-member idiom); plain members would be constructed after the base,
// which reads them.
struct FakeGcsResourceManagerDeps {
  instrumented_io_context io_context;
  ClusterResourceManager cluster_resource_manager{PeriodicalRunner::Create(io_context)};
  FakeGcsNodeManager node_manager;
};

// Hand-written fake for GcsResourceManager. Overrides the two autoscaler RPC
// handlers with no-op recording bodies.
class FakeGcsResourceManager : private FakeGcsResourceManagerDeps,
                               public GcsResourceManager {
 public:
  // Inherit the real constructors for tests that supply their own io_context,
  // cluster resource manager, and node manager.
  using GcsResourceManager::GcsResourceManager;

  explicit FakeGcsResourceManager()
      : GcsResourceManager(
            io_context, cluster_resource_manager, node_manager, NodeID::FromRandom()) {}

  explicit FakeGcsResourceManager(ClusterResourceManager &cluster_resource_manager,
                                  GcsNodeManager &gcs_node_manager)
      : GcsResourceManager(io_context,
                           cluster_resource_manager,
                           gcs_node_manager,
                           NodeID::FromRandom()) {}

  void HandleGetAllAvailableResources(
      rpc::GetAllAvailableResourcesRequest request,
      rpc::GetAllAvailableResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    get_all_available_resources_calls.push_back(request);
    if (handle_get_all_available_resources) {
      handle_get_all_available_resources(
          std::move(request), reply, std::move(send_reply_callback));
    }
  }

  void HandleGetAllResourceUsage(rpc::GetAllResourceUsageRequest request,
                                 rpc::GetAllResourceUsageReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override {
    get_all_resource_usage_calls.push_back(request);
    if (handle_get_all_resource_usage) {
      handle_get_all_resource_usage(
          std::move(request), reply, std::move(send_reply_callback));
    }
  }

  // Recorded calls.
  std::vector<rpc::GetAllAvailableResourcesRequest> get_all_available_resources_calls;
  std::vector<rpc::GetAllResourceUsageRequest> get_all_resource_usage_calls;

  // Optional hooks.
  std::function<void(rpc::GetAllAvailableResourcesRequest,
                     rpc::GetAllAvailableResourcesReply *,
                     rpc::SendReplyCallback)>
      handle_get_all_available_resources;
  std::function<void(rpc::GetAllResourceUsageRequest,
                     rpc::GetAllResourceUsageReply *,
                     rpc::SendReplyCallback)>
      handle_get_all_resource_usage;
};

}  // namespace gcs
}  // namespace ray
