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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "ray/gcs/gcs_node_manager.h"
#include "ray/observability/fake_ray_event_recorder.h"
#include "ray/pubsub/fake_publisher.h"
#include "ray/pubsub/gcs_publisher.h"
#include "ray/util/clock.h"

namespace ray {
namespace gcs {

// Hand-written fake for GcsNodeManager. Subclasses the concrete manager and
// overrides the RPC handlers/DrainNode with no-op recording bodies. Public
// record vectors + optional std::function hooks let tests inspect calls and
// program behavior (replaces gmock EXPECT_CALL usage).
class FakeGcsNodeManager : public GcsNodeManager {
 public:
  FakeGcsNodeManager()
      : GcsNodeManager(/*gcs_publisher=*/nullptr,
                       /*gcs_table_storage=*/nullptr,
                       /*io_context=*/fake_io_context_,
                       /*raylet_client_pool=*/nullptr,
                       /*cluster_id=*/ClusterID::Nil(),
                       /*ray_event_recorder=*/fake_ray_event_recorder_,
                       /*session_name=*/"",
                       /*observability_publisher=*/FakeObsPublisher(),
                       /*clock=*/clock_) {}

  static pubsub::ObservabilityPublisher *FakeObsPublisher() {
    static auto holder = std::make_unique<pubsub::ObservabilityPublisher>(
        std::make_unique<pubsub::FakePublisher>());
    return holder.get();
  }

  void HandleRegisterNode(rpc::RegisterNodeRequest request,
                          rpc::RegisterNodeReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override {
    register_node_calls.push_back(request);
    if (handle_register_node) {
      handle_register_node(std::move(request), reply, std::move(send_reply_callback));
    }
  }

  void HandleDrainNode(rpc::DrainNodeRequest request,
                       rpc::DrainNodeReply *reply,
                       rpc::SendReplyCallback send_reply_callback) override {
    drain_node_calls.push_back(request);
    if (handle_drain_node) {
      handle_drain_node(std::move(request), reply, std::move(send_reply_callback));
    }
  }

  void HandleGetAllNodeInfo(rpc::GetAllNodeInfoRequest request,
                            rpc::GetAllNodeInfoReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override {
    get_all_node_info_calls.push_back(request);
    if (handle_get_all_node_info) {
      handle_get_all_node_info(std::move(request), reply, std::move(send_reply_callback));
    }
  }

  void DrainNode(const NodeID &node_id) override {
    drain_node_id_calls.push_back(node_id);
    if (drain_node) {
      drain_node(node_id);
    }
  }

  // Recorded calls.
  std::vector<rpc::RegisterNodeRequest> register_node_calls;
  std::vector<rpc::DrainNodeRequest> drain_node_calls;
  std::vector<rpc::GetAllNodeInfoRequest> get_all_node_info_calls;
  std::vector<NodeID> drain_node_id_calls;

  // Optional hooks.
  std::function<void(
      rpc::RegisterNodeRequest, rpc::RegisterNodeReply *, rpc::SendReplyCallback)>
      handle_register_node;
  std::function<void(
      rpc::DrainNodeRequest, rpc::DrainNodeReply *, rpc::SendReplyCallback)>
      handle_drain_node;
  std::function<void(
      rpc::GetAllNodeInfoRequest, rpc::GetAllNodeInfoReply *, rpc::SendReplyCallback)>
      handle_get_all_node_info;
  std::function<void(const NodeID &)> drain_node;

  instrumented_io_context fake_io_context_;
  observability::FakeRayEventRecorder fake_ray_event_recorder_;
  Clock clock_;
};

}  // namespace gcs
}  // namespace ray
