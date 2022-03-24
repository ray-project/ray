// Copyright 2022 The Ray Authors.
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
#include <type_traits>

#include "ray/common/asio/periodical_runner.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_report_poller.h"
#include "ray/gcs/gcs_server/grpc_based_resource_broadcaster.h"

namespace ray {
class GcsPlacementGroupSchedulerTest;
namespace syncer {

// RaySyncer is a service to sync components in the cluster.
// It's supposed to be used to synchronize resource usage and scheduling information
// in ray cluster. The gold of this component is to make sure each node in the cluster
// eventually consistent on the information of each other.
// Right now, RaySyncer has two sub-components:
//    - poller: periodically get the message from the nodes in the cluster.
//    - broadcaster: periodically broadcast the information to the nodes.
// This component is still in developing. The goal of this node is to make raylet
// and GCS be able to use the same code and synchronize the information.
class RaySyncer {
 public:
  RaySyncer(instrumented_io_context &main_thread,
            std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
            ::ray::gcs::GcsResourceManager &gcs_resource_manager)
      : ticker_(main_thread), gcs_resource_manager_(gcs_resource_manager) {
    poller_ = std::make_unique<::ray::gcs::GcsResourceReportPoller>(
        raylet_client_pool, [this, &main_thread](const rpc::ResourcesData &data) {
          main_thread.post([this, data]() mutable { Update(std::move(data)); },
                           "ResourceUpdate");
        });

    broadcaster_ =
        std::make_unique<::ray::gcs::GrpcBasedResourceBroadcaster>(raylet_client_pool);
  }

  void Start() {
    poller_->Start();
    broadcast_thread_ = std::make_unique<std::thread>([this]() {
      SetThreadName("resource_bcast");
      boost::asio::io_service::work work(broadcast_service_);
      broadcast_service_.run();
    });

    // Perodically broadcast the messages received to other nodes.
    ticker_.RunFnPeriodically(
        [this] {
          auto beg = resources_buffer_.begin();
          auto ptr = beg;
          static auto max_batch = RayConfig::instance().resource_broadcast_batch_size();
          // Prepare the to-be-sent messages.
          for (size_t cnt = resources_buffer_proto_.batch().size();
               cnt < max_batch && cnt < resources_buffer_.size();
               ++ptr, ++cnt) {
            resources_buffer_proto_.add_batch()->mutable_data()->Swap(&ptr->second);
          }
          resources_buffer_.erase(beg, ptr);

          // Broadcast the messages to other nodes.
          broadcast_service_.dispatch(
              [this, resources = std::move(resources_buffer_proto_)]() mutable {
                broadcaster_->SendBroadcast(std::move(resources));
              },
              "SendBroadcast");
          resources_buffer_proto_.Clear();
        },
        RayConfig::instance().raylet_report_resources_period_milliseconds(),
        "RaySyncer.deadline_timer.report_resource_report");
  }

  void Stop() {
    poller_->Stop();
    if (broadcast_thread_ != nullptr) {
      broadcast_service_.stop();
      if (broadcast_thread_->joinable()) {
        broadcast_thread_->join();
      }
    }
  }

  /// This method should be used by:
  ///     1) GCS to report resource updates;
  ///     2) poller to report what have received.
  /// Right now it only put the received message into the buffer
  /// and broadcaster will use this to send the messages to other
  /// nodes.
  ///
  /// \\\param update: The messages should be sent. Right now we only support
  ///     rpc::NodeResourceChange and rpc::ResourcesData.
  ///
  /// TODO(iycheng): This API should be deleted in later PRs in favor of a more general
  /// solution.
  template <typename T>
  void Update(T update) {
    static_assert(std::is_same_v<T, rpc::NodeResourceChange> ||
                      std::is_same_v<T, rpc::ResourcesData>,
                  "unknown type");
    if constexpr (std::is_same_v<T, rpc::NodeResourceChange>) {
      resources_buffer_proto_.add_batch()->mutable_change()->Swap(&update);
    } else if constexpr (std::is_same_v<T, rpc::ResourcesData>) {
      gcs_resource_manager_.UpdateFromResourceReport(update);
      if (update.should_global_gc() || update.resources_total_size() > 0 ||
          update.resources_available_changed() || update.resource_load_changed()) {
        update.clear_resource_load();
        update.clear_resource_load_by_shape();
        update.clear_resources_normal_task();
        auto &orig = resources_buffer_[update.node_id()];
        orig.Swap(&update);
      }
    }
  }

  void Initialize(const ::ray::gcs::GcsInitData &gcs_init_data) {
    poller_->Initialize(gcs_init_data);
    broadcaster_->Initialize(gcs_init_data);
  }

  /// Handle a node registration.
  /// This will call the sub-components add function.
  ///
  /// \param node The specified node to add.
  void AddNode(const rpc::GcsNodeInfo &node_info) {
    broadcaster_->HandleNodeAdded(node_info);
    poller_->HandleNodeAdded(node_info);
  }

  /// Handle a node removal.
  /// This will call the sub-components removal function
  ///
  /// \param node The specified node to remove.
  void RemoveNode(const rpc::GcsNodeInfo &node_info) {
    broadcaster_->HandleNodeRemoved(node_info);
    poller_->HandleNodeRemoved(node_info);
    resources_buffer_.erase(node_info.node_id());
  }

  std::string DebugString() { return broadcaster_->DebugString(); }

 private:
  // Right now the threading is messy here due to legacy reason.
  // TODO (iycheng): Clean these up in follow-up PRs.

  // ticker is running from main thread.
  PeriodicalRunner ticker_;
  // The receiver of this syncer.
  // TODO (iycheng): Generalize this module in the future PR.
  ::ray::gcs::GcsResourceManager &gcs_resource_manager_;
  // All operations in broadcaster is supposed to be put in broadcast thread
  std::unique_ptr<std::thread> broadcast_thread_;
  instrumented_io_context broadcast_service_;
  std::unique_ptr<::ray::gcs::GrpcBasedResourceBroadcaster> broadcaster_;
  // Poller is running in its own thread.
  std::unique_ptr<::ray::gcs::GcsResourceReportPoller> poller_;

  // Accessing data related fields should be from main thread.
  // These fields originally were put in GcsResourceManager.
  // resources_buffer_ is the place where global view is stored.
  // resources_buffer_proto_ is the actual message to be broadcasted.
  // Right now there are two type of messages: rpc::NodeResourceChange and
  // rpc::ResourcesData. Ideally we should unify these two, but as the first
  // step, we keep them separate as before.
  // rpc::NodeResourceChange is about placement group, it'll be put to
  // resources_buffer_proto_ so that it'll be sent in next sending cycle.
  // rpc::ResourcesData is stored in resources_buffer_ and when we do broadcasting,
  // it'll be copied to resources_buffer_proto_ and sent to other nodes.
  // resources_buffer_proto_ will be cleared after each broadcasting.
  absl::flat_hash_map<std::string, rpc::ResourcesData> resources_buffer_;
  rpc::ResourceUsageBroadcastData resources_buffer_proto_;
  friend class ray::GcsPlacementGroupSchedulerTest;
};

}  // namespace syncer
}  // namespace ray
