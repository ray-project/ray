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

#include <memory>
#include <type_traits>

namespace ray {
namespace sync {

class RaySync {
 public:
  RaySync(instrumented_io_context& main_thread,
          std::unique_ptr<ray::gcs::GrpcBasedResourceBroadcaster> braodcaster,
          std::unique_ptr<ray::gcs::GcsResourceReportPoller> poller)
      : main_thread_(main_thread), 
      broadcaster_(std::move(braodcaster)), 
      poller_(std::move(poller)) {}

  void Start() {}

  void Stop() {}

  // External API
  template <typename T>
  void Update(T update) {
    if constexpr (std::is_same_v<T, rpc::NodeResourceChange>) {
      resources_buffer_proto_.add_batch()->mutable_change()->Swap(&update);
    } else if constexpr (std::is_same_v<T, rpc::ResourcesData>) {
      if (update.should_global_gc() ||
          update.resources_total_size() > 0 ||
          update.resources_available_changed() ||
          update.resource_load_changed()) {
        update.clear_resource_load();
        update.clear_resource_load_by_shape();
        update.clear_resources_normal_task();
        resources_buffer_[node_id].Swap(std::move(update));
      }
    } else {
      static_assert(false, "unknown type");
    }
  }

  void AddNode(const rpc::GcsNodeInfo &node_info) {
    broadcaster_->HandleNodeAdded(node_info);
    gcs_resource_report_poller_->HandleNodeAdded(node_info);
  }

  void RemoveNode(const rpc::GcsNodeInfo &node_info) {
    broadcaster_->HandleNodeRemoved(*node);
    gcs_resource_report_poller_->HandleNodeRemoved(*node);
    NodeID node_id = NodeID::FromBinary(node_info.node_id());
    resources_buffer_.erase(node_id);
  }

  std::string DebugString() { return broadcaster_->DebugString(); }

 private:
  instrumented_io_context &main_service_;
  rpc::ResourceUsageBroadcastData resources_buffer_proto_;
  std::unique_ptr<ray::gcs::GrpcBasedResourceBroadcaster> broadcaster_;
  std::unique_ptr<ray::gcs::GcsResourceReportPoller> poller_;
  absl::flat_hash_map<NodeID, rpc::ResourcesData> resources_buffer_;
  std::unique_ptr<std::thread> broadcast_thread_;
  instrumented_io_context broadcast_service_;

  
};

}  // namespace sync
}  // namespace ray
