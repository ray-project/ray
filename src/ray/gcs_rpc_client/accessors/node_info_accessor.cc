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

#include "ray/gcs_rpc_client/accessors/node_info_accessor.h"

#include <future>

#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/pubsub/gcs_subscriber.h"
#include "ray/util/container_util.h"

namespace ray {
namespace gcs {

NodeInfoAccessor::NodeInfoAccessor(GcsClientContext *context) : context_(context) {}

Status NodeInfoAccessor::RegisterSelf(const rpc::GcsNodeInfo &local_node_info,
                                      const StatusCallback &callback) {
  auto node_id = NodeID::FromBinary(local_node_info.node_id());
  RAY_LOG(DEBUG).WithField(node_id)
      << "Registering node info, address is = " << local_node_info.node_manager_address();
  RAY_CHECK(local_node_id_.IsNil()) << "This node is already connected.";
  RAY_CHECK(local_node_info.state() == rpc::GcsNodeInfo::ALIVE);
  rpc::RegisterNodeRequest request;
  request.mutable_node_info()->CopyFrom(local_node_info);
  context_->GetGcsRpcClient().RegisterNode(
      std::move(request),
      [this, node_id, local_node_info, callback](const Status &status,
                                                 rpc::RegisterNodeReply &&reply) {
        if (status.ok()) {
          local_node_info_.CopyFrom(local_node_info);
          local_node_id_ = NodeID::FromBinary(local_node_info.node_id());
        }
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG).WithField(node_id)
            << "Finished registering node info, status = " << status;
      });

  return Status::OK();
}

void NodeInfoAccessor::UnregisterSelf(const rpc::NodeDeathInfo &node_death_info,
                                      std::function<void()> unregister_done_callback) {
  if (local_node_id_.IsNil()) {
    RAY_LOG(INFO) << "The node is already unregistered.";
    return;
  }
  auto node_id = NodeID::FromBinary(local_node_info_.node_id());
  RAY_LOG(INFO).WithField(node_id) << "Unregistering node";

  rpc::UnregisterNodeRequest request;
  request.set_node_id(local_node_info_.node_id());
  request.mutable_node_death_info()->CopyFrom(node_death_info);
  context_->GetGcsRpcClient().UnregisterNode(
      std::move(request),
      [this, node_id, unregister_done_callback](const Status &status,
                                                rpc::UnregisterNodeReply &&reply) {
        if (status.ok()) {
          local_node_info_.set_state(rpc::GcsNodeInfo::DEAD);
          local_node_id_ = NodeID::Nil();
        }
        RAY_LOG(INFO).WithField(node_id)
            << "Finished unregistering node info, status = " << status;
        unregister_done_callback();
      });
}

const NodeID &NodeInfoAccessor::GetSelfId() const { return local_node_id_; }

const rpc::GcsNodeInfo &NodeInfoAccessor::GetSelfInfo() const { return local_node_info_; }

void NodeInfoAccessor::AsyncRegister(const rpc::GcsNodeInfo &node_info,
                                     const StatusCallback &callback) {
  NodeID node_id = NodeID::FromBinary(node_info.node_id());
  RAY_LOG(DEBUG).WithField(node_id) << "Registering node info";
  rpc::RegisterNodeRequest request;
  request.mutable_node_info()->CopyFrom(node_info);
  context_->GetGcsRpcClient().RegisterNode(
      std::move(request),
      [node_id, callback](const Status &status, rpc::RegisterNodeReply &&reply) {
        if (callback) {
          callback(status);
        }
        RAY_LOG(DEBUG).WithField(node_id)
            << "Finished registering node info, status = " << status;
      });
}

void NodeInfoAccessor::AsyncCheckSelfAlive(
    const std::function<void(Status, bool)> &callback, int64_t timeout_ms = -1) {
  std::vector<NodeID> node_ids = {local_node_id_};

  AsyncCheckAlive(node_ids,
                  timeout_ms,
                  [callback](const Status &status, const std::vector<bool> &nodes_alive) {
                    if (!status.ok()) {
                      callback(status, false);
                      return;
                    } else {
                      RAY_CHECK_EQ(nodes_alive.size(), static_cast<size_t>(1));
                      callback(status, nodes_alive[0]);
                    }
                  });
}

void NodeInfoAccessor::AsyncCheckAlive(const std::vector<NodeID> &node_ids,
                                       int64_t timeout_ms,
                                       const MultiItemCallback<bool> &callback) {
  rpc::CheckAliveRequest request;
  for (const auto &node_id : node_ids) {
    request.add_node_ids(node_id.Binary());
  }
  size_t num_raylets = node_ids.size();
  context_->GetGcsRpcClient().CheckAlive(
      std::move(request),
      [num_raylets, callback](const Status &status, rpc::CheckAliveReply &&reply) {
        if (status.ok()) {
          RAY_CHECK_EQ(static_cast<size_t>(reply.raylet_alive().size()), num_raylets);
          std::vector<bool> is_alive;
          is_alive.reserve(num_raylets);
          for (const bool &alive : reply.raylet_alive()) {
            is_alive.push_back(alive);
          }
          callback(status, std::move(is_alive));
        } else {
          callback(status, {});
        }
      },
      timeout_ms);
}

Status NodeInfoAccessor::DrainNodes(const std::vector<NodeID> &node_ids,
                                    int64_t timeout_ms,
                                    std::vector<std::string> &drained_node_ids) {
  RAY_LOG(DEBUG) << "Draining nodes, node id = " << debug_string(node_ids);
  rpc::DrainNodeRequest request;
  rpc::DrainNodeReply reply;
  for (const auto &node_id : node_ids) {
    auto draining_request = request.add_drain_node_data();
    draining_request->set_node_id(node_id.Binary());
  }
  RAY_RETURN_NOT_OK(
      context_->GetGcsRpcClient().SyncDrainNode(std::move(request), &reply, timeout_ms));
  drained_node_ids.clear();
  for (const auto &s : reply.drain_node_status()) {
    drained_node_ids.push_back(s.node_id());
  }
  return Status::OK();
}

void NodeInfoAccessor::AsyncGetAll(const MultiItemCallback<rpc::GcsNodeInfo> &callback,
                                   int64_t timeout_ms,
                                   const std::vector<NodeID> &node_ids) {
  RAY_LOG(DEBUG) << "Getting information of all nodes.";
  rpc::GetAllNodeInfoRequest request;
  for (const auto &node_id : node_ids) {
    request.add_node_selectors()->set_node_id(node_id.Binary());
  }
  context_->GetGcsRpcClient().GetAllNodeInfo(
      std::move(request),
      [callback](const Status &status, rpc::GetAllNodeInfoReply &&reply) {
        std::vector<rpc::GcsNodeInfo> result;
        result.reserve((reply.node_info_list_size()));
        for (int index = 0; index < reply.node_info_list_size(); ++index) {
          result.emplace_back(reply.node_info_list(index));
        }
        callback(status, std::move(result));
        RAY_LOG(DEBUG) << "Finished getting information of all nodes, status = "
                       << status;
      },
      timeout_ms);
}

void NodeInfoAccessor::AsyncSubscribeToNodeChange(
    std::function<void(NodeID, const rpc::GcsNodeInfo &)> subscribe,
    StatusCallback done) {
  /**
  1. Subscribe to node info
  2. Once the subscription is made, ask for all node info.
  3. Once all node info is received, call done callback.
  4. HandleNotification can handle conflicts between the subscription updates and
     GetAllNodeInfo because nodes can only go from alive to dead, never back to alive.
     Note that this only works because state is the only mutable field, otherwise we'd
     have to queue processing subscription updates until the initial population from
     AsyncGetAll is done.
  */

  RAY_CHECK(node_change_callback_ == nullptr);
  node_change_callback_ = std::move(subscribe);
  RAY_CHECK(node_change_callback_ != nullptr);

  fetch_node_data_operation_ = [this](const StatusCallback &done_callback) {
    AsyncGetAll(
        [this, done_callback](const Status &status,
                              std::vector<rpc::GcsNodeInfo> &&node_info_list) {
          for (auto &node_info : node_info_list) {
            HandleNotification(std::move(node_info));
          }
          if (done_callback) {
            done_callback(status);
          }
        },
        /*timeout_ms=*/-1);
  };

  context_->GetGcsSubscriber().SubscribeAllNodeInfo(
      /*subscribe=*/[this](
                        rpc::GcsNodeInfo &&data) { HandleNotification(std::move(data)); },
      /*done=*/[this, done = std::move(done)](
                   const Status &) { fetch_node_data_operation_(done); });
}

const rpc::GcsNodeInfo *NodeInfoAccessor::Get(const NodeID &node_id,
                                              bool filter_dead_nodes) const {
  RAY_CHECK(!node_id.IsNil());
  auto entry = node_cache_.find(node_id);
  if (entry != node_cache_.end()) {
    if (filter_dead_nodes && entry->second.state() == rpc::GcsNodeInfo::DEAD) {
      return nullptr;
    }
    return &entry->second;
  }
  return nullptr;
}

const absl::flat_hash_map<NodeID, rpc::GcsNodeInfo> &NodeInfoAccessor::GetAll() const {
  return node_cache_;
}

StatusOr<std::vector<rpc::GcsNodeInfo>> NodeInfoAccessor::GetAllNoCache(
    int64_t timeout_ms,
    std::optional<rpc::GcsNodeInfo::GcsNodeState> state_filter,
    std::optional<rpc::GetAllNodeInfoRequest::NodeSelector> node_selector) {
  rpc::GetAllNodeInfoRequest request;
  if (state_filter.has_value()) {
    request.set_state_filter(state_filter.value());
  }
  if (node_selector.has_value()) {
    *request.add_node_selectors() = std::move(node_selector.value());
  }
  rpc::GetAllNodeInfoReply reply;
  RAY_RETURN_NOT_OK(context_->GetGcsRpcClient().SyncGetAllNodeInfo(
      std::move(request), &reply, timeout_ms));
  return VectorFromProtobuf(std::move(*reply.mutable_node_info_list()));
}

Status NodeInfoAccessor::CheckAlive(const std::vector<NodeID> &node_ids,
                                    int64_t timeout_ms,
                                    std::vector<bool> &nodes_alive) {
  std::promise<Status> ret_promise;
  AsyncCheckAlive(
      node_ids,
      timeout_ms,
      [&ret_promise, &nodes_alive](Status status, const std::vector<bool> &alive) {
        nodes_alive = alive;
        ret_promise.set_value(status);
      });
  return ret_promise.get_future().get();
}

bool NodeInfoAccessor::IsNodeDead(const NodeID &node_id) const {
  auto node_iter = node_cache_.find(node_id);
  return node_iter != node_cache_.end() &&
         node_iter->second.state() == rpc::GcsNodeInfo::DEAD;
}

void NodeInfoAccessor::HandleNotification(rpc::GcsNodeInfo &&node_info) {
  NodeID node_id = NodeID::FromBinary(node_info.node_id());
  bool is_alive = (node_info.state() == rpc::GcsNodeInfo::ALIVE);
  auto entry = node_cache_.find(node_id);
  bool is_notif_new;
  if (entry == node_cache_.end()) {
    // If the entry is not in the cache, then the notification is new.
    is_notif_new = true;
  } else {
    // If the entry is in the cache, then the notification is new if the node
    // was alive and is now dead or resources have been updated.
    bool was_alive = (entry->second.state() == rpc::GcsNodeInfo::ALIVE);
    is_notif_new = was_alive && !is_alive;

    // Once a node with a given ID has been removed, it should never be added
    // again. If the entry was in the cache and the node was deleted, we should check
    // that this new notification is not an insertion.
    // However, when a new node(node-B) registers with GCS, it subscribes to all node
    // information. It will subscribe to redis and then get all node information from GCS
    // through RPC. If node-A fails after GCS replies to node-B, GCS will send another
    // message(node-A is dead) to node-B through redis publish. Because RPC and redis
    // subscribe are two different sessions, node-B may process node-A dead message first
    // and then node-A alive message. So we use `RAY_LOG` instead of `RAY_CHECK ` as a
    // workaround.
    if (!was_alive && is_alive) {
      RAY_LOG(INFO) << "Notification for addition of a node that was already removed:"
                    << node_id;
      return;
    }
  }

  // Add the notification to our cache.
  RAY_LOG(INFO).WithField(node_id)
      << "Received notification for node, IsAlive = " << is_alive;

  auto &node = node_cache_[node_id];
  if (is_alive) {
    node = std::move(node_info);
  } else {
    node.set_node_id(node_info.node_id());
    node.set_state(rpc::GcsNodeInfo::DEAD);
    node.mutable_death_info()->CopyFrom(node_info.death_info());
    node.set_end_time_ms(node_info.end_time_ms());
  }

  // If the notification is new, call registered callback.
  if (is_notif_new && node_change_callback_ != nullptr) {
    node_change_callback_(node_id, node_cache_[node_id]);
  }
}

void NodeInfoAccessor::AsyncResubscribe() {
  RAY_LOG(DEBUG) << "Reestablishing subscription for node info.";
  if (IsSubscribedToNodeChange()) {
    context_->GetGcsSubscriber().SubscribeAllNodeInfo(
        /*subscribe=*/[this](rpc::GcsNodeInfo
                                 &&data) { HandleNotification(std::move(data)); },
        /*done=*/
        [this](const Status &) {
          fetch_node_data_operation_([](const Status &) {
            RAY_LOG(INFO)
                << "Finished fetching all node information from gcs server after gcs "
                   "server or pub-sub server is restarted.";
          });
        });
  }
}

bool NodeInfoAccessor::IsSubscribedToNodeChange() const {
  return node_change_callback_ != nullptr;
}

}  // namespace gcs
}  // namespace ray
