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

#include "ray/gcs/gcs_server/gcs_resource_manager.h"

namespace ray {
namespace gcs {

GcsResourceManager::GcsResourceManager(
    std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage)
    : gcs_pub_sub_(gcs_pub_sub), gcs_table_storage_(gcs_table_storage) {}

void GcsResourceManager::HandleGetResources(const rpc::GetResourcesRequest &request,
                                            rpc::GetResourcesReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  auto iter = cluster_resources_.find(node_id);
  if (iter != cluster_resources_.end()) {
    for (const auto &resource : iter->second.items()) {
      (*reply->mutable_resources())[resource.first] = resource.second;
    }
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_RESOURCES_REQUEST];
}

void GcsResourceManager::HandleUpdateResources(
    const rpc::UpdateResourcesRequest &request, rpc::UpdateResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Updating resources, node id = " << node_id;
  auto iter = cluster_resources_.find(node_id);
  std::unordered_map<std::string, double> to_be_updated_resources;
  for (const auto &entry : request.resources()) {
    to_be_updated_resources.emplace(entry.first, entry.second.resource_capacity());
  }

  if (iter != cluster_resources_.end()) {
    for (const auto &entry : request.resources()) {
      (*iter->second.mutable_items())[entry.first] = entry.second;
    }
    UpdateResourceCapacity(node_id, to_be_updated_resources);
    auto on_done = [this, node_id, to_be_updated_resources, reply,
                    send_reply_callback](const Status &status) {
      RAY_CHECK_OK(status);
      rpc::NodeResourceChange node_resource_change;
      node_resource_change.set_node_id(node_id.Binary());
      for (const auto &it : to_be_updated_resources) {
        const auto &resource_name = it.first;
        const auto &resource_capacity = it.second;
        auto &node_updated_resources =
            (*node_resource_change.mutable_updated_resources());
        node_updated_resources[resource_name] = resource_capacity;
      }
      RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_RESOURCE_CHANNEL, node_id.Hex(),
                                         node_resource_change.SerializeAsString(),
                                         nullptr));

      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      RAY_LOG(DEBUG) << "Finished updating resources, node id = " << node_id;
    };

    RAY_CHECK_OK(
        gcs_table_storage_->NodeResourceTable().Put(node_id, iter->second, on_done));
  } else {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid("Node is not exist."));
    RAY_LOG(ERROR) << "Failed to update resources as node " << node_id
                   << " is not registered.";
  }
  ++counts_[CountType::UPDATE_RESOURCES_REQUEST];
}

void GcsResourceManager::HandleDeleteResources(
    const rpc::DeleteResourcesRequest &request, rpc::DeleteResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Deleting node resources, node id = " << node_id;
  auto resource_names = VectorFromProtobuf(request.resource_name_list());
  auto iter = cluster_resources_.find(node_id);
  if (iter != cluster_resources_.end()) {
    DeleteResources(node_id, resource_names);

    for (const auto &resource_name : resource_names) {
      RAY_IGNORE_EXPR(iter->second.mutable_items()->erase(resource_name));
    }
    auto on_done = [this, node_id, resource_names, reply,
                    send_reply_callback](const Status &status) {
      RAY_CHECK_OK(status);
      rpc::NodeResourceChange node_resource_change;
      node_resource_change.set_node_id(node_id.Binary());
      for (const auto &resource_name : resource_names) {
        node_resource_change.add_deleted_resources(resource_name);
      }
      RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_RESOURCE_CHANNEL, node_id.Hex(),
                                         node_resource_change.SerializeAsString(),
                                         nullptr));

      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    };
    RAY_CHECK_OK(
        gcs_table_storage_->NodeResourceTable().Put(node_id, iter->second, on_done));
  } else {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    RAY_LOG(DEBUG) << "Finished deleting node resources, node id = " << node_id;
  }
  ++counts_[CountType::DELETE_RESOURCES_REQUEST];
}

void GcsResourceManager::HandleGetAllAvailableResources(
    const rpc::GetAllAvailableResourcesRequest &request,
    rpc::GetAllAvailableResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  for (const auto &iter : cluster_scheduling_resources_) {
    rpc::AvailableResources resource;
    resource.set_node_id(iter.first.Binary());
    for (const auto &res : iter.second.GetAvailableResources().GetResourceAmountMap()) {
      (*resource.mutable_resources_available())[res.first] = res.second.ToDouble();
    }
    reply->add_resources_list()->CopyFrom(resource);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_AVAILABLE_RESOURCES_REQUEST];
}

void GcsResourceManager::Initialize(const GcsInitData &gcs_init_data) {
  const auto &nodes = gcs_init_data.Nodes();
  for (auto &entry : gcs_init_data.ClusterResources()) {
    const auto &iter = nodes.find(entry.first);
    if (iter->second.state() == rpc::GcsNodeInfo::ALIVE) {
      cluster_resources_[entry.first] = entry.second;
    }
  }
}

const absl::flat_hash_map<NodeID, SchedulingResources>
    &GcsResourceManager::GetClusterResources() const {
  return cluster_scheduling_resources_;
}

void GcsResourceManager::SetAvailableResources(const NodeID &node_id,
                                               const ResourceSet &resources) {
  cluster_scheduling_resources_[node_id].SetAvailableResources(ResourceSet(resources));
}

void GcsResourceManager::UpdateResourceCapacity(
    const NodeID &node_id,
    const std::unordered_map<std::string, double> &changed_resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    SchedulingResources &scheduling_resources = iter->second;
    for (const auto &entry : changed_resources) {
      scheduling_resources.UpdateResourceCapacity(entry.first, entry.second);
    }
  } else {
    cluster_scheduling_resources_.emplace(
        node_id, SchedulingResources(ResourceSet(changed_resources)));
  }
}

void GcsResourceManager::DeleteResources(
    const NodeID &node_id, const std::vector<std::string> &deleted_resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    for (auto &resource_name : deleted_resources) {
      iter->second.DeleteResource(resource_name);
    }
  }
}

void GcsResourceManager::OnNodeAdd(const NodeID &node_id) {
  // Add an empty resources for this node.
  cluster_resources_.emplace(node_id, rpc::ResourceMap());
}

void GcsResourceManager::OnNodeDead(const NodeID &node_id) {
  cluster_resources_.erase(node_id);
  cluster_scheduling_resources_.erase(node_id);
}

bool GcsResourceManager::AcquireResources(const NodeID &node_id,
                                          const ResourceSet &required_resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    if (!required_resources.IsSubset(iter->second.GetAvailableResources())) {
      return false;
    }
    iter->second.Acquire(required_resources);
  }
  // If node dead, we will not find the node. This is a normal scenario, so it returns
  // true.
  return true;
}

bool GcsResourceManager::ReleaseResources(const NodeID &node_id,
                                          const ResourceSet &acquired_resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    iter->second.Release(acquired_resources);
  }
  // If node dead, we will not find the node. This is a normal scenario, so it returns
  // true.
  return true;
}

std::string GcsResourceManager::DebugString() const {
  std::ostringstream stream;
  stream << "GcsResourceManager: {GetResources request count: "
         << counts_[CountType::GET_RESOURCES_REQUEST]
         << ", GetAllAvailableResources request count"
         << counts_[CountType::GET_ALL_AVAILABLE_RESOURCES_REQUEST]
         << ", UpdateResources request count: "
         << counts_[CountType::UPDATE_RESOURCES_REQUEST]
         << ", DeleteResources request count: "
         << counts_[CountType::DELETE_RESOURCES_REQUEST] << "}";
  return stream.str();
}

}  // namespace gcs
}  // namespace ray
