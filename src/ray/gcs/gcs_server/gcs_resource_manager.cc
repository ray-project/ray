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
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    const auto &resource_map = iter->second.GetTotalResources().GetResourceMap();
    rpc::ResourceTableData resource_table_data;
    for (const auto &resource : resource_map) {
      resource_table_data.set_resource_capacity(resource.second);
      (*reply->mutable_resources())[resource.first] = resource_table_data;
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
  auto changed_resources = std::make_shared<std::unordered_map<std::string, double>>();
  for (const auto &entry : request.resources()) {
    changed_resources->emplace(entry.first, entry.second.resource_capacity());
  }

  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    // Update `cluster_scheduling_resources_`.
    SchedulingResources &scheduling_resources = iter->second;
    for (const auto &entry : *changed_resources) {
      scheduling_resources.UpdateResourceCapacity(entry.first, entry.second);
    }

    // Update gcs storage.
    rpc::ResourceMap resource_map;
    for (const auto &entry : iter->second.GetTotalResources().GetResourceMap()) {
      (*resource_map.mutable_items())[entry.first].set_resource_capacity(entry.second);
    }
    for (const auto &entry : *changed_resources) {
      (*resource_map.mutable_items())[entry.first].set_resource_capacity(entry.second);
    }

    auto on_done = [this, node_id, changed_resources, reply,
                    send_reply_callback](const Status &status) {
      RAY_CHECK_OK(status);
      rpc::NodeResourceChange node_resource_change;
      node_resource_change.set_node_id(node_id.Binary());
      node_resource_change.mutable_updated_resources()->insert(changed_resources->begin(),
                                                               changed_resources->end());
      RAY_CHECK_OK(gcs_pub_sub_->Publish(NODE_RESOURCE_CHANNEL, node_id.Hex(),
                                         node_resource_change.SerializeAsString(),
                                         nullptr));

      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      RAY_LOG(DEBUG) << "Finished updating resources, node id = " << node_id;
    };

    RAY_CHECK_OK(
        gcs_table_storage_->NodeResourceTable().Put(node_id, resource_map, on_done));
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
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    // Update `cluster_scheduling_resources_`.
    for (const auto &resource_name : resource_names) {
      iter->second.DeleteResource(resource_name);
    }

    // Update gcs storage.
    rpc::ResourceMap resource_map;
    auto resources = iter->second.GetTotalResources().GetResourceMap();
    for (const auto &resource_name : resource_names) {
      resources.erase(resource_name);
    }
    for (const auto &entry : resources) {
      (*resource_map.mutable_items())[entry.first].set_resource_capacity(entry.second);
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
        gcs_table_storage_->NodeResourceTable().Put(node_id, resource_map, on_done));
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
  for (const auto &entry : nodes) {
    if (entry.second.state() == rpc::GcsNodeInfo::ALIVE) {
      OnNodeAdd(entry.second);
    }
  }

  const auto &cluster_resources = gcs_init_data.ClusterResources();
  for (const auto &entry : cluster_resources) {
    const auto &iter = cluster_scheduling_resources_.find(entry.first);
    if (iter != cluster_scheduling_resources_.end()) {
      for (const auto &resource : entry.second.items()) {
        iter->second.UpdateResourceCapacity(resource.first,
                                            resource.second.resource_capacity());
      }
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
    for (const auto &resource_name : deleted_resources) {
      iter->second.DeleteResource(resource_name);
    }
  }
}

void GcsResourceManager::OnNodeAdd(const rpc::GcsNodeInfo &node) {
  auto node_id = NodeID::FromBinary(node.node_id());
  if (!cluster_scheduling_resources_.contains(node_id)) {
    cluster_scheduling_resources_.emplace(node_id, SchedulingResources());
  }
}

void GcsResourceManager::OnNodeDead(const NodeID &node_id) {
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
