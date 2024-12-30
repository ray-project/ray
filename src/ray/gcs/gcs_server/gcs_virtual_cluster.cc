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

#include "ray/gcs/gcs_server/gcs_virtual_cluster.h"

namespace ray {
namespace gcs {

std::string DebugString(const ReplicaInstances &replica_instances, int indent = 0) {
  std::ostringstream stream;
  stream << "{\n";
  for (const auto &[template_id, job_cluster_instances] : replica_instances) {
    stream << std::string(indent + 2, ' ') << template_id << " : {\n";
    for (const auto &[job_cluster_id, node_instances] : job_cluster_instances) {
      stream << std::string(indent + 4, ' ') << job_cluster_id << " : {\n";
      for (const auto &[node_instance_id, node_instance] : node_instances) {
        stream << std::string(indent + 6, ' ') << node_instance_id << ":"
               << node_instance->DebugString() << ",\n";
      }
      stream << std::string(indent + 4, ' ') << "},\n";
    }
    stream << std::string(indent + 2, ' ') << "},\n";
  }
  stream << std::string(indent, ' ') << "}\n";
  return stream.str();
}

///////////////////////// VirtualCluster /////////////////////////
void VirtualCluster::UpdateNodeInstances(ReplicaInstances replica_instances_to_add,
                                         ReplicaInstances replica_instances_to_remove) {
  // Insert node instances to the virtual cluster.
  InsertNodeInstances(std::move(replica_instances_to_add));
  // Remove node instances from the virtual cluster.
  RemoveNodeInstances(std::move(replica_instances_to_remove));
  // Update the revision of the cluster.
  revision_ = current_sys_time_ns();
}

void VirtualCluster::InsertNodeInstances(ReplicaInstances replica_instances) {
  for (auto &[template_id, job_node_instances] : replica_instances) {
    auto &template_node_instances = visible_node_instances_[template_id];
    auto &replicas = replica_sets_[template_id];
    for (auto &[job_cluster_id, node_instances] : job_node_instances) {
      auto &job_node_instances = template_node_instances[job_cluster_id];
      for (auto &[id, node_instance] : node_instances) {
        job_node_instances[id] = std::move(node_instance);
        ++replicas;
      }
    }
  }
}

void VirtualCluster::RemoveNodeInstances(ReplicaInstances replica_instances) {
  for (auto &[template_id, job_node_instances] : replica_instances) {
    auto template_iter = visible_node_instances_.find(template_id);
    if (template_iter == visible_node_instances_.end()) {
      RAY_LOG(WARNING) << "The template id " << template_id << " is not found in cluster "
                       << GetID();
      continue;
    }

    auto replica_set_iter = replica_sets_.find(template_id);
    RAY_CHECK(replica_set_iter != replica_sets_.end());

    for (auto &[job_cluster_id, node_instances] : job_node_instances) {
      auto job_cluster_iter = template_iter->second.find(job_cluster_id);
      RAY_CHECK(job_cluster_iter != template_iter->second.end());

      for (auto &[id, _] : node_instances) {
        if (job_cluster_iter->second.erase(id) == 0) {
          RAY_LOG(WARNING) << "The node instance " << id << " is not found in cluster "
                           << GetID();
        } else {
          RAY_CHECK(--(replica_set_iter->second) >= 0);
        }
      }

      if (job_cluster_iter->second.empty()) {
        template_iter->second.erase(job_cluster_iter);
      }
    }

    if (template_iter->second.empty()) {
      visible_node_instances_.erase(template_iter);
    }

    if (replica_set_iter->second == 0) {
      replica_sets_.erase(replica_set_iter);
    }
  }
}

Status VirtualCluster::LookupIdleNodeInstances(
    const ReplicaSets &replica_sets, ReplicaInstances &replica_instances) const {
  bool success = true;
  for (const auto &[template_id, replicas] : replica_sets) {
    auto &template_node_instances = replica_instances[template_id];
    if (replicas <= 0) {
      continue;
    }

    auto iter = visible_node_instances_.find(template_id);
    if (iter == visible_node_instances_.end()) {
      success = false;
      continue;
    }

    auto empty_iter = iter->second.find(kEmptyJobClusterId);
    if (empty_iter == iter->second.end()) {
      success = false;
      continue;
    }

    auto &job_node_instances = template_node_instances[kEmptyJobClusterId];
    for (const auto &[id, node_instance] : empty_iter->second) {
      if (!IsIdleNodeInstance(kEmptyJobClusterId, *node_instance)) {
        continue;
      }
      job_node_instances.emplace(id, node_instance);
      if (job_node_instances.size() == static_cast<size_t>(replicas)) {
        break;
      }
    }
    if (job_node_instances.size() < static_cast<size_t>(replicas)) {
      success = false;
    }
  }

  if (!success) {
    // TODO(Shanly): Give a more detailed error message about the demand replica set and
    // the idle replica instances.
    return Status::OutOfResource("No enough node instances to assign.");
  }

  return Status::OK();
}

bool VirtualCluster::MarkNodeInstanceAsDead(const std::string &template_id,
                                            const std::string &node_instance_id) {
  auto iter = visible_node_instances_.find(template_id);
  if (iter == visible_node_instances_.end()) {
    return false;
  }

  for (auto &[job_cluster_id, node_instances] : iter->second) {
    auto iter = node_instances.find(node_instance_id);
    if (iter != node_instances.end()) {
      iter->second->set_is_dead(true);
      return true;
    }
  }

  return false;
}

bool VirtualCluster::ContainsNodeInstance(const std::string &node_instance_id) {
  // TODO(sule): Use the index from node instance id to cluster id to optimize this code.
  // Iterate through visible node instances to find a matching node instance ID.
  for (auto &[template_id, job_node_instances] : visible_node_instances_) {
    for (auto &[job_cluster_id, node_instances] : job_node_instances) {
      if (node_instances.find(node_instance_id) != node_instances.end()) {
        return true;
      }
    }
  }
  return false;
}

std::shared_ptr<rpc::VirtualClusterTableData> VirtualCluster::ToProto() const {
  auto data = std::make_shared<rpc::VirtualClusterTableData>();
  data->set_id(GetID());
  data->set_mode(GetMode());
  data->set_revision(GetRevision());
  for (auto &[template_id, job_node_instances] : visible_node_instances_) {
    for (auto &[job_cluster_id, node_instances] : job_node_instances) {
      for (auto &[id, node_instance] : node_instances) {
        (*data->mutable_node_instances())[id] = std::move(*node_instance->ToProto());
      }
    }
  }
  return data;
}

std::string VirtualCluster::DebugString() const {
  int indent = 2;
  std::ostringstream stream;
  stream << "VirtualCluster[" << GetID() << "]:\n";
  stream << std::string(indent, ' ') << "revision: " << GetRevision() << ",\n";
  stream << std::string(indent, ' ') << "replica_sets: {\n";
  for (const auto &[template_id, replicas] : replica_sets_) {
    stream << std::string(indent + 2, ' ') << template_id << ": " << replicas << ",\n";
  }
  stream << std::string(indent, ' ') << "}, \n";
  stream << std::string(indent, ' ') << "visible_node_instances: "
         << ray::gcs::DebugString(visible_node_instances_, indent);
  return stream.str();
}

///////////////////////// ExclusiveCluster /////////////////////////
void ExclusiveCluster::LoadJobCluster(const rpc::VirtualClusterTableData &data) {
  RAY_CHECK(GetMode() == rpc::AllocationMode::EXCLUSIVE);

  const auto &job_cluster_id = data.id();
  RAY_CHECK(VirtualClusterID::FromBinary(job_cluster_id).IsJobClusterID());
  RAY_CHECK(job_clusters_.find(job_cluster_id) == job_clusters_.end());

  DoCreateJobCluster(data.id(), toReplicaInstances(data.node_instances()));
}

Status ExclusiveCluster::CreateJobCluster(const std::string &job_cluster_id,
                                          ReplicaSets replica_sets,
                                          CreateOrUpdateVirtualClusterCallback callback) {
  if (GetMode() != rpc::AllocationMode::EXCLUSIVE) {
    std::ostringstream ostr;
    ostr << "The job cluster can only be created in exclusive mode, virtual_cluster_id: "
         << GetID();
    return Status::InvalidArgument(ostr.str());
  }

  auto iter = job_clusters_.find(job_cluster_id);
  if (iter != job_clusters_.end()) {
    std::ostringstream ostr;
    ostr << "The job cluster " << job_cluster_id << " already exists.";
    return Status::InvalidArgument(ostr.str());
  }

  ReplicaInstances replica_instances_to_add;
  // Lookup idle node instances from main cluster based on `replica_sets_to_add`.
  auto status = LookupIdleNodeInstances(replica_sets, replica_instances_to_add);
  if (!status.ok()) {
    // TODO(Shanly): Give a more detailed error message about the demand replica set and
    // the idle replica instances.
    std::ostringstream ostr;
    ostr << "No enough node instances to create the job cluster " << job_cluster_id;
    return Status::OutOfResource(ostr.str());
  }

  auto job_cluster =
      DoCreateJobCluster(job_cluster_id, std::move(replica_instances_to_add));
  // Flush and publish the job cluster data.
  return async_data_flusher_(job_cluster->ToProto(), std::move(callback));
}

std::shared_ptr<JobCluster> ExclusiveCluster::DoCreateJobCluster(
    const std::string &job_cluster_id, ReplicaInstances replica_instances_to_add) {
  auto replica_instances_to_remove_from_current_cluster = replica_instances_to_add;
  auto replica_instances_to_add_to_current_cluster = replica_instances_to_add;
  for (auto &[template_id, job_node_instances] :
       replica_instances_to_add_to_current_cluster) {
    auto node_instances = std::move(job_node_instances[kEmptyJobClusterId]);
    job_node_instances.erase(kEmptyJobClusterId);
    job_node_instances[job_cluster_id] = std::move(node_instances);
  }
  UpdateNodeInstances(std::move(replica_instances_to_add_to_current_cluster),
                      std::move(replica_instances_to_remove_from_current_cluster));

  auto job_cluster = std::make_shared<JobCluster>(job_cluster_id);
  job_cluster->UpdateNodeInstances(std::move(replica_instances_to_add),
                                   ReplicaInstances());
  RAY_CHECK(job_clusters_.emplace(job_cluster_id, job_cluster).second);
  return job_cluster;
}

Status ExclusiveCluster::RemoveJobCluster(const std::string &job_cluster_id,
                                          RemoveVirtualClusterCallback callback) {
  if (GetMode() != rpc::AllocationMode::EXCLUSIVE) {
    std::ostringstream ostr;
    ostr << "The job cluster can only be removed in exclusive mode, virtual_cluster_id: "
         << GetID();
    return Status::InvalidArgument(ostr.str());
  }

  auto iter = job_clusters_.find(job_cluster_id);
  if (iter == job_clusters_.end()) {
    return Status::NotFound("The job cluster " + job_cluster_id + " does not exist.");
  }
  auto job_cluster = iter->second;

  const auto &replica_instances_to_remove = job_cluster->GetVisibleNodeInstances();

  auto replica_instances_to_add_to_current_cluster = replica_instances_to_remove;
  auto replica_instances_to_remove_from_current_cluster = replica_instances_to_remove;
  for (auto &[template_id, job_node_instances] :
       replica_instances_to_remove_from_current_cluster) {
    auto node_instances = std::move(job_node_instances[kEmptyJobClusterId]);
    job_node_instances.erase(kEmptyJobClusterId);
    job_node_instances[job_cluster_id] = std::move(node_instances);
  }
  UpdateNodeInstances(std::move(replica_instances_to_add_to_current_cluster),
                      std::move(replica_instances_to_remove_from_current_cluster));

  // Update the job cluster.
  // job_cluster->UpdateNodeInstances(ReplicaInstances(),
  //                                  std::move(replica_instances_to_remove));
  job_clusters_.erase(iter);

  auto data = job_cluster->ToProto();
  // Mark the data as removed.
  data->set_is_removed(true);
  // Flush and publish the job cluster data.
  return async_data_flusher_(std::move(data), std::move(callback));
}

std::shared_ptr<JobCluster> ExclusiveCluster::GetJobCluster(
    const std::string &job_cluster_id) const {
  auto iter = job_clusters_.find(job_cluster_id);
  return iter != job_clusters_.end() ? iter->second : nullptr;
}

bool ExclusiveCluster::InUse() const { return !job_clusters_.empty(); }

bool ExclusiveCluster::IsIdleNodeInstance(const std::string &job_cluster_id,
                                          const gcs::NodeInstance &node_instance) const {
  RAY_CHECK(GetMode() == rpc::AllocationMode::EXCLUSIVE);
  return job_cluster_id == kEmptyJobClusterId;
}

void ExclusiveCluster::ForeachJobCluster(
    const std::function<void(const std::shared_ptr<JobCluster> &)> &fn) const {
  if (fn == nullptr) {
    return;
  }
  for (const auto &[_, job_cluster] : job_clusters_) {
    fn(job_cluster);
  }
}

///////////////////////// MixedCluster /////////////////////////
bool MixedCluster::IsIdleNodeInstance(const std::string &job_cluster_id,
                                      const gcs::NodeInstance &node_instance) const {
  // TODO(Shanly): The job_cluster_id will always be empty in mixed mode although the node
  // instance is assigned to one or two jobs, so we need to check the node resources
  // usage.
  return node_instance.is_dead();
}

bool MixedCluster::InUse() const {
  // TODO(Shanly): Check if the virtual cluster still running jobs or placement groups.
  return true;
}

///////////////////////// PrimaryCluster /////////////////////////
void PrimaryCluster::Initialize(const GcsInitData &gcs_init_data) {
  const auto &nodes = gcs_init_data.Nodes();
  absl::flat_hash_map<std::string, std::string> dead_node_instances;
  absl::flat_hash_map<VirtualClusterID, const rpc::VirtualClusterTableData *>
      logical_clusters_data;
  absl::flat_hash_map<VirtualClusterID, const rpc::VirtualClusterTableData *>
      job_clusters_data;
  for (const auto &[virtual_cluster_id, virtual_cluster_data] :
       gcs_init_data.VirtualClusters()) {
    if (virtual_cluster_id.IsJobClusterID()) {
      job_clusters_data[virtual_cluster_id] = &virtual_cluster_data;
    } else {
      logical_clusters_data[virtual_cluster_id] = &virtual_cluster_data;
    }
    for (auto &[id, node_instance] : virtual_cluster_data.node_instances()) {
      auto node_id = NodeID::FromHex(id);
      if (!nodes.contains(node_id) ||
          nodes.at(node_id).state() == rpc::GcsNodeInfo::DEAD) {
        dead_node_instances.emplace(id, node_instance.template_id());
      }
    }
  }

  for (const auto &[_, node] : nodes) {
    if (node.state() == rpc::GcsNodeInfo::ALIVE) {
      OnNodeAdd(node);
    }
  }

  for (const auto &[_, virtual_cluster_data] : logical_clusters_data) {
    LoadLogicalCluster(*virtual_cluster_data);
  }

  for (auto &[job_cluster_id, virtual_cluster_data] : job_clusters_data) {
    auto parent_cluster_id = job_cluster_id.ParentID().Binary();
    if (parent_cluster_id == kPrimaryClusterID) {
      LoadJobCluster(*virtual_cluster_data);
    } else {
      auto logical_cluster = std::dynamic_pointer_cast<ExclusiveCluster>(
          GetLogicalCluster(parent_cluster_id));
      RAY_CHECK(logical_cluster != nullptr &&
                logical_cluster->GetMode() == rpc::AllocationMode::EXCLUSIVE);
      logical_cluster->LoadJobCluster(*virtual_cluster_data);
    }
  }

  for (const auto &[id, node_type_name] : dead_node_instances) {
    OnNodeInstanceDead(id, node_type_name);
  }
}

std::shared_ptr<VirtualCluster> PrimaryCluster::GetLogicalCluster(
    const std::string &logical_cluster_id) const {
  auto iter = logical_clusters_.find(logical_cluster_id);
  return iter != logical_clusters_.end() ? iter->second : nullptr;
}

void PrimaryCluster::ForeachVirtualCluster(
    const std::function<void(const std::shared_ptr<VirtualCluster> &)> &fn) const {
  if (fn == nullptr) {
    return;
  }
  for (const auto &[_, logical_cluster] : logical_clusters_) {
    fn(logical_cluster);
    if (logical_cluster->GetMode() == rpc::AllocationMode::EXCLUSIVE) {
      auto exclusive_cluster =
          std::dynamic_pointer_cast<ExclusiveCluster>(logical_cluster);
      exclusive_cluster->ForeachJobCluster(fn);
    }
  }
  ForeachJobCluster(fn);
}

std::shared_ptr<VirtualCluster> PrimaryCluster::LoadLogicalCluster(
    const rpc::VirtualClusterTableData &data) {
  const auto &logical_cluster_id = data.id();
  std::shared_ptr<VirtualCluster> logical_cluster;
  if (data.mode() == rpc::AllocationMode::EXCLUSIVE) {
    logical_cluster =
        std::make_shared<ExclusiveCluster>(logical_cluster_id, async_data_flusher_);
  } else {
    logical_cluster = std::make_shared<MixedCluster>(logical_cluster_id);
  }
  RAY_CHECK(logical_clusters_.emplace(logical_cluster_id, logical_cluster).second);

  auto replica_instances_to_add_to_logical_cluster =
      toReplicaInstances(data.node_instances());
  auto replica_instances_to_remove_from_primary_cluster =
      replica_instances_to_add_to_logical_cluster;
  UpdateNodeInstances(ReplicaInstances(),
                      std::move(replica_instances_to_remove_from_primary_cluster));

  // Update the virtual cluster replica sets and node instances.
  logical_cluster->UpdateNodeInstances(
      std::move(replica_instances_to_add_to_logical_cluster), ReplicaInstances());
  return logical_cluster;
}

Status PrimaryCluster::CreateOrUpdateVirtualCluster(
    rpc::CreateOrUpdateVirtualClusterRequest request,
    CreateOrUpdateVirtualClusterCallback callback) {
  // Calculate the node instances that to be added and to be removed.
  ReplicaInstances replica_instances_to_add_to_logical_cluster;
  ReplicaInstances replica_instances_to_remove_from_logical_cluster;
  auto status = DetermineNodeInstanceAdditionsAndRemovals(
      request,
      replica_instances_to_add_to_logical_cluster,
      replica_instances_to_remove_from_logical_cluster);
  if (!status.ok()) {
    return status;
  }

  auto logical_cluster = GetLogicalCluster(request.virtual_cluster_id());
  if (logical_cluster == nullptr) {
    // replica_instances_to_remove must be empty as the virtual cluster is a new one.
    RAY_CHECK(replica_instances_to_remove_from_logical_cluster.empty());
    if (request.mode() == rpc::AllocationMode::EXCLUSIVE) {
      logical_cluster = std::make_shared<ExclusiveCluster>(request.virtual_cluster_id(),
                                                           async_data_flusher_);
    } else {
      logical_cluster = std::make_shared<MixedCluster>(request.virtual_cluster_id());
    }
    logical_clusters_[request.virtual_cluster_id()] = logical_cluster;
  }

  // Update the main cluster replica sets and node instances.
  // NOTE: The main cluster unnecessary to flush and pub data to other nodes.
  auto replica_instances_to_add_to_primary_cluster =
      replica_instances_to_remove_from_logical_cluster;
  auto replica_instances_to_remove_from_primary_cluster =
      replica_instances_to_add_to_logical_cluster;
  UpdateNodeInstances(std::move(replica_instances_to_add_to_primary_cluster),
                      std::move(replica_instances_to_remove_from_primary_cluster));

  // Update the virtual cluster replica sets and node instances.
  logical_cluster->UpdateNodeInstances(
      std::move(replica_instances_to_add_to_logical_cluster),
      std::move(replica_instances_to_remove_from_logical_cluster));
  return async_data_flusher_(logical_cluster->ToProto(), std::move(callback));
}

Status PrimaryCluster::DetermineNodeInstanceAdditionsAndRemovals(
    const rpc::CreateOrUpdateVirtualClusterRequest &request,
    ReplicaInstances &replica_instances_to_add,
    ReplicaInstances &replica_instances_to_remove) {
  replica_instances_to_add.clear();
  replica_instances_to_remove.clear();

  const auto &logical_cluster_id = request.virtual_cluster_id();
  auto logical_cluster = GetLogicalCluster(logical_cluster_id);
  if (logical_cluster != nullptr) {
    auto replica_sets_to_remove =
        ReplicasDifference(logical_cluster->GetReplicaSets(), request.replica_sets());
    // Lookup idle node instances from the logical cluster based on
    // `replica_sets_to_remove`.
    auto status = logical_cluster->LookupIdleNodeInstances(replica_sets_to_remove,
                                                           replica_instances_to_remove);
    if (!status.ok()) {
      return status;
    }
  }

  auto replica_sets_to_add = ReplicasDifference(
      request.replica_sets(),
      logical_cluster ? logical_cluster->GetReplicaSets() : ReplicaSets());
  // Lookup idle node instances from main cluster based on `replica_sets_to_add`.
  return LookupIdleNodeInstances(replica_sets_to_add, replica_instances_to_add);
}

bool PrimaryCluster::IsIdleNodeInstance(const std::string &job_cluster_id,
                                        const gcs::NodeInstance &node_instance) const {
  RAY_CHECK(GetMode() == rpc::AllocationMode::EXCLUSIVE);
  return job_cluster_id == kEmptyJobClusterId;
}

void PrimaryCluster::OnNodeAdd(const rpc::GcsNodeInfo &node) {
  const auto &template_id = node.node_type_name();
  auto node_instance_id = NodeID::FromBinary(node.node_id()).Hex();
  auto node_instance = std::make_shared<gcs::NodeInstance>();
  node_instance->set_template_id(template_id);
  node_instance->set_hostname(node.node_manager_hostname());
  node_instance->set_is_dead(false);

  InsertNodeInstances(
      {{template_id,
        {{kEmptyJobClusterId, {{node_instance_id, std::move(node_instance)}}}}}});
}

void PrimaryCluster::OnNodeDead(const rpc::GcsNodeInfo &node) {
  const auto &node_type_name = node.node_type_name();
  auto node_instance_id = NodeID::FromBinary(node.node_id()).Hex();
  OnNodeInstanceDead(node_instance_id, node_type_name);
}

void PrimaryCluster::OnNodeInstanceDead(const std::string &node_instance_id,
                                        const std::string &node_type_name) {
  // TODO(Shanly): Build an index from node instance id to cluster id.
  if (MarkNodeInstanceAsDead(node_type_name, node_instance_id)) {
    return;
  }

  for (const auto &[_, logical_cluster] : logical_clusters_) {
    if (logical_cluster->MarkNodeInstanceAsDead(node_type_name, node_instance_id)) {
      return;
    }
  }
}

Status PrimaryCluster::RemoveVirtualCluster(const std::string &virtual_cluster_id,
                                            RemoveVirtualClusterCallback callback) {
  auto cluster_id = VirtualClusterID::FromBinary(virtual_cluster_id);
  if (cluster_id.IsJobClusterID()) {
    auto parent_cluster_id = cluster_id.ParentID().Binary();
    auto virtual_cluster = GetVirtualCluster(parent_cluster_id);
    if (virtual_cluster == nullptr) {
      std::ostringstream ostr;
      ostr << "Failed to remove virtual cluster, parent cluster not exists, virtual "
              "cluster id: "
           << virtual_cluster_id;
      auto message = ostr.str();
      return Status::NotFound(message);
    }
    if (virtual_cluster->GetMode() != rpc::AllocationMode::EXCLUSIVE) {
      std::ostringstream ostr;
      ostr << "Failed to remove virtual cluster, parent cluster is not exclusive, "
              "virtual cluster id: "
           << virtual_cluster_id;
      auto message = ostr.str();
      return Status::InvalidArgument(message);
    }
    ExclusiveCluster *exclusive_cluster =
        dynamic_cast<ExclusiveCluster *>(virtual_cluster.get());
    return exclusive_cluster->RemoveJobCluster(virtual_cluster_id, callback);
  } else {
    return RemoveLogicalCluster(virtual_cluster_id, callback);
  }
}

Status PrimaryCluster::RemoveLogicalCluster(const std::string &logical_cluster_id,
                                            RemoveVirtualClusterCallback callback) {
  auto logical_cluster = GetLogicalCluster(logical_cluster_id);
  if (logical_cluster == nullptr) {
    return Status::NotFound("The logical cluster " + logical_cluster_id +
                            " does not exist.");
  }

  // Check if the virtual cluster is in use.
  if (logical_cluster->InUse()) {
    std::ostringstream ostr;
    ostr << "The virtual cluster " << logical_cluster_id
         << " can not be removed as it still in use.";
    auto message = ostr.str();
    RAY_LOG(ERROR) << message;
    // TODO(Shanly): build a new status.
    return Status::InvalidArgument(message);
  }

  const auto &replica_instances_to_remove = logical_cluster->GetVisibleNodeInstances();

  auto replica_instances_to_add_to_primary_cluster = replica_instances_to_remove;
  UpdateNodeInstances(std::move(replica_instances_to_add_to_primary_cluster),
                      ReplicaInstances());

  // Update the logical_cluster cluster.
  // logical_cluster->UpdateNodeInstances(ReplicaInstances(),
  //                                      std::move(replica_instances_to_remove));
  logical_clusters_.erase(logical_cluster_id);

  auto data = logical_cluster->ToProto();
  data->set_is_removed(true);
  return async_data_flusher_(std::move(data), std::move(callback));
}

std::shared_ptr<VirtualCluster> PrimaryCluster::GetVirtualCluster(
    const std::string &virtual_cluster_id) {
  if (virtual_cluster_id == kPrimaryClusterID) {
    return shared_from_this();
  }

  // Check if it is a logical cluster
  auto logical_cluster = GetLogicalCluster(virtual_cluster_id);
  if (logical_cluster != nullptr) {
    return logical_cluster;
  }
  // Check if it is a job cluster
  auto job_cluster = GetJobCluster(virtual_cluster_id);
  if (job_cluster != nullptr) {
    return job_cluster;
  }
  // Check if it is a job cluster of any logical cluster
  for (auto &[cluster_id, logical_cluster] : logical_clusters_) {
    if (logical_cluster->GetMode() == rpc::AllocationMode::EXCLUSIVE) {
      ExclusiveCluster *exclusive_cluster =
          dynamic_cast<ExclusiveCluster *>(logical_cluster.get());
      auto job_cluster = exclusive_cluster->GetJobCluster(virtual_cluster_id);
      if (job_cluster != nullptr) {
        return job_cluster;
      }
    }
  }
  return nullptr;
}

void PrimaryCluster::GetVirtualClustersData(rpc::GetVirtualClustersRequest request,
                                            GetVirtualClustersDataCallback callback) {
  std::vector<std::shared_ptr<rpc::VirtualClusterTableData>> virtual_cluster_data_list;
  auto virtual_cluster_id = request.virtual_cluster_id();
  bool include_job_clusters = request.include_job_clusters();
  bool only_include_mixed_cluster = request.only_include_mixed_clusters();

  auto visit_proto_data = [&](const VirtualCluster *cluster) {
    if (include_job_clusters && cluster->GetMode() == rpc::AllocationMode::EXCLUSIVE) {
      auto exclusive_cluster = dynamic_cast<const ExclusiveCluster *>(cluster);
      exclusive_cluster->ForeachJobCluster(
          [&](const auto &job_cluster) { callback(job_cluster->ToProto()); });
    }
    if (only_include_mixed_cluster &&
        cluster->GetMode() == rpc::AllocationMode::EXCLUSIVE) {
      return;
    }
    if (cluster->GetID() != kPrimaryClusterID) {
      // Skip the primary cluster's proto data.
      callback(cluster->ToProto());
    }
  };

  if (virtual_cluster_id.empty()) {
    // Get all virtual clusters data.
    for (const auto &[_, logical_cluster] : logical_clusters_) {
      visit_proto_data(logical_cluster.get());
    }
    visit_proto_data(this);
    return;
  }

  if (virtual_cluster_id == kPrimaryClusterID) {
    visit_proto_data(this);
    return;
  }

  auto logical_cluster = GetLogicalCluster(virtual_cluster_id);
  if (logical_cluster != nullptr) {
    visit_proto_data(logical_cluster.get());
  }
}

}  // namespace gcs
}  // namespace ray