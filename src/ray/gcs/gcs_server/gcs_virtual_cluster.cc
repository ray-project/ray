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
        RAY_CHECK(job_cluster_iter->second.erase(id) == 1);
        RAY_CHECK(--(replica_set_iter->second) >= 0);
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

std::shared_ptr<rpc::VirtualClusterTableData> VirtualCluster::ToProto() const {
  auto data = std::make_shared<rpc::VirtualClusterTableData>();
  data->set_id(GetID());
  data->set_mode(GetMode());
  data->set_revision(GetRevision());
  data->mutable_replica_sets()->insert(replica_sets_.begin(), replica_sets_.end());
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
Status ExclusiveCluster::CreateJobCluster(const std::string &job_name,
                                          ReplicaSets replica_sets,
                                          CreateOrUpdateVirtualClusterCallback callback) {
  if (GetMode() != rpc::AllocationMode::Exclusive) {
    std::ostringstream ostr;
    ostr << "The job cluster can only be created in exclusive mode, virtual_cluster_id: "
         << GetID() << ", job_name: " << job_name;
    return Status::InvalidArgument(ostr.str());
  }

  auto job_cluster_id =
      VirtualClusterID::FromBinary(GetID()).BuildJobClusterID(job_name).Binary();

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

  // Create a job cluster.
  auto job_cluster = std::make_shared<JobCluster>(job_cluster_id);
  job_cluster->UpdateNodeInstances(std::move(replica_instances_to_add),
                                   ReplicaInstances());
  RAY_CHECK(job_clusters_.emplace(job_cluster_id, job_cluster).second);

  // Flush and publish the job cluster data.
  return async_data_flusher_(job_cluster->ToProto(), std::move(callback));
}

Status ExclusiveCluster::RemoveJobCluster(const std::string &job_name,
                                          RemoveVirtualClusterCallback callback) {
  if (GetMode() != rpc::AllocationMode::Exclusive) {
    std::ostringstream ostr;
    ostr << "The job cluster can only be removed in exclusive mode, virtual_cluster_id: "
         << GetID() << ", job_name: " << job_name;
    return Status::InvalidArgument(ostr.str());
  }

  auto job_cluster_id =
      VirtualClusterID::FromBinary(GetID()).BuildJobClusterID(job_name).Binary();
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
    const std::string &job_id) const {
  auto job_cluster_id =
      VirtualClusterID::FromBinary(GetID()).BuildJobClusterID(job_id).Binary();
  auto iter = job_clusters_.find(job_cluster_id);
  return iter != job_clusters_.end() ? iter->second : nullptr;
}

bool ExclusiveCluster::InUse() const { return !job_clusters_.empty(); }

bool ExclusiveCluster::IsIdleNodeInstance(const std::string &job_cluster_id,
                                          const gcs::NodeInstance &node_instance) const {
  RAY_CHECK(GetMode() == rpc::AllocationMode::Exclusive);
  return job_cluster_id == kEmptyJobClusterId;
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
std::shared_ptr<VirtualCluster> PrimaryCluster::GetLogicalCluster(
    const std::string &logical_cluster_id) const {
  auto iter = logical_clusters_.find(logical_cluster_id);
  return iter != logical_clusters_.end() ? iter->second : nullptr;
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
    if (request.mode() == rpc::AllocationMode::Exclusive) {
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
  RAY_CHECK(GetMode() == rpc::AllocationMode::Exclusive);
  return job_cluster_id == kEmptyJobClusterId;
}

void PrimaryCluster::OnNodeAdd(const rpc::GcsNodeInfo &node) {
  const auto &template_id = node.template_id();
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
  const auto &template_id = node.template_id();
  auto node_instance_id = NodeID::FromBinary(node.node_id()).Hex();

  // TODO(Shanly): Build an index from node instance id to cluster id.
  if (MarkNodeInstanceAsDead(template_id, node_instance_id)) {
    return;
  }

  for (const auto &[_, logical_cluster] : logical_clusters_) {
    if (logical_cluster->MarkNodeInstanceAsDead(template_id, node_instance_id)) {
      return;
    }
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

}  // namespace gcs
}  // namespace ray