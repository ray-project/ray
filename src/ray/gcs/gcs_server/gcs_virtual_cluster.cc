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

ReplicaSets buildReplicaSets(const ReplicaInstances &replica_instances) {
  ReplicaSets result;
  for (const auto &[template_id, job_node_instances] : replica_instances) {
    int32_t count = 0;
    for (const auto &[_, node_instances] : job_node_instances) {
      count += node_instances.size();
    }
    result[template_id] = count;
  }
  return result;
}

/// Calculate the difference between two replica instances.
ReplicaInstances ReplicaInstancesDifference(const ReplicaInstances &left,
                                            const ReplicaInstances &right) {
  ReplicaInstances result;
  for (const auto &[template_id, job_node_instances] : left) {
    for (const auto &[job_cluster_id, node_instances] : job_node_instances) {
      auto right_iter = right.find(template_id);
      if (right_iter == right.end()) {
        result[template_id][job_cluster_id] = node_instances;
      } else {
        auto right_job_cluster_iter = right_iter->second.find(job_cluster_id);
        if (right_job_cluster_iter == right_iter->second.end()) {
          result[template_id][job_cluster_id] = node_instances;
        } else {
          for (const auto &[node_instance_id, node_instance] : node_instances) {
            if (right_job_cluster_iter->second.find(node_instance_id) ==
                right_job_cluster_iter->second.end()) {
              result[template_id][job_cluster_id][node_instance_id] = node_instance;
            }
          }
        }
      }
    }
  }
  return result;
}

std::string DebugString(const ReplicaInstances &replica_instances, int indent /* = 0*/) {
  std::ostringstream stream;
  stream << "{\n";
  for (const auto &[template_id, job_cluster_instances] : replica_instances) {
    stream << std::string(indent + 2, ' ') << template_id << " : {\n";
    for (const auto &[job_cluster_id, node_instances] : job_cluster_instances) {
      stream << std::string(indent + 4, ' ') << job_cluster_id << " : {\n";
      for (const auto &[node_instance_id, node_instance] : node_instances) {
        stream << std::string(indent + 6, ' ') << node_instance_id << " : "
               << node_instance->DebugString() << ",\n";
      }
      stream << std::string(indent + 4, ' ') << "},\n";
    }
    stream << std::string(indent + 2, ' ') << "},\n";
  }
  stream << std::string(indent, ' ') << "}\n";
  return stream.str();
}

std::string DebugString(const ReplicaSets &replica_sets, int indent /* = 0*/) {
  std::ostringstream stream;
  stream << "{\n";
  for (const auto &[template_id, replica] : replica_sets) {
    stream << std::string(indent + 2, ' ') << template_id << " : " << replica << ",\n";
  }
  stream << std::string(indent, ' ') << "}\n";
  return stream.str();
}

///////////////////////// VirtualCluster /////////////////////////
void VirtualCluster::UpdateNodeInstances(ReplicaInstances replica_instances_to_add,
                                         ReplicaInstances replica_instances_to_remove) {
  // Remove node instances from the virtual cluster.
  RemoveNodeInstances(std::move(replica_instances_to_remove));
  // Insert node instances to the virtual cluster.
  InsertNodeInstances(std::move(replica_instances_to_add));
  // Update the revision of the cluster.
  revision_ = current_sys_time_ns();
}

void VirtualCluster::InsertNodeInstances(ReplicaInstances replica_instances) {
  for (auto &[template_id, job_node_instances] : replica_instances) {
    auto &cached_job_node_instances = visible_node_instances_[template_id];
    auto &replicas = replica_sets_[template_id];
    for (auto &[job_cluster_id, node_instances] : job_node_instances) {
      auto &cached_node_instances = cached_job_node_instances[job_cluster_id];
      for (auto &[id, node_instance] : node_instances) {
        node_instances_map_[id] = node_instance;
        cached_node_instances[id] = std::move(node_instance);
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
          node_instances_map_.erase(id);
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

bool VirtualCluster::LookupUndividedNodeInstances(
    const ReplicaSets &replica_sets,
    ReplicaInstances &replica_instances,
    NodeInstanceFilter node_instance_filter) const {
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
    auto empty_iter = iter->second.find(kUndividedClusterId);
    if (empty_iter == iter->second.end()) {
      success = false;
      continue;
    }

    auto &job_node_instances = template_node_instances[kUndividedClusterId];
    for (const auto &[id, node_instance] : empty_iter->second) {
      if (node_instance_filter != nullptr && !node_instance_filter(*node_instance)) {
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

  return success;
}

bool VirtualCluster::MarkNodeInstanceAsDead(const std::string &template_id,
                                            const std::string &node_instance_id) {
  auto iter = node_instances_map_.find(node_instance_id);
  if (iter != node_instances_map_.end() && iter->second->template_id() == template_id) {
    iter->second->set_is_dead(true);
    return true;
  }
  return false;
}

bool VirtualCluster::ContainsNodeInstance(const std::string &node_instance_id) {
  return node_instances_map_.contains(node_instance_id);
}

void VirtualCluster::ForeachNodeInstance(
    const std::function<void(const std::shared_ptr<NodeInstance> &)> &fn) const {
  if (fn == nullptr) {
    return;
  }
  for (const auto &[template_id, job_node_instances] : visible_node_instances_) {
    for (const auto &[job_cluster_id, node_instances] : job_node_instances) {
      for (const auto &[_, node_instance] : node_instances) {
        fn(node_instance);
      }
    }
  }
}

bool VirtualCluster::ReplenishUndividedNodeInstances(
    const NodeInstanceReplenishCallback &callback) {
  RAY_CHECK(callback != nullptr);
  bool any_node_instance_replenished = false;
  for (auto &[template_id, job_node_instances] : visible_node_instances_) {
    auto iter = job_node_instances.find(kUndividedClusterId);
    if (iter == job_node_instances.end()) {
      continue;
    }

    absl::flat_hash_map<std::string, std::shared_ptr<gcs::NodeInstance>>
        node_instances_to_add;
    absl::flat_hash_map<std::string, std::shared_ptr<gcs::NodeInstance>>
        node_instances_to_remove;
    for (auto &[node_instance_id, node_instance] : iter->second) {
      if (node_instance->is_dead()) {
        if (auto replenished_node_instance = callback(node_instance)) {
          RAY_LOG(INFO) << "Replenish node instance " << node_instance->node_instance_id()
                        << " in virtual cluster " << GetID() << " with node instance "
                        << replenished_node_instance->node_instance_id();
          node_instances_to_remove.emplace(node_instance_id, node_instance);
          node_instances_to_add.emplace(replenished_node_instance->node_instance_id(),
                                        replenished_node_instance);
          any_node_instance_replenished = true;
          break;
        }
      }
    }
    for (auto &[node_instance_id, node_instance] : node_instances_to_remove) {
      iter->second.erase(node_instance_id);
      node_instances_map_.erase(node_instance_id);
    }
    for (auto &[node_instance_id, node_instance] : node_instances_to_add) {
      iter->second.emplace(node_instance_id, node_instance);
      node_instances_map_[node_instance_id] = node_instance;
    }
  }

  return any_node_instance_replenished;
}

std::shared_ptr<NodeInstance> VirtualCluster::ReplenishUndividedNodeInstance(
    std::shared_ptr<NodeInstance> node_instance_to_replenish) {
  if (node_instance_to_replenish == nullptr || !node_instance_to_replenish->is_dead()) {
    return nullptr;
  }

  auto template_iter =
      visible_node_instances_.find(node_instance_to_replenish->template_id());
  if (template_iter == visible_node_instances_.end()) {
    return nullptr;
  }

  auto job_iter = template_iter->second.find(kUndividedClusterId);
  if (job_iter == template_iter->second.end()) {
    return nullptr;
  }

  std::shared_ptr<NodeInstance> replenished_node_instance;
  for (auto &[node_instance_id, node_instance] : job_iter->second) {
    if (!node_instance->is_dead()) {
      replenished_node_instance = node_instance;
      break;
    }
  }
  if (replenished_node_instance != nullptr) {
    job_iter->second.erase(replenished_node_instance->node_instance_id());
    job_iter->second.emplace(node_instance_to_replenish->node_instance_id(),
                             node_instance_to_replenish);

    node_instances_map_.erase(replenished_node_instance->node_instance_id());
    node_instances_map_[node_instance_to_replenish->node_instance_id()] =
        node_instance_to_replenish;
  }

  return replenished_node_instance;
}

std::shared_ptr<rpc::VirtualClusterTableData> VirtualCluster::ToProto() const {
  auto data = std::make_shared<rpc::VirtualClusterTableData>();
  data->set_id(GetID());
  data->set_divisible(Divisible());
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

///////////////////////// DivisibleCluster /////////////////////////
void DivisibleCluster::LoadJobCluster(const std::string &job_cluster_id,
                                      ReplicaInstances replica_instances) {
  RAY_CHECK(VirtualClusterID::FromBinary(job_cluster_id).IsJobClusterID());
  RAY_CHECK(job_clusters_.find(job_cluster_id) == job_clusters_.end());

  DoCreateJobCluster(job_cluster_id, std::move(replica_instances));
}

Status DivisibleCluster::CreateJobCluster(const std::string &job_cluster_id,
                                          ReplicaSets replica_sets,
                                          CreateOrUpdateVirtualClusterCallback callback,
                                          ReplicaSets *replica_sets_to_recommend) {
  if (!Divisible()) {
    std::ostringstream ostr;
    ostr << "The job cluster can not be created in indivisible cluster " << GetID();
    return Status::InvalidArgument(ostr.str());
  }

  auto iter = job_clusters_.find(job_cluster_id);
  if (iter != job_clusters_.end()) {
    std::ostringstream ostr;
    ostr << "The job cluster " << job_cluster_id << " already exists.";
    return Status::InvalidArgument(ostr.str());
  }

  ReplicaInstances replica_instances_to_add;
  // Lookup undivided alive node instances based on `replica_sets_to_add`.
  auto success = LookupUndividedNodeInstances(
      replica_sets, replica_instances_to_add, [](const auto &node_instance) {
        return !node_instance.is_dead();
      });
  if (!success) {
    if (replica_sets_to_recommend != nullptr) {
      *replica_sets_to_recommend = buildReplicaSets(replica_instances_to_add);
    }
    std::ostringstream ostr;
    ostr << "No enough node instances to create the job cluster " << job_cluster_id;
    return Status::OutOfResource(ostr.str());
  }

  auto job_cluster =
      DoCreateJobCluster(job_cluster_id, std::move(replica_instances_to_add));
  // Flush and publish the job cluster data.
  return async_data_flusher_(job_cluster->ToProto(), std::move(callback));
}

std::shared_ptr<JobCluster> DivisibleCluster::DoCreateJobCluster(
    const std::string &job_cluster_id, ReplicaInstances replica_instances_to_add) {
  auto replica_instances_to_remove_from_current_cluster = replica_instances_to_add;
  auto replica_instances_to_add_to_current_cluster = replica_instances_to_add;
  for (auto &[template_id, job_node_instances] :
       replica_instances_to_add_to_current_cluster) {
    auto node_instances = std::move(job_node_instances[kUndividedClusterId]);
    job_node_instances.erase(kUndividedClusterId);
    job_node_instances[job_cluster_id] = std::move(node_instances);
  }
  UpdateNodeInstances(std::move(replica_instances_to_add_to_current_cluster),
                      std::move(replica_instances_to_remove_from_current_cluster));

  // Create a job cluster.
  auto job_cluster =
      std::make_shared<JobCluster>(job_cluster_id, cluster_resource_manager_);
  job_cluster->UpdateNodeInstances(std::move(replica_instances_to_add),
                                   ReplicaInstances());
  RAY_CHECK(job_clusters_.emplace(job_cluster_id, job_cluster).second);
  return job_cluster;
}

Status DivisibleCluster::RemoveJobCluster(const std::string &job_cluster_id,
                                          RemoveVirtualClusterCallback callback) {
  if (!Divisible()) {
    std::ostringstream ostr;
    ostr << "The job cluster " << job_cluster_id
         << " can not be removed from indivisible cluster " << GetID();
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
    auto node_instances = std::move(job_node_instances[kUndividedClusterId]);
    job_node_instances.erase(kUndividedClusterId);
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

std::shared_ptr<JobCluster> DivisibleCluster::GetJobCluster(
    const std::string &job_cluster_id) const {
  auto iter = job_clusters_.find(job_cluster_id);
  return iter != job_clusters_.end() ? iter->second : nullptr;
}

bool DivisibleCluster::InUse() const { return !job_clusters_.empty(); }

bool DivisibleCluster::IsUndividedNodeInstanceIdle(
    const gcs::NodeInstance &node_instance) const {
  auto template_iter = visible_node_instances_.find(node_instance.template_id());
  if (template_iter == visible_node_instances_.end()) {
    return false;
  }
  auto job_iter = template_iter->second.find(kUndividedClusterId);
  if (job_iter == template_iter->second.end()) {
    return false;
  }
  return job_iter->second.contains(node_instance.node_instance_id());
}

void DivisibleCluster::ForeachJobCluster(
    const std::function<void(const std::shared_ptr<JobCluster> &)> &fn) const {
  if (fn == nullptr) {
    return;
  }
  for (const auto &[_, job_cluster] : job_clusters_) {
    fn(job_cluster);
  }
}

bool DivisibleCluster::ReplenishNodeInstances(
    const NodeInstanceReplenishCallback &callback) {
  RAY_CHECK(callback != nullptr);
  bool any_node_instance_replenished = false;
  for (const auto &[_, job_cluster] : job_clusters_) {
    // Explicitly capture the `job_cluster` by value to avoid the compile error.
    // e.g. error: 'job_cluster_id' in capture list does not name a variable
    const auto &job_cluster_id = job_cluster->GetID();
    bool replenished = job_cluster->ReplenishNodeInstances(
        [this, &job_cluster_id, &any_node_instance_replenished, &callback](
            std::shared_ptr<NodeInstance> node_instance) {
          const auto &template_id = node_instance->template_id();
          if (auto replenished_node_instance = callback(node_instance)) {
            RAY_LOG(INFO) << "Replenish node instance "
                          << node_instance->node_instance_id() << " in virtual cluster "
                          << job_cluster_id << " with node instance "
                          << replenished_node_instance->node_instance_id()
                          << " in primary cluster.";
            ReplicaInstances replica_instances_to_add;
            replica_instances_to_add[template_id][job_cluster_id]
                                    [replenished_node_instance->node_instance_id()] =
                                        replenished_node_instance;

            ReplicaInstances replica_instances_to_remove;
            replica_instances_to_remove[template_id][job_cluster_id]
                                       [node_instance->node_instance_id()] =
                                           node_instance;

            UpdateNodeInstances(std::move(replica_instances_to_add),
                                std::move(replica_instances_to_remove));

            any_node_instance_replenished = true;
            return replenished_node_instance;
          }

          auto replenished_node_instance = ReplenishUndividedNodeInstance(node_instance);
          if (replenished_node_instance != nullptr) {
            RAY_LOG(INFO) << "Replenish node instance "
                          << node_instance->node_instance_id() << " in job cluster "
                          << job_cluster_id << " with node instance "
                          << replenished_node_instance->node_instance_id()
                          << " in virtual cluster " << GetID();
            return replenished_node_instance;
          }

          return replenished_node_instance;
        });

    if (replenished) {
      // Flush and publish the job cluster data.
      auto status = async_data_flusher_(job_cluster->ToProto(), nullptr);
      if (!status.ok()) {
        RAY_LOG(ERROR) << "Failed to flush and publish the job cluster " << job_cluster_id
                       << " data: " << status.message();
      }
    }
  }

  if (ReplenishUndividedNodeInstances(callback)) {
    any_node_instance_replenished = true;
  }

  return any_node_instance_replenished;
}

///////////////////////// IndivisibleCluster /////////////////////////
bool IndivisibleCluster::IsUndividedNodeInstanceIdle(
    const gcs::NodeInstance &node_instance) const {
  if (node_instance.is_dead()) {
    return true;
  }
  auto node_id =
      scheduling::NodeID(NodeID::FromHex(node_instance.node_instance_id()).Binary());
  const auto &node_resources = cluster_resource_manager_.GetNodeResources(node_id);
  // TODO(Chong-Li): the resource view sync message may lag.
  if (node_resources.normal_task_resources.IsEmpty() &&
      node_resources.total == node_resources.available) {
    return true;
  }
  return false;
}

bool IndivisibleCluster::InUse() const {
  for (const auto &[template_id, job_cluster_instances] : visible_node_instances_) {
    for (const auto &[job_cluster_id, node_instances] : job_cluster_instances) {
      for (const auto &[node_instance_id, node_instance] : node_instances) {
        if (!IsUndividedNodeInstanceIdle(*node_instance)) {
          return true;
        }
      }
    }
  }
  return false;
}

bool IndivisibleCluster::ReplenishNodeInstances(
    const NodeInstanceReplenishCallback &callback) {
  return ReplenishUndividedNodeInstances(callback);
}

///////////////////////// JobCluster /////////////////////////
void JobCluster::OnDetachedActorRegistration(const ActorID &actor_id) {
  detached_actors_.insert(actor_id);
}

void JobCluster::OnDetachedActorDestroy(const ActorID &actor_id) {
  detached_actors_.erase(actor_id);
}

void JobCluster::OnDetachedPlacementGroupRegistration(
    const PlacementGroupID &placement_group_id) {
  detached_placement_groups_.insert(placement_group_id);
}

void JobCluster::OnDetachedPlacementGroupDestroy(
    const PlacementGroupID &placement_group_id) {
  detached_placement_groups_.erase(placement_group_id);
}

bool JobCluster::InUse() const {
  // TODO(xsuler) this should consider normal task if job cluster
  // is removed asynchronously.
  return !detached_actors_.empty() || !detached_placement_groups_.empty();
}

///////////////////////// PrimaryCluster /////////////////////////
void PrimaryCluster::Initialize(const GcsInitData &gcs_init_data) {
  // Let indivisible cluster be Vi, divisible cluster be Vd, job cluster be J, empty job
  // cluster be E, then
  //
  //   Nodes(Vd) = Σ(Nodes(J)) + Nodes(E).
  //
  // When node belongs to Vi is dead (it's a simple case):
  // 1. Find a new replica instance from primary cluster to replace the dead one in Vi,
  // then flush and publish Vi.
  //
  // When node belongs to J is dead(It will also be dead in Vd as both Vd and J hold the
  // shared node instance).
  // 1. Find a new replica instance from E to replace the dead one in J, then flush and
  // publish J.
  // 2. If there is no enough node instances in E, then find a new replica instance from
  // primary cluster to replace the dead one in J, then:
  //   a. flush and publish J.
  //   b. flush Vd.
  // 3. If there is no enough node instances in primary cluster, then wait for the new
  // node to register.
  //
  // When node belongs to E is dead (it's simple case).
  // 1. Find a new replica instance from primary cluster to replace the dead one in E,
  // then flush Vd.
  // 2. If there is no enough node instances in primary cluster, then wait for the new
  // node to register.
  //
  // When failover happens, we need to load the logical clusters and job clusters from the
  // GCS tables, and repair the Vd based on J.
  // 1. Find the different node instances between Σ(Nodes(J)) and Nodes(Vd), let the
  // difference be D.
  //    D = Σ(Nodes(J)) - Vd
  // 2. Remove the dead node instances from Vd based on the replica sets of D and then
  // flush Vd.
  const auto &nodes = gcs_init_data.Nodes();
  for (const auto &[_, node] : nodes) {
    if (node.state() == rpc::GcsNodeInfo::ALIVE) {
      OnNodeAdd(node);
    }
  }

  absl::flat_hash_map<VirtualClusterID, ReplicaInstances>
      job_cluster_replica_instances_map;
  for (const auto &[virtual_cluster_id, virtual_cluster_data] :
       gcs_init_data.VirtualClusters()) {
    // Convert the node instances to replica instances and mark the dead node instances.
    auto replica_instances = toReplicaInstances(virtual_cluster_data.node_instances());
    for (auto &[_, job_node_instances] : replica_instances) {
      for (auto &[_, node_instances] : job_node_instances) {
        for (auto &[node_instance_id, node_instance] : node_instances) {
          auto node_id = NodeID::FromHex(node_instance_id);
          auto it = nodes.find(node_id);
          if (it == nodes.end() || it->second.state() == rpc::GcsNodeInfo::DEAD) {
            node_instance->set_is_dead(true);
          }
        }
      }
    }
    // Stash the job clusters and load the logical clusters.
    if (virtual_cluster_id.IsJobClusterID()) {
      job_cluster_replica_instances_map[virtual_cluster_id] =
          std::move(replica_instances);
    } else {
      // Load the logical cluster.
      LoadLogicalCluster(virtual_cluster_data.id(),
                         virtual_cluster_data.divisible(),
                         std::move(replica_instances));
    }
  }

  // Repair the divisible cluster's node instances to ensure that the node instances in
  // the virtual clusters contains all the node instances in the job clusters. Calculate
  // the different node instances between Σ(Nodes(J)) and Nodes(Ve),
  //   D = Σ(Nodes(J)) - Ve
  for (auto &[job_cluster_id, replica_instances] : job_cluster_replica_instances_map) {
    auto parent_cluster_id = job_cluster_id.ParentID().Binary();
    auto virtual_cluster = GetVirtualCluster(parent_cluster_id);
    RAY_CHECK(virtual_cluster != nullptr);
    auto replica_instances_to_repair = ReplicaInstancesDifference(
        replica_instances, virtual_cluster->GetVisibleNodeInstances());
    if (replica_instances_to_repair.empty()) {
      continue;
    }

    ReplicaInstances replica_instances_to_remove;
    auto replica_sets_to_repair = buildReplicaSets(replica_instances_to_repair);
    // Lookup undivided dead node instances best effort.
    virtual_cluster->LookupUndividedNodeInstances(
        replica_sets_to_repair,
        replica_instances_to_remove,
        [](const auto &node_instance) { return node_instance.is_dead(); });
    RAY_LOG(INFO) << "Repair the divisible cluster " << parent_cluster_id
                  << " based on the job cluster " << job_cluster_id
                  << "\nreplica_sets_to_repair: "
                  << ray::gcs::DebugString(replica_sets_to_repair)
                  << "replica_instances_to_repair: "
                  << ray::gcs::DebugString(replica_instances_to_repair)
                  << "replica_instances_to_remove: "
                  << ray::gcs::DebugString(replica_instances_to_remove);
    virtual_cluster->UpdateNodeInstances(std::move(replica_instances_to_repair),
                                         std::move(replica_instances_to_remove));
  }

  // Replay the job clusters.
  for (auto &[job_cluster_id, replica_instances] : job_cluster_replica_instances_map) {
    auto parent_cluster_id = job_cluster_id.ParentID().Binary();
    if (parent_cluster_id == kPrimaryClusterID) {
      LoadJobCluster(job_cluster_id.Binary(), std::move(replica_instances));
    } else {
      auto logical_cluster = std::dynamic_pointer_cast<DivisibleCluster>(
          GetLogicalCluster(parent_cluster_id));
      RAY_CHECK(logical_cluster != nullptr && logical_cluster->Divisible());
      logical_cluster->LoadJobCluster(job_cluster_id.Binary(),
                                      std::move(replica_instances));
    }
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
    if (logical_cluster->Divisible()) {
      auto divisible_cluster =
          std::dynamic_pointer_cast<DivisibleCluster>(logical_cluster);
      divisible_cluster->ForeachJobCluster(fn);
    }
  }
  ForeachJobCluster(fn);
}

std::shared_ptr<VirtualCluster> PrimaryCluster::LoadLogicalCluster(
    const std::string &virtual_cluster_id,
    bool divisible,
    ReplicaInstances replica_instances) {
  std::shared_ptr<VirtualCluster> logical_cluster;
  if (divisible) {
    logical_cluster = std::make_shared<DivisibleCluster>(
        virtual_cluster_id, async_data_flusher_, cluster_resource_manager_);
  } else {
    logical_cluster = std::make_shared<IndivisibleCluster>(virtual_cluster_id,
                                                           cluster_resource_manager_);
  }
  RAY_CHECK(logical_clusters_.emplace(virtual_cluster_id, logical_cluster).second);

  auto replica_instances_to_remove_from_primary_cluster = replica_instances;
  UpdateNodeInstances(ReplicaInstances(),
                      std::move(replica_instances_to_remove_from_primary_cluster));

  // Update the virtual cluster replica sets and node instances.
  logical_cluster->UpdateNodeInstances(std::move(replica_instances), ReplicaInstances());
  return logical_cluster;
}

Status PrimaryCluster::CreateOrUpdateVirtualCluster(
    rpc::CreateOrUpdateVirtualClusterRequest request,
    CreateOrUpdateVirtualClusterCallback callback,
    ReplicaSets *replica_sets_to_recommend) {
  // Calculate the node instances that to be added and to be removed.
  ReplicaInstances replica_instances_to_add_to_logical_cluster;
  ReplicaInstances replica_instances_to_remove_from_logical_cluster;
  auto status = DetermineNodeInstanceAdditionsAndRemovals(
      request,
      replica_instances_to_add_to_logical_cluster,
      replica_instances_to_remove_from_logical_cluster);
  if (!status.ok()) {
    // Calculate the replica sets that we can fulfill the
    // request at most. It can be used as a suggestion to adjust the request if it fails.
    if (replica_sets_to_recommend) {
      if (status.IsOutOfResource()) {
        *replica_sets_to_recommend =
            buildReplicaSets(replica_instances_to_add_to_logical_cluster);
      } else if (status.IsUnsafeToRemove()) {
        *replica_sets_to_recommend =
            buildReplicaSets(replica_instances_to_remove_from_logical_cluster);
      }
    }
    return status;
  }

  auto logical_cluster = GetLogicalCluster(request.virtual_cluster_id());
  if (logical_cluster == nullptr) {
    // replica_instances_to_remove must be empty as the virtual cluster is a new one.
    RAY_CHECK(replica_instances_to_remove_from_logical_cluster.empty());
    if (request.divisible()) {
      logical_cluster = std::make_shared<DivisibleCluster>(
          request.virtual_cluster_id(), async_data_flusher_, cluster_resource_manager_);
    } else {
      logical_cluster = std::make_shared<IndivisibleCluster>(request.virtual_cluster_id(),
                                                             cluster_resource_manager_);
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
    // TODO(Shanly): Iterate the dead node instances in advance.
    auto success = logical_cluster->LookupUndividedNodeInstances(
        replica_sets_to_remove,
        replica_instances_to_remove,
        [logical_cluster](const auto &node_instance) {
          if (logical_cluster->Divisible()) {
            return true;
          }
          return logical_cluster->IsUndividedNodeInstanceIdle(node_instance);
        });
    if (!success) {
      return Status::UnsafeToRemove(
          "No enough nodes to remove from the virtual cluster. The replica sets that gcs "
          "can remove "
          "at most are shown below. Use it as a suggestion to "
          "adjust your request or cluster.");
    }
  }

  auto replica_sets_to_add = ReplicasDifference(
      request.replica_sets(),
      logical_cluster ? logical_cluster->GetReplicaSets() : ReplicaSets());
  // Lookup undivided alive node instances from main cluster based on
  // `replica_sets_to_add`.
  auto success = LookupUndividedNodeInstances(
      replica_sets_to_add, replica_instances_to_add, [](const auto &node_instance) {
        return !node_instance.is_dead();
      });
  if (!success) {
    return Status::OutOfResource(
        "No enough nodes to add to the virtual cluster. The replica sets that gcs can "
        "add "
        "at most are shown below. Use it as a suggestion to "
        "adjust your request or cluster.");
  }
  return Status::OK();
}

void PrimaryCluster::OnNodeAdd(const rpc::GcsNodeInfo &node) {
  const auto &template_id = node.node_type_name();
  auto node_instance_id = NodeID::FromBinary(node.node_id()).Hex();
  auto node_instance = std::make_shared<gcs::NodeInstance>(node_instance_id);
  node_instance->set_template_id(template_id);
  node_instance->set_hostname(node.node_manager_hostname());
  node_instance->set_is_dead(false);

  InsertNodeInstances(
      {{template_id,
        {{kUndividedClusterId, {{node_instance_id, std::move(node_instance)}}}}}});
}

void PrimaryCluster::OnNodeDead(const rpc::GcsNodeInfo &node) {
  const auto &node_type_name = node.node_type_name();
  auto node_instance_id = NodeID::FromBinary(node.node_id()).Hex();
  OnNodeInstanceDead(node_instance_id, node_type_name);
}

void PrimaryCluster::OnNodeInstanceDead(const std::string &node_instance_id,
                                        const std::string &node_type_name) {
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
    auto parent_cluster = GetVirtualCluster(parent_cluster_id);
    if (parent_cluster == nullptr) {
      std::ostringstream ostr;
      ostr << "Failed to remove virtual cluster, parent cluster not exists, virtual "
              "cluster id: "
           << virtual_cluster_id;
      auto message = ostr.str();
      return Status::NotFound(message);
    }
    if (!parent_cluster->Divisible()) {
      std::ostringstream ostr;
      ostr << "Failed to remove virtual cluster, parent cluster is not divisible, "
              "virtual cluster id: "
           << virtual_cluster_id;
      auto message = ostr.str();
      return Status::InvalidArgument(message);
    }
    auto virtual_cluster = GetVirtualCluster(virtual_cluster_id);
    if (virtual_cluster != nullptr) {
      JobCluster *job_cluster = dynamic_cast<JobCluster *>(virtual_cluster.get());
      if (job_cluster->InUse()) {
        std::ostringstream ostr;
        ostr << "Failed to remove virtual cluster, job cluster is in use, "
             << "virtual cluster id: " << virtual_cluster_id;
        auto message = ostr.str();
        return Status::Invalid(message);
      }
    }
    DivisibleCluster *divisible_cluster =
        dynamic_cast<DivisibleCluster *>(parent_cluster.get());
    return divisible_cluster->RemoveJobCluster(virtual_cluster_id, callback);
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
  ReplicaInstances in_use_instances;
  if (logical_cluster->InUse()) {
    std::ostringstream ostr;
    ostr << "The virtual cluster " << logical_cluster_id
         << " can not be removed as it is still in use. ";
    auto message = ostr.str();
    RAY_LOG(ERROR) << message;

    return Status::UnsafeToRemove(message);
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
    if (logical_cluster->Divisible()) {
      DivisibleCluster *divisible_cluster =
          dynamic_cast<DivisibleCluster *>(logical_cluster.get());
      auto job_cluster = divisible_cluster->GetJobCluster(virtual_cluster_id);
      if (job_cluster != nullptr) {
        return job_cluster;
      }
    }
  }
  return nullptr;
}

void PrimaryCluster::ForeachVirtualClustersData(
    rpc::GetVirtualClustersRequest request, VirtualClustersDataVisitCallback callback) {
  std::vector<std::shared_ptr<rpc::VirtualClusterTableData>> virtual_cluster_data_list;
  auto virtual_cluster_id = request.virtual_cluster_id();
  bool include_job_clusters = request.include_job_clusters();
  bool only_include_indivisible_cluster = request.only_include_indivisible_clusters();

  auto visit_proto_data = [&](const VirtualCluster *cluster) {
    if (include_job_clusters && cluster->Divisible()) {
      auto divisible_cluster = dynamic_cast<const DivisibleCluster *>(cluster);
      divisible_cluster->ForeachJobCluster(
          [&](const auto &job_cluster) { callback(job_cluster->ToProto()); });
    }
    if (only_include_indivisible_cluster && cluster->Divisible()) {
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

void PrimaryCluster::ReplenishAllClusterNodeInstances() {
  auto node_instance_replenish_callback = [this](auto node_instance) {
    return ReplenishUndividedNodeInstance(std::move(node_instance));
  };

  for (auto &[_, logical_cluster] : logical_clusters_) {
    if (logical_cluster->ReplenishNodeInstances(node_instance_replenish_callback)) {
      // Flush the logical cluster data.
      auto status = async_data_flusher_(logical_cluster->ToProto(), nullptr);
      if (!status.ok()) {
        RAY_LOG(ERROR) << "Failed to flush logical cluster " << logical_cluster->GetID()
                       << " data, status: " << status.ToString();
      }
    }
  }

  if (ReplenishNodeInstances(node_instance_replenish_callback)) {
    // Primary cluster proto data need not to flush.
  }
}

}  // namespace gcs
}  // namespace ray