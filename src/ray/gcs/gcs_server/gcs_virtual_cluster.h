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

#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/status.h"
#include "ray/common/virtual_cluster_id.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"
#include "src/ray/raylet/scheduling/cluster_resource_manager.h"

namespace ray {
namespace gcs {

struct NodeInstance {
  NodeInstance(const std::string &node_instance_id) {
    node_instance_id_ = node_instance_id;
  }

  const std::string &node_instance_id() const { return node_instance_id_; }

  const std::string &hostname() const { return hostname_; }
  void set_hostname(const std::string &hostname) { hostname_ = hostname; }

  const std::string &template_id() const { return template_id_; }
  void set_template_id(const std::string &template_id) { template_id_ = template_id; }

  bool is_dead() const { return is_dead_; }
  void set_is_dead(bool is_dead) { is_dead_ = is_dead; }

  std::shared_ptr<rpc::NodeInstance> ToProto() const {
    auto node_instance = std::make_shared<rpc::NodeInstance>();
    node_instance->set_template_id(template_id_);
    node_instance->set_hostname(hostname_);
    return node_instance;
  }

  std::string DebugString() const {
    std::ostringstream stream;
    stream << "NodeInstance(" << node_instance_id_ << "," << hostname_ << ","
           << template_id_ << ", " << is_dead_ << ")";
    return stream.str();
  }

 private:
  std::string node_instance_id_;
  std::string hostname_;
  std::string template_id_;
  bool is_dead_ = false;
};

static const std::string kEmptyJobClusterId = "NIL";

/// <template_id, _>
///               |
///               |--> <job_cluster_id, _>
///                                     |
///                                     |--> <node_instance_id,
///                                     std::shared_ptr<gcs::NodeInstance>>
using ReplicaInstances = absl::flat_hash_map<
    std::string,
    absl::flat_hash_map<
        std::string,
        absl::flat_hash_map<std::string, std::shared_ptr<gcs::NodeInstance>>>>;

using ReplicaSets = absl::flat_hash_map<std::string, int32_t>;

using CreateOrUpdateVirtualClusterCallback = std::function<void(
    const Status &, std::shared_ptr<rpc::VirtualClusterTableData>, const ReplicaSets *)>;

using RemoveVirtualClusterCallback = CreateOrUpdateVirtualClusterCallback;

using VirtualClustersDataVisitCallback =
    std::function<void(std::shared_ptr<rpc::VirtualClusterTableData>)>;

using AsyncClusterDataFlusher = std::function<Status(
    std::shared_ptr<rpc::VirtualClusterTableData>, CreateOrUpdateVirtualClusterCallback)>;

using NodeInstanceFilter = std::function<bool(const gcs::NodeInstance &node_instance)>;

/// Calculate the difference between two replica sets.
template <typename T1, typename T2>
ReplicaSets ReplicasDifference(const T1 &left, const T2 &right) {
  ReplicaSets result;
  for (const auto &[template_id, replicas] : left) {
    auto right_iter = right.find(template_id);
    if (right_iter == right.end()) {
      result[template_id] = replicas;
    } else {
      if (right_iter->second < replicas) {
        result[template_id] = replicas - right_iter->second;
      }
    }
  }
  return result;
}

template <typename T>
ReplicaInstances toReplicaInstances(const T &node_instances) {
  ReplicaInstances result;
  for (const auto &[id, node_instance] : node_instances) {
    auto inst = std::make_shared<NodeInstance>(id);
    inst->set_hostname(node_instance.hostname());
    inst->set_template_id(node_instance.template_id());
    result[node_instance.template_id()][kEmptyJobClusterId].emplace(id, std::move(inst));
  }
  return result;
}

ReplicaSets buildReplicaSets(const ReplicaInstances &replica_instances);

std::string DebugString(const ReplicaInstances &replica_instances, int indent = 0);
std::string DebugString(const ReplicaSets &replica_sets, int indent = 0);

using NodeInstanceReplenishCallback = std::function<std::shared_ptr<NodeInstance>(
    std::shared_ptr<NodeInstance> node_instance_to_replenish)>;

class VirtualCluster {
 public:
  VirtualCluster(const std::string &id,
                 const ClusterResourceManager &cluster_resource_manager)
      : id_(id), cluster_resource_manager_(cluster_resource_manager) {}
  virtual ~VirtualCluster() = default;

  /// Get the id of the cluster.
  virtual const std::string &GetID() const = 0;

  /// Whether the virtual cluster can splite into one or more child virtual clusters or
  /// not.
  virtual bool Divisible() const = 0;

  /// Get the revision number of the cluster.
  uint64_t GetRevision() const { return revision_; }

  /// Get the replica sets corresponding to the cluster.
  const ReplicaSets &GetReplicaSets() const { return replica_sets_; }

  /// Get the visible node instances of the cluster.
  const ReplicaInstances &GetVisibleNodeInstances() const {
    return visible_node_instances_;
  }

  /// Update the node instances of the cluster.
  ///
  /// \param replica_instances_to_add The node instances to be added.
  /// \param replica_instances_to_remove The node instances to be removed.
  void UpdateNodeInstances(ReplicaInstances replica_instances_to_add,
                           ReplicaInstances replica_instances_to_remove);

  /// Lookup node instances from queue of kEmptyJobClusterId in `visible_node_instances_`
  /// based on the desired final replica sets and node_instance_filter.
  ///
  /// \param replica_sets The demand final replica sets.
  /// \param replica_instances The node instances lookuped best effort from the visible
  /// node instances.
  /// \param node_instance_filter The filter to check if the node instance is desired.
  /// \return True if the lookup is successful, otherwise return an error.
  bool LookupNodeInstances(const ReplicaSets &replica_sets,
                           ReplicaInstances &replica_instances,
                           NodeInstanceFilter node_instance_filter) const;

  /// Mark the node instance as dead.
  ///
  /// \param node_instance_id The id of the node instance to be marked as dead.
  /// \return True if the node instance is marked as dead, false otherwise.
  bool MarkNodeInstanceAsDead(const std::string &template_id,
                              const std::string &node_instance_id);

  /// Check the virtual cluster contains node instance
  ///
  /// \param node_instance_id The id of the node instance to check
  /// \return True if the node instance is in this virtual cluster, false otherwise.
  bool ContainsNodeInstance(const std::string &node_instance_id);

  /// Iterate all node instances of the virtual cluster.
  ///
  /// \param fn The callback to iterate all node instances.
  void ForeachNodeInstance(
      const std::function<void(const std::shared_ptr<NodeInstance> &)> &fn) const;

  /// Check if the virtual cluster is in use.
  ///
  /// \param in_use_instances The node instances that are still in use.
  /// \return True if the virtual cluster is in use, false otherwise.
  virtual bool InUse() const = 0;

  /// Convert the virtual cluster to proto data which usually is used for flushing
  /// to redis or publishing to raylet.
  /// \return A shared pointer to the proto data.
  std::shared_ptr<rpc::VirtualClusterTableData> ToProto() const;

  /// Get the debug string of the virtual cluster.
  virtual std::string DebugString() const;

  /// Check if the node instance is idle. If a node instance is idle, it can be
  /// removed from the virtual cluster safely.
  ///
  /// \param node_instance The node instance to be checked.
  /// \return True if the node instance is idle, false otherwise.
  virtual bool IsIdleNodeInstance(const gcs::NodeInstance &node_instance) const = 0;

  /// Replenish the node instances of the virtual cluster.
  ///
  /// \param callback The callback to replenish the dead node instances.
  /// \return True if any dead node instances are replenished, false otherwise.
  virtual bool ReplenishNodeInstances(const NodeInstanceReplenishCallback &callback);

  /// Replenish a node instance.
  ///
  /// \param node_instance_to_replenish The node instance to replenish.
  /// \return True if the node instances is replenished, false otherwise.
  std::shared_ptr<NodeInstance> ReplenishNodeInstance(
      std::shared_ptr<NodeInstance> node_instance_to_replenish);

 protected:
  /// Insert the node instances to the cluster.
  ///
  /// \param replica_instances The node instances to be inserted.
  void InsertNodeInstances(ReplicaInstances replica_instances);

  /// Remove the node instances from the cluster.
  ///
  /// \param replica_instances The node instances to be removed.
  void RemoveNodeInstances(ReplicaInstances replica_instances);

  /// The id of the virtual cluster.
  std::string id_;
  /// Node instances that are visible to the cluster.
  ReplicaInstances visible_node_instances_;
  /// Replica sets to express the visible node instances.
  ReplicaSets replica_sets_;
  // Version number of the last modification to the cluster.
  uint64_t revision_{0};

  const ClusterResourceManager &cluster_resource_manager_;
};

class JobCluster;
class DivisibleCluster : public VirtualCluster {
 public:
  DivisibleCluster(const std::string &id,
                   const AsyncClusterDataFlusher &async_data_flusher,
                   const ClusterResourceManager &cluster_resource_manager)
      : VirtualCluster(id, cluster_resource_manager),
        async_data_flusher_(async_data_flusher) {}

  const std::string &GetID() const override { return id_; }
  bool Divisible() const override { return true; }

  /// Load a job cluster to the divisible cluster.
  ///
  /// \param job_cluster_id The id of the job cluster.
  /// \param replica_instances The replica instances of the job cluster.
  void LoadJobCluster(const std::string &job_cluster_id,
                      ReplicaInstances replica_instances);

  /// Build the job cluster id.
  ///
  /// \param job_name The name of the job.
  /// \return The job cluster id.
  std::string BuildJobClusterID(const std::string &job_name) {
    return VirtualClusterID::FromBinary(GetID()).BuildJobClusterID(job_name).Binary();
  }

  /// Create a job cluster.
  ///
  /// \param job_cluster_id The id of the job cluster.
  /// \param replica_sets The replica sets of the job cluster.
  /// \param[out] replica_sets_to_recommend The replica sets that we can fulfill the
  /// request at most. It can be used as a suggestion to adjust the request if it fails.
  /// \return Status The status of the creation.
  Status CreateJobCluster(const std::string &job_cluster_id,
                          ReplicaSets replica_sets,
                          CreateOrUpdateVirtualClusterCallback callback,
                          ReplicaSets *replica_sets_to_recommend = nullptr);

  /// Remove a job cluster.
  ///
  /// \param job_cluster_id The id of the job cluster to be removed.
  /// \param callback The callback that will be called after the job cluster is removed.
  /// \return Status The status of the removal.
  Status RemoveJobCluster(const std::string &job_cluster_id,
                          RemoveVirtualClusterCallback callback);

  /// Get the job cluster by the job cluster id.
  ///
  /// \param job_cluster_id The id of the job cluster.
  /// \return The job cluster if it exists, otherwise return nullptr.
  std::shared_ptr<JobCluster> GetJobCluster(const std::string &job_cluster_id) const;

  /// Iterate all job clusters.
  void ForeachJobCluster(
      const std::function<void(const std::shared_ptr<JobCluster> &)> &fn) const;

  /// Check if the virtual cluster is in use.
  ///
  /// \return True if the virtual cluster is in use, false otherwise.
  bool InUse() const override;

  /// Check if the node instance is idle. If a node instance is idle, it can be
  /// removed from the virtual cluster safely.
  ///
  /// \param node_instance The node instance to be checked.
  /// \return True if the node instance is idle, false otherwise.
  bool IsIdleNodeInstance(const gcs::NodeInstance &node_instance) const override;

  /// Replenish the node instances of the virtual cluster.
  ///
  /// \param callback The callback to replenish the dead node instances.
  /// \return True if any dead node instances are replenished, false otherwise.
  bool ReplenishNodeInstances(const NodeInstanceReplenishCallback &callback) override;

 protected:
  /// Do create a job cluster from the divisible cluster.
  ///
  /// \param job_cluster_id The id of the job cluster.
  /// \param replica_instances_to_add The node instances to be added.
  /// \return The created job cluster.
  std::shared_ptr<JobCluster> DoCreateJobCluster(
      const std::string &job_cluster_id, ReplicaInstances replica_instances_to_add);

  // The mapping from job cluster id to `JobCluster` instance.
  absl::flat_hash_map<std::string, std::shared_ptr<JobCluster>> job_clusters_;
  // The async data flusher.
  AsyncClusterDataFlusher async_data_flusher_;
};

class IndivisibleCluster : public VirtualCluster {
 public:
  IndivisibleCluster(const std::string &id,
                     const ClusterResourceManager &cluster_resource_manager)
      : VirtualCluster(id, cluster_resource_manager) {}
  IndivisibleCluster &operator=(const IndivisibleCluster &) = delete;

  const std::string &GetID() const override { return id_; }
  bool Divisible() const override { return false; }

  /// Check if the virtual cluster is in use.
  ///
  /// \return True if the virtual cluster is in use, false otherwise.
  bool InUse() const override;

  /// Check if the node instance is idle. If a node instance is idle, it can be
  /// removed from the virtual cluster safely.
  ///
  /// \param node_instance The node instance to be checked.
  /// \return True if the node instance is idle, false otherwise.
  bool IsIdleNodeInstance(const gcs::NodeInstance &node_instance) const override;
};

class JobCluster : public IndivisibleCluster {
 public:
  using IndivisibleCluster::IndivisibleCluster;
};

class PrimaryCluster : public DivisibleCluster,
                       public std::enable_shared_from_this<PrimaryCluster> {
 public:
  PrimaryCluster(const AsyncClusterDataFlusher &async_data_flusher,
                 const ClusterResourceManager &cluster_resource_manager)
      : DivisibleCluster(
            kPrimaryClusterID, async_data_flusher, cluster_resource_manager) {}
  PrimaryCluster &operator=(const PrimaryCluster &) = delete;

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  /// Load a logical cluster to the primary cluster.
  ///
  /// \param virtual_cluster_id The id of the logical cluster.
  /// \param divisible Whether the logical cluster is divisible or not.
  /// \param replica_instances The replica instances of the logical cluster.
  /// \return The loaded logical cluster.
  std::shared_ptr<VirtualCluster> LoadLogicalCluster(
      const std::string &virtual_cluster_id,
      bool divisible,
      ReplicaInstances replica_instances);

  const std::string &GetID() const override { return kPrimaryClusterID; }
  bool Divisible() const override { return true; }

  /// Create or update a new virtual cluster.
  ///
  /// \param request The request to create or update a virtual cluster.
  /// \param callback The callback that will be called after the virtual cluster
  /// is flushed.
  /// \param[out] replica_sets_to_recommend The replica sets that we can fulfill the
  /// request at most. It can be used as a suggestion to adjust the request if it fails.
  /// \return Status.
  Status CreateOrUpdateVirtualCluster(rpc::CreateOrUpdateVirtualClusterRequest request,
                                      CreateOrUpdateVirtualClusterCallback callback,
                                      ReplicaSets *replica_sets_to_recommend = nullptr);

  /// Get the virtual cluster by the logical cluster id.
  ///
  /// \param logical_cluster_id The id of the virtual cluster.
  /// \return The logical cluster if it exists, otherwise return nullptr.
  std::shared_ptr<VirtualCluster> GetLogicalCluster(
      const std::string &logical_cluster_id) const;

  /// Remove logical cluster by the logical cluster id.
  ///
  /// \param logical_cluster_id The id of the logical cluster to be removed.
  /// \param callback The callback that will be called after the logical cluster is
  /// removed.
  /// \return Status The status of the removal.
  Status RemoveLogicalCluster(const std::string &logical_cluster_id,
                              RemoveVirtualClusterCallback callback);

  /// Remove virtual cluster by the virtual cluster id.
  ///
  /// \param virtual_cluster_id The id of the virtual cluster to be removed.
  /// \param callback The callback that will be called after the virtual cluster is
  /// removed.
  /// \return Status The status of the removal.
  Status RemoveVirtualCluster(const std::string &virtual_cluster_id,
                              RemoveVirtualClusterCallback callback);

  /// Get virtual cluster by virtual cluster id
  ///
  /// \param virtual_cluster_id The id of virtual cluster
  /// \return the virtual cluster
  std::shared_ptr<VirtualCluster> GetVirtualCluster(
      const std::string &virtual_cluster_id);

  /// Iterate all virtual clusters.
  ///
  /// \param fn The function to be called for each logical cluster.
  void ForeachVirtualCluster(
      const std::function<void(const std::shared_ptr<VirtualCluster> &)> &fn) const;

  /// Iterate virtual clusters data matching the request.
  ///
  /// \param request The request to get the virtual clusters data.
  /// \param callback The callback to visit each virtual cluster data.
  void ForeachVirtualClustersData(rpc::GetVirtualClustersRequest request,
                                  VirtualClustersDataVisitCallback callback);

  /// Handle the node added event.
  ///
  /// \param node The node that is added.
  void OnNodeAdd(const rpc::GcsNodeInfo &node);

  /// Handle the node dead event.
  ///
  /// \param node The node that is dead.
  void OnNodeDead(const rpc::GcsNodeInfo &node);

  /// Replenish dead node instances of all the virtual clusters.
  void ReplenishAllClusterNodeInstances();

 protected:
  /// Handle the node dead event.
  ///
  /// \param node_instance_id The id of the node instance that is dead.
  /// \param node_type_name The type name of the node instance that is dead.
  void OnNodeInstanceDead(const std::string &node_instance_id,
                          const std::string &node_type_name);

 private:
  /// Calculate the node instances that to be added and to be removed
  /// based on the demand final replica sets inside the request.
  ///
  /// \param request The request to create or update virtual cluster.
  /// \param node_instances_to_add The node instances that to be added.
  /// \param node_instances_to_remove The node instances that to be removed.
  /// \return status The status of the calculation.
  Status DetermineNodeInstanceAdditionsAndRemovals(
      const rpc::CreateOrUpdateVirtualClusterRequest &request,
      ReplicaInstances &node_instances_to_add,
      ReplicaInstances &node_instances_to_remove);

  /// The map of virtual clusters.
  /// Mapping from virtual cluster id to the virtual cluster.
  absl::flat_hash_map<std::string, std::shared_ptr<VirtualCluster>> logical_clusters_;
};

}  // namespace gcs
}  // namespace ray