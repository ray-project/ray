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
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

struct NodeInstance {
  NodeInstance() = default;

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
    stream << "NodeInstance(" << hostname_ << "," << template_id_ << ", " << is_dead_
           << ")";
    return stream.str();
  }

 private:
  std::string hostname_;
  std::string template_id_;
  bool is_dead_ = false;
};

static const std::string kEmptyJobClusterId = "NIL";
using CreateOrUpdateVirtualClusterCallback =
    std::function<void(const Status &, std::shared_ptr<rpc::VirtualClusterTableData>)>;

using RemoveVirtualClusterCallback = CreateOrUpdateVirtualClusterCallback;

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

using AsyncClusterDataFlusher = std::function<Status(
    std::shared_ptr<rpc::VirtualClusterTableData>, CreateOrUpdateVirtualClusterCallback)>;

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

class VirtualCluster {
 public:
  VirtualCluster(const std::string &id) : id_(id) {}
  virtual ~VirtualCluster() = default;

  /// Get the id of the cluster.
  virtual const std::string &GetID() const = 0;

  /// Get the allocation mode of the cluster.
  /// There are two modes of the cluster:
  ///  - Exclusive mode means that a signle node in the cluster can execute one or
  ///  more tasks belongs to only one job.
  ///  - Mixed mode means that a single node in the cluster can execute tasks
  ///  belongs to multiple jobs.
  /// \return The allocation mode of the cluster.
  virtual rpc::AllocationMode GetMode() const = 0;

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

  /// Lookup idle node instances from `visible_node_instances_` based on the demand
  /// final replica sets.
  ///
  /// \param replica_sets The demand final replica sets.
  /// \param replica_instances The node instances lookuped best effort from the visible
  /// node instances.
  /// \return OK if the lookup is successful, otherwise return an error.
  Status LookupIdleNodeInstances(const ReplicaSets &replica_sets,
                                 ReplicaInstances &replica_instances) const;

  /// Mark the node instance as dead.
  ///
  /// \param node_instance_id The id of the node instance to be marked as dead.
  /// \return True if the node instance is marked as dead, false otherwise.
  bool MarkNodeInstanceAsDead(const std::string &template_id,
                              const std::string &node_instance_id);

  /// Check if the virtual cluster is in use.
  ///
  /// \return True if the virtual cluster is in use, false otherwise.
  virtual bool InUse() const = 0;

  /// Convert the virtual cluster to proto data which usually is used for flushing
  /// to redis or publishing to raylet.
  /// \return A shared pointer to the proto data.
  std::shared_ptr<rpc::VirtualClusterTableData> ToProto() const;

  /// Get the debug string of the virtual cluster.
  virtual std::string DebugString() const;

 protected:
  /// Check if the node instance is idle. If a node instance is idle, it can be
  /// removed from the virtual cluster safely.
  /// \param node_instance The node instance to be checked.
  /// \return True if the node instance is idle, false otherwise.
  virtual bool IsIdleNodeInstance(const std::string &job_cluster_id,
                                  const gcs::NodeInstance &node_instance) const = 0;

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
};

class JobCluster;
class ExclusiveCluster : public VirtualCluster {
 public:
  ExclusiveCluster(const std::string &id,
                   const AsyncClusterDataFlusher &async_data_flusher)
      : VirtualCluster(id), async_data_flusher_(async_data_flusher) {}

  const std::string &GetID() const override { return id_; }
  rpc::AllocationMode GetMode() const override { return rpc::AllocationMode::EXCLUSIVE; }

  /// Create a job cluster.
  ///
  /// \param job_name The name of job to create the job cluster.
  /// \param replica_sets The replica sets of the job cluster.
  /// \return Status The status of the creation.
  Status CreateJobCluster(const std::string &job_name,
                          ReplicaSets replica_sets,
                          CreateOrUpdateVirtualClusterCallback callback);

  /// Remove a job cluster.
  ///
  /// \param job_name The name of job to remove the job cluster.
  /// \param callback The callback that will be called after the job cluster is removed.
  /// \return Status The status of the removal.
  Status RemoveJobCluster(const std::string &job_name,
                          RemoveVirtualClusterCallback callback);

  /// Get the job cluster by the job cluster id.
  ///
  /// \param job_name The name of job to get the job cluster.
  /// \return The job cluster if it exists, otherwise return nullptr.
  std::shared_ptr<JobCluster> GetJobCluster(const std::string &job_name) const;

  /// Check if the virtual cluster is in use.
  ///
  /// \return True if the virtual cluster is in use, false otherwise.
  bool InUse() const override;

 protected:
  bool IsIdleNodeInstance(const std::string &job_cluster_id,
                          const gcs::NodeInstance &node_instance) const override;

  /// The id of the virtual cluster.
  std::string id_;
  // The mapping from job cluster id to `JobCluster` instance.
  absl::flat_hash_map<std::string, std::shared_ptr<JobCluster>> job_clusters_;
  // The async data flusher.
  AsyncClusterDataFlusher async_data_flusher_;
};

class MixedCluster : public VirtualCluster {
 public:
  MixedCluster(const std::string &id) : VirtualCluster(id) {}
  MixedCluster &operator=(const MixedCluster &) = delete;

  const std::string &GetID() const override { return id_; }
  rpc::AllocationMode GetMode() const override { return rpc::AllocationMode::MIXED; }

  /// Check if the virtual cluster is in use.
  ///
  /// \return True if the virtual cluster is in use, false otherwise.
  bool InUse() const override;

 protected:
  bool IsIdleNodeInstance(const std::string &job_cluster_id,
                          const gcs::NodeInstance &node_instance) const override;
};

class JobCluster : public MixedCluster {
 public:
  using MixedCluster::MixedCluster;
};

class PrimaryCluster : public ExclusiveCluster {
 public:
  PrimaryCluster(const AsyncClusterDataFlusher &async_data_flusher)
      : ExclusiveCluster(kPrimaryClusterID, async_data_flusher) {}
  PrimaryCluster &operator=(const PrimaryCluster &) = delete;

  const std::string &GetID() const override { return kPrimaryClusterID; }
  rpc::AllocationMode GetMode() const override { return rpc::AllocationMode::EXCLUSIVE; }

  /// Create or update a new virtual cluster.
  ///
  /// \param request The request to create or update a virtual cluster.
  /// cluster. \param callback The callback that will be called after the virtual
  /// cluster is flushed. \return Status.
  Status CreateOrUpdateVirtualCluster(rpc::CreateOrUpdateVirtualClusterRequest request,
                                      CreateOrUpdateVirtualClusterCallback callback);

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

  /// Handle the node added event.
  ///
  /// \param node The node that is added.
  void OnNodeAdd(const rpc::GcsNodeInfo &node);

  /// Handle the node dead event.
  ///
  /// \param node The node that is dead.
  void OnNodeDead(const rpc::GcsNodeInfo &node);

 protected:
  bool IsIdleNodeInstance(const std::string &job_cluster_id,
                          const gcs::NodeInstance &node_instance) const override;

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