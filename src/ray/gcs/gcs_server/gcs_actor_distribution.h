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

#include <memory>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_actor_scheduler.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/util/event_label.h"
#include "ray/util/resource_util.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
using rpc::Language;
namespace gcs {

struct GcsJobConfig {
  explicit GcsJobConfig(const JobID &job_id, uint32_t num_java_workers_per_process,
                        uint32_t num_initial_java_worker_processes,
                        uint64_t java_worker_process_default_memory_units,
                        uint64_t total_memory_units)
      : job_id_(job_id) {
    if (num_java_workers_per_process != 0) {
      num_java_workers_per_process_ = num_java_workers_per_process;
    }

    num_initial_java_worker_processes_ = num_initial_java_worker_processes;

    if (java_worker_process_default_memory_units > 0) {
      java_worker_process_default_memory_units_ =
          java_worker_process_default_memory_units;
    }

    if (total_memory_units > 0) {
      total_memory_units_ = total_memory_units;
    }
  }

  GcsJobConfig(const JobID &job_id) : job_id_(job_id) {}

  std::string ToString() const {
    std::ostringstream ostr;
    ostr << "{ job_id: " << job_id_
         << ", num_java_workers_per_process: " << num_java_workers_per_process_
         << ", num_initial_java_worker_processes: " << num_initial_java_worker_processes_
         << ", java_worker_process_default_memory_gb: "
         << FromMemoryUnitsToGiB(java_worker_process_default_memory_units_)
         << ", python_worker_process_default_memory_gb: "
         << FromMemoryUnitsToGiB(python_worker_process_default_memory_units_)
         << ", total_memory_gb: " << FromMemoryUnitsToGiB(total_memory_units_) << " }";
    return ostr.str();
  }

  JobID job_id_;
  // The number of workers per worker process.
  uint32_t num_java_workers_per_process_ = 1;
  // The number of initial java worker processes of the job.
  uint32_t num_initial_java_worker_processes_ = 0;
  // The default memory of java worker process in memory units.
  uint64_t java_worker_process_default_memory_units_ = 5;
  // The default memory of python worker process in units.
  uint64_t python_worker_process_default_memory_units_ = 1;
  // The total memory units that the job can use.
  uint64_t total_memory_units_ = 80;
};

/// `GcsActorWorkerAssignment` represents the assignment from one or multiple actors to a
/// worker process. It contains multiple slots, and each of them can bind to an actor.
class GcsActorWorkerAssignment
    : public std::enable_shared_from_this<GcsActorWorkerAssignment> {
  enum ActorWorkerAssignmentStatus {
    IN_MEMORY,
    FLUSHING,
    FLUSHED,
  };

 public:
  /// Create a GcsActorWorkerAssignment
  ///
  /// \param actor_worker_assignment_id ID uniquely identify this assignment.
  /// \param node_id ID of node on which this actor worker assignment is allocated.
  /// \param job_id ID of job that this actor worker assignment belongs to.
  /// \param acquired_resources Resources owned by this actor worker assignment.
  /// \param is_shared A flag to represent that whether the worker process can be shared.
  /// \param slot_capacity The capacity of slots inside this worker
  /// process.
  explicit GcsActorWorkerAssignment(const UniqueID &actor_worker_assignment_id,
                                    const NodeID &node_id, const JobID &job_id,
                                    Language language,
                                    const ResourceSet &acquired_resources, bool is_shared,
                                    size_t slot_capacity = 1, bool is_flushed = false)
      : actor_worker_assignment_id_(actor_worker_assignment_id),
        node_id_(node_id),
        job_id_(job_id),
        language_(language),
        acquired_resources_(acquired_resources),
        is_shared_(is_shared),
        slot_capacity_(slot_capacity),
        actor_worker_assignment_status_(is_flushed
                                            ? ActorWorkerAssignmentStatus::FLUSHED
                                            : ActorWorkerAssignmentStatus::IN_MEMORY) {
    if (!is_shared) {
      RAY_CHECK(slot_capacity == 1);
    }
  }

  /// Create a GcsActorWorkerAssignment
  ///
  /// \param actor_worker_assignment_id ID uniquely identify this assignment.
  /// \param node_id ID of node on which this actor worker assignment is allocated.
  /// \param job_id ID of job that this actor worker assignment belongs to.
  /// \param acquired_resources Resources owned by this actor worker assignment.
  /// \param is_shared A flag to represent that whether the worker process can be shared.
  /// \param slot_capacity The capacity of slots inside this worker
  /// process.
  static std::shared_ptr<GcsActorWorkerAssignment> Create(
      const UniqueID &actor_worker_assignment_id, const NodeID &node_id,
      const JobID &job_id, Language language, const ResourceSet &acquired_resources,
      bool is_shared, size_t slot_capacity = 1, bool is_flushed = false) {
    return std::make_shared<GcsActorWorkerAssignment>(
        actor_worker_assignment_id, node_id, job_id, language, acquired_resources,
        is_shared, slot_capacity, is_flushed);
  }

  /// Create a GcsActorWorkerAssignment with a random actor worker assignment id.
  ///
  /// \param node_id ID of node on which this gcs actor worker assignment is allocated.
  /// \param job_id ID of job that this gcs actor worker assignment belongs to.
  /// \param acquired_resources Resources owned by this gcs actor worker assignment.
  /// \param is_shared A flag to represent that whether the worker process can be shared.
  /// \param slot_capacity The capacity of slots inside this worker
  /// process.
  static std::shared_ptr<GcsActorWorkerAssignment> Create(
      const NodeID &node_id, const JobID &job_id, Language language,
      const ResourceSet &acquired_resources, bool is_shared, size_t slot_capacity = 1,
      bool is_flushed = false) {
    return Create(UniqueID::FromRandom(), node_id, job_id, language, acquired_resources,
                  is_shared, slot_capacity, is_flushed);
  }

  const UniqueID &GetActorWorkerAssignmentID() const;

  const NodeID &GetNodeID() const;

  void SetNodeID(const NodeID &node_id);

  const JobID &GetJobID() const;

  const Language &GetLanguage() const;

  size_t GetAvailableSlotCount() const;

  size_t GetUsedSlotCount() const;

  bool IsShared() const;

  bool IsDummy() const;

  bool AssignActor(const ActorID &actor_id);

  bool RemoveActor(const ActorID &actor_id);

  void SetResources(const ResourceSet &acquired_resources);

  ResourceSet GetConstraintResources();

  const ResourceSet &GetResources() const;

  std::string ToString(int indent = 0) const;

 private:
  /// ID uniquely identify this actor worker assignment.
  UniqueID actor_worker_assignment_id_;
  /// Process id of the worker.
  int32_t pid_ = 0;
  /// ID of node on which this actor worker assignment is allocated.
  NodeID node_id_;
  /// ID of job that this actor worker assignment belongs to.
  JobID job_id_;
  /// Language of the worker process.
  Language language_;
  /// Resources owned by this actor worker assignment.
  ResourceSet acquired_resources_;
  /// A flag to represent that whether the worker process can be shared.
  bool is_shared_ = true;
  /// The capacity of slots inside the worker process.
  size_t slot_capacity_ = 1;
  // A flag to identify whether the assignment is flushed to the storage.
  ActorWorkerAssignmentStatus actor_worker_assignment_status_ =
      ActorWorkerAssignmentStatus::IN_MEMORY;
  std::vector<std::function<void(const ray::Status &)>> flush_callbacks_;

  /// IDs of actors that the actor worker assignment acceptted.
  absl::flat_hash_set<ActorID> actor_ids_;
};

using ActorWorkerAssignmentMap =
    absl::flat_hash_map<UniqueID, std::shared_ptr<GcsActorWorkerAssignment>>;
using NodeToActorWorkerAssignmentsMap =
    absl::flat_hash_map<NodeID, ActorWorkerAssignmentMap>;

/// `GcsJobSchedulingContext` represents scheduling status of a job.
/// It contains the job configuration, resources claimed when submiting, shared or sole
/// actor worker assignment of the job as well as the actor worker assignment distribution
/// on the cluster nodes.
class GcsJobSchedulingContext {
 public:
  /// Create a `GcsJobSchedulingContext`
  ///
  /// \param job_config Configuration of the job.
  explicit GcsJobSchedulingContext(const GcsJobConfig &job_config);

  /// Add a actor worker assignment.
  ///
  /// \param actor_worker_assignment to be added.
  /// \return true if the job available resources are enough, else false.
  bool AddActorWorkerAssignment(
      std::shared_ptr<GcsActorWorkerAssignment> actor_worker_assignment);

  /// Get available resources of the job.
  const ResourceSet &GetAvailableResources() const;

  /// Get configuration of the job.
  const GcsJobConfig &GetJobConfig() const;

  /// Get mutable configuration of the job.
  GcsJobConfig *GetMutableJobConfig();

  /// Get shared actor worker assignments.
  const ActorWorkerAssignmentMap &GetSharedActorWorkerAssignments() const;

  /// Get node to actor worker assignments distribution.
  const NodeToActorWorkerAssignmentsMap &GetNodeToActorWorkerAssignments() const;

  /// Update the node to actor worker assignments distribution.
  ///
  /// \param actor_worker_assignment actor worker assignment which node id is updated.
  bool UpdateNodeToActorWorkerAssignment(
      std::shared_ptr<GcsActorWorkerAssignment> actor_worker_assignment);

  /// Remove actor worker assignment by the specified node id and actor worker assignment
  /// id.
  ///
  /// The resources of removed actor worker assignment will be released to the job
  /// scheduling resources.
  ///
  /// \param node_id ID of the specified node.
  /// \param actor_worker_assignment_id ID of the gcs actor worker assignment to be
  /// removed. \return actor worker assignment associated with the specified node id and
  /// actor worker assignment id.
  std::shared_ptr<GcsActorWorkerAssignment>
  RemoveActorWorkerAssignmentByActorWorkerAssignmentID(
      const NodeID &node_id, const UniqueID &actor_worker_assignment_id);

  /// Add dummy shared actor worker assignment which does not actually allocate resources
  /// from the cluster, but only subtracts from the resources declared by the job.
  bool AddDummySharedActorWorkerAssignment();

  std::shared_ptr<GcsActorWorkerAssignment> GetActorWorkerAssignmentById(
      const UniqueID &actor_worker_assignment_id) const;

 private:
  /// Configuration of the job.
  GcsJobConfig job_config_;
  /// Shared actor worker assignments.
  ActorWorkerAssignmentMap shared_actor_worker_assignments_;
  /// Sole actor worker assignments.
  ActorWorkerAssignmentMap sole_actor_worker_assignments_;
  /// Node to worker distribution.
  NodeToActorWorkerAssignmentsMap node_to_actor_worker_assignments_;
  /// The job claimed resources when submitting.
  SchedulingResources scheduling_resources_;
  uint64_t last_report_time_ms_ = 0;
};

/// `GcsJobDistribution` represents job distribution on the cluster nodes.
/// It is responsible for the lifetime of all the `GcsJobSchedulingContext` and records
/// the distribution of jobs on each node.
class GcsJobDistribution {
 public:
  /// Create a `GcsJobDistribution`
  ///
  /// \param gcs_job_scheduling_factory Factory to create GcsJobSchedulingContext.
  explicit GcsJobDistribution(
      std::function<std::shared_ptr<GcsJobSchedulingContext>(const JobID &)>
          gcs_job_scheduling_factory);

  /// Add a actor worker assignment.
  ///
  /// \param actor_worker_assignment to be added.
  bool AddActorWorkerAssignment(
      std::shared_ptr<GcsActorWorkerAssignment> actor_worker_assignment);

  /// Update the distribution of job on each node.
  ///
  /// \param actor_worker_assignment actor worker assignment which node id is updated.
  bool UpdateNodeToJob(std::shared_ptr<GcsActorWorkerAssignment> actor_worker_assignment);

  /// Get job scheduling context by the specified job id.
  std::shared_ptr<GcsJobSchedulingContext> GetJobSchedulingContext(
      const JobID &job_id) const;

  /// Find or create a job scheduling context if not exist.
  ///
  /// \param job_id ID of the specified job.
  /// \return job scheduling context found or created.
  std::shared_ptr<GcsJobSchedulingContext> FindOrCreateJobSchedulingContext(
      const JobID &job_id);

  /// Remove actor worker assignments by the tuple(node_id, actor_worker_assignment_id,
  /// job_id) and update the distribution of jobs on the node if needed.
  ///
  /// \param node_id ID of the specified node.
  /// \param actor_worker_assignment_id ID of the gcs actor worker assignment to be
  /// removed. \param job_id ID of the job related with the actor worker assignment to be
  /// removed. \return actor worker assignment associated with the specified
  /// tuple(node_id, actor_worker_assignment_id,job_id).
  std::shared_ptr<GcsActorWorkerAssignment>
  RemoveActorWorkerAssignmentByActorWorkerAssignmentID(
      const NodeID &node_id, const UniqueID &actor_worker_assignment_id,
      const JobID &job_id);

  std::shared_ptr<GcsActorWorkerAssignment> GetActorWorkerAssignmentById(
      const JobID &job_id, const UniqueID &actor_worker_assignment_id) const;

 private:
  void RemoveJobFromNode(const JobID &job_id, const NodeID &node_id);

 private:
  /// The distribution of jobs on each node.
  absl::flat_hash_map<NodeID, absl::flat_hash_set<JobID>> node_to_jobs_;
  /// Map from job id to job scheduling context.
  absl::flat_hash_map<JobID, std::shared_ptr<GcsJobSchedulingContext>>
      job_scheduling_contexts_;
  /// Factory to create GcsJobSchedulingContext.
  std::function<std::shared_ptr<GcsJobSchedulingContext>(const JobID &)>
      gcs_job_scheduling_factory_;
};

/// GcsBasedActorScheduler implements a resource-based node selection.
class GcsBasedActorScheduler : public GcsActorScheduler {
 public:
  /// Create a GcsActorScheduler
  ///
  /// \param io_context The main event loop.
  /// \param gcs_actor_table Used to flush actor info to storage.
  /// \param gcs_node_manager The node manager which is used when scheduling.
  /// \param gcs_resource_manager The resource manager that maintains cluster resources.
  /// \param gcs_resource_scheduler The scheduler to select nodes based on cluster
  /// resources.
  /// \param gcs_job_distribution Recording scheduling information of jobs
  /// running on each node.
  /// \param schedule_failure_handler Invoked when there are no available nodes to
  /// schedule actors.
  /// \param schedule_success_handler Invoked when actors are created on the worker
  /// successfully.
  /// \param raylet_client_pool Raylet client pool to construct connections to raylets.
  /// \param client_factory Factory to create remote core worker client, default factor
  /// will be used if not set.
  explicit GcsBasedActorScheduler(
      instrumented_io_context &io_context, gcs::GcsActorTable &gcs_actor_table,
      const GcsNodeManager &gcs_node_manager, std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
      std::shared_ptr<GcsResourceManager> gcs_resource_manager,
      std::shared_ptr<GcsResourceScheduler> gcs_resource_scheduler,
      std::shared_ptr<GcsJobDistribution> gcs_job_distribution,
      std::function<void(std::shared_ptr<GcsActor>)> schedule_failure_handler,
      std::function<void(std::shared_ptr<GcsActor>)> schedule_success_handler,
      std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
      rpc::ClientFactoryFn client_factory = nullptr)
      : GcsActorScheduler(io_context, gcs_actor_table, gcs_node_manager, gcs_pub_sub,
                          schedule_failure_handler, schedule_success_handler,
                          raylet_client_pool, client_factory),
        gcs_resource_manager_(std::move(gcs_resource_manager)),
        gcs_resource_scheduler_(std::move(gcs_resource_scheduler)),
        gcs_job_distribution_(std::move(gcs_job_distribution)) {}

  virtual ~GcsBasedActorScheduler() = default;

 protected:
  /// Select a node for the actor based on cluster resources.
  ///
  /// \param actor The actor to be scheduled.
  /// \return The selected node's ID. If the selection fails, NodeID::Nil() is returned.
  NodeID SelectNode(std::shared_ptr<GcsActor> actor) override;

  /// Handler to process a worker lease reply.
  /// If a rejection is received, it means resources were preempted by normal
  /// tasks. Then update the the cluster resource view and reschedule immediately.
  ///
  /// \param actor The actor to be scheduled.
  /// \param node The selected node at which a worker is to be leased.
  /// \param status Status of the reply of `RequestWorkerLeaseRequest`.
  /// \param reply The reply of `RequestWorkerLeaseRequest`.
  void HandleWorkerLeaseReply(std::shared_ptr<GcsActor> actor,
                              std::shared_ptr<rpc::GcsNodeInfo> node,
                              const Status &status,
                              const rpc::RequestWorkerLeaseReply &reply) override;

 private:
  /// Select an existing or allocate a new actor worker assignment for the actor.
  std::shared_ptr<GcsActorWorkerAssignment> SelectOrAllocateActorWorkerAssignment(
      std::shared_ptr<GcsActor> actor, bool need_sole_actor_worker_assignment);

  /// Allocate a new actor worker assignment.
  ///
  /// \param job_scheduling_context Scheduling context of the job.
  /// \param required_resources The resources that the worker required.
  /// \param is_shared If the worker is shared by multiple actors or not.
  /// \param task_spec The specification of the task.
  std::shared_ptr<GcsActorWorkerAssignment> AllocateNewActorWorkerAssignment(
      std::shared_ptr<ray::gcs::GcsJobSchedulingContext> job_scheduling_context,
      const ResourceSet &required_resources, bool is_shared,
      const TaskSpecification &task_spec);

  /// Allocate resources for the actor.
  ///
  /// \param required_resources The resources to be allocated.
  /// \return ID of the node from which the resources are allocated.
  NodeID AllocateResources(const ResourceSet &required_resources);

  NodeID GetHighestScoreNodeResource(const ResourceSet &required_resources) const;

  void WarnResourceAllocationFailure(
      std::shared_ptr<GcsJobSchedulingContext> job_scheduling_context,
      const TaskSpecification &task_spec, const ResourceSet &required_resources) const;

  /// A rejected rely means resources were preempted by normal tasks. Then
  /// update the the cluster resource view and reschedule immediately.
  void HandleWorkerLeaseRejectedReply(std::shared_ptr<GcsActor> actor,
                                      const rpc::RequestWorkerLeaseReply &reply);

  void CancelOnActorWorkerAssignment(const ActorID &actor_id,
                                     const UniqueID &actor_worker_assignment_id);

  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;

  /// Gcs resource scheduler
  std::shared_ptr<GcsResourceScheduler> gcs_resource_scheduler_;

  /// Instance of the `GcsJobDistribution`, which records scheduling information of jobs
  /// running on each node.
  std::shared_ptr<GcsJobDistribution> gcs_job_distribution_;
};
}  // namespace gcs
}  // namespace ray
