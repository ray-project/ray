#ifndef RAY_RAYLET_LINEAGE_CACHE_H
#define RAY_RAYLET_LINEAGE_CACHE_H

#include <boost/optional.hpp>

// clang-format off
#include "common_protocol.h"
#include "ray/raylet/task.h"
#include "ray/gcs/tables.h"
#include "ray/id.h"
#include "ray/status.h"
// clang-format on

namespace ray {

namespace raylet {

/// The status of a lineage cache entry according to its status in the GCS.
enum class GcsStatus {
  /// The task is not in the lineage cache.
  NONE = 0,
  /// The task is being executed or created on a remote node.
  UNCOMMITTED_REMOTE,
  /// The task is waiting to be executed or created locally.
  UNCOMMITTED_WAITING,
  /// The task has started execution, but the entry has not been written to the
  /// GCS yet.
  UNCOMMITTED_READY,
  /// The task has been written to the GCS and we are waiting for an
  /// acknowledgement of the commit.
  COMMITTING,
};

/// \class LineageEntry
///
/// A task entry in the data lineage. Each entry's parents are the tasks that
/// created the entry's arguments.
class LineageEntry {
 public:
  /// Create an entry for a task.
  ///
  /// \param task The task data to eventually be written back to the GCS.
  /// \param status The status of this entry, according to its write status in
  /// the GCS.
  LineageEntry(const Task &task, GcsStatus status);

  /// Get this entry's GCS status.
  ///
  /// \return The entry's status in the GCS.
  GcsStatus GetStatus() const;

  /// Set this entry's GCS status. The status is only set if the new status
  /// is strictly greater than the entry's previous status, according to the
  /// GcsStatus enum.
  ///
  /// \param new_status Set the entry's status to this value if it is greater
  /// than the current status.
  /// \return Whether the entry was set to the new status.
  bool SetStatus(GcsStatus new_status);

  /// Reset this entry's GCS status to a lower status. The new status must
  /// be lower than the current status.
  ///
  /// \param new_status This must be lower than the current status.
  void ResetStatus(GcsStatus new_status);

  /// Mark this entry as having been explicitly forwarded to a remote node manager.
  ///
  /// \param node_id The ID of the remote node manager.
  void MarkExplicitlyForwarded(const ClientID &node_id);

  /// Gets whether this entry was explicitly forwarded to a remote node.
  ///
  /// \param node_id The ID of the remote node manager.
  /// \return Whether this entry was explicitly forwarded to the remote node.
  bool WasExplicitlyForwarded(const ClientID &node_id) const;

  /// Get this entry's ID.
  ///
  /// \return The entry's ID.
  const TaskID GetEntryId() const;

  /// Get the IDs of this entry's parent tasks. These are the IDs of the tasks
  /// that created its arguments.
  ///
  /// \return The IDs of the parent entries.
  const std::unordered_set<TaskID> &GetParentTaskIds() const;

  /// Get the task data.
  ///
  /// \return The task data.
  const Task &TaskData() const;

  /// Get a mutable version of the task data.
  ///
  /// \return The task data.
  /// TODO(swang): This is pretty ugly.
  Task &TaskDataMutable();

  /// Update the task data with a new task.
  ///
  /// \return Void.
  void UpdateTaskData(const Task &task);

 private:
  /// Compute cached parent task IDs. This task is dependent on values returned
  /// by these tasks.
  void ComputeParentTaskIds();

  /// The current state of this entry according to its status in the GCS.
  GcsStatus status_;
  /// The task data to be written to the GCS. This is nullptr if the entry is
  /// an object.
  //  const Task task_;
  Task task_;
  /// A cached copy of the parent task IDs. This task is dependent on values
  /// returned by these tasks.
  std::unordered_set<TaskID> parent_task_ids_;
  /// IDs of node managers that this task has been explicitly forwarded to.
  std::unordered_set<ClientID> forwarded_to_;
};

/// \class Lineage
///
/// A lineage DAG, according to the data dependency graph. Each node is a task,
/// with an outgoing edge to each of its parent tasks. For a given task, the
/// parents are the tasks that created its arguments.  Each entry also records
/// the current status in the GCS for that task or object.
class Lineage {
 public:
  /// Construct an empty Lineage.
  Lineage();

  /// Construct a Lineage from a ForwardTaskRequest.
  ///
  /// \param task_request The request to construct the lineage from. All
  /// uncommitted tasks in the request will be added to the lineage.
  Lineage(const protocol::ForwardTaskRequest &task_request);

  /// Get an entry from the lineage.
  ///
  /// \param entry_id The ID of the entry to get.
  /// \return An optional reference to the entry. If this is empty, then the
  /// entry ID is not in the lineage.
  boost::optional<const LineageEntry &> GetEntry(const TaskID &entry_id) const;
  boost::optional<LineageEntry &> GetEntryMutable(const TaskID &task_id);

  /// Set an entry in the lineage. If an entry with this ID already exists,
  /// then the entry is overwritten if and only if the new entry has a higher
  /// GCS status than the current. The current entry's object or task data will
  /// also be overwritten.
  ///
  /// \param task The task data to set, if status is greater than the current entry.
  /// \param status The GCS status.
  /// \return Whether the entry was set.
  bool SetEntry(const Task &task, GcsStatus status);

  /// Delete and return an entry from the lineage.
  ///
  /// \param entry_id The ID of the entry to pop.
  /// \return An optional reference to the popped entry. If this is empty, then
  /// the entry ID is not in the lineage.
  boost::optional<LineageEntry> PopEntry(const TaskID &entry_id);

  /// Get all entries in the lineage.
  ///
  /// \return A const reference to the lineage entries.
  const std::unordered_map<const TaskID, LineageEntry> &GetEntries() const;

  /// Serialize this lineage to a ForwardTaskRequest flatbuffer.
  ///
  /// \param entry_id The task ID to include in the ForwardTaskRequest
  /// flatbuffer.
  /// \return An offset to the serialized lineage. The serialization includes
  /// all task and object entries in the lineage.
  flatbuffers::Offset<protocol::ForwardTaskRequest> ToFlatbuffer(
      flatbuffers::FlatBufferBuilder &fbb, const TaskID &entry_id) const;

 private:
  /// The lineage entries.
  std::unordered_map<const TaskID, LineageEntry> entries_;
};

/// \class LineageCache
///
/// A cache of the task table. This consists of all tasks that this node owns,
/// as well as their lineage, that have not yet been added durably to the GCS.
class LineageCache {
 public:
  /// Create a lineage cache for the given task storage system.
  /// TODO(swang): Pass in the policy (interface?).
  LineageCache(const ClientID &client_id,
               gcs::TableInterface<TaskID, protocol::Task> &task_storage,
               gcs::PubsubInterface<TaskID> &task_pubsub, uint64_t max_lineage_size);

  /// Add a task that is waiting for execution and its uncommitted lineage.
  /// These entries will not be written to the GCS until set to ready.
  ///
  /// \param task The waiting task to add.
  /// \param uncommitted_lineage The task's uncommitted lineage. These are the
  /// tasks that the given task is data-dependent on, but that have not
  /// been made durable in the GCS, as far the task's submitter knows.
  /// \return Whether the task was successfully marked as waiting to be
  /// committed. This will return false if the task is already waiting to be
  /// committed (UNCOMMITTED_WAITING), ready to be committed
  /// (UNCOMMITTED_READY), or committing (COMMITTING).
  bool AddWaitingTask(const Task &task, const Lineage &uncommitted_lineage);

  /// Add a task that is ready for GCS writeback. This overwrites the task’s
  /// mutable fields in the execution specification.
  ///
  /// \param task The task to set as ready.
  /// \return Whether the task was successfully marked as ready to be
  /// committed. This will return false if the task is already ready to be
  /// committed (UNCOMMITTED_READY) or committing (COMMITTING).
  bool AddReadyTask(const Task &task);

  /// Remove a task that was waiting for execution. Its uncommitted lineage
  /// will remain unchanged.
  ///
  /// \param task_id The ID of the waiting task to remove.
  /// \return Whether the task was successfully removed. This will return false
  /// if the task is not waiting to be committed. Then, the waiting task has
  /// already been removed (UNCOMMITTED_REMOTE), or if it's ready to be
  /// committed (UNCOMMITTED_READY) or committing (COMMITTING).
  bool RemoveWaitingTask(const TaskID &task_id);

  /// Mark a task as having been explicitly forwarded to a node.
  /// The lineage of the task is implicitly assumed to have also been forwarded.
  ///
  /// \param task_id The ID of the task to get the uncommitted lineage for.
  /// \param node_id The ID of the node to get the uncommitted lineage for.
  void MarkTaskAsForwarded(const TaskID &task_id, const ClientID &node_id);

  /// Get the uncommitted lineage of a task that hasn't been forwarded to a node yet.
  /// The uncommitted lineage consists of all tasks in the given task's lineage
  /// that have not been committed in the GCS, as far as we know.
  ///
  /// \param task_id The ID of the task to get the uncommitted lineage for.
  /// \param node_id The ID of the receiving node.
  /// \return The uncommitted, unforwarded lineage of the task. The returned lineage
  /// includes the entry for the requested entry_id.
  Lineage GetUncommittedLineage(const TaskID &task_id, const ClientID &node_id) const;

  /// Asynchronously write any tasks that are in the UNCOMMITTED_READY state
  /// and for which all parents have been committed to the GCS. These tasks
  /// will be transitioned in this method to state COMMITTING. Once the write
  /// is acknowledged, the task's state will be transitioned to state
  /// COMMITTED.
  void Flush();

  /// Handle the commit of a task entry in the GCS. This sets the task to
  /// COMMITTED and cleans up any ancestor tasks that are in the cache.
  ///
  /// \param task_id The ID of the task entry that was committed.
  void HandleEntryCommitted(const TaskID &task_id);

  /// Get a task. The task must be in the lineage cache.
  ///
  /// \param task_id The ID of the task to get.
  /// \return A const reference to the task data.
  const Task &GetTask(const TaskID &task_id) const;

  /// Get whether the lineage cache contains the task.
  ///
  /// \param task_id The ID of the task to get.
  /// \return Whether the task is in the lineage cache.
  bool ContainsTask(const TaskID &task_id) const;

 private:
  /// Try to flush a task that is in UNCOMMITTED_READY state. If the task has
  /// parents that are not committed yet, then the child will be flushed once
  /// the parents have been committed.
  bool FlushTask(const TaskID &task_id);
  /// Evict a single task. This should only be called if we are sure that the
  /// task has been committed and will trigger an attempt to flush any of the
  /// evicted task's children that are in UNCOMMITTED_READY state.  Returns an
  /// optional reference to the evicted task that is empty if the task was not
  /// in the lineage cache.
  boost::optional<LineageEntry> EvictTask(const TaskID &task_id);
  /// Evict a remote task and its lineage. This should only be called if we
  /// are sure that the remote task and its lineage are committed.
  void EvictRemoteLineage(const TaskID &task_id);
  /// Subscribe to notifications for a task. Returns whether the operation
  /// was successful (whether we were not already subscribed).
  bool SubscribeTask(const TaskID &task_id);
  /// Unsubscribe from notifications for a task. Returns whether the operation
  /// was successful (whether we were subscribed).
  bool UnsubscribeTask(const TaskID &task_id);
  /// Count the size of unsubscribed and uncommitted lineage of the given task
  /// excluding the values that have already been visited.
  ///
  /// \param task_id The task whose lineage should be counted.
  /// \param seen This set contains the keys of lineage entries counted so far,
  /// so that we don't revisit those nodes.
  /// \void The number of tasks that were counted.
  uint64_t CountUnsubscribedLineage(const TaskID &task_id,
                                    std::unordered_set<TaskID> &seen) const;

  /// The client ID, used to request notifications for specific tasks.
  /// TODO(swang): Move the ClientID into the generic Table implementation.
  ClientID client_id_;
  /// The durable storage system for task information.
  gcs::TableInterface<TaskID, protocol::Task> &task_storage_;
  /// The pubsub storage system for task information. This can be used to
  /// request notifications for the commit of a task entry.
  gcs::PubsubInterface<TaskID> &task_pubsub_;
  /// The maximum size that a remote task's uncommitted lineage can get to. If
  /// a remote task's uncommitted lineage exceeds this size, then a
  /// notification will be requested from the pubsub storage system so that
  /// the task and its lineage can be evicted from the stash.
  uint64_t max_lineage_size_;
  /// The set of tasks that are in UNCOMMITTED_READY state. This is a cache of
  /// the tasks that may be flushable.
  // TODO(swang): As an optimization, we may also want to further distinguish
  // which tasks are flushable, to avoid iterating over tasks that are in
  // UNCOMMITTED_READY, but that have dependencies that have not been committed
  // yet.
  std::unordered_set<TaskID> uncommitted_ready_tasks_;
  /// A mapping from each task that hasn't been committed yet, to all dependent
  /// children tasks that are in UNCOMMITTED_READY state. This is used when the
  /// parent task is committed, for fast lookup of children that may now be
  /// flushed.
  std::unordered_map<TaskID, std::unordered_set<TaskID>> uncommitted_ready_children_;
  /// All tasks and objects that we are responsible for writing back to the
  /// GCS, and the tasks and objects in their lineage.
  Lineage lineage_;
  /// The tasks that we've subscribed to notifications for from the pubsub
  /// storage system. We will receive a notification for these tasks on commit.
  std::unordered_set<TaskID> subscribed_tasks_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_LINEAGE_CACHE_H
