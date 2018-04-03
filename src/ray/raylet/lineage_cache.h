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
enum GcsStatus {
  /// The task is not in the lineage cache.
  GcsStatus_NONE = 0,
  /// The task is being executed or created on a remote node.
  GcsStatus_UNCOMMITTED_REMOTE,
  /// The task is waiting to be executed or created locally.
  GcsStatus_UNCOMMITTED_WAITING,
  /// The task has started execution, but the entry has not been written to the
  /// GCS yet.
  GcsStatus_UNCOMMITTED_READY,
  /// The task has been written to the GCS and we are waiting for an
  /// acknowledgement of the commit.
  GcsStatus_COMMITTING,
  /// The task has been committed in the GCS. It's safe to remove this entry
  /// from the lineage cache.
  GcsStatus_COMMITTED,
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

  /// Get this entry's ID.
  ///
  /// \return The entry's ID.
  const TaskID GetEntryId() const;

  /// Get the IDs of this entry's parent tasks. These are the IDs of the tasks
  /// that created its arguments.
  ///
  /// \return The IDs of the parent entries.
  const std::unordered_set<TaskID, UniqueIDHasher> GetParentTaskIds() const;

  /// Get the task data.
  ///
  /// \return The task data.
  const Task &TaskData() const;

  Task &TaskDataMutable();

 private:
  /// The current state of this entry according to its status in the GCS.
  GcsStatus status_;
  /// The task data to be written to the GCS. This is nullptr if the entry is
  /// an object.
  //  const Task task_;
  Task task_;
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
  boost::optional<LineageEntry &> GetEntryMutable(const UniqueID &task_id);

  /// Set an entry in the lineage. If an entry with this ID already exists,
  /// then the entry is overwritten if and only if the new entry has a higher
  /// GCS status than the current. The current entry's object or task data will
  /// also be overwritten.
  ///
  /// \param entry The new entry to set in the lineage, if its GCS status is
  /// greater than the current entry.
  /// \return Whether the entry was set.
  bool SetEntry(LineageEntry &&entry);

  /// Delete and return an entry from the lineage.
  ///
  /// \param entry_id The ID of the entry to pop.
  /// \return An optional reference to the popped entry. If this is empty, then
  /// the entry ID is not in the lineage.
  boost::optional<LineageEntry> PopEntry(const TaskID &entry_id);

  /// Get all entries in the lineage.
  ///
  /// \return A const reference to the lineage entries.
  const std::unordered_map<const TaskID, LineageEntry, UniqueIDHasher> &GetEntries()
      const;

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
  std::unordered_map<const TaskID, LineageEntry, UniqueIDHasher> entries_;
};

/// \class LineageCache
///
/// A cache of the task table. This consists of all tasks that this node owns,
/// as well as their lineage, that have not yet been added durably to the GCS.
class LineageCache {
 public:
  /// Create a lineage cache for the given task storage system.
  /// TODO(swang): Pass in the policy (interface?).
  LineageCache(gcs::TableInterface<TaskID, protocol::Task> &task_storage);

  /// Add a task that is waiting for execution and its uncommitted lineage.
  /// These entries will not be written to the GCS until set to ready.
  ///
  /// \param task The waiting task to add.
  /// \param uncommitted_lineage The task's uncommitted lineage. These are the
  /// tasks that the given task is data-dependent on, but that have not
  /// been made durable in the GCS, as far the task's submitter knows.
  void AddWaitingTask(const Task &task, const Lineage &uncommitted_lineage);

  /// Add a task that is ready for GCS writeback. This overwrites the task’s
  /// mutable fields in the execution specification.
  ///
  /// \param task The task to set as ready.
  void AddReadyTask(const Task &task);

  void RemoveWaitingTask(const TaskID &entry_id);

  /// Get the uncommitted lineage of a task. The uncommitted lineage consists
  /// of all tasks in the given task's lineage that have not been committed in
  /// the GCS, as far as we know.
  ///
  /// \param entry_id The ID of the task to get the uncommitted lineage for.
  /// \return The uncommitted lineage of the task. The returned lineage
  /// includes the entry for the requested entry_id.
  Lineage GetUncommittedLineage(const TaskID &entry_id) const;

  /// Asynchronously write any tasks that have been added since the last flush
  /// to the GCS. When each write is acknowledged, its entry will be marked as
  /// committed.
  ///
  /// \return Status.
  Status Flush();

 private:
  void HandleEntryCommitted(const TaskID &unique_id);

  /// The durable storage system for task information.
  gcs::TableInterface<TaskID, protocol::Task> &task_storage_;
  /// All tasks and objects that we are responsible for writing back to the
  /// GCS, and the tasks and objects in their lineage.
  Lineage lineage_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_LINEAGE_CACHE_H
