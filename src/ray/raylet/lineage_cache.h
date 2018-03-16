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

/// \class Object
///
/// A class to represent an object's metadata.
/// TODO(swang): Replace or extend this with the actual metadata.
class Object {
 public:
  Object(const ObjectID &object_id) : object_id_(object_id) {}
  const ObjectID &GetObjectId() { return object_id_; }

 private:
  const ObjectID object_id_;
};

/// The status of a lineage cache entry according to its status in the GCS.
enum GcsStatus {
  /// The task or object is not in the lineage cache.
  GcsStatus_NONE = 0,
  /// The task or object is being executed or created on a remote node.
  GcsStatus_UNCOMMITTED_REMOTE,
  /// The task or object is waiting to be executed or created locally.
  GcsStatus_UNCOMMITTED_WAITING,
  /// The task has started execution or the object has been created locally.
  /// This entry is ready to be written to the GCS.
  GcsStatus_UNCOMMITTED_READY,
  /// The entry has been written to the GCS and we are waiting for an
  /// acknowledgement of the commit.
  GcsStatus_COMMITTING,
  /// The entry has been committed in the GCS. It's safe to remove this entry
  /// from the lineage cache.
  GcsStatus_COMMITTED,
};

/// \class LineageEntry
///
/// An entry in the lineage. Each entry contains either an object or a task.
class LineageEntry {
 public:
  /// Create an entry for a task.
  ///
  /// \param task_id The task's ID.
  /// \param task The task data to eventually be written back to the GCS.
  /// \param status The status of this entry, according to its write status in
  ///               the GCS.
  LineageEntry(const TaskID &task_id, const Task &task, GcsStatus status);

  /// Create an entry for an object.
  ///
  /// \param object_id The object's ID.
  /// \param object The object data to eventually be written back to the GCS.
  /// \param status The status of this entry, according to its write status in
  ///               the GCS.
  LineageEntry(const ObjectID &entry_id, const Object &object, GcsStatus status);

  /// A copy constructor.
  ///
  /// \param entry A reference to the entry to copy.
  LineageEntry(const LineageEntry &entry);

  /// Get this entry's GCS status.
  ///
  /// \return The entry's status in the GCS.
  GcsStatus GetStatus() const;

  /// Set this entry's GCS status. The status is only set if the new status
  /// is strictly greater than the entry's previous status, according to the
  /// GcsStatus enum.
  ///
  /// \param new_status Set the entry's status to this value if it is greater
  ///        than the current status.
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
  const UniqueID &GetUniqueId() const;

  /// Get the IDs of this entry's parents. For an object entry, this is the ID
  /// of the task that created it. For a task entry, these are the IDs of its
  /// arguments.
  ///
  /// \return The IDs of the parent entries.
  const std::vector<UniqueID> GetParentIds() const;

  /// Get whether this entry is a task or an object.
  ///
  /// \return True if the entry is a task and false if the entry is an object.
  bool IsTask() const;

  const Task &TaskData() const;

  /// Serialize this task entry to a flatbuffer. The entry must contain task
  /// data.
  ///
  /// \return An offset to the serialized task.
  flatbuffers::Offset<TaskFlatbuffer> ToTaskFlatbuffer(
      flatbuffers::FlatBufferBuilder &fbb) const;

  /// Serialize this object entry to a flatbuffer. The entry must contain
  /// object data.
  ///
  /// \return An offset to the object task.
  flatbuffers::Offset<flatbuffers::String> ToObjectFlatbuffer(
      flatbuffers::FlatBufferBuilder &fbb) const;

 private:
  /// The entry ID. For tasks, this is the TaskID. For objects, this is the
  /// ObjectID.
  const UniqueID entry_id_;
  /// The current state of this entry according to its status in the GCS.
  GcsStatus status_;
  /// The task data to be written to the GCS. This is nullptr if the entry is
  /// an object.
  std::unique_ptr<Task> task_;
  /// The object data to be written to the GCS. This is nullptr if the entry is
  /// an task.
  std::unique_ptr<Object> object_;
};

/// \class Lineage
///
/// A lineage DAG, according to the data dependency graph. This consists of
/// task and object entries.  Each entry has an outgoing edge to each of its
/// parent entries. For a task, these are the arguments. For an object, this is
/// the task that created it. Each entry also records the current status in the
/// GCS for that task or object.
class Lineage {
 public:
  /// Construct an empty Lineage.
  Lineage();

  /// Construct a Lineage from a ForwardTaskRequest.
  ///
  /// \param task_request The request to construct the lineage from. All
  ///        uncommitted tasks and objects included in the request will be
  ///        added to the lineage.
  Lineage(const ForwardTaskRequest &task_request);

  /// Get an entry from the lineage.
  ///
  /// \param entry_id The ID of the entry to get.
  /// \return An optional reference to the entry. If this is empty, then the
  ///         entry ID is not in the lineage.
  boost::optional<const LineageEntry &> GetEntry(const UniqueID &entry_id) const;

  /// Set an entry in the lineage. If an entry with this ID already exists,
  /// then the entry is overwritten if and only if the new entry has a higher
  /// GCS status than the current. The current entry's object or task data will
  /// also be overwritten.
  ///
  /// \param entry The new entry to set in the lineage, if its GCS status is
  ///        greater than the current entry.
  /// \return Whether the entry was set.
  bool SetEntry(LineageEntry &&entry);

  /// Delete and return an entry from the lineage.
  ///
  /// \param entry_id The ID of the entry to pop.
  /// \return An optional reference to the popped entry. If this is empty, then
  ///         the entry ID is not in the lineage.
  boost::optional<LineageEntry> PopEntry(const UniqueID &entry_id);

  /// Get all entries in the lineage.
  ///
  /// \return A const reference to the lineage entries.
  const std::unordered_map<const UniqueID, LineageEntry, UniqueIDHasher> &GetEntries()
      const;

  /// Serialize this lineage to a ForwardTaskRequest flatbuffer.
  ///
  /// \param task_id The task ID to include in the ForwardTaskRequest
  ///        flatbuffer.
  /// \return An offset to the serialized lineage. The serialization includes
  ///         all task and object entries in the lineage.
  flatbuffers::Offset<ForwardTaskRequest> ToFlatbuffer(
      flatbuffers::FlatBufferBuilder &fbb, const TaskID &task_id) const;

 private:
  /// The lineage entries.
  std::unordered_map<const UniqueID, LineageEntry, UniqueIDHasher> entries_;
};

/// \class LineageCache
///
/// A cache of the object and task tables. This consists of all tasks and
/// objects that this node owns, as well as their lineage, that have not yet
/// been added durably to the GCS.
class LineageCache {
 public:
  /// Create a lineage cache for the given task and object storage system.
  /// TODO(swang): Pass in the policy (interface?) and a GCS client.
  LineageCache(const ClientID &client_id,
               gcs::Storage<TaskID, TaskFlatbuffer> &task_storage,
               gcs::Storage<ObjectID, ObjectTableData> &object_storage);

  /// Add a task that is waiting for execution and its uncommitted lineage.
  /// These entries will not be written to the GCS until set to ready.
  ///
  /// \param task The waiting task to add.
  /// \param uncommitted_lineage The task's uncommitted lineage. These are the
  ///        tasks that the given task is data-dependent on, but that have not
  ///        been made durable in the GCS, as far the task's submitter knows.
  void AddWaitingTask(const Task &task, const Lineage &uncommitted_lineage);

  /// Add a task that is ready for GCS writeback. This overwrites the task’s
  /// mutable fields in the execution specification.
  ///
  /// \param task The task to set as ready.
  void AddReadyTask(const Task &task);

  /// Add an object that appeared on this node. The object is marked as ready
  /// for GCS writeback.
  /// the GCS.
  ///
  /// // TODO(swang): First parameter should be object metadata, not just ID.
  /// \param object_id The object to add.
  /// \param remote Whether the object was created at a remote node.
  void AddReadyObject(const ObjectID &object_id, bool remote);

  void RemoveWaitingTask(const TaskID &task_id);

  /// Get the uncommitted lineage of a task. The uncommitted lineage consists
  /// of all tasks and objects in the given task's lineage that have not been
  /// committed in the GCS, as far as we know.
  ///
  /// \param task_id The ID of the task to get the uncommitted lineage for.
  /// \return The uncommitted lineage of the task. The returned lineage
  ///         includes the entry for the requested task_id.
  Lineage GetUncommittedLineage(const TaskID &task_id) const;

  /// Asynchronously write any tasks and object locations that have been added
  /// since the last flush to the GCS. When each write is acknowledged, its
  /// entry will be marked as committed.
  ///
  /// \return Status.
  Status Flush();

 private:
  void HandleEntryCommitted(const UniqueID &unique_id);

  ClientID client_id_;
  gcs::Storage<TaskID, TaskFlatbuffer> &task_storage_;
  gcs::Storage<ObjectID, ObjectTableData> &object_storage_;
  /// All tasks and objects that we are responsible for writing back to the
  /// GCS, and the tasks and objects in their lineage.
  Lineage lineage_;
};

} // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_LINEAGE_CACHE_H
