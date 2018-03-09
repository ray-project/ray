#ifndef RAY_RAYLET_LINEAGE_CACHE_H
#define RAY_RAYLET_LINEAGE_CACHE_H

// clang-format off
#include "ray/raylet/task.h"
#include "ray/id.h"
#include "ray/status.h"
// clang-format on

namespace ray {

// TODO(swang): Define this class.
class Lineage {};

class LineageCacheEntry {
 private:
  // TODO(swang): This should be an enum of the state of the entry - goes from
  // completely local, to dirty, to in flight, to committed.
  // bool dirty_;
};

class LineageCacheTaskEntry : public LineageCacheEntry {};
class LineageCacheObjectEntry : public LineageCacheEntry {};

/// \class LineageCache
///
/// A cache of the object and task tables. This consists of all tasks that this
/// node owns, as well as their task lineage, that have not yet been added
/// durably to the GCS.
class LineageCache {
 public:
  /// Create a lineage cache policy.
  /// TODO(swang): Pass in the policy (interface?) and a GCS client.
  LineageCache();

  /// Add a task and its object outputs asynchronously to the GCS. This
  /// overwrites the task’s mutable fields in the execution specification.
  ///
  /// \param task The task to add.
  /// \return Status.
  ray::Status AddTask(const Task &task);

  /// Add a task and its uncommitted lineage asynchronously to the GCS. The
  /// mutable fields for the given task will be overwritten, but not for the
  /// tasks in the uncommitted lineage.
  ///
  /// \param task The task to add.
  /// \param uncommitted_lineage The task's uncommitted lineage. These are the
  ///        tasks that the given task is data-dependent on, but that have not
  ///        been made durable in the GCS, as far as we know.
  /// \return Status.
  ray::Status AddTask(const Task &task, const Lineage &uncommitted_lineage);

  /// Add this node as an object location, to be asynchronously committed to
  /// the GCS.
  ///
  /// \param object_id The object to add a location for.
  /// \return Status.
  ray::Status AddObjectLocation(const ObjectID &object_id);

  /// Get the uncommitted lineage of an object. These are the tasks that the
  /// given object is data-dependent on, but that have not been made durable in
  /// the GCS, as far as we know.
  ///
  /// \param object_id The object to get the uncommitted lineage for.
  /// \return The uncommitted lineage of the object.
  Lineage &GetUncommittedLineage(const ObjectID &object_id);

  /// Asynchronously write any tasks and object locations that have been added
  /// since the last flush to the GCS. When each write is acknowledged, its
  /// entry will be marked as committed.
  ///
  /// \return Status.
  Status Flush();

 private:
  std::unordered_map<TaskID, LineageCacheTaskEntry, UniqueIDHasher> task_table_;
  std::unordered_map<ObjectID, LineageCacheObjectEntry, UniqueIDHasher> object_table_;
};

}  // namespace ray

#endif  // RAY_RAYLET_LINEAGE_CACHE_H
